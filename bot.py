"""
RivX bot.py — main loop orchestrator (v2 strategy).

Architecture (every box in this list is one of our files):

  scanner ──▶ candidates ──▶ brain ──▶ decisions ──▶ safety ──▶ execution
                                          │
   prices ──▶ (used by scanner, brain context, mark-to-market)
                                          │
   strategy ──▶ defines all rules used above
                                          │
   safety ──▶ circuit breakers BEFORE every buy/sell

The orchestrator's job is timing and wiring:
  - When to scan (once a day for swing, twice a day for momentum)
  - When to mark-to-market (every 5 min)
  - When to check kill switch / manual orders / telegram (every 30 sec)
  - When to write heartbeat (every loop)
  - How to handle errors (log loud, sleep, retry — never crash the loop)

Yesterday's lessons baked in:

  - Setup is wrapped in try/except with Telegram alert on crash. Render
    won't show silent restarts again.

  - PYTHONUNBUFFERED is enforced via sys.stdout.reconfigure() so logs flush.

  - Heartbeat is written every iteration. External monitors (or the bot
    itself on next start) can detect silent death.

  - Snapshots and manual orders run regardless of kill switch. Trading
    loops gate on the switch so the user can /pause + /sell.

  - Manual orders use the safety layer: even a force-sell goes through
    check_can_sell with is_forced=True for accountability.
"""

from __future__ import annotations

import os
import sys
import time
import logging
import traceback
from datetime import datetime, timezone, timedelta

# Force stdout/stderr unbuffered so Render captures crashes
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("rivx")

# Lazy imports of heavy deps so import-time errors are easier to debug
from bot.config import (
    PAPER_MODE, ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_DATA_URL,
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, ANTHROPIC_API_KEY,
)
from bot import prices
from bot import strategy
from bot import safety
from bot import scanner
from bot import brain
from bot.supabase_logger import SupabaseLogger
from bot.telegram_notify import TelegramNotifier
from bot.alpaca_trader import AlpacaTrader
from bot.coinspot_trader import CoinSpotTrader


# ── Loop cadence ──────────────────────────────────────────────────────────

MAIN_TICK_SECONDS       = 30      # outer loop: kill switch, manual orders, heartbeat
SNAPSHOT_INTERVAL_SEC   = 300     # 5 min — mark portfolio to market
SWING_CRYPTO_TIMES_AEST = ["08:00"]              # once a day at 8 AM AEST
MOMENTUM_TIMES_AEST     = ["08:00", "16:00"]     # twice a day
SWING_STOCK_TIMES_AEST  = ["08:00"]
HEARTBEAT_FLAG          = "last_heartbeat"


# ── Time helpers ──────────────────────────────────────────────────────────

AEST = timezone(timedelta(hours=10))

def aest_now() -> datetime:
    return datetime.now(AEST)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def at_or_past_time_today(target_hhmm: str, last_run_iso: str | None) -> bool:
    """
    Returns True if AEST-now is past target time today AND we haven't run
    since the last target time. Used for "once a day at 8 AM" type schedules.
    """
    now = aest_now()
    target_h, target_m = map(int, target_hhmm.split(":"))
    target_today = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
    if now < target_today:
        return False  # too early
    if not last_run_iso:
        return True
    try:
        last = datetime.fromisoformat(last_run_iso.replace("Z", "+00:00"))
        # Convert to AEST for comparison
        last_aest = last.astimezone(AEST)
        return last_aest < target_today
    except Exception:
        return True


# ── Anthropic client lazy-load ────────────────────────────────────────────

_anthropic_client = None

def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        try:
            import anthropic
            _anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        except Exception as e:
            log.error(f"Anthropic client init failed: {e}")
            return None
    return _anthropic_client


# ── Slot accounting ───────────────────────────────────────────────────────

def compute_slot_state(positions: dict) -> dict:
    """{bucket: count_used} from current positions."""
    state = {
        strategy.Bucket.SWING_CRYPTO:    0,
        strategy.Bucket.MOMENTUM_CRYPTO: 0,
        strategy.Bucket.SWING_STOCK:     0,
    }
    for sym, p in (positions or {}).items():
        b = (p.get("bucket") or "").strip()
        if b in state:
            state[b] += 1
        # Legacy positions without bucket: infer
        elif (p.get("market") or "").lower() == "alpaca":
            state[strategy.Bucket.SWING_STOCK] += 1
        else:
            # Default uncategorised crypto to swing (most conservative bucket)
            state[strategy.Bucket.SWING_CRYPTO] += 1
    return state


def compute_cash_aud(positions: dict) -> float:
    """
    Cash = STARTING_CAPITAL - sum of aud_amount across open positions.
    Simple version — production might track explicit cash flows.
    """
    deployed = sum(float(p.get("aud_amount") or 0) for p in (positions or {}).values())
    return max(0.0, strategy.STARTING_CAPITAL_AUD - deployed)


# ── Heartbeat ─────────────────────────────────────────────────────────────

def write_heartbeat(db: SupabaseLogger):
    try:
        db.set_flag(HEARTBEAT_FLAG, safety.now_utc_iso())
    except Exception as e:
        log.warning(f"heartbeat write failed: {e}")


def check_prior_heartbeat(db: SupabaseLogger, tg: TelegramNotifier):
    """
    On startup, check if previous instance died silently. If heartbeat is
    >10 min old, alert the user that the prior run had a problem.
    """
    try:
        last = db.get_flag(HEARTBEAT_FLAG)
        stale, mins = safety.is_heartbeat_stale(last)
        if stale and mins < 60 * 24:  # ignore if >24h, that's just a fresh deploy
            tg.send(f"⚠️ RivX restart: previous instance heartbeat was {mins} min old. "
                    f"Possible silent crash. Check Render logs.")
            log.warning(f"Detected stale prior heartbeat: {mins} min")
    except Exception as e:
        log.debug(f"prior heartbeat check failed: {e}")


# ── Snapshot (mark to market, save daily totals) ─────────────────────────

def run_snapshot(db: SupabaseLogger, alpaca: AlpacaTrader):
    """
    Every 5 min: pull live prices, update each position's current_price + pnl_pct,
    write a snapshot row. Also updates the portfolio peak for drawdown tracking.
    """
    try:
        positions = db.get_positions()
        if not positions:
            return

        # Crypto: use prices.py validated quotes
        crypto_syms = [s for s, p in positions.items()
                       if (p.get("market") or "").lower() != "alpaca"]
        for sym in crypto_syms:
            quote = prices.get_crypto_price(sym)
            if not quote:
                log.warning(f"snapshot: no price for {sym}, skipping")
                continue
            # We mark-to-market even when not validated — for held positions
            # we want current value; we just don't TRADE on unvalidated prices
            mark_aud = quote.aud if quote.aud > 0 else (quote.usd * quote.fx_rate)
            if mark_aud <= 0:
                continue
            try:
                pos = positions.get(sym, {})
                entry = float(pos.get("entry_price") or 0)
                if entry <= 0:
                    # Backfill missing entry from current spot (only acceptable
                    # if validated, otherwise don't touch)
                    if quote.validated and quote.cs_aud > 0:
                        db.update_position_from_alpaca(
                            symbol=sym, current_price=quote.cs_aud,
                            qty=pos.get("qty"), pnl_pct=0.0,
                        )
                        # Also write entry_price = current
                        db._patch("positions",
                                  {"entry_price": quote.cs_aud},
                                  "symbol", sym)
                        log.info(f"snapshot: backfilled {sym} entry to ${quote.cs_aud:.4f}")
                    continue
                pnl_pct = (mark_aud - entry) / entry
                db.update_position_pnl_direct(symbol=sym, pnl_pct=pnl_pct)
            except Exception as e:
                log.warning(f"snapshot crypto {sym}: {e}")

        # Stocks: pull live from Alpaca's positions endpoint
        stock_syms = [s for s, p in positions.items()
                      if (p.get("market") or "").lower() == "alpaca"]
        if stock_syms and alpaca:
            try:
                _sync_alpaca_stocks(db, alpaca, stock_syms)
            except Exception as e:
                log.warning(f"snapshot alpaca sync: {e}")

        # Compute portfolio value + update drawdown peak
        portfolio = db.get_portfolio_value()
        total = float(portfolio.get("total_aud", strategy.STARTING_CAPITAL_AUD))
        peak = float(db.get_flag("portfolio_peak") or strategy.STARTING_CAPITAL_AUD)
        new_peak = safety.update_peak(total, peak)
        if new_peak > peak:
            db.set_flag("portfolio_peak", str(new_peak))

        # Write daily snapshot row (cheap; Supabase handles upsert via date PK)
        try:
            db.save_snapshot(
                total_aud=total,
                day_pnl=portfolio.get("day_pnl", 0),
                total_pnl=portfolio.get("total_pnl", 0),
            )
        except Exception as e:
            log.debug(f"snapshot save: {e}")

    except Exception as e:
        log.error(f"run_snapshot crashed: {e}")
        log.debug(traceback.format_exc())


def _sync_alpaca_stocks(db, alpaca, symbols):
    """Pull current_price + pnl from Alpaca for held stocks."""
    import requests
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    for sym in symbols:
        try:
            r = requests.get(
                f"https://paper-api.alpaca.markets/v2/positions/{sym}",
                headers=headers, timeout=8,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            current_price = float(data.get("current_price") or 0)
            qty = float(data.get("qty") or 0)
            pnl_pct = float(data.get("unrealized_plpc") or 0)
            db.update_position_from_alpaca(
                symbol=sym,
                current_price=current_price,
                qty=qty,
                pnl_pct=pnl_pct,
            )
        except Exception as e:
            log.debug(f"alpaca sync {sym}: {e}")


# ── Trade execution ──────────────────────────────────────────────────────

def execute_buy(
    *, symbol: str, bucket: str, db, alpaca, coinspot,
) -> tuple[bool, str]:
    """
    Execute a buy decision after all gates have passed.
    Returns (success, message).
    """
    is_stock = bucket == strategy.Bucket.SWING_STOCK
    size_aud = strategy.position_size_for(bucket)

    if is_stock:
        # Stocks: Alpaca, USD-denominated. Convert AUD→USD via prices.fx
        usd_aud = prices.get_usd_aud_rate()
        size_usd = size_aud / usd_aud if usd_aud > 0 else 0
        if size_usd <= 0:
            return False, "FX rate unavailable"
        try:
            res = alpaca.buy(symbol, size_usd)
            if res:
                # Wait briefly then fetch fill price
                time.sleep(1.5)
                # Save with placeholder; snapshot loop will fix
                db.save_position(
                    symbol=symbol, entry_price=0, aud_amount=size_aud,
                    market="alpaca",
                )
                # Patch in bucket
                db._patch("positions", {"bucket": bucket}, "symbol", symbol)
                log.info(f"BUY {symbol}: ${size_aud:.0f} AUD via alpaca (entry pending fill)")
                return True, "ok"
            return False, "alpaca returned None"
        except Exception as e:
            return False, f"alpaca error: {e}"

    # Crypto: prices.get_crypto_price MUST validate before we trade
    quote = prices.get_crypto_price(symbol)
    if not quote:
        return False, "no price quote available"
    if not quote.validated:
        return False, (f"price not validated: Binance ${quote.usd:.4f} USD vs "
                       f"CoinSpot ${quote.cs_aud:.4f} AUD, disagree {quote.disagreement_pct:.1f}%")

    # Trade through CoinSpot
    try:
        res = coinspot.buy(symbol, size_aud)
        if not res:
            return False, "coinspot returned None"
        entry_price = float(res.get("price") or quote.aud)
        if entry_price <= 0:
            entry_price = quote.aud
        db.save_position(
            symbol=symbol,
            entry_price=entry_price,
            aud_amount=size_aud,
            market="coinspot",
        )
        db._patch("positions", {"bucket": bucket}, "symbol", symbol)
        log.info(f"BUY {symbol}: ${size_aud:.0f} AUD @ ${entry_price:.4f} via coinspot ({bucket})")
        return True, "ok"
    except Exception as e:
        return False, f"coinspot error: {e}"


def execute_sell(
    *, symbol: str, position: dict, db, alpaca, coinspot,
    is_forced: bool = False, reason: str = "exit rule",
) -> tuple[bool, str]:
    """
    Execute a sell. Goes through safety.check_can_sell first.
    is_forced=True bypasses max-loss guard (user override).
    """
    market = (position.get("market") or "").lower()
    is_stock = market == "alpaca"
    entry_aud = float(position.get("entry_price") or 0)

    # Need a current price to compute exit value
    if is_stock:
        # Use stored current_price from last snapshot
        current_aud = float(position.get("current_price") or 0) * prices.get_usd_aud_rate()
    else:
        quote = prices.get_crypto_price(symbol)
        if not quote or quote.aud <= 0:
            if not is_forced:
                return False, "no validated price for crypto sell"
            # Forced sell with no price — let exchange decide
            current_aud = 0.0
        else:
            current_aud = quote.aud

    # Safety gate
    if entry_aud > 0 and current_aud > 0:
        verdict = safety.check_can_sell(
            symbol=symbol,
            entry_aud=entry_aud,
            exit_aud=current_aud,
            is_forced=is_forced,
        )
        if not verdict.allowed:
            return False, f"safety blocked: {verdict.reason}"

    # Execute
    try:
        if is_stock:
            res = alpaca.sell(symbol)
        else:
            res = coinspot.sell(symbol)
        if not res:
            return False, "exchange returned None"
        exit_price = float(res.get("price") or current_aud or 0)
        pnl_pct = (exit_price - entry_aud) / entry_aud if entry_aud > 0 else 0
        db.close_position(symbol=symbol, exit_price=exit_price, pnl_pct=pnl_pct)

        # Update consecutive-losses counter
        prior = int(db.get_flag("consec_losses") or 0)
        new_count = safety.update_consecutive_losses(prior, last_trade_was_loss=(pnl_pct < 0))
        db.set_flag("consec_losses", str(new_count))

        log.info(f"SELL {symbol}: ${exit_price:.4f} ({pnl_pct*100:+.2f}%) — {reason}")

        # Attribution: update the most recent claude_decisions row for this
        # symbol with the realized outcome. Lets us later answer "did Claude's
        # high-confidence calls actually outperform low-confidence ones?"
        try:
            recent = db._get("claude_decisions", {
                "symbol": f"eq.{symbol}",
                "executed": "eq.true",
                "closed_at": "is.null",
                "order": "decided_at.desc",
                "limit": "1",
            })
            if recent:
                row_id = recent[0].get("id")
                if row_id:
                    db._patch("claude_decisions", {
                        "closed_at": safety.now_utc_iso(),
                        "realized_pnl_pct": pnl_pct,
                        "exit_reason": reason[:200] if reason else "",
                    }, "id", str(row_id))
        except Exception as e:
            log.debug(f"claude_decisions outcome update {symbol}: {e}")

        return True, f"sold @ ${exit_price:.4f} ({pnl_pct*100:+.2f}%)"
    except Exception as e:
        return False, f"sell error: {e}"


# ── Position management (apply exit rules) ──────────────────────────────

def manage_open_positions(db, alpaca, coinspot, tg: TelegramNotifier):
    """
    Walk every open position, apply the appropriate exit rule, sell if needed.
    Runs every snapshot interval.
    """
    positions = db.get_positions()
    if not positions:
        return

    for sym, pos in positions.items():
        try:
            bucket = (pos.get("bucket") or "").strip()
            entry = float(pos.get("entry_price") or 0)
            if entry <= 0:
                continue  # repair will handle these

            pnl_pct = float(pos.get("pnl_pct") or 0)
            peak = float(pos.get("peak_pnl_pct") or pnl_pct)
            age_days = _position_age_days(pos)

            if bucket == strategy.Bucket.SWING_CRYPTO:
                d = strategy.decide_exit_swing_crypto(
                    pnl_pct=pnl_pct, peak_pnl_pct=peak, age_days=age_days,
                )
            elif bucket == strategy.Bucket.MOMENTUM_CRYPTO:
                d = strategy.decide_exit_momentum(pnl_pct=pnl_pct, age_days=age_days)
            elif bucket == strategy.Bucket.SWING_STOCK:
                d = strategy.decide_exit_swing_stock(
                    pnl_pct=pnl_pct, peak_pnl_pct=peak, age_days=age_days,
                )
            else:
                continue

            # Update peak watermark if changed
            if hasattr(d, "new_peak_pnl_pct") and d.new_peak_pnl_pct > peak:
                try:
                    db._patch("positions",
                              {"peak_pnl_pct": d.new_peak_pnl_pct},
                              "symbol", sym)
                except Exception:
                    pass

            if d.should_exit:
                ok, msg = execute_sell(
                    symbol=sym, position=pos, db=db, alpaca=alpaca, coinspot=coinspot,
                    is_forced=False, reason=d.reason,
                )
                if ok:
                    tg.send(f"📤 SELL {sym}: {d.reason}\n{msg}")
                else:
                    log.warning(f"sell {sym} failed: {msg}")
                    tg.send(f"⚠️ SELL {sym} FAILED: {msg}")
        except Exception as e:
            log.warning(f"manage {sym}: {e}")


def _position_age_days(pos: dict) -> float:
    try:
        opened = pos.get("opened_at") or pos.get("created_at")
        if not opened:
            return 0.0
        dt = datetime.fromisoformat(opened.replace("Z", "+00:00"))
        return (utc_now() - dt).total_seconds() / 86400.0
    except Exception:
        return 0.0


# ── Scan + decide cycle (the main "look for buys" job) ──────────────────

def run_buy_cycle(
    *, mode: str, db, alpaca, coinspot, tg: TelegramNotifier,
):
    """
    mode: 'swing_crypto' | 'momentum_crypto' | 'swing_stock' | 'all'
    """
    log.info(f"buy cycle: {mode}")
    try:
        # Get candidates
        if mode == "swing_stock":
            candidates = scanner.scan_stocks()
        elif mode == "all":
            scan_result = scanner.scan_all()
            candidates = (scan_result["swing_crypto"]
                          + scan_result["momentum_crypto"]
                          + scan_result["swing_stock"])
        else:
            crypto = scanner.scan_crypto()
            candidates = [c for c in crypto if c["bucket"] == mode]

        if not candidates:
            log.info(f"buy cycle {mode}: no candidates")
            return

        # Portfolio context
        positions = db.get_positions()
        slot_state = compute_slot_state(positions)
        cash = compute_cash_aud(positions)
        peak = float(db.get_flag("portfolio_peak") or strategy.STARTING_CAPITAL_AUD)
        portfolio = db.get_portfolio_value()
        total = float(portfolio.get("total_aud", strategy.STARTING_CAPITAL_AUD))
        consec = int(db.get_flag("consec_losses") or 0)
        kill = (db.get_flag("kill_switch") or "").lower() in ("on", "1", "true")
        buys_today = int(db.get_flag(f"buys_today_{utc_now().strftime('%Y%m%d')}") or 0)

        # Safety gate (check before spending Claude tokens)
        verdict = safety.check_can_buy(
            current_total_aud=total, peak_total_aud=peak,
            buys_today=buys_today, consecutive_losses=consec,
            manual_kill=kill,
        )
        if not verdict.allowed:
            log.info(f"buy cycle {mode}: blocked — {verdict.reason}")
            return

        # Daily Claude spend
        spent_str = db.get_flag(f"claude_spend_{utc_now().strftime('%Y%m%d')}") or "0"
        try:
            spent = float(spent_str)
        except ValueError:
            spent = 0.0

        # Ask Claude
        client = get_anthropic_client()
        result = brain.decide_buys(
            candidates=candidates,
            positions=positions,
            slot_state=slot_state,
            cash_aud=cash,
            anthropic_client=client,
            daily_spent_usd=spent,
        )

        # Update spend tracking
        new_spent = spent + result.estimated_cost_usd
        db.set_flag(f"claude_spend_{utc_now().strftime('%Y%m%d')}", f"{new_spent:.4f}")

        if result.error:
            tg.send(f"⚠️ Brain error: {result.error}")
            return
        if not result.decisions:
            log.info(f"buy cycle {mode}: Claude returned no decisions ({result.summary})")
            return

        # Apply safety filter
        allowed, rejected = brain.filter_decisions_by_safety(
            result.decisions, cash_aud=cash, slot_state=slot_state,
        )
        for d, reason in rejected:
            log.info(f"safety filter rejected {d.symbol}: {reason}")

        # Attribution log: write EVERY Claude decision (buy/skip/rejected)
        # so we can later evaluate whether Claude added value vs pure rules.
        allowed_syms = {d.symbol for d in allowed}
        rejected_syms = {d.symbol for d, _ in rejected}
        for d in result.decisions:
            try:
                if d.action == "buy" and d.symbol in allowed_syms:
                    final_action = "buy"
                    executed = True
                elif d.action == "buy" and d.symbol in rejected_syms:
                    final_action = "rejected_by_safety"
                    executed = False
                else:
                    final_action = "skip"
                    executed = False
                db._post("claude_decisions", {
                    "symbol": d.symbol,
                    "bucket": d.bucket,
                    "action": final_action,
                    "confidence": d.confidence,
                    "reason": d.reason[:300] if d.reason else "",
                    "executed": executed,
                })
            except Exception as e:
                log.debug(f"claude_decisions log {d.symbol}: {e}")

        # Execute approved buys
        for d in allowed:
            if d.action != "buy":
                continue
            ok, msg = execute_buy(
                symbol=d.symbol, bucket=d.bucket,
                db=db, alpaca=alpaca, coinspot=coinspot,
            )
            if ok:
                # Increment daily counter
                key = f"buys_today_{utc_now().strftime('%Y%m%d')}"
                cur = int(db.get_flag(key) or 0)
                db.set_flag(key, str(cur + 1))
                tg.send(f"📥 BUY {d.symbol} ({d.bucket}): conf {d.confidence:.0%}\n{d.reason}")
            else:
                tg.send(f"⚠️ BUY {d.symbol} blocked: {msg}")

    except Exception as e:
        log.error(f"buy cycle {mode} crashed: {e}")
        log.debug(traceback.format_exc())
        tg.send(f"⚠️ buy cycle error ({mode}): {e}")


# ── Manual orders (Telegram /sell, dashboard force-sell) ─────────────────

def run_manual_orders(db, alpaca, coinspot, tg: TelegramNotifier):
    """Poll the manual_orders table and execute pending entries."""
    try:
        orders = db._get("manual_orders", {"status": "eq.pending",
                                            "order": "requested_at.asc",
                                            "limit": "10"})
    except Exception as e:
        # 403 spam reduction: log once per minute
        if int(time.time()) % 60 == 0:
            log.debug(f"manual_orders read: {e}")
        return

    for order in (orders or []):
        oid = order.get("id")
        sym = order.get("symbol", "").upper()
        action = (order.get("action") or "").lower()

        try:
            if action == "sell":
                positions = db.get_positions()
                pos = positions.get(sym)
                if not pos:
                    db._patch("manual_orders",
                              {"status": "error", "error": f"no open position {sym}"},
                              "id", str(oid))
                    continue
                ok, msg = execute_sell(
                    symbol=sym, position=pos, db=db, alpaca=alpaca, coinspot=coinspot,
                    is_forced=True, reason="manual order",
                )
                db._patch("manual_orders",
                          {"status": "done" if ok else "error",
                           "executed_at": safety.now_utc_iso(),
                           "error": "" if ok else msg},
                          "id", str(oid))
                if ok:
                    tg.send(f"✅ Manual SELL {sym} done: {msg}")
                else:
                    tg.send(f"❌ Manual SELL {sym} failed: {msg}")
            else:
                db._patch("manual_orders",
                          {"status": "error", "error": f"action {action} not supported here"},
                          "id", str(oid))
        except Exception as e:
            log.warning(f"manual order {oid}: {e}")


# ── Main loop ────────────────────────────────────────────────────────────

def main():
    """The orchestrator. Runs forever. All errors caught and logged."""
    try:
        log.info(f"RivX v2 starting — {'PAPER' if PAPER_MODE else 'LIVE'} mode")
        log.info(f"Strategy: $4K swing crypto / $2K momentum crypto / $3.5K stocks / $500 ops floor")
        sys.stdout.flush()

        db = SupabaseLogger()
        log.info("SupabaseLogger ready")
        tg = TelegramNotifier()
        log.info("TelegramNotifier ready")
        alpaca = AlpacaTrader()
        log.info("AlpacaTrader ready")
        coinspot = CoinSpotTrader()
        log.info("CoinSpotTrader ready")

        check_prior_heartbeat(db, tg)

        # Daily startup announcement
        today = aest_now().date().isoformat()
        if db.get_flag("last_startup") != today:
            db.set_flag("last_startup", today)
            tg.send(f"🟢 RivX v2 online. {'PAPER' if PAPER_MODE else 'LIVE'} mode. "
                    f"Strategy: swing+momentum dual bucket. /help for commands.")

        log.info("setup complete — entering main loop")
        sys.stdout.flush()
    except Exception as e:
        # Setup-time crashes get loud failure: log + telegram + sleep so Render captures
        tb = traceback.format_exc()
        sys.stderr.write(f"\n!!! SETUP CRASH !!!\n{tb}\n")
        sys.stderr.flush()
        try:
            if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                import requests
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_CHAT_ID,
                          "text": f"⚠️ RivX SETUP CRASH:\n{type(e).__name__}: {str(e)[:300]}"},
                    timeout=5,
                )
        except Exception:
            pass
        time.sleep(10)
        raise

    last_snapshot = 0.0
    last_swing_crypto_run = db.get_flag("last_swing_crypto_run")
    last_momentum_runs = {t: db.get_flag(f"last_momentum_{t}") for t in MOMENTUM_TIMES_AEST}
    last_stock_run = db.get_flag("last_stock_run")

    while True:
        try:
            now_ts = time.time()

            write_heartbeat(db)

            # Always: kill switch + manual orders + telegram polling
            try:
                tg.check_kill_switch(db)
            except Exception as e:
                log.debug(f"telegram poll: {e}")
            run_manual_orders(db, alpaca, coinspot, tg)

            # Snapshot every 5 min
            if now_ts - last_snapshot >= SNAPSHOT_INTERVAL_SEC:
                run_snapshot(db, alpaca)
                manage_open_positions(db, alpaca, coinspot, tg)
                last_snapshot = now_ts

            kill = (db.get_flag("kill_switch") or "").lower() in ("on", "1", "true")
            if not kill:
                # Swing crypto: once a day at 8 AM AEST
                for t in SWING_CRYPTO_TIMES_AEST:
                    if at_or_past_time_today(t, last_swing_crypto_run):
                        run_buy_cycle(mode=strategy.Bucket.SWING_CRYPTO,
                                      db=db, alpaca=alpaca, coinspot=coinspot, tg=tg)
                        last_swing_crypto_run = safety.now_utc_iso()
                        db.set_flag("last_swing_crypto_run", last_swing_crypto_run)

                # Momentum: 8 AM and 4 PM
                for t in MOMENTUM_TIMES_AEST:
                    if at_or_past_time_today(t, last_momentum_runs.get(t)):
                        run_buy_cycle(mode=strategy.Bucket.MOMENTUM_CRYPTO,
                                      db=db, alpaca=alpaca, coinspot=coinspot, tg=tg)
                        last_momentum_runs[t] = safety.now_utc_iso()
                        db.set_flag(f"last_momentum_{t}", last_momentum_runs[t])

                # Swing stocks: 8 AM AEST (during US market hours window — actually
                # 8am AEST = 6pm ET previous day, after-hours. We still scan; trades
                # execute when market opens. Alpaca handles queueing during off-hours).
                for t in SWING_STOCK_TIMES_AEST:
                    if at_or_past_time_today(t, last_stock_run):
                        run_buy_cycle(mode=strategy.Bucket.SWING_STOCK,
                                      db=db, alpaca=alpaca, coinspot=coinspot, tg=tg)
                        last_stock_run = safety.now_utc_iso()
                        db.set_flag("last_stock_run", last_stock_run)

            time.sleep(MAIN_TICK_SECONDS)

        except KeyboardInterrupt:
            log.info("shutdown signal received")
            tg.send("🛑 RivX shutting down (manual)")
            break
        except Exception as e:
            # Loop-level errors: log loud, sleep, retry. Never crash the loop.
            log.error(f"main loop iteration error: {e}")
            log.debug(traceback.format_exc())
            time.sleep(60)


if __name__ == "__main__":
    main()
