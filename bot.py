# RIVX_VERSION: v2.9.4-orphan-stock-heal-2026-05-07
"""
RivX bot.py — main loop orchestrator (v2 strategy).

v2.9.4 changes from v2.9.3:
  - SELF-HEALING ORPHAN STOCK CLOSE. Previously: when Alpaca said "no live
    position" for a stock that Supabase still showed as open, execute_sell
    returned False with the message "alpaca reports no live position
    (already closed?)". The next snapshot would try the same sell again.
    Forever. Telegram alert every 5 minutes until manually intervened.

    This is exactly what happened to AMD on 2026-05-06: pending_sells'
    Supabase status PATCH failed silently (the v2.8 migration adding
    pending_order_id / pending_since columns may not have run), so the
    position stayed status='open'. Alpaca filled the sell. Resolver had
    nothing to find. execute_sell looped on the dead guard.

    FIX: when alpaca.get_position returns None for a stock we still show
    as open, fetch the most recent filled SELL order for that symbol from
    Alpaca's /v2/orders endpoint and use its fill price to close the
    Supabase position properly. Bookkeeping (consec_losses, claude_decisions)
    is updated as for any normal close. The Telegram alert that used to
    spam every 5 minutes now becomes a single one-shot reconciliation
    note inside the manage loop.

  - No other behavioural changes. v2.9.3 trailing-stop fix and all earlier
    cadence behaviour preserved.

v2.9.3 changes from v2.9.0:
  - TRAILING STOP BUG FIX. peak_pnl_pct was never being written to
    Supabase, so trailing stops weren't actually active. Two issues
    in manage_open_positions():

    1. The fallback `peak = pos.get("peak_pnl_pct") or pnl_pct` made
       null-peak indistinguishable from zero-peak. When peak was null,
       it fell back to current pnl_pct, then `decide_exit_swing_stock`
       returned new_peak = max(stored_peak, pnl_pct) = pnl_pct, then
       the update condition `if new_peak > peak` became `pnl_pct > pnl_pct`
       which is False. So the database never got updated. Forever.

    2. Swing stocks were completely skipped when US market was closed.
       That meant peak_pnl_pct couldn't be updated for stock positions
       outside of 11:30 PM–6 AM AEST. AMD ran from +12% to +19% during
       a market session but the snapshot loop never saw it because the
       five-minute cycles outside market hours just `continue`d past it.

    FIX:
    - Read stored peak as None-aware (use a sentinel)
    - Always write peak_pnl_pct on every snapshot if current pnl_pct is
      higher than stored peak (or if stored is null)
    - For swing_stock when market is closed: still update peak from
      Alpaca's after-hours/extended price feed (Alpaca returns prices
      24/7), still skip the actual exit-rule firing (because we can't
      sell when market is closed anyway)

  - No other changes. All other v2.9.0 cadence behaviour preserved.

v2.9.0 changes from v2.8.2:
  - Crypto scan cadence overhaul. Crypto markets are 24/7 but the bot
    was only scanning during Australian daytime, missing breakouts that
    happen during US trading hours.
      * Momentum crypto: was 2x/day (8 AM + 4 PM AEST), now every 2 hours
        24/7 (12 scans/day). Catches breakouts that develop overnight
        Brisbane-time when US markets are most active.
      * Swing crypto: was 1x/day (8 AM AEST), now 2x/day (8 AM + 8 PM
        AEST). Catches pullbacks that develop during US hours.
  - Daily buy cap raised from 6 to 10 in safety.py to match the new
    momentum cadence — otherwise on a strong-trend day the cap would
    block valid swing/stock buys later in the day.
  - Stock scan cadence unchanged (11 PM + 3 AM AEST weekdays).
  - Cost impact at $0.05/scan: ~$0.80/day total, well under the $2 cap.

v2.8.2 changes from v2.8.1:
  - Cost tab fix: every Claude API call (buy decisions + Q&A) now writes
    to the token_usage table via db.record_token_usage(). Was previously
    only writing daily spend to a bot_flags entry, so the dashboard's
    Cost tab showed nothing. Q&A path refactored to also capture token
    counts (was previously silent on usage).

v2.8.1 changes from v2.8:
  - ASX analyser fully removed. Yahoo Finance was blocking ASX requests
    from Render's Singapore IPs (every ticker returning empty responses).
    All ASX module imports, tick calls, and startup messages stripped.
    Will revisit with a paid data provider in a future version.

v2.8 changes from v2.7:
  - SELL flow no longer marks Supabase position as 'closed' until Alpaca
    confirms the fill. New status 'pending_close' bridges the gap. The
    pending_sells.resolve_pending_closes() job runs every snapshot to
    move pending_close → closed once Alpaca confirms a real fill price.
  - Scanner candidates are filtered through scanner_exclusions.filter_blocked_symbols()
    so we never try to buy a symbol that has an open Alpaca order
    (prevents wash-trade rejections like AVGO tonight).
  - reconciler.tick() runs every 10 minutes in read-only mode, comparing
    Alpaca's actual state to Supabase and writing mismatches to the new
    reconciliation_log table. One Telegram warning per day if anything
    diverges.

v2.6 fix: claude_decisions.executed was being set to True BEFORE
execute_buy() ran. Now we write executed=False first, capture the row id,
and only PATCH executed=True after a successful fill.

v2.4 fix: stock entry prices now read from actual Alpaca fill, stored
as USD-per-share. AUD conversion happens at display time.
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

# v2.8.1: ASX analyser removed. Yahoo Finance blocking ASX requests from
# Render IPs. Will revisit with a paid data provider in a future version.

# v2.8: reconciliation, pending sells, scanner exclusions
try:
    from bot import reconciler
    from bot import pending_sells
    from bot import scanner_exclusions
    _RECONCILIATION_AVAILABLE = True
except Exception as _rec_err:
    log.warning(f"v2.8 reconciliation modules unavailable: {_rec_err}")
    reconciler = None
    pending_sells = None
    scanner_exclusions = None
    _RECONCILIATION_AVAILABLE = False


# ── Loop cadence ──────────────────────────────────────────────────────────
# v2.9.0: crypto markets are 24/7. Scan times reflect that.
#   - Swing crypto: 2x/day (was 1x). Catches pullbacks during US hours.
#   - Momentum crypto: 12x/day every 2 hours (was 2x). Catches breakouts
#     that fire during US trading hours when Brisbane is asleep.
#   - Stock cadence unchanged — US market hours are already covered by
#     11 PM + 3 AM AEST scans.

MAIN_TICK_SECONDS         = 30
SNAPSHOT_INTERVAL_SEC     = 300
SWING_CRYPTO_TIMES_AEST   = ["08:00", "20:00"]
MOMENTUM_TIMES_AEST       = ["00:00", "02:00", "04:00", "06:00",
                             "08:00", "10:00", "12:00", "14:00",
                             "16:00", "18:00", "20:00", "22:00"]
SWING_STOCK_TIMES_AEST    = ["23:00", "03:00"]
DAILY_SUMMARY_TIMES_AEST  = ["08:00", "20:00"]
HEARTBEAT_FLAG            = "last_heartbeat"


# ── Time helpers ──────────────────────────────────────────────────────────

AEST = timezone(timedelta(hours=10))

def aest_now() -> datetime:
    return datetime.now(AEST)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def at_or_past_time_today(target_hhmm: str, last_run_iso: str | None) -> bool:
    now = aest_now()
    target_h, target_m = map(int, target_hhmm.split(":"))
    target_today = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
    if now < target_today:
        return False
    if not last_run_iso:
        return True
    try:
        last = datetime.fromisoformat(last_run_iso.replace("Z", "+00:00"))
        last_aest = last.astimezone(AEST)
        return last_aest < target_today
    except Exception:
        return True


def is_us_trading_weekday_aest() -> bool:
    now_aest = aest_now()
    et_now = now_aest - timedelta(hours=14)
    return et_now.weekday() < 5


def is_us_market_open_aest() -> bool:
    """
    True iff the US equity market is currently open (M-F, 09:30-16:00 ET).
    """
    try:
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now_aest = aest_now()
        et_offset = 14 if 3 <= now_aest.month <= 10 else 15
        now_et = (now_aest - timedelta(hours=et_offset)).replace(tzinfo=None)

    if now_et.weekday() >= 5:
        return False
    minutes = now_et.hour * 60 + now_et.minute
    return (9 * 60 + 30) <= minutes < (16 * 60)


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
    state = {
        strategy.Bucket.SWING_CRYPTO:    0,
        strategy.Bucket.MOMENTUM_CRYPTO: 0,
        strategy.Bucket.SWING_STOCK:     0,
    }
    for sym, p in (positions or {}).items():
        b = (p.get("bucket") or "").strip()
        if b in state:
            state[b] += 1
        elif (p.get("market") or "").lower() == "alpaca":
            state[strategy.Bucket.SWING_STOCK] += 1
        else:
            state[strategy.Bucket.SWING_CRYPTO] += 1
    return state


def compute_cash_aud(positions: dict) -> float:
    deployed = sum(float(p.get("aud_amount") or 0) for p in (positions or {}).values())
    return max(0.0, strategy.STARTING_CAPITAL_AUD - deployed)


# ── Heartbeat ─────────────────────────────────────────────────────────────

def write_heartbeat(db: SupabaseLogger):
    try:
        db.set_flag(HEARTBEAT_FLAG, safety.now_utc_iso())
    except Exception as e:
        log.warning(f"heartbeat write failed: {e}")


def check_prior_heartbeat(db: SupabaseLogger, tg: TelegramNotifier):
    try:
        last = db.get_flag(HEARTBEAT_FLAG)
        stale, mins = safety.is_heartbeat_stale(last)
        if stale and mins < 60 * 24:
            tg.send(f"⚠️ RivX restart: previous instance heartbeat was {mins} min old. "
                    f"Possible silent crash. Check Render logs.")
            log.warning(f"Detected stale prior heartbeat: {mins} min")
    except Exception as e:
        log.debug(f"prior heartbeat check failed: {e}")


# ── Snapshot (mark to market, save daily totals) ─────────────────────────

def run_snapshot(db: SupabaseLogger, alpaca: AlpacaTrader):
    try:
        positions = db.get_positions() or {}

        if positions:
            crypto_syms = [s for s, p in positions.items()
                           if (p.get("market") or "").lower() != "alpaca"]
            for sym in crypto_syms:
                quote = prices.get_crypto_price(sym)
                if not quote:
                    log.warning(f"snapshot: no price for {sym}, skipping")
                    continue
                mark_aud = quote.cs_aud if quote.cs_aud > 0 else (quote.usd * quote.fx_rate)
                if mark_aud <= 0:
                    continue
                try:
                    pos = positions.get(sym, {})
                    entry = float(pos.get("entry_price") or 0)
                    if entry <= 0:
                        if quote.validated and quote.cs_aud > 0:
                            db.update_position_from_alpaca(
                                symbol=sym, current_price=quote.cs_aud,
                                qty=pos.get("qty"), pnl_pct=0.0,
                            )
                            db._patch("positions",
                                      {"entry_price": quote.cs_aud},
                                      "symbol", sym)
                            log.info(f"snapshot: backfilled {sym} entry to ${quote.cs_aud:.4f}")
                        continue
                    pnl_pct = (mark_aud - entry) / entry
                    db.update_position_from_alpaca(
                        symbol=sym, current_price=mark_aud,
                        qty=pos.get("qty"), pnl_pct=pnl_pct,
                    )
                except Exception as e:
                    log.warning(f"snapshot crypto {sym}: {e}")

            stock_syms = [s for s, p in positions.items()
                          if (p.get("market") or "").lower() == "alpaca"]
            if stock_syms and alpaca:
                try:
                    _sync_alpaca_stocks(db, alpaca, stock_syms)
                except Exception as e:
                    log.warning(f"snapshot alpaca sync: {e}")

        portfolio = db.get_portfolio_value() or {}
        total = float(portfolio.get("total_aud", strategy.STARTING_CAPITAL_AUD))
        peak = float(db.get_flag("portfolio_peak") or strategy.STARTING_CAPITAL_AUD)
        new_peak = safety.update_peak(total, peak)
        if new_peak > peak:
            db.set_flag("portfolio_peak", str(new_peak))

        try:
            db.save_snapshot(
                total_aud=total,
                day_pnl=portfolio.get("day_pnl", 0),
                total_pnl=portfolio.get("total_pnl", 0),
            )
            log.info(f"snapshot saved: total=${total:.2f}, "
                     f"{len(positions)} positions, peak=${peak:.2f}")
        except Exception as e:
            log.warning(f"snapshot save FAILED: {e}")

    except Exception as e:
        log.error(f"run_snapshot crashed: {e}")
        log.error(traceback.format_exc())


def _sync_alpaca_stocks(db, alpaca, symbols):
    """
    Pull current_price + pnl + avg_entry from Alpaca for held stocks.
    """
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
            current_price_usd = float(data.get("current_price") or 0)
            qty = float(data.get("qty") or 0)
            pnl_pct = float(data.get("unrealized_plpc") or 0)
            avg_entry_usd = float(data.get("avg_entry_price") or 0)

            db.update_position_from_alpaca(
                symbol=sym,
                current_price=current_price_usd,
                qty=qty,
                pnl_pct=pnl_pct,
                avg_entry_price=avg_entry_usd,
            )
        except Exception as e:
            log.debug(f"alpaca sync {sym}: {e}")


# ── v2.9.4: Self-healing orphan stock close ──────────────────────────────

def _heal_orphan_stock_close(*, symbol: str, position: dict, db,
                              reason: str = "auto-heal") -> tuple[bool, str]:
    """
    Called when Alpaca says "no position" for a stock that Supabase still
    shows as open. Looks up the most recent filled SELL order on Alpaca for
    this symbol, computes pnl from stored entry vs that fill price, and
    closes the Supabase position properly. All bookkeeping (consec_losses,
    claude_decisions outcome row) is updated as for any normal close.

    Returns (ok, msg) compatible with execute_sell's return contract.
    """
    import requests

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }

    try:
        r = requests.get(
            "https://paper-api.alpaca.markets/v2/orders",
            headers=headers,
            params={"status": "closed", "symbols": symbol,
                    "direction": "desc", "limit": "20"},
            timeout=10,
        )
        r.raise_for_status()
        orders = r.json() or []
    except Exception as e:
        return False, f"{symbol}: heal failed querying alpaca orders: {e}"

    # Find the most recent FILLED SELL with a real fill price.
    sell_order = None
    for o in orders:
        if (o.get("side") or "").lower() != "sell":
            continue
        if (o.get("status") or "").lower() != "filled":
            continue
        fp = float(o.get("filled_avg_price") or 0)
        if fp <= 0:
            continue
        sell_order = o
        break

    if not sell_order:
        return False, (f"{symbol}: alpaca reports no live position and no recent "
                       f"filled sell order found — manual investigation needed")

    fill_price_usd = float(sell_order.get("filled_avg_price") or 0)
    filled_at      = (sell_order.get("filled_at") or "")[:19]
    order_id       = sell_order.get("id") or ""

    avg_entry_usd = float(position.get("entry_price") or 0)
    if avg_entry_usd <= 0:
        return False, (f"{symbol}: heal can't compute pnl — stored entry_price is 0. "
                       f"Manual close needed (fill was ${fill_price_usd:.4f} USD, "
                       f"order {order_id[:8]} at {filled_at})")

    pnl_pct = (fill_price_usd - avg_entry_usd) / avg_entry_usd

    try:
        db.close_position(symbol=symbol, exit_price=fill_price_usd, pnl_pct=pnl_pct)
    except Exception as e:
        return False, f"{symbol}: heal close_position failed: {e}"

    # Clear any pending_close residue (best-effort; columns may not exist).
    try:
        db._patch("positions", {
            "pending_order_id": None,
            "pending_since": None,
        }, "symbol", symbol)
    except Exception:
        pass

    # consec_losses bookkeeping
    try:
        prior = int(db.get_flag("consec_losses") or 0)
        new_count = safety.update_consecutive_losses(
            prior, last_trade_was_loss=(pnl_pct < 0),
        )
        db.set_flag("consec_losses", str(new_count))
    except Exception as e:
        log.debug(f"heal consec_losses {symbol}: {e}")

    # claude_decisions outcome row
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
                    "exit_reason": f"healed orphan: {reason}"[:200],
                }, "id", str(row_id))
    except Exception as e:
        log.debug(f"heal claude_decisions {symbol}: {e}")

    log.info(f"HEALED orphan SELL {symbol}: ${fill_price_usd:.4f} USD "
             f"({pnl_pct*100:+.2f}%) from order {order_id[:8]} filled {filled_at}")

    return True, (f"healed orphan — closed @ ${fill_price_usd:.4f} USD "
                  f"({pnl_pct*100:+.2f}%) using order {order_id[:8]} "
                  f"filled {filled_at}")


# ── Trade execution ──────────────────────────────────────────────────────

def execute_buy(
    *, symbol: str, bucket: str, db, alpaca, coinspot,
) -> tuple[bool, str]:
    """
    v2.4: stock branch reads actual Alpaca fill price and stores
    AUD/share entry up front. No more entry_price=0 placeholder.
    """
    is_stock = bucket == strategy.Bucket.SWING_STOCK
    size_aud = strategy.position_size_for(bucket)

    if is_stock:
        try:
            order = alpaca.buy(symbol, size_aud)
            if not order:
                return False, "alpaca returned None"

            fill_usd, qty = _resolve_alpaca_fill(alpaca, order)

            if fill_usd <= 0 or qty <= 0:
                log.warning(f"BUY {symbol}: order accepted but not yet filled "
                            f"(id={order.get('id')}) — saving entry=0, will heal")
                db.save_position(
                    symbol=symbol, entry_price=0, aud_amount=size_aud,
                    market="alpaca",
                )
                db._patch("positions", {"bucket": bucket, "qty": 0},
                          "symbol", symbol)
                return True, "ok (fill pending)"

            db.save_position(
                symbol=symbol,
                entry_price=fill_usd,
                aud_amount=size_aud,
                market="alpaca",
            )
            db._patch("positions",
                      {"bucket": bucket, "qty": qty},
                      "symbol", symbol)
            log.info(f"BUY {symbol}: {qty:.4f} sh @ ${fill_usd:.2f} USD "
                     f"· ${size_aud:.0f} AUD total")
            return True, "ok"
        except Exception as e:
            log.error(f"alpaca buy {symbol}: {e}")
            return False, f"alpaca error: {e}"

    quote = prices.get_crypto_price(symbol)
    if not quote:
        return False, "no price quote available"
    if not quote.validated:
        return False, (f"price not validated: Binance ${quote.usd:.4f} USD vs "
                       f"CoinSpot ${quote.cs_aud:.4f} AUD, disagree {quote.disagreement_pct:.1f}%")

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


def _resolve_alpaca_fill(alpaca, order: dict) -> tuple[float, float]:
    """Returns (filled_avg_price_usd, filled_qty)."""
    fill_price = float(order.get("filled_avg_price") or 0)
    qty = float(order.get("filled_qty") or 0)
    if fill_price > 0 and qty > 0:
        return fill_price, qty

    order_id = order.get("id")
    if not order_id:
        return 0.0, 0.0

    for attempt in range(5):
        time.sleep(1.0)
        try:
            updated = alpaca._get(f"/v2/orders/{order_id}")
            if not updated:
                continue
            fill_price = float(updated.get("filled_avg_price") or 0)
            qty = float(updated.get("filled_qty") or 0)
            status = updated.get("status", "")
            if fill_price > 0 and qty > 0:
                return fill_price, qty
            if status in ("rejected", "canceled", "expired"):
                log.warning(f"order {order_id} ended without fill: {status}")
                return 0.0, 0.0
        except Exception as e:
            log.debug(f"order poll {attempt}: {e}")
    return 0.0, 0.0


def execute_sell(
    *, symbol: str, position: dict, db, alpaca, coinspot,
    is_forced: bool = False, reason: str = "exit rule",
) -> tuple[bool, str]:
    """Close a position."""
    market = (position.get("market") or "").lower()
    is_stock = market == "alpaca"

    if is_stock:
        try:
            live = alpaca.get_position(symbol)
        except Exception as e:
            return False, f"{symbol}: alpaca position fetch failed: {e}"

        if not live:
            # v2.9.4: self-heal. Alpaca says no position but Supabase still
            # shows one open — almost certainly a sell that filled but never
            # propagated back to Supabase. Find the actual fill on Alpaca,
            # close Supabase using that fill price, write all bookkeeping.
            log.warning(f"{symbol}: alpaca reports no live position — attempting heal")
            return _heal_orphan_stock_close(
                symbol=symbol, position=position, db=db,
                reason=reason,
            )

        try:
            avg_entry_usd = float(live.get("avg_entry_price") or 0)
            current_usd   = float(live.get("current_price") or 0)
            pnl_pct_alp   = float(live.get("unrealized_plpc") or 0)
        except (TypeError, ValueError) as e:
            return False, f"{symbol}: malformed alpaca data: {e}"

        if avg_entry_usd <= 0 or current_usd <= 0:
            return False, (f"{symbol}: alpaca returned zero/missing prices "
                           f"(entry={avg_entry_usd}, current={current_usd}) — refusing sell")

        if not is_forced:
            v = safety.check_can_sell(
                symbol=symbol,
                entry_aud=avg_entry_usd,
                exit_aud=current_usd,
                is_forced=False,
            )
            if not v.allowed:
                return False, f"safety blocked: {v.reason}"

        # v2.8: route through pending_sells module so we don't close
        # Supabase position until Alpaca actually confirms the fill
        if _RECONCILIATION_AVAILABLE and pending_sells is not None:
            ok, msg = pending_sells.submit_sell_for_stock(
                symbol=symbol, position=position, db=db, alpaca=alpaca, log_obj=log,
            )
            if ok:
                # Update consec losses + claude_decisions only if we got a
                # confirmed fill at submission time. For pending closes,
                # the resolver does this when the fill confirms.
                if "closed @" in msg:
                    pnl_pct = (current_usd - avg_entry_usd) / avg_entry_usd if avg_entry_usd > 0 else 0
                    prior = int(db.get_flag("consec_losses") or 0)
                    new_count = safety.update_consecutive_losses(prior, last_trade_was_loss=(pnl_pct < 0))
                    db.set_flag("consec_losses", str(new_count))
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
                return True, msg
            return False, msg

        # Fallback (old path) if v2.8 modules aren't loaded
        try:
            res = alpaca.sell(symbol)
        except Exception as e:
            return False, f"alpaca sell error: {e}"
        if not res:
            return False, "alpaca sell returned None"

        exit_price_usd = current_usd
        pnl_pct        = pnl_pct_alp
        db.close_position(symbol=symbol, exit_price=exit_price_usd, pnl_pct=pnl_pct)

        prior = int(db.get_flag("consec_losses") or 0)
        new_count = safety.update_consecutive_losses(prior, last_trade_was_loss=(pnl_pct < 0))
        db.set_flag("consec_losses", str(new_count))

        log.info(f"SELL {symbol}: ${exit_price_usd:.4f} USD ({pnl_pct*100:+.2f}%) — {reason}")

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

        return True, f"sold @ ${exit_price_usd:.4f} USD ({pnl_pct*100:+.2f}%)"

    # ── Crypto branch ──
    entry_aud = float(position.get("entry_price") or 0)
    quote = prices.get_crypto_price(symbol)
    if not quote or quote.aud <= 0:
        if not is_forced:
            return False, "no validated price for crypto sell"
        current_aud = 0.0
    else:
        current_aud = quote.aud

    if entry_aud > 0 and current_aud > 0:
        v = safety.check_can_sell(
            symbol=symbol, entry_aud=entry_aud, exit_aud=current_aud,
            is_forced=is_forced,
        )
        if not v.allowed:
            return False, f"safety blocked: {v.reason}"

    try:
        res = coinspot.sell(symbol)
    except Exception as e:
        return False, f"coinspot sell error: {e}"
    if not res:
        return False, "coinspot returned None"

    exit_price = float(res.get("price") or current_aud or 0)
    pnl_pct = (exit_price - entry_aud) / entry_aud if entry_aud > 0 else 0
    db.close_position(symbol=symbol, exit_price=exit_price, pnl_pct=pnl_pct)

    prior = int(db.get_flag("consec_losses") or 0)
    new_count = safety.update_consecutive_losses(prior, last_trade_was_loss=(pnl_pct < 0))
    db.set_flag("consec_losses", str(new_count))

    log.info(f"SELL {symbol}: ${exit_price:.4f} AUD ({pnl_pct*100:+.2f}%) — {reason}")

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

    return True, f"sold @ ${exit_price:.4f} AUD ({pnl_pct*100:+.2f}%)"


# ── Position management ─────────────────────────────────────────────────

def manage_open_positions(db, alpaca, coinspot, tg: TelegramNotifier):
    """
    v2.9.3: TRAILING STOP BUG FIX.

    The previous version had two bugs that meant trailing stops were never
    actually active:

    (1) When peak_pnl_pct was null in the DB (which is the initial state for
        every position), the fallback `pos.get("peak_pnl_pct") or pnl_pct`
        used the current pnl_pct as the peak. Then the strategy returned
        new_peak = max(stored, current) which equals current. Then the
        update guard `if new_peak > stored` became `current > current`
        which is False. Peak was never written. Forever.

    (2) Swing stocks were skipped entirely when US market was closed. But
        Alpaca returns prices 24/7 (after-hours, pre-market, etc), so the
        peak should still update — we just can't actually fire a sell
        outside market hours. By skipping the whole loop iteration we
        also skipped the peak update.

    FIX: track stored peak as None-aware. Always write peak when current
    exceeds stored (or when stored is null). For swing stocks when market
    is closed: still update peak, just don't try to execute a sell.
    """
    positions = db.get_positions()
    if not positions:
        return

    stock_market_open = is_us_market_open_aest()

    for sym, pos in positions.items():
        try:
            # v2.8: skip positions in pending_close — the resolver handles them
            if (pos.get("status") or "").lower() == "pending_close":
                continue

            bucket = (pos.get("bucket") or "").strip()

            entry = float(pos.get("entry_price") or 0)
            if entry <= 0:
                continue

            pnl_pct = float(pos.get("pnl_pct") or 0)

            # v2.9.3: read stored peak as None-aware. We need to know
            # whether peak has ever been recorded vs whether it's zero.
            raw_peak = pos.get("peak_pnl_pct")
            if raw_peak is None:
                stored_peak = None
            else:
                try:
                    stored_peak = float(raw_peak)
                except (TypeError, ValueError):
                    stored_peak = None

            # Compute the peak we should use for this iteration's exit logic.
            # If nothing's stored, current pnl_pct is the floor.
            effective_peak = stored_peak if stored_peak is not None else pnl_pct

            # v2.9.3: ALWAYS update peak in DB if current pnl is higher,
            # OR if stored is null (first time we've seen this position).
            # This is the actual fix — was previously gated by an
            # if-greater-than check that never triggered when stored was null.
            should_write_peak = (
                stored_peak is None or pnl_pct > stored_peak
            )
            if should_write_peak:
                new_peak_value = max(effective_peak, pnl_pct)
                try:
                    db._patch("positions",
                              {"peak_pnl_pct": new_peak_value},
                              "symbol", sym)
                    # Reflect the write back into our local variable so
                    # the exit-decision logic below sees it.
                    effective_peak = new_peak_value
                    log.debug(f"peak updated: {sym} → {new_peak_value*100:+.2f}%")
                except Exception as e:
                    log.warning(f"peak write {sym}: {e}")

            # v2.9.3: stock market closed → we updated the peak above,
            # but don't try to fire an actual sell (Alpaca will reject
            # market orders outside market hours anyway).
            if bucket == strategy.Bucket.SWING_STOCK and not stock_market_open:
                continue

            age_days = _position_age_days(pos)

            if bucket == strategy.Bucket.SWING_CRYPTO:
                d = strategy.decide_exit_swing_crypto(
                    pnl_pct=pnl_pct, peak_pnl_pct=effective_peak, age_days=age_days,
                )
            elif bucket == strategy.Bucket.MOMENTUM_CRYPTO:
                d = strategy.decide_exit_momentum(pnl_pct=pnl_pct, age_days=age_days)
            elif bucket == strategy.Bucket.SWING_STOCK:
                d = strategy.decide_exit_swing_stock(
                    pnl_pct=pnl_pct, peak_pnl_pct=effective_peak, age_days=age_days,
                )
            else:
                continue

            # Defensive: strategy may also return new_peak_pnl_pct higher
            # than we just wrote (shouldn't happen since we computed it,
            # but keep the existing logic intact for safety).
            if hasattr(d, "new_peak_pnl_pct") and d.new_peak_pnl_pct > effective_peak:
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
                    # v2.9.4: heal-path messages start with "healed orphan"
                    # so we can tell the user this was reconciliation, not
                    # a normal sell.
                    if msg.startswith("healed orphan"):
                        tg.send(f"🔧 RECONCILED {sym}: original sell never propagated. {msg}")
                    else:
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


# ── Scan + decide cycle ──────────────────────────────────────────────────

def run_buy_cycle(
    *, mode: str, db, alpaca, coinspot, tg: TelegramNotifier,
):
    """
    v2.6: writes claude_decisions rows with executed=False FIRST, captures
    the row id, then patches executed=True only after execute_buy returns
    success.
    v2.8: filters stock candidates against open Alpaca orders before
    running them through the brain, preventing wash-trade rejections.
    v2.8.2: writes Claude API token usage to token_usage table.
    """
    log.info(f"buy cycle: {mode}")
    try:
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

        # v2.8: filter out symbols with open Alpaca orders (prevents wash-trade
        # rejections). Only stock candidates can hit this — crypto is unaffected.
        if _RECONCILIATION_AVAILABLE and scanner_exclusions is not None:
            stock_candidates = [c for c in candidates if c.get("bucket") == strategy.Bucket.SWING_STOCK]
            non_stock_candidates = [c for c in candidates if c.get("bucket") != strategy.Bucket.SWING_STOCK]
            stock_candidates = scanner_exclusions.filter_blocked_symbols(stock_candidates, log_obj=log)
            candidates = stock_candidates + non_stock_candidates

        # Always log the scan event, even if no candidates — so the daily
        # summary can say "8 AM scan ran, 0 candidates found" instead of
        # silently producing nothing.
        try:
            db._post("claude_decisions", {
                "symbol": "_scan",
                "bucket": mode,
                "action": "scan_summary",
                "confidence": 0,
                "reason": f"scan complete: {len(candidates)} candidates qualified",
                "executed": False,
            })
        except Exception:
            pass

        if not candidates:
            log.info(f"buy cycle {mode}: no candidates")
            return

        positions = db.get_positions()
        slot_state = compute_slot_state(positions)
        cash = compute_cash_aud(positions)
        peak = float(db.get_flag("portfolio_peak") or strategy.STARTING_CAPITAL_AUD)
        portfolio = db.get_portfolio_value()
        total = float(portfolio.get("total_aud", strategy.STARTING_CAPITAL_AUD))
        consec = int(db.get_flag("consec_losses") or 0)
        kill = (db.get_flag("kill_switch") or "").lower() in ("on", "1", "true")
        buys_today = int(db.get_flag(f"buys_today_{utc_now().strftime('%Y%m%d')}") or 0)

        verdict = safety.check_can_buy(
            current_total_aud=total, peak_total_aud=peak,
            buys_today=buys_today, consecutive_losses=consec,
            manual_kill=kill,
        )
        if not verdict.allowed:
            log.info(f"buy cycle {mode}: blocked — {verdict.reason}")
            return

        spent_str = db.get_flag(f"claude_spend_{utc_now().strftime('%Y%m%d')}") or "0"
        try:
            spent = float(spent_str)
        except ValueError:
            spent = 0.0

        client = get_anthropic_client()
        result = brain.decide_buys(
            candidates=candidates,
            positions=positions,
            slot_state=slot_state,
            cash_aud=cash,
            anthropic_client=client,
            daily_spent_usd=spent,
        )

        new_spent = spent + result.estimated_cost_usd
        db.set_flag(f"claude_spend_{utc_now().strftime('%Y%m%d')}", f"{new_spent:.4f}")

        # v2.8.2: write token usage to token_usage table for the Cost tab.
        # Only record if we actually called Claude (non-zero tokens).
        if result.used_input_tokens > 0 or result.used_output_tokens > 0:
            try:
                db.record_token_usage(
                    input_tokens=result.used_input_tokens,
                    output_tokens=result.used_output_tokens,
                    cost_usd=result.estimated_cost_usd,
                )
            except Exception as e:
                log.debug(f"record_token_usage (buy cycle) failed: {e}")

        if result.error:
            tg.send(f"⚠️ Brain error: {result.error}")
            return
        if not result.decisions:
            log.info(f"buy cycle {mode}: Claude returned no decisions ({result.summary})")
            return

        allowed, rejected = brain.filter_decisions_by_safety(
            result.decisions, cash_aud=cash, slot_state=slot_state,
        )
        for d, reason in rejected:
            log.info(f"safety filter rejected {d.symbol}: {reason}")

        # ── v2.6: write decision rows FIRST with executed=False, capture
        # ids so we can update them after execute_buy returns. ─────────
        allowed_syms = {d.symbol for d in allowed}
        rejected_syms_with_reason = {d.symbol: r for d, r in rejected}

        decision_row_ids = {}  # symbol -> claude_decisions.id

        for d in result.decisions:
            try:
                if d.action == "buy" and d.symbol in allowed_syms:
                    final_action = "buy"
                elif d.action == "buy" and d.symbol in rejected_syms_with_reason:
                    final_action = "rejected_by_safety"
                else:
                    final_action = "skip"

                # Build reason string. For safety-rejected, append why.
                reason_text = (d.reason or "")[:300]
                if final_action == "rejected_by_safety":
                    rej_reason = rejected_syms_with_reason.get(d.symbol, "")
                    reason_text = f"{reason_text} | SAFETY: {rej_reason}"[:300]

                row = db._post("claude_decisions", {
                    "symbol": d.symbol,
                    "bucket": d.bucket,
                    "action": final_action,
                    "confidence": d.confidence,
                    "reason": reason_text,
                    "executed": False,    # v2.6: always start False, patch after exec
                })
                if row and final_action == "buy":
                    decision_row_ids[d.symbol] = row.get("id")
            except Exception as e:
                log.debug(f"claude_decisions log {d.symbol}: {e}")

        # ── Now execute buys and update rows based on actual outcome ─────
        for d in allowed:
            if d.action != "buy":
                continue
            ok, msg = execute_buy(
                symbol=d.symbol, bucket=d.bucket,
                db=db, alpaca=alpaca, coinspot=coinspot,
            )

            row_id = decision_row_ids.get(d.symbol)
            if ok:
                # Patch row to executed=True
                if row_id:
                    try:
                        db._patch("claude_decisions",
                                  {"executed": True},
                                  "id", str(row_id))
                    except Exception as e:
                        log.debug(f"failed to mark {d.symbol} executed=True: {e}")
                key = f"buys_today_{utc_now().strftime('%Y%m%d')}"
                cur = int(db.get_flag(key) or 0)
                db.set_flag(key, str(cur + 1))
                tg.send(f"📥 BUY {d.symbol} ({d.bucket}): conf {d.confidence:.0%}\n{d.reason}")
            else:
                # Patch row reason to include execution failure detail.
                # executed stays False (its initial value).
                if row_id:
                    try:
                        full_reason = f"{(d.reason or '')[:200]} | EXECUTION FAILED: {msg}"[:300]
                        db._patch("claude_decisions",
                                  {"reason": full_reason,
                                   "action": "execution_failed"},
                                  "id", str(row_id))
                    except Exception as e:
                        log.debug(f"failed to mark {d.symbol} execution_failed: {e}")
                tg.send(f"⚠️ BUY {d.symbol} blocked: {msg}")

    except Exception as e:
        log.error(f"buy cycle {mode} crashed: {e}")
        log.debug(traceback.format_exc())
        tg.send(f"⚠️ buy cycle error ({mode}): {e}")


# ── Daily summary push ───────────────────────────────────────────────────

def run_daily_summary(db, tg: TelegramNotifier):
    """v2.5+: delegates to bot.rich_summary for a comprehensive report."""
    from bot.rich_summary import run_rich_daily_summary
    run_rich_daily_summary(db, tg, log)


# ── Manual orders ────────────────────────────────────────────────────────

def run_manual_orders(db, alpaca, coinspot, tg: TelegramNotifier):
    try:
        orders = db._get("manual_orders", {"status": "eq.pending",
                                            "order": "requested_at.asc",
                                            "limit": "10"})
    except Exception as e:
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


# ── Q&A ──────────────────────────────────────────────────────────────────

# Sonnet 4.6 pricing per Anthropic's docs at the time of writing:
#   $3 per 1M input tokens
#   $15 per 1M output tokens
def _qa_estimate_cost_usd(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens * 3.0 + output_tokens * 15.0) / 1_000_000


QA_MODEL = "claude-sonnet-4-6"
QA_MAX_TOKENS = 600
QA_POLL_LIMIT = 3

QA_SYSTEM_PROMPT = """You are RivX, a paper-trading bot answering questions from your owner.

You trade three buckets with $10K total starting capital:
- Swing crypto ($4000 budget, 5 slots, $800/buy): buy on 5-15% pullbacks from 7d high in top 30 by market cap, above 50d MA
- Momentum crypto ($2000, 4 slots, $500/buy): buy when something breaks its 7d high TODAY with 2x average volume, rank 30-200
- Swing stocks ($3500, 3 slots, $1167/buy): 3-8% pullbacks above 50d MA, quality list (NVDA AAPL MSFT META GOOGL AMZN AMD AVGO TSM TSLA NFLX ADBE CRM SPY QQQ IWM)
- $500 always-cash ops floor

Auto-exits per bucket:
- Swing crypto: -8% stop / +15% target (take half) / 5% trail / 30d review
- Momentum: -10% stop / +30% target (full exit) / 7d hard exit
- Swing stocks: -5% stop / +12% target (take half) / 4% trail / 30d review

Schedule (v2.9):
- Swing crypto scans: 8 AM + 8 PM AEST (twice daily)
- Momentum crypto scans: every 2 hours, 24/7 (12 scans/day) — catches breakouts during US trading hours
- Stock scans: 11 PM + 3 AM AEST (US weekdays)
- Snapshots every 5 min, heartbeat every 30 sec
- Daily buy cap: 10 buys per UTC day

When answering:
- Be direct, conversational, no fluff
- Reference actual current data when relevant
- If you don't know something, say so
- Use markdown sparingly for clarity (bold for emphasis, lists when actually a list)
- Keep answers under 250 words unless the question demands detail
- If asked why no trades fired, the most common reason is "0 candidates met the entry rules" — patience is a feature, not a bug
"""


def process_pending_questions(db):
    try:
        pending = db._get("user_questions",
                          {"status": "eq.pending",
                           "order": "asked_at.asc",
                           "limit": str(QA_POLL_LIMIT)})
    except Exception as e:
        if int(time.time()) % 60 == 0:
            log.debug(f"Q&A poll: {e}")
        return

    if not pending:
        return

    client = get_anthropic_client()
    if client is None:
        log.warning("Q&A: anthropic client unavailable — skipping")
        return

    try:
        positions = db.get_positions() or {}
        portfolio = db.get_portfolio_value() or {}
        recent = db.get_recent_trades(limit=15) or []
    except Exception as e:
        log.error(f"Q&A context build: {e}")
        return

    context_msg = _build_qa_context(positions, portfolio, recent, db)

    for q in pending:
        qid = q.get("id")
        question_text = (q.get("question") or "").strip()
        if not question_text:
            db._patch("user_questions",
                      {"status": "error", "answer": "(empty question)"},
                      "id", str(qid))
            continue

        log.info(f"Q&A: answering q{qid}: {question_text[:60]!r}")
        try:
            # v2.8.2: returns (answer, input_tokens, output_tokens)
            answer, in_tok, out_tok = _call_claude_for_qa(client, context_msg, question_text)
        except Exception as e:
            log.error(f"Q&A Claude call failed for q{qid}: {e}")
            db._patch("user_questions",
                      {"status": "error",
                       "answer": f"Sorry — Claude call failed: {e}"},
                      "id", str(qid))
            continue

        # v2.8.2: write Q&A token usage to token_usage table
        if in_tok > 0 or out_tok > 0:
            try:
                cost = _qa_estimate_cost_usd(in_tok, out_tok)
                db.record_token_usage(
                    input_tokens=in_tok,
                    output_tokens=out_tok,
                    cost_usd=cost,
                )
            except Exception as e:
                log.debug(f"record_token_usage (Q&A) failed: {e}")

        ok = db._patch("user_questions",
                       {"status": "complete",
                        "answer": answer,
                        "answered_at": safety.now_utc_iso()},
                       "id", str(qid))
        if ok:
            log.info(f"Q&A: q{qid} answered ({len(answer)} chars)")
        else:
            log.warning(f"Q&A: PATCH user_questions failed for q{qid}")


def _build_qa_context(positions: dict, portfolio: dict, recent: list, db) -> str:
    parts = []

    total = float(portfolio.get("total_aud") or 0)
    cash = float(portfolio.get("cash_aud") or 0)
    deployed = float(portfolio.get("deployed_aud") or 0)
    total_pnl = float(portfolio.get("total_pnl") or 0)
    parts.append(
        f"PORTFOLIO: ${total:,.2f} AUD total · ${cash:,.0f} cash · "
        f"${deployed:,.0f} deployed · P&L {total_pnl:+,.2f}"
    )

    if positions:
        parts.append(f"\nOPEN POSITIONS ({len(positions)}):")
        for sym, p in positions.items():
            bucket = p.get("bucket") or "(legacy)"
            aud = float(p.get("aud_amount") or 0)
            pnl_pct = float(p.get("pnl_pct") or 0) * 100
            market = p.get("market") or "?"
            created = (p.get("created_at") or "")[:10]
            parts.append(
                f"  - {sym} [{bucket}] ${aud:.0f} on {market} · "
                f"{pnl_pct:+.2f}% P&L · opened {created}"
            )
    else:
        parts.append("\nOPEN POSITIONS: none — entirely in cash")

    if recent:
        n = min(len(recent), 10)
        parts.append(f"\nRECENT TRADES (last {n}):")
        for t in recent[:10]:
            sym = t.get("symbol") or "?"
            action = (t.get("action") or "?").upper()
            aud = float(t.get("aud_amount") or 0)
            pnl = t.get("pnl_pct")
            pnl_str = f" P&L {float(pnl)*100:+.1f}%" if pnl is not None else ""
            details = (t.get("details") or "")[:90]
            ts = (t.get("created_at") or "")[:16]
            parts.append(f"  - {ts} {action} {sym} ${aud:.0f}{pnl_str} · {details}")
    else:
        parts.append("\nRECENT TRADES: none yet")

    try:
        decisions = db._get("claude_decisions",
                            {"order": "decided_at.desc", "limit": "5"}) or []
        if decisions:
            parts.append("\nLAST 5 CLAUDE DECISIONS:")
            for d in decisions:
                sym = d.get("symbol") or "?"
                action = (d.get("action") or "?").upper()
                conf = d.get("confidence")
                conf_str = f" ({float(conf)*100:.0f}%)" if conf is not None else ""
                reason = (d.get("reason") or "")[:120]
                ts = (d.get("decided_at") or "")[:16]
                parts.append(f"  - {ts} {action} {sym}{conf_str}: {reason}")
    except Exception:
        pass

    aest = timezone(timedelta(hours=10))
    now_aest = datetime.now(aest)
    parts.append(f"\nCURRENT TIME: {now_aest.strftime('%A %Y-%m-%d %H:%M')} AEST")

    return "\n".join(parts)


def _call_claude_for_qa(client, context: str, question: str) -> tuple[str, int, int]:
    """
    v2.8.2: returns (answer_text, input_tokens, output_tokens) so the caller
    can write to token_usage.
    """
    user_msg = f"{context}\n\n---\n\nQUESTION FROM USER: {question}"

    resp = client.messages.create(
        model=QA_MODEL,
        max_tokens=QA_MAX_TOKENS,
        system=QA_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )

    answer = "(no answer generated)"
    if resp.content and len(resp.content) > 0:
        first = resp.content[0]
        if hasattr(first, "text"):
            answer = first.text.strip()

    in_tok = getattr(getattr(resp, "usage", None), "input_tokens", 0) or 0
    out_tok = getattr(getattr(resp, "usage", None), "output_tokens", 0) or 0
    return answer, in_tok, out_tok


# ── Main loop ────────────────────────────────────────────────────────────

def main():
    try:
        log.info(f"RivX v2.9.4 starting — {'PAPER' if PAPER_MODE else 'LIVE'} mode")
        log.info(f"Strategy: $4K swing crypto / $2K momentum crypto / $3.5K stocks / $500 ops floor")
        log.info(f"Schedule: swing crypto 8 AM + 8 PM AEST | momentum crypto every 2 hrs 24/7 | stocks 11 PM + 3 AM AEST (weekdays)")
        log.info("ASX analyser: removed in v2.8.1")
        log.info("Token usage tracking: enabled (v2.8.2)")
        log.info("Cadence v2.9.0: momentum every 2 hrs (12/day), swing crypto twice daily, daily buy cap 10")
        log.info("v2.9.3: trailing stop peak tracking fix — peak_pnl_pct now writes correctly")
        log.info("v2.9.4: orphan stock close auto-heal — recovers from sells that filled on Alpaca but didn't write to Supabase")
        log.info(f"Reconciliation: {'enabled (read-only)' if _RECONCILIATION_AVAILABLE else 'DISABLED (import failed)'}")
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

        today = aest_now().date().isoformat()
        if db.get_flag("last_startup") != today:
            db.set_flag("last_startup", today)
            rec_status = "Reconciler online" if _RECONCILIATION_AVAILABLE else "Reconciler DISABLED"
            tg.send(f"🟢 RivX v2.9.4 online. {'PAPER' if PAPER_MODE else 'LIVE'} mode. "
                    f"{rec_status}. Orphan stock heal active.")

        log.info("setup complete — entering main loop")
        sys.stdout.flush()
    except Exception as e:
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
    # v2.9.0: swing crypto now has multiple slots, track per-time like momentum/stock
    last_swing_crypto_runs = {t: db.get_flag(f"last_swing_crypto_{t}") for t in SWING_CRYPTO_TIMES_AEST}
    last_momentum_runs = {t: db.get_flag(f"last_momentum_{t}") for t in MOMENTUM_TIMES_AEST}
    last_stock_runs = {t: db.get_flag(f"last_stock_{t}") for t in SWING_STOCK_TIMES_AEST}
    last_summary_runs = {t: db.get_flag(f"last_summary_{t}") for t in DAILY_SUMMARY_TIMES_AEST}

    while True:
        try:
            now_ts = time.time()

            write_heartbeat(db)

            try:
                tg.check_kill_switch(db)
            except Exception as e:
                log.debug(f"telegram poll: {e}")
            run_manual_orders(db, alpaca, coinspot, tg)
            process_pending_questions(db)

            # v2.8.1: ASX tick removed

            # v2.8: reconciliation tick (read-only mode for now). Cheap when
            # nothing's due — only does real work every 10 minutes.
            if _RECONCILIATION_AVAILABLE and reconciler is not None:
                try:
                    reconciler.tick(db, alpaca, tg, log)
                except Exception as e:
                    log.warning(f"reconciler tick error (non-fatal): {e}")

            if now_ts - last_snapshot >= SNAPSHOT_INTERVAL_SEC:
                run_snapshot(db, alpaca)
                # v2.8: resolve any pending stock closes (Alpaca confirmations)
                if _RECONCILIATION_AVAILABLE and pending_sells is not None:
                    try:
                        pending_sells.resolve_pending_closes(db, alpaca, log)
                    except Exception as e:
                        log.warning(f"resolve_pending_closes error (non-fatal): {e}")
                manage_open_positions(db, alpaca, coinspot, tg)
                last_snapshot = now_ts

            for t in DAILY_SUMMARY_TIMES_AEST:
                if at_or_past_time_today(t, last_summary_runs.get(t)):
                    run_daily_summary(db, tg)
                    last_summary_runs[t] = safety.now_utc_iso()
                    db.set_flag(f"last_summary_{t}", last_summary_runs[t])

            kill = (db.get_flag("kill_switch") or "").lower() in ("on", "1", "true")
            if not kill:
                # v2.9.0: swing crypto now twice daily (8 AM + 8 PM AEST)
                for t in SWING_CRYPTO_TIMES_AEST:
                    if at_or_past_time_today(t, last_swing_crypto_runs.get(t)):
                        run_buy_cycle(mode=strategy.Bucket.SWING_CRYPTO,
                                      db=db, alpaca=alpaca, coinspot=coinspot, tg=tg)
                        last_swing_crypto_runs[t] = safety.now_utc_iso()
                        db.set_flag(f"last_swing_crypto_{t}", last_swing_crypto_runs[t])

                # v2.9.0: momentum crypto every 2 hours, 24/7
                for t in MOMENTUM_TIMES_AEST:
                    if at_or_past_time_today(t, last_momentum_runs.get(t)):
                        run_buy_cycle(mode=strategy.Bucket.MOMENTUM_CRYPTO,
                                      db=db, alpaca=alpaca, coinspot=coinspot, tg=tg)
                        last_momentum_runs[t] = safety.now_utc_iso()
                        db.set_flag(f"last_momentum_{t}", last_momentum_runs[t])

                if is_us_trading_weekday_aest():
                    for t in SWING_STOCK_TIMES_AEST:
                        if at_or_past_time_today(t, last_stock_runs.get(t)):
                            run_buy_cycle(mode=strategy.Bucket.SWING_STOCK,
                                          db=db, alpaca=alpaca, coinspot=coinspot, tg=tg)
                            last_stock_runs[t] = safety.now_utc_iso()
                            db.set_flag(f"last_stock_{t}", last_stock_runs[t])

            time.sleep(MAIN_TICK_SECONDS)

        except KeyboardInterrupt:
            log.info("shutdown signal received")
            tg.send("🛑 RivX shutting down (manual)")
            break
        except Exception as e:
            log.error(f"main loop iteration error: {e}")
            log.debug(traceback.format_exc())
            time.sleep(60)


if __name__ == "__main__":
    main()
