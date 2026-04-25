"""
RivX AutoTrader — main scheduler.

Cost-optimised loop intervals:
  - Evening briefing     — 8pm AEST, once/day (Sonnet ~$0.05)
  - Crypto check         — every 15 min, 24/7 (Haiku ~$0.003, often skipped)
  - Intraday stock check — every 5 min during US hours (Haiku ~$0.003, often skipped)
  - Snapshot             — every 5 min (pure Python, no cost)
  - Question polling     — every 60 sec (Sonnet ~$0.012/question, only when asked)

Target: ~$25 USD/month total Claude spend with hard $2/day cap.
"""

import time
import logging
import json
from datetime import datetime, timezone, timedelta

from bot.config import (
    PORTFOLIO, PAPER_MODE,
    EVENING_BRIEFING_HOUR_AEST, MORNING_SUMMARY_HOUR_AEST,
    APPROVAL_TIMEOUT_SECONDS, MIN_CONFIDENCE_TO_TRADE,
    ANTHROPIC_API_KEY,
)
from bot.brain import (
    evening_briefing, intraday_check, crypto_check, get_market_data,
    MODEL_QA,
)
from bot.alpaca_trader  import AlpacaTrader, get_aud_usd_rate
from bot.coinspot_trader import CoinSpotTrader
from bot.supabase_logger import SupabaseLogger
from bot.telegram_notify import TelegramNotifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

AEST = timezone(timedelta(hours=10))

# Loop intervals (seconds)
CRYPTO_LOOP_INTERVAL   = 15 * 60   # 15 min (was 5)
INTRADAY_LOOP_INTERVAL = 5 * 60    # 5 min (was 2)
SNAPSHOT_INTERVAL      = 5 * 60    # 5 min (pure Python, cheap)
QUESTION_POLL_INTERVAL = 60        # 1 min (only spends tokens if question asked)
MAIN_TICK              = 15        # how often the main loop wakes to check schedules


# ─── Time helpers ──────────────────────────────────────────────────────────

def aest_now() -> datetime:
    return datetime.now(AEST)


def is_us_market_hours() -> bool:
    """True during US market hours expressed in AEST (11:30pm–6am weekdays)."""
    now = aest_now()
    if now.weekday() >= 5:  # Sat/Sun
        return False
    hour, minute = now.hour, now.minute
    return (hour == 23 and minute >= 30) or (hour < 6)


# ─── Trade execution ───────────────────────────────────────────────────────

def _fetch_alpaca_fill_price(trader, symbol: str, retries: int = 5) -> float:
    """
    After placing a market order with Alpaca, poll /v2/positions/{symbol}
    to get the actual fill price. Returns 0.0 if it can't be resolved.
    """
    for attempt in range(retries):
        time.sleep(1.5)
        try:
            pos = trader.get_position(symbol)
            if pos:
                price = float(pos.get("avg_entry_price", 0) or 0)
                if price > 0:
                    return price
        except Exception as e:
            log.debug(f"Alpaca fill lookup attempt {attempt+1} for {symbol}: {e}")
    log.warning(f"Could not resolve Alpaca fill price for {symbol} after {retries} tries")
    return 0.0


def execute_action(symbol, action, reason, alpaca, coinspot, db, tg,
                   positions, market_data, confidence=1.0, notify=True,
                   aud_amount=None):
    """Execute a single trade. Defensive: never calls .buy/.sell on None."""
    crypto_coins = ["BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC",
                    "BCH","DOT","UNI","AAVE","MATIC","ATOM","ALGO","NEAR",
                    "FTM","SAND","MANA","CRV","GRT","SUSHI","MKR","SNX",
                    "PEPE","SHIB","FLOKI","WIF","BONK","FET","RNDR","TAO"]

    if symbol in PORTFOLIO:
        config = PORTFOLIO[symbol]
        market = config.get("market", "coinspot")
        default_amount = config.get("allocated_aud", 400)
    else:
        market = "coinspot" if symbol in crypto_coins else "alpaca"
        default_amount = 400

    allocated = aud_amount if aud_amount else default_amount

    # Resolve trader object safely
    trader = alpaca if market == "alpaca" else coinspot
    if trader is None:
        if market == "alpaca" and coinspot is not None:
            log.info(f"Rerouting {symbol} from alpaca to coinspot (loop scope)")
            market = "coinspot"
            trader = coinspot
        else:
            log.warning(f"Skip {symbol} — no {market} trader in scope")
            return False

    if action == "BUY" and symbol not in positions:
        log.info(f"Executing BUY {symbol} on {market} ({allocated} AUD) — {reason}")
        try:
            order = trader.buy(symbol, allocated)
        except Exception as e:
            log.error(f"BUY call failed for {symbol}: {e}")
            return False
        if order:
            # Resolve entry price. For Alpaca we need to wait for the fill
            # and pull the actual avg_entry_price — market_data is crypto-only.
            price = market_data.get(symbol, {}).get("price", 0) or 0
            if market == "alpaca":
                fill_price = _fetch_alpaca_fill_price(trader, symbol)
                if fill_price > 0:
                    price = fill_price
                    log.info(f"Alpaca fill: {symbol} @ ${price:.4f} USD")

            db.log_trade(symbol, "BUY", allocated, order, confidence, reason)
            db.save_position(symbol, price, allocated, market)
            if notify:
                tg.send(f"{'[PAPER] ' if PAPER_MODE else ''}Bought {symbol} — {reason}")
            return True

    elif action == "SELL" and symbol in positions:
        log.info(f"Executing SELL {symbol} on {market} — {reason}")
        try:
            order = trader.sell(symbol)
        except Exception as e:
            log.error(f"SELL call failed for {symbol}: {e}")
            return False
        if order:
            pos = positions[symbol]
            price = market_data.get(symbol, {}).get("price", 0) or 0
            # For Alpaca, prefer the last known current_price from the position
            # (stored by the snapshot sync) over market_data which is crypto-only.
            if market == "alpaca":
                stored_current = float(pos.get("current_price", 0) or 0)
                if stored_current > 0:
                    price = stored_current
            pnl = pos.get("pnl_pct", 0)
            db.log_trade(symbol, "SELL", pos.get("aud_amount", 0), order, confidence, reason)
            db.close_position(symbol, price, pnl)
            if notify:
                tg.send(f"{'[PAPER] ' if PAPER_MODE else ''}Sold {symbol} — {reason} — P&L {pnl:+.1%}")
            return True

    return False


# ─── Loops ─────────────────────────────────────────────────────────────────

def run_evening_briefing(db, tg, alpaca, coinspot):
    """Runs ONCE per day, gated by flag + minute < 3."""
    log.info("Evening briefing starting")
    tg.send("RivX is analysing the market for tonight. Give me a moment...")

    positions = db.get_positions()
    trade_history = db.get_recent_trades(30)
    weights = db.get_signal_weights()

    analysis = evening_briefing(db, positions, trade_history, weights)
    if not analysis or not analysis.get("decisions"):
        tg.send("Evening briefing: no strong setups tonight. Holding existing positions.")
        db.save_approved_plan({})
        return

    decisions = analysis.get("decisions", {})
    lines = [
        f"RivX evening briefing — {aest_now().strftime('%d %b %Y')}",
        "",
        f"Market: {analysis.get('market_summary', 'N/A')}",
        f"Risk: {analysis.get('risk_level', 'MEDIUM')}",
        f"Portfolio: {analysis.get('portfolio_health', '')}",
        "",
        "Plan:",
    ]

    buys = sells = holds = 0
    for sym, dec in decisions.items():
        action = dec.get("action", "HOLD")
        conf = dec.get("confidence", 0)
        reasoning = dec.get("reasoning", "")
        lines.append(f"  {action} {sym} ({conf:.0%}) — {reasoning}")
        if action == "BUY": buys += 1
        elif action == "SELL": sells += 1
        else: holds += 1

    lines += [
        "",
        f"Summary: {buys} buys, {sells} sells, {holds} holds",
        f"Watch: {analysis.get('watch_for_overnight', '')}",
        "",
        "Reply YES to approve or NO to skip.",
        "Auto-approves in 1 hour if no response.",
    ]

    approved = tg.send_and_wait("\n".join(lines), timeout_seconds=APPROVAL_TIMEOUT_SECONDS)

    if not approved:
        tg.send("Tonight's plan cancelled. Bot will only manage stops overnight.")
        db.save_approved_plan({})
        return

    tg.send("Plan approved. RivX on watch overnight.")

    # Execute approved trades
    positions = db.get_positions()
    market_data = analysis.get("market_data", {})
    for sym, dec in decisions.items():
        action = dec.get("action", "HOLD")
        conf = dec.get("confidence", 0)
        if conf < MIN_CONFIDENCE_TO_TRADE or action == "HOLD":
            continue
        execute_action(
            sym, action, dec.get("reasoning", ""),
            alpaca, coinspot, db, tg,
            positions, market_data, confidence=conf,
            notify=True, aud_amount=dec.get("aud_amount"),
        )

    db.save_approved_plan(analysis)
    log.info("Evening briefing complete")


def run_intraday_loop(db, tg, alpaca, coinspot):
    """Every 5 min during US market hours."""
    positions = db.get_positions()
    approved_plan = db.get_approved_plan()

    result = intraday_check(db, positions, approved_plan)
    actions = result.get("actions", [])

    if not actions:
        log.debug("Intraday: no actions")
        return

    market_data = {}
    for act in actions:
        sym = act.get("symbol")
        action = act.get("action")
        reason = act.get("reason", "")
        if action not in ("BUY", "SELL"):
            continue
        if sym not in market_data:
            market_data.update(get_market_data([sym]))
        positions = db.get_positions()
        execute_action(sym, action, reason, alpaca, coinspot, db, tg,
                      positions, market_data, confidence=0.8, notify=True)


def run_crypto_loop(db, tg, coinspot):
    """Every 15 min, 24/7. Mechanical exits + optional Claude entries."""
    positions = db.get_positions()
    approved_plan = db.get_approved_plan()

    result = crypto_check(db, positions, approved_plan)
    actions = result.get("actions", [])
    reasoning = result.get("reasoning", "")
    opportunities = result.get("opportunities", [])

    # Log every check so the dashboard can display activity
    try:
        db._post("crypto_checks", {
            "checked_at": datetime.utcnow().isoformat(),
            "reasoning": reasoning,
            "actions": json.dumps(actions),
            "opportunities": json.dumps(opportunities[:10]) if opportunities else "[]",
        })
    except Exception as e:
        log.warning(f"crypto_checks log failed: {e}")

    if not actions:
        log.debug(f"Crypto: no actions — {reasoning}")
        return

    market_data = get_market_data(["BTC", "ETH"])
    for act in actions:
        sym = act.get("symbol")
        action = act.get("action")
        reason = act.get("reason", "")
        aud = act.get("aud_amount")
        if action not in ("BUY", "SELL"):
            continue
        if sym not in market_data:
            market_data.update(get_market_data([sym]))
        positions = db.get_positions()
        execute_action(sym, action, reason, None, coinspot, db, tg,
                      positions, market_data, confidence=0.75,
                      notify=True, aud_amount=aud)


def _sync_alpaca_positions(db, alpaca):
    """
    Pull live data for every Alpaca-held position and push it into Supabase.
    Returns {symbol: {current_price_usd, pnl_pct, qty, market_value_usd, market_value_aud}}
    so the caller can use it for the snapshot total.
    """
    out = {}
    if not alpaca:
        return out
    try:
        alp_positions = alpaca.get_all_positions()
    except Exception as e:
        log.warning(f"Alpaca get_all_positions failed: {e}")
        return out

    if not alp_positions:
        return out

    # FX rate for AUD conversion
    try:
        aud_to_usd = get_aud_usd_rate()  # 1 AUD = X USD
    except Exception:
        aud_to_usd = 0.635
    usd_to_aud = (1.0 / aud_to_usd) if aud_to_usd else 1.57

    for ap in alp_positions:
        try:
            sym = ap.get("symbol")
            if not sym:
                continue
            current_price_usd = float(ap.get("current_price", 0) or 0)
            avg_entry_usd = float(ap.get("avg_entry_price", 0) or 0)
            qty = float(ap.get("qty", 0) or 0)
            market_value_usd = float(ap.get("market_value", 0) or 0)
            pnl_pct = float(ap.get("unrealized_plpc", 0) or 0)
            change_today = float(ap.get("change_today", 0) or 0)  # today's % change

            out[sym] = {
                "current_price_usd": current_price_usd,
                "avg_entry_usd": avg_entry_usd,
                "pnl_pct": pnl_pct,
                "qty": qty,
                "market_value_usd": market_value_usd,
                "market_value_aud": round(market_value_usd * usd_to_aud, 2),
                "change_today": change_today,
            }

            # Push to Supabase. update_position_from_alpaca also heals entry_price=0 rows.
            try:
                db.update_position_from_alpaca(
                    symbol=sym,
                    current_price=current_price_usd,
                    pnl_pct=pnl_pct,
                    qty=qty,
                    change_today=change_today,
                    avg_entry_price=avg_entry_usd,
                )
            except Exception as e:
                log.warning(f"update_position_from_alpaca({sym}) failed: {e}")
        except Exception as e:
            log.warning(f"Alpaca sync row failed: {e}")
    return out


def run_snapshot(db, alpaca=None):
    """
    Pure Python. Every 5 min.
    Writes portfolio value with live prices (CoinGecko for crypto, Alpaca for stocks).
    Also pushes live Alpaca position data back to Supabase so the dashboard sees it.
    """
    try:
        positions = db.get_positions()

        # 1) Live crypto prices via get_market_data (CoinGecko under the hood)
        crypto_symbols = [s for s, p in positions.items()
                          if (p.get("market") or "").lower() != "alpaca"]
        market_data = get_market_data(crypto_symbols) if crypto_symbols else {}

        # 2) Live Alpaca positions — also writes fresh data into Supabase
        alpaca_data = _sync_alpaca_positions(db, alpaca)

        # 3) Compute portfolio total with mark-to-market
        current_value = 0.0
        deployed_entry = 0.0
        for sym, pos in positions.items():
            entry = float(pos.get("entry_price", 0) or 0)
            amt = float(pos.get("aud_amount", 0) or 0)
            deployed_entry += amt
            market = (pos.get("market") or "").lower()

            if market == "alpaca" and sym in alpaca_data:
                # AUD-correct market value: current_price_usd × qty × USD_TO_AUD.
                # This includes FX impact, unlike just amt × (1 + USD pnl).
                aud_value = alpaca_data[sym].get("market_value_aud", 0)
                pnl = alpaca_data[sym]["pnl_pct"]
                if aud_value > 0:
                    current_value += aud_value
                else:
                    current_value += amt * (1 + pnl)
                # Update pnl_pct on the position row (USD-based — dashboard does its
                # own AUD math from current_price + qty + live FX).
                try:
                    db.update_position_pnl_direct(sym, pnl)
                except Exception:
                    pass
            else:
                # Crypto path
                price = market_data.get(sym, {}).get("price", 0) or 0
                if entry > 0 and price > 0:
                    pnl = (price - entry) / entry
                    current_value += amt * (1 + pnl)
                    try:
                        db.update_position_pnl(sym, price)
                    except Exception:
                        pass
                else:
                    current_value += amt

        cash = max(0, 5000 - deployed_entry)
        total = current_value + cash

        db._post("intraday_snapshots", {
            "recorded_at": datetime.utcnow().isoformat(),
            "total_aud": round(total, 2),
            "deployed_aud": round(current_value, 2),
            "cash_aud": round(cash, 2),
            "open_positions": len(positions),
        })
    except Exception as e:
        log.warning(f"Snapshot failed: {e}")


def run_question_poll(db, tg):
    """Every 60s. Uses Sonnet only when there's a pending question."""
    try:
        pending = db._get("user_questions",
                         {"status": "eq.pending", "order": "asked_at.asc", "limit": "3"})
    except Exception as e:
        log.debug(f"Q poll failed: {e}")
        return

    if not pending:
        return

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    for q in pending:
        qid = q.get("id")
        question = (q.get("question") or "").strip()
        if not question:
            continue

        # Build a rich context so Claude can answer accurately about both
        # what's happening NOW (open positions) and what HAS happened (closed
        # positions, recent trades, recent crypto scans). Without this Claude
        # hallucinates — e.g. claiming the bot doesn't trade crypto when it does.
        try:
            positions = db.get_positions()
            trades = db.get_recent_trades(20)
            closed = db._get("positions",
                             {"status": "eq.closed", "order": "closed_at.desc", "limit": "10"}) or []
            crypto_scans = db._get("crypto_checks",
                                   {"order": "checked_at.desc", "limit": "5"}) or []
            plan = db.get_approved_plan()
            portfolio = db.get_portfolio_value()
        except Exception:
            positions = {}; trades = []; closed = []; crypto_scans = []; plan = {}; portfolio = {}

        # Compact summaries so token usage stays reasonable
        open_summary = {
            s: {k: v for k, v in p.items()
                if k in ['entry_price','pnl_pct','aud_amount','market','current_price','qty']}
            for s, p in positions.items()
        }
        closed_summary = [{
            'symbol': p.get('symbol'),
            'market': p.get('market'),
            'aud_amount': p.get('aud_amount'),
            'pnl_pct': p.get('pnl_pct'),
            'opened': (p.get('created_at') or '')[:10],
            'closed': (p.get('closed_at') or '')[:10],
        } for p in closed]
        recent_trades_summary = [{
            'symbol': t.get('symbol'),
            'action': t.get('action'),
            'aud_amount': t.get('aud_amount'),
            'when': (t.get('created_at') or '')[:16],
            'detail': (t.get('details') or '')[:120],
        } for t in trades[:12]]
        recent_scans_summary = [{
            'when': (c.get('checked_at') or '')[:16],
            'reasoning': (c.get('reasoning') or '')[:240],
        } for c in crypto_scans]

        system = (
            "You are RivX, an autonomous paper-trading bot. You actively trade BOTH "
            "US stocks (via Alpaca) AND crypto (via CoinSpot). Crypto is scanned every "
            "15 min, 24/7. Stocks are scanned every 5 min during US market hours. "
            "Mechanical stops and targets execute automatically; the rest goes through "
            "Claude Haiku for decision-making. "
            "ANSWER ONLY FROM THE CONTEXT BELOW. Do not invent trades, strategies, or "
            "facts not present in the data. If the user asks about something that isn't "
            "in the context, say so explicitly rather than guessing. Closed positions "
            "are real history — reference them. Paper mode means no real money at risk, "
            "but the trades are otherwise real decisions. Under 200 words."
        )
        user_msg = (
            f"Q: {question}\n\n"
            f"PORTFOLIO: {json.dumps(portfolio)}\n\n"
            f"OPEN POSITIONS ({len(open_summary)}):\n{json.dumps(open_summary, indent=1)}\n\n"
            f"CLOSED POSITIONS ({len(closed_summary)} most recent):\n{json.dumps(closed_summary, indent=1)}\n\n"
            f"RECENT TRADES ({len(recent_trades_summary)} most recent):\n{json.dumps(recent_trades_summary, indent=1)}\n\n"
            f"RECENT CRYPTO SCANS ({len(recent_scans_summary)} most recent — these show what the bot is deciding every 15 min):\n{json.dumps(recent_scans_summary, indent=1)}\n\n"
            f"PLAN ACTIVE: {'yes' if plan else 'no'}"
        )

        try:
            resp = client.messages.create(
                model=MODEL_QA,
                max_tokens=500,
                system=system,
                messages=[{"role": "user", "content": user_msg}],
            )
            # Record Q&A token usage
            try:
                from bot.brain import _record_usage
                _record_usage(db, MODEL_QA, resp.usage.input_tokens, resp.usage.output_tokens)
            except Exception:
                pass
            answer = resp.content[0].text.strip()
            db._patch("user_questions",
                     {"answer": answer, "answered_at": datetime.utcnow().isoformat(),
                      "status": "answered"},
                     "id", str(qid))
        except Exception as e:
            log.error(f"Q#{qid} failed: {e}")
            db._patch("user_questions",
                     {"answer": f"Sorry — couldn't answer: {str(e)[:100]}",
                      "answered_at": datetime.utcnow().isoformat(), "status": "failed"},
                     "id", str(qid))


def run_4hr_summary(db, tg):
    """Every 4 hours — brief portfolio status to Telegram. Pure Python, no Claude."""
    try:
        positions = db.get_positions()
        trades = db.get_recent_trades(50)
        from datetime import date, timedelta
        today_str = date.today().isoformat()

        # Today's trades
        today_trades = [t for t in trades if t.get("created_at", "")[:10] == today_str]
        last_4h = datetime.utcnow() - timedelta(hours=4)
        recent_trades = [t for t in trades
                        if t.get("created_at") and
                        datetime.fromisoformat(t["created_at"].replace("Z", "+00:00")).replace(tzinfo=None) >= last_4h.replace(tzinfo=None)]

        # Portfolio value from latest snapshot
        try:
            latest = db._get("intraday_snapshots",
                            {"order": "recorded_at.desc", "limit": "1"})
            total = float(latest[0]["total_aud"]) if latest else 5000
        except Exception:
            total = 5000

        # Today's Claude cost
        try:
            usage = db._get("token_usage", {"date": f"eq.{today_str}"})
            cost = float(usage[0].get("cost_usd", 0)) if usage else 0
        except Exception:
            cost = 0

        net = total - 5000
        lines = [
            f"📊 RivX 4hr update — {aest_now().strftime('%a %d %b, %H:%M')}",
            "",
            f"Portfolio: ${total:,.2f} AUD ({'+' if net>=0 else ''}${net:.2f})",
            f"Open positions: {len(positions)}",
            f"Trades last 4hr: {len(recent_trades)}",
            f"Trades today: {len(today_trades)}",
            f"Claude cost today: ${cost:.3f} USD",
        ]

        if positions:
            lines.append("")
            lines.append("Holdings:")
            for sym, pos in list(positions.items())[:6]:
                pnl = (pos.get("pnl_pct", 0) or 0) * 100
                lines.append(f"  {sym}: {pnl:+.1f}% ({pos.get('market', '?')})")

        if recent_trades:
            lines.append("")
            lines.append("Recent trades:")
            for t in recent_trades[:5]:
                pnl = f" ({(t.get('pnl_pct', 0) or 0)*100:+.1f}%)" if t.get("pnl_pct") else ""
                lines.append(f"  {t.get('action')} {t.get('symbol')}{pnl}")

        tg.send("\n".join(lines))
    except Exception as e:
        log.warning(f"4hr summary failed: {e}")


def run_morning_summary(db, tg):
    """6:30am AEST — overnight summary."""
    positions = db.get_positions()
    portfolio = db.get_portfolio_value()
    trades = db.get_recent_trades(20)

    from datetime import date
    today_trades = [t for t in trades if t.get("created_at", "")[:10] == date.today().isoformat()]

    lines = [
        f"RivX morning report — {aest_now().strftime('%d %b')}",
        "",
        f"Portfolio: ${portfolio.get('total_aud', 5000):,.2f} AUD",
        f"Overnight: {'+' if portfolio.get('day_pnl', 0) >= 0 else ''}${portfolio.get('day_pnl', 0):.2f}",
        "",
    ]

    if today_trades:
        lines.append("Overnight trades:")
        for t in today_trades:
            pnl = f" ({t.get('pnl_pct', 0)*100:+.1f}%)" if t.get("pnl_pct") else ""
            lines.append(f"  {t.get('action')} {t.get('symbol')}{pnl}")
    else:
        lines.append("No trades overnight.")

    if positions:
        lines.append("")
        lines.append("Open positions:")
        for sym, pos in positions.items():
            pnl = pos.get("pnl_pct", 0) or 0
            lines.append(f"  {sym}: {pnl*100:+.1f}%")

    # Cost report
    try:
        today = date.today().isoformat()
        usage = db._get("token_usage", {"date": f"eq.{today}"})
        if usage:
            cost = float(usage[0].get("cost_usd", 0))
            calls = int(usage[0].get("call_count", 0))
            lines.append("")
            lines.append(f"Yesterday's Claude cost: ${cost:.2f} USD ({calls} calls)")
    except Exception:
        pass

    tg.send("\n".join(lines))
    db.save_approved_plan({})
    db.save_snapshot(portfolio.get("total_aud", 5000),
                     portfolio.get("day_pnl", 0),
                     portfolio.get("total_pnl", 0))


# ─── Main loop ─────────────────────────────────────────────────────────────

def run_daily_cleanup(db):
    """
    Trim old chatter so the dashboard and Supabase tables don't bloat.

    Keep:
      - trades (audit trail) — forever
      - positions (closed/open) — forever
      - flags — forever (state)

    Trim:
      - user_questions older than 7 days  (Q&A is conversational, not learned-from)
      - crypto_checks  older than 7 days  (just shows scanner activity)
      - intraday_snapshots older than 30 days  (chart history we don't need long-term)
      - usage_log     older than 60 days  (cost tracking)

    The bot's actual learning happens in evening_briefing via signal_weights,
    which reads from `trades` (kept forever) — so deleting Q&A history is safe.
    """
    cutoffs = {
        "user_questions":     7,
        "crypto_checks":      7,
        "intraday_snapshots": 30,
        "usage_log":          60,
    }
    deleted_total = 0
    for table, days in cutoffs.items():
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        try:
            # Supabase REST: DELETE with filter on created_at/checked_at/snapshot_time/date
            time_col = {
                "user_questions":     "asked_at",
                "crypto_checks":      "checked_at",
                "intraday_snapshots": "snapshot_time",
                "usage_log":          "date",
            }[table]
            # Use _patch_with_fallback's underlying mechanism via a raw DELETE
            url = f"{db.base}/rest/v1/{table}?{time_col}=lt.{cutoff}"
            import requests
            r = requests.delete(url, headers={**db.headers, "Prefer": "return=representation"}, timeout=15)
            if r.ok:
                rows = r.json() if r.headers.get("content-type", "").startswith("application/json") else []
                count = len(rows) if isinstance(rows, list) else 0
                deleted_total += count
                if count > 0:
                    log.info(f"Cleanup: removed {count} rows from {table} older than {days}d")
            else:
                log.warning(f"Cleanup {table}: HTTP {r.status_code} — {r.text[:120]}")
        except Exception as e:
            log.warning(f"Cleanup {table} failed: {e}")
    log.info(f"Daily cleanup complete — {deleted_total} total rows removed")


def main():
    log.info(f"RivX starting — {'PAPER' if PAPER_MODE else 'LIVE'} mode")

    db = SupabaseLogger()
    tg = TelegramNotifier()
    alpaca = AlpacaTrader()
    coinspot = CoinSpotTrader()

    # One-time startup announcement per day
    today = aest_now().date().isoformat()
    if db.get_flag("last_startup") != today:
        db.set_flag("last_startup", today)
        tg.send(f"RivX is online. {'PAPER' if PAPER_MODE else 'LIVE'} trading mode.")

    # Timers
    last_crypto = 0
    last_intraday = 0
    last_snapshot = 0
    last_question = 0
    last_4hr_summary = time.time()  # don't fire immediately at startup

    while True:
        try:
            # Kill switch — persistent Supabase flag + fresh Telegram messages
            if tg.check_kill_switch(db):
                # No spam — telegram_notify handles single-fire announcement
                time.sleep(60)
                continue

            now = aest_now()
            today = now.date().isoformat()

            # Evening briefing — Mon-Fri ONLY, only minute 0-2 of hour 20, flag-locked
            is_weekday = now.weekday() < 5  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
            if (is_weekday
                and now.hour == EVENING_BRIEFING_HOUR_AEST
                and now.minute < 3
                and db.get_flag("last_evening_briefing") != today):

                # Set flag FIRST (before any API call)
                db.set_flag("last_evening_briefing", today)
                time.sleep(1)
                if db.get_flag("last_evening_briefing") == today:
                    log.info(f"Briefing flag set for {today} — running")
                    run_evening_briefing(db, tg, alpaca, coinspot)
                else:
                    log.error("Flag did not persist — check flags table permissions")
                    tg.send("RivX: flag error — skipping briefing. Check Supabase.")
                    time.sleep(180)  # back off to avoid retry storm

            # Morning summary — same gating pattern
            if (now.hour == MORNING_SUMMARY_HOUR_AEST and now.minute >= 30
                and db.get_flag("last_morning_summary") != today):
                db.set_flag("last_morning_summary", today)
                time.sleep(1)
                if db.get_flag("last_morning_summary") == today:
                    run_morning_summary(db, tg)

            # Daily cleanup — once per day at 3am AEST (low activity)
            if (now.hour == 3 and now.minute < 5
                and db.get_flag("last_cleanup") != today):
                db.set_flag("last_cleanup", today)
                time.sleep(1)
                if db.get_flag("last_cleanup") == today:
                    run_daily_cleanup(db)

            now_ts = time.time()

            # Crypto check — every 15 min
            if (now_ts - last_crypto) >= CRYPTO_LOOP_INTERVAL:
                last_crypto = now_ts
                run_crypto_loop(db, tg, coinspot)

            # Intraday stock check — every 5 min during US hours
            if is_us_market_hours() and (now_ts - last_intraday) >= INTRADAY_LOOP_INTERVAL:
                last_intraday = now_ts
                run_intraday_loop(db, tg, alpaca, coinspot)

            # Snapshot — every 5 min (pure Python). Now includes Alpaca live sync.
            if (now_ts - last_snapshot) >= SNAPSHOT_INTERVAL:
                last_snapshot = now_ts
                run_snapshot(db, alpaca=alpaca)

            # Question poll — every 60s (only calls Claude if pending questions)
            if (now_ts - last_question) >= QUESTION_POLL_INTERVAL:
                last_question = now_ts
                run_question_poll(db, tg)

            # 4-hourly Telegram summary — pure Python, no API cost
            if (now_ts - last_4hr_summary) >= (4 * 60 * 60):
                last_4hr_summary = now_ts
                run_4hr_summary(db, tg)

            time.sleep(MAIN_TICK)

        except KeyboardInterrupt:
            log.info("Stopped by user")
            tg.send("RivX stopped manually.")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
            # Don't spam Telegram on every error — only severe ones
            time.sleep(60)


if __name__ == "__main__":
    main()
