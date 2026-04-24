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
from bot.alpaca_trader  import AlpacaTrader
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
            price = market_data.get(symbol, {}).get("price", 0)
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
            price = market_data.get(symbol, {}).get("price", 0)
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


def run_snapshot(db):
    """Pure Python. Writes portfolio value with live prices every 5 min."""
    try:
        positions = db.get_positions()
        symbols = list(positions.keys())
        market_data = get_market_data(symbols) if symbols else {}

        current_value = 0.0
        deployed_entry = 0.0
        for sym, pos in positions.items():
            entry = pos.get("entry_price", 0) or 0
            amt = pos.get("aud_amount", 0) or 0
            deployed_entry += amt
            price = market_data.get(sym, {}).get("price", 0) or 0
            if entry > 0 and price > 0:
                current_value += amt * (price / entry)
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

        try:
            positions = db.get_positions()
            trades = db.get_recent_trades(10)
            plan = db.get_approved_plan()
            portfolio = db.get_portfolio_value()
        except Exception:
            positions = {}; trades = []; plan = {}; portfolio = {}

        system = ("You are RivX, the user's autonomous trading bot. Answer their dashboard "
                  "question concisely using the context. Paper mode — no real money at risk. "
                  "Under 200 words.")
        user_msg = (f"Q: {question}\n\n"
                   f"Portfolio: {json.dumps(portfolio)}\n"
                   f"Positions: {json.dumps({s: {k:v for k,v in p.items() if k in ['entry_price','pnl_pct','aud_amount','market']} for s,p in positions.items()})}\n"
                   f"Recent trades: {len(trades)}\n"
                   f"Plan active: {'yes' if plan else 'no'}")

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
    kill_switch_announced = False  # prevents repeat "kill switch activated" messages

    while True:
        try:
            # Kill switch — send ONE message, then halt silently
            if tg.check_kill_switch():
                if not kill_switch_announced:
                    log.warning("Kill switch activated — bot halting")
                    tg.send("🛑 Kill switch activated. Bot halted. No more trades will execute.")
                    kill_switch_announced = True
                # Sleep longer while halted to reduce noise
                time.sleep(60)
                continue

            now = aest_now()
            today = now.date().isoformat()

            # Evening briefing — STRICT gating: only minute 0-2 of hour 20, flag-locked
            if (now.hour == EVENING_BRIEFING_HOUR_AEST
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

            now_ts = time.time()

            # Crypto check — every 15 min
            if (now_ts - last_crypto) >= CRYPTO_LOOP_INTERVAL:
                last_crypto = now_ts
                run_crypto_loop(db, tg, coinspot)

            # Intraday stock check — every 5 min during US hours
            if is_us_market_hours() and (now_ts - last_intraday) >= INTRADAY_LOOP_INTERVAL:
                last_intraday = now_ts
                run_intraday_loop(db, tg, alpaca, coinspot)

            # Snapshot — every 5 min (pure Python)
            if (now_ts - last_snapshot) >= SNAPSHOT_INTERVAL:
                last_snapshot = now_ts
                run_snapshot(db)

            # Question poll — every 60s (only calls Claude if pending questions)
            if (now_ts - last_question) >= QUESTION_POLL_INTERVAL:
                last_question = now_ts
                run_question_poll(db, tg)

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
