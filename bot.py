"""
RivX AutoTrader — Main Bot
Runs continuously on Render.

Loops:
  1. Evening briefing  — 9pm AEST, full analysis, sends plan to Telegram for approval
  2. Intraday loop     — every 2 mins during US market hours (11:30pm-6am AEST)
  3. Crypto loop       — every 5 mins 24/7
  4. Question loop     — every 30s, answers user questions from the dashboard
  5. Snapshot loop     — every 5 mins, writes intraday portfolio value for chart
"""

import time
import logging
import json
from datetime import datetime, timezone, timedelta
from bot.config import (
    PORTFOLIO, PAPER_MODE,
    EVENING_BRIEFING_HOUR_AEST, MORNING_SUMMARY_HOUR_AEST,
    US_MARKET_OPEN_HOUR_AEST, US_MARKET_CLOSE_HOUR_AEST,
    INTRADAY_CHECK_INTERVAL, CRYPTO_CHECK_INTERVAL,
    MIN_CONFIDENCE_TO_TRADE, APPROVAL_TIMEOUT_SECONDS,
    ANTHROPIC_API_KEY,
)
from bot.brain import evening_briefing, intraday_check, crypto_check, get_market_data
from bot.alpaca_trader import AlpacaTrader
from bot.coinspot_trader import CoinSpotTrader
from bot.supabase_logger import SupabaseLogger
from bot.telegram_notify import TelegramNotifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

AEST = timezone(timedelta(hours=10))


def aest_now() -> datetime:
    return datetime.now(AEST)


def is_us_market_hours() -> bool:
    """True between 11:30pm and 6:00am AEST on weekdays."""
    now     = aest_now()
    weekday = now.weekday()
    hour    = now.hour
    minute  = now.minute
    after_open  = (hour == 23 and minute >= 30) or (hour < 6)
    is_weekday  = weekday < 5
    not_weekend = not (weekday == 5 and hour < 6)
    return after_open and is_weekday and not_weekend


def execute_action(symbol, action, reason, alpaca, coinspot, db, tg,
                   positions, market_data, confidence=1.0, notify=True):
    """Execute a single trade action. Returns True if executed.

    Trader resolution is defensive: we never call .buy or .sell on None.
    """
    # Resolve config for this symbol
    if symbol in PORTFOLIO:
        config    = PORTFOLIO[symbol]
        market    = config.get("market", "coinspot")
        allocated = config.get("allocated_aud", 400)
    else:
        crypto_coins = ["BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC",
                        "BCH","DOT","UNI","AAVE","MATIC","ATOM","ALGO","NEAR",
                        "FTM","SAND","MANA","CRV","GRT","SUSHI","MKR","SNX",
                        "PEPE","SHIB","FLOKI","WIF","BONK","FET","RNDR","TAO"]
        market    = "coinspot" if symbol in crypto_coins else "alpaca"
        allocated = 400
        config    = {"stop_loss_pct": 0.10 if market == "coinspot" else 0.07,
                     "take_profit_pct": 0.12 if market == "coinspot" else 0.08}

    # Pick the right trader object for this market
    trader = alpaca if market == "alpaca" else coinspot

    # If the chosen trader isn't available in this loop (e.g. alpaca is None
    # during the crypto-only loop), try to fall back to coinspot for crypto.
    if trader is None:
        if market == "alpaca" and coinspot is not None:
            log.info(f"Rerouting {symbol} from alpaca to coinspot (this loop is crypto-only)")
            market = "coinspot"
            trader = coinspot
        else:
            log.warning(f"Skip {symbol} — no {market} trader available in this loop")
            return False

    # Belt-and-braces — should never fire, but guarantees we never call .buy on None
    if trader is None:
        log.error(f"Skip {symbol} — trader resolved to None unexpectedly (market={market})")
        return False

    if action == "BUY" and symbol not in positions:
        log.info(f"Executing BUY {symbol} on {market} — {reason}")
        try:
            order = trader.buy(symbol, allocated)
        except Exception as e:
            log.error(f"BUY call failed for {symbol} on {market}: {e}")
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
            log.error(f"SELL call failed for {symbol} on {market}: {e}")
            return False
        if order:
            pos   = positions[symbol]
            price = market_data.get(symbol, {}).get("price", 0)
            pnl   = pos.get("pnl_pct", 0)
            db.log_trade(symbol, "SELL", pos["aud_amount"], order, confidence, reason)
            db.close_position(symbol, price, pnl)
            if notify:
                tg.send(f"{'[PAPER] ' if PAPER_MODE else ''}Sold {symbol} — {reason} — P&L {pnl:+.1%}")
            return True
    return False


def run_evening_briefing(db, tg, alpaca, coinspot):
    """9pm AEST — Full analysis, send plan to Telegram, wait for approval."""
    log.info("Running evening briefing...")
    tg.send("RivX is analysing the market for tonight. Give me a moment...")

    positions     = db.get_positions()
    trade_history = db.get_recent_trades(30)
    weights       = db.get_signal_weights()

    analysis = evening_briefing(positions, trade_history, weights)
    if not analysis:
        tg.send("Evening briefing failed — Claude unavailable. Holding all positions tonight.")
        return

    decisions = analysis.get("decisions", {})
    market_data = analysis.get("market_data", {})

    lines = [
        f"Good evening. RivX evening briefing — {aest_now().strftime('%d %b %Y')}",
        f"",
        f"Market: {analysis.get('market_summary', '')}",
        f"Risk level: {analysis.get('risk_level', 'MEDIUM')}",
        f"Portfolio: {analysis.get('portfolio_health', '')}",
        f"",
        f"Tonight's plan:",
    ]

    buy_count = sell_count = hold_count = 0
    for sym, dec in decisions.items():
        action     = dec["action"]
        confidence = dec["confidence"]
        reasoning  = dec["reasoning"]
        emoji      = "BUY" if action == "BUY" else ("SELL" if action == "SELL" else "HOLD")
        lines.append(f"  {emoji} {sym} ({confidence:.0%}) — {reasoning}")
        if action == "BUY":   buy_count  += 1
        elif action == "SELL": sell_count += 1
        else:                  hold_count += 1

    lines += [
        f"",
        f"Watch for: {analysis.get('watch_for_overnight', '')}",
        f"",
        f"Summary: {buy_count} buys, {sell_count} sells, {hold_count} holds",
        f"",
        f"Reply YES to approve or NO to skip tonight.",
        f"Auto-approves in 1 hour if no response.",
    ]

    approved = tg.send_and_wait("\n".join(lines), timeout_seconds=APPROVAL_TIMEOUT_SECONDS)

    if not approved:
        tg.send("Tonight's plan cancelled. Bot will only manage stop-losses overnight.")
        db.save_approved_plan({})
        return

    tg.send("Plan approved. RivX is on watch overnight. Sleep well.")

    positions = db.get_positions()
    for sym, dec in decisions.items():
        action     = dec["action"]
        confidence = dec["confidence"]
        if confidence < MIN_CONFIDENCE_TO_TRADE:
            continue
        execute_action(
            sym, action, dec["reasoning"],
            alpaca, coinspot, db, tg,
            positions, market_data, confidence, notify=True
        )

    db.save_approved_plan(analysis)
    log.info("Evening briefing complete — plan saved")


def run_intraday_check(db, tg, alpaca, coinspot):
    """Every 2 mins during US market hours — adapt, take profits, cut losses."""
    positions     = db.get_positions()
    approved_plan = db.get_approved_plan()

    if not positions and not approved_plan:
        return

    result      = intraday_check(positions, approved_plan)
    actions     = result.get("actions", [])
    market_data = {}

    for act in actions:
        sym     = act["symbol"]
        action  = act["action"]
        reason  = act["reason"]
        urgency = act.get("urgency", "normal")

        if action == "HOLD":
            continue

        log.info(f"Intraday action: {action} {sym} — {reason} ({urgency})")

        if sym not in market_data:
            market_data.update(get_market_data([sym]))

        positions = db.get_positions()
        executed  = execute_action(
            sym, action, reason,
            alpaca, coinspot, db, tg,
            positions, market_data,
            confidence=0.8, notify=True
        )

        if executed and urgency == "immediate":
            log.info(f"Immediate action executed: {action} {sym}")

    positions = db.get_positions()
    for sym, pos in positions.items():
        pnl_pct   = pos.get("pnl_pct", 0)
        stop_loss = PORTFOLIO[sym]["stop_loss_pct"] if sym in PORTFOLIO else 0.07
        if pnl_pct <= -stop_loss:
            log.warning(f"STOP-LOSS: {sym} at {pnl_pct:.1%}")
            md = get_market_data([sym])
            execute_action(
                sym, "SELL", f"Stop-loss triggered at {pnl_pct:.1%}",
                alpaca, coinspot, db, tg,
                positions, md, confidence=1.0, notify=True
            )
            tg.send(f"Stop-loss fired on {sym} ({pnl_pct:.1%}). Capital protected.")


def run_crypto_check(db, tg, coinspot):
    """Every 5 mins, 24/7 — crypto monitoring + log reasoning for dashboard."""
    positions     = db.get_positions()
    approved_plan = db.get_approved_plan()

    result = crypto_check(positions, approved_plan)
    actions = result.get("actions", [])
    reasoning = result.get("reasoning", "")
    opportunities = result.get("opportunities", [])
    log.info(f"Crypto check: {reasoning}")

    # Log every check to Supabase so dashboard can show live activity
    db._post("crypto_checks", {
        "checked_at":    datetime.utcnow().isoformat(),
        "reasoning":     reasoning,
        "actions":       json.dumps(actions),
        "opportunities": json.dumps(opportunities[:10]) if opportunities else "[]",
    })

    market_data = get_market_data(["BTC", "ETH"])

    for act in actions:
        sym    = act["symbol"]
        action = act["action"]
        reason = act["reason"]
        log.info(f"Crypto decision: {action} {sym} — {reason}")

        if action == "HOLD":
            continue

        positions = db.get_positions()
        execute_action(
            sym, action, reason,
            None, coinspot, db, tg,
            positions, market_data,
            confidence=0.75, notify=True
        )

    positions = db.get_positions()
    for sym, pos in list(positions.items()):
        if pos.get("market") != "coinspot":
            continue
        pnl_pct   = pos.get("pnl_pct", 0)
        stop_loss = PORTFOLIO[sym]["stop_loss_pct"] if sym in PORTFOLIO else 0.10
        if pnl_pct <= -stop_loss:
            log.warning(f"CRYPTO STOP-LOSS: {sym} at {pnl_pct:.1%}")
            execute_action(
                sym, "SELL", f"Crypto stop-loss at {pnl_pct:.1%}",
                None, coinspot, db, tg,
                positions, market_data,
                confidence=1.0, notify=True
            )


def run_intraday_snapshot(db):
    """
    Write a portfolio value snapshot every 5 mins.
    Feeds the dashboard's intraday chart. Non-critical — errors logged, not raised.
    """
    try:
        positions = db.get_positions()
        deployed = sum(p.get("aud_amount", 0) for p in positions.values())
        # For paper mode, "cash" is whatever isn't deployed out of the $5000 starting balance.
        # Later when we track real P&L here, this becomes meaningful.
        cash = max(0, 5000 - deployed)
        total = deployed + cash  # placeholder — real version uses broker balance once trades execute

        db._post("intraday_snapshots", {
            "recorded_at":    datetime.utcnow().isoformat(),
            "total_aud":      round(total, 2),
            "deployed_aud":   round(deployed, 2),
            "cash_aud":       round(cash, 2),
            "open_positions": len(positions),
        })
    except Exception as e:
        log.warning(f"Intraday snapshot write failed: {e}")


def answer_user_questions(db, tg):
    """
    Poll user_questions for pending rows. For each, call Claude with market context,
    write the answer back. Runs often enough (every 30s) that dashboard Q&A feels snappy.
    """
    try:
        pending = db._get("user_questions", {"status": "eq.pending",
                                             "order": "asked_at.asc",
                                             "limit": "5"})
    except Exception as e:
        log.warning(f"Question poll failed: {e}")
        return

    if not pending:
        return

    # Lazy import — only needed when a question exists
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    for q in pending:
        qid = q.get("id")
        question = q.get("question", "").strip()
        if not question:
            continue

        log.info(f"Answering user question #{qid}: {question[:80]}")

        # Gather context so Claude's answer is grounded in actual bot state
        try:
            positions = db.get_positions()
            trades    = db.get_recent_trades(10)
            weights   = db.get_signal_weights()
            plan      = db.get_approved_plan()
            portfolio = db.get_portfolio_value()
        except Exception:
            positions = {}; trades = []; weights = {}; plan = {}; portfolio = {}

        system = (
            "You are RivX, the user's autonomous trading bot. They are asking you a question "
            "via the dashboard. Answer directly, factually, and concisely. Use the context "
            "provided. If the question is about something the bot doesn't track, say so plainly. "
            "Don't invent trades or positions. Keep answers under 250 words. "
            "Paper mode is currently active — no real money is at risk."
        )
        user_msg = (
            f"Question: {question}\n\n"
            f"--- Current bot state ---\n"
            f"Portfolio: {json.dumps(portfolio)}\n"
            f"Open positions: {json.dumps({s: {k:v for k,v in p.items() if k in ['entry_price','pnl_pct','aud_amount','market']} for s,p in positions.items()})}\n"
            f"Recent trades: {json.dumps([{'symbol':t.get('symbol'),'action':t.get('action'),'pnl_pct':t.get('pnl_pct'),'date':t.get('created_at','')[:10]} for t in trades])}\n"
            f"Signal weights: {json.dumps({k: weights.get(k) for k in ['rsi','macd','bollinger','volume','ma_cross']})}\n"
            f"Approved plan active: {'yes' if plan else 'no'}\n"
        )

        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=600,
                system=system,
                messages=[{"role":"user","content":user_msg}],
            )
            answer = resp.content[0].text.strip()
            db._patch("user_questions",
                      {"answer": answer,
                       "answered_at": datetime.utcnow().isoformat(),
                       "status": "answered"},
                      "id", str(qid))
            log.info(f"Question #{qid} answered")
        except Exception as e:
            log.error(f"Question #{qid} failed: {e}")
            db._patch("user_questions",
                      {"answer": f"Sorry — couldn't answer this one. ({str(e)[:80]})",
                       "answered_at": datetime.utcnow().isoformat(),
                       "status": "failed"},
                      "id", str(qid))


def run_morning_summary(db, tg):
    """6:30am AEST — overnight results waiting with your coffee."""
    positions = db.get_positions()
    portfolio = db.get_portfolio_value()
    trades    = db.get_recent_trades(20)

    from datetime import date
    today_trades = [t for t in trades if t.get("created_at", "")[:10] == date.today().isoformat()]

    lines = [
        f"Good morning. RivX overnight report — {aest_now().strftime('%d %b')}",
        f"",
        f"Portfolio: ${portfolio['total_aud']:,.2f} AUD",
        f"Overnight: {'+' if portfolio['day_pnl'] >= 0 else ''}${portfolio['day_pnl']:.2f}",
        f"Total P&L: {'+' if portfolio['total_pnl'] >= 0 else ''}${portfolio['total_pnl']:.2f}",
        f"",
    ]
    if today_trades:
        lines.append("Trades overnight:")
        for t in today_trades:
            pnl = f" P&L {t['pnl_pct']:+.1%}" if t.get("pnl_pct") else ""
            lines.append(f"  {t['action']} {t['symbol']}{pnl} — {t.get('details', '')[:60]}")
    else:
        lines.append("No trades overnight — all positions held.")

    if positions:
        lines.append(f"")
        lines.append("Open positions:")
        for sym, pos in positions.items():
            pnl = pos.get("pnl_pct", 0)
            lines.append(f"  {sym}: {'+' if pnl >= 0 else ''}{pnl:.1%}")

    lines.append(f"")
    lines.append("Have a great day. RivX is watching the crypto markets.")

    tg.send("\n".join(lines))
    db.save_approved_plan({})
    db.save_snapshot(portfolio["total_aud"], portfolio["day_pnl"], portfolio["total_pnl"])
    log.info("Morning summary sent")


def main():
    log.info(f"RivX AutoTrader starting — {'PAPER' if PAPER_MODE else 'LIVE'} mode")

    db       = SupabaseLogger()
    tg       = TelegramNotifier()
    alpaca   = AlpacaTrader()
    coinspot = CoinSpotTrader()

    startup_flag = db.get_flag("last_startup")
    today = datetime.now(AEST).date().isoformat()
    if startup_flag != today:
        db.set_flag("last_startup", today)
        tg.send(f"RivX is online. {'PAPER trading mode.' if PAPER_MODE else 'LIVE trading mode.'}")
    else:
        log.info("Startup suppressed — already sent today")

    last_intraday_check = 0
    last_crypto_check   = 0
    last_snapshot       = 0
    last_question_check = 0

    while True:
        try:
            if tg.check_kill_switch():
                log.warning("Kill switch — bot stopped")
                break

            now   = aest_now()
            hour  = now.hour
            today = now.date().isoformat()

            # Evening briefing — once per day at 9pm AEST
            if hour == EVENING_BRIEFING_HOUR_AEST:
                if db.get_flag("last_evening_briefing") != today:
                    db.set_flag("last_evening_briefing", today)
                    run_evening_briefing(db, tg, alpaca, coinspot)

            # Morning summary — once per day at 6:30am AEST
            if hour == MORNING_SUMMARY_HOUR_AEST and now.minute >= 30:
                if db.get_flag("last_morning_summary") != today:
                    db.set_flag("last_morning_summary", today)
                    run_morning_summary(db, tg)

            now_ts = time.time()

            # Intraday check — every 2 mins during US market hours
            if is_us_market_hours() and (now_ts - last_intraday_check) >= INTRADAY_CHECK_INTERVAL:
                last_intraday_check = now_ts
                run_intraday_check(db, tg, alpaca, coinspot)

            # Crypto check — every 5 mins always
            if (now_ts - last_crypto_check) >= CRYPTO_CHECK_INTERVAL:
                last_crypto_check = now_ts
                run_crypto_check(db, tg, coinspot)

            # Intraday snapshot — every 5 mins always (for dashboard chart)
            if (now_ts - last_snapshot) >= 300:
                last_snapshot = now_ts
                run_intraday_snapshot(db)

            # User questions — every 30 seconds (dashboard Q&A)
            if (now_ts - last_question_check) >= 30:
                last_question_check = now_ts
                answer_user_questions(db, tg)

            time.sleep(15)  # main loop ticks every 15 seconds now

        except KeyboardInterrupt:
            log.info("RivX stopped by user")
            tg.send("RivX stopped manually.")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
            tg.send(f"RivX error: {e}. Continuing...")
            time.sleep(60)


if __name__ == "__main__":
    main()
