"""
RivX AutoTrader — Main Bot
Runs continuously on Render.

Three loops:
  1. Evening briefing  — 9pm AEST, full analysis, sends plan to Telegram for approval
  2. Intraday loop     — every 2 mins during US market hours (11:30pm-6am AEST)
  3. Crypto loop       — every 5 mins 24/7

You approve once before bed. Bot trades autonomously overnight.
Wake-up alert only for major unexpected events.
Morning summary at 6:30am AEST.
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
)
from bot.brain import evening_briefing, intraday_check, crypto_check
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
    weekday = now.weekday()  # 0=Mon, 6=Sun
    hour    = now.hour
    minute  = now.minute
    # Market open: 11:30pm (hour=23, min>=30) through midnight and until 6am
    after_open  = (hour == 23 and minute >= 30) or (hour < 6)
    is_weekday  = weekday < 5  # Mon-Fri
    # But not Saturday morning (market closed Friday night in AEST = Sat morning)
    not_weekend = not (weekday == 5 and hour < 6)
    return after_open and is_weekday and not_weekend


def execute_action(symbol: str, action: str, reason: str,
                   alpaca: AlpacaTrader, coinspot: CoinSpotTrader,
                   db: SupabaseLogger, tg: TelegramNotifier,
                   positions: dict, market_data: dict,
                   confidence: float = 1.0, notify: bool = True) -> bool:
    """Execute a single trade action. Returns True if executed."""
    config    = PORTFOLIO[symbol]
    market    = config["market"]
    allocated = config["allocated_aud"]

    if action == "BUY" and symbol not in positions:
        log.info(f"Executing BUY {symbol} — {reason}")
        order = alpaca.buy(symbol, allocated) if market == "alpaca" else coinspot.buy(symbol, allocated)
        if order:
            price = market_data.get(symbol, {}).get("price", 0)
            db.log_trade(symbol, "BUY", allocated, order, confidence, reason)
            db.save_position(symbol, price, allocated, market)
            if notify:
                tg.send(f"{'[PAPER] ' if PAPER_MODE else ''}Bought {symbol} — {reason}")
            return True

    elif action == "SELL" and symbol in positions:
        log.info(f"Executing SELL {symbol} — {reason}")
        order = alpaca.sell(symbol) if market == "alpaca" else coinspot.sell(symbol)
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


def run_evening_briefing(db: SupabaseLogger, tg: TelegramNotifier,
                         alpaca: AlpacaTrader, coinspot: CoinSpotTrader):
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

    # Format the evening briefing message
    lines = [
        f"Good evening. RivX evening briefing — {aest_now().strftime('%d %b %Y')}",
        f"",
        f"Market: {analysis.get('market_summary', '')}",
        f"Risk level: {analysis.get('risk_level', 'MEDIUM')}",
        f"Portfolio: {analysis.get('portfolio_health', '')}",
        f"",
        f"Tonight's plan:",
    ]

    buy_count  = 0
    sell_count = 0
    hold_count = 0

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

    # Execute immediate buys/sells from the approved plan
    positions = db.get_positions()  # refresh
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

    # Save approved plan to Supabase so intraday loop can reference it
    db.save_approved_plan(analysis)
    log.info("Evening briefing complete — plan saved")


def run_intraday_check(db: SupabaseLogger, tg: TelegramNotifier,
                       alpaca: AlpacaTrader, coinspot: CoinSpotTrader):
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

        # Refresh market data for this symbol if needed
        if sym not in market_data:
            from bot.brain import get_market_data
            market_data.update(get_market_data([sym]))

        positions = db.get_positions()  # refresh before each trade
        executed  = execute_action(
            sym, action, reason,
            alpaca, coinspot, db, tg,
            positions, market_data,
            confidence=0.8, notify=True
        )

        if executed and urgency == "immediate":
            log.info(f"Immediate action executed: {action} {sym}")

    # Always check stop-losses regardless of Claude's recommendations
    positions = db.get_positions()
    for sym, pos in positions.items():
        pnl_pct   = pos.get("pnl_pct", 0)
        stop_loss = PORTFOLIO[sym]["stop_loss_pct"]
        if pnl_pct <= -stop_loss:
            log.warning(f"STOP-LOSS: {sym} at {pnl_pct:.1%}")
            from bot.brain import get_market_data
            md = get_market_data([sym])
            execute_action(
                sym, "SELL", f"Stop-loss triggered at {pnl_pct:.1%}",
                alpaca, coinspot, db, tg,
                positions, md, confidence=1.0, notify=True
            )
            tg.send(f"Stop-loss fired on {sym} ({pnl_pct:.1%}). Capital protected.")


def run_crypto_check(db: SupabaseLogger, tg: TelegramNotifier,
                     coinspot: CoinSpotTrader):
    """Every 5 mins, 24/7 — BTC and ETH monitoring."""
    positions     = db.get_positions()
    approved_plan = db.get_approved_plan()

    result  = crypto_check(positions, approved_plan)
    actions = result.get("actions", [])
    reasoning = result.get("reasoning", "")
    log.info(f"Crypto check: {reasoning}")

    from bot.brain import get_market_data
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

    # Crypto stop-losses — always check
    positions = db.get_positions()
    for sym in ["BTC", "ETH"]:
        if sym not in positions:
            continue
        pos       = positions[sym]
        pnl_pct   = pos.get("pnl_pct", 0)
        stop_loss = PORTFOLIO[sym]["stop_loss_pct"]
        if pnl_pct <= -stop_loss:
            log.warning(f"CRYPTO STOP-LOSS: {sym} at {pnl_pct:.1%}")
            execute_action(
                sym, "SELL", f"Crypto stop-loss at {pnl_pct:.1%}",
                None, coinspot, db, tg,
                positions, market_data,
                confidence=1.0, notify=True
            )


def run_morning_summary(db: SupabaseLogger, tg: TelegramNotifier):
    """6:30am AEST — overnight results waiting with your coffee."""
    positions = db.get_positions()
    portfolio = db.get_portfolio_value()
    trades    = db.get_recent_trades(20)

    # Overnight trades
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
    db.save_approved_plan({})  # clear overnight plan
    db.save_snapshot(portfolio["total_aud"], portfolio["day_pnl"], portfolio["total_pnl"])
    log.info("Morning summary sent")


def main():
    log.info(f"RivX AutoTrader starting — {'PAPER' if PAPER_MODE else 'LIVE'} mode")

    db       = SupabaseLogger()
    tg       = TelegramNotifier()
    alpaca   = AlpacaTrader()
    coinspot = CoinSpotTrader()

    tg.send(f"RivX is online. {'PAPER trading mode.' if PAPER_MODE else 'LIVE trading mode.'}")

    last_intraday_check   = 0
    last_crypto_check     = 0
    last_intraday_check   = 0
    last_crypto_check     = 0

    while True:
        try:
            # Kill switch check
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

            # Intraday check — every 2 mins during US market hours
            now_ts = time.time()
            if is_us_market_hours() and (now_ts - last_intraday_check) >= INTRADAY_CHECK_INTERVAL:
                last_intraday_check = now_ts
                run_intraday_check(db, tg, alpaca, coinspot)

            # Crypto check — every 5 mins always
            if (now_ts - last_crypto_check) >= CRYPTO_CHECK_INTERVAL:
                last_crypto_check = now_ts
                run_crypto_check(db, tg, coinspot)

            time.sleep(30)  # main loop ticks every 30 seconds

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
