"""
RivX brain.py — Claude is the trading intelligence.

Strategy (rewritten for active intraday trading):
  - Crypto: 4-8% targets, 2.5% stops, 4hr time exits, trailing stops on winners
  - Stocks: 3% targets, 1.5% stops during US session, close-all by 5:45am AEST
  - Up to 6 crypto + 3 stock positions concurrent
  - Max $3000 crypto, $1500 stocks, $1000 cash buffer

Three modes:
  evening_briefing  — full analysis, generates tonight's approved plan
  intraday_check    — quick scan of open positions, adapt or take profit
  crypto_check      — 24/7 crypto monitoring, active day-trading
"""

import json
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import anthropic
from bot.config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_DATA_URL,
    ANTHROPIC_API_KEY, PORTFOLIO, AUD_USD_FALLBACK
)

log = logging.getLogger(__name__)

HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

AEST = timezone(timedelta(hours=10))

# ─── Strategy constants (tuned for moderate-hot day trading) ──────────────────
CRYPTO_TAKE_PROFIT_PCT = 0.05   # 5% — enough to clear CoinSpot's 0.1% fees comfortably
CRYPTO_STOP_LOSS_PCT   = 0.025  # 2.5% — tight stop
CRYPTO_TIME_EXIT_HOURS = 4      # sell after 4hrs if no movement
CRYPTO_TRAIL_TRIGGER   = 0.03   # once +3%, start trailing
CRYPTO_TRAIL_FLOOR     = 0.015  # lock in minimum +1.5%
CRYPTO_MAX_POSITIONS   = 6
CRYPTO_MAX_DEPLOYED    = 3000   # AUD
CRYPTO_POSITION_SIZE   = 500    # AUD per trade, scales up/down with confidence

STOCK_TAKE_PROFIT_PCT  = 0.03   # 3% — stocks move less than crypto
STOCK_STOP_LOSS_PCT    = 0.015  # 1.5%
STOCK_MAX_POSITIONS    = 3
STOCK_MAX_DEPLOYED     = 1500   # AUD
STOCK_POSITION_SIZE    = 500

MIN_CASH_RESERVE       = 1000   # AUD, always kept aside

STOCK_EXIT_ALL_BEFORE_CLOSE_MIN = 15  # close all stock positions 15min before US close


# ─── Market data ──────────────────────────────────────────────────────────────

def get_aud_usd() -> float:
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=AUD&to=USD", timeout=5)
        return r.json()["rates"]["USD"]
    except Exception:
        return AUD_USD_FALLBACK


def fetch_bars(symbol: str, days: int = 30) -> pd.DataFrame:
    """Daily bars for trend context (RSI, MAs)."""
    try:
        start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        sym_cfg = PORTFOLIO.get(symbol, {})
        is_crypto = sym_cfg.get("type") == "crypto" if sym_cfg else symbol in _CRYPTO_LIST

        if is_crypto:
            crypto_map = {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD",
                          "XRP": "XRP/USD", "AVAX": "AVAX/USD", "LINK": "LINK/USD",
                          "DOGE": "DOGE/USD", "LTC": "LTC/USD", "BCH": "BCH/USD"}
            if symbol not in crypto_map:
                return pd.DataFrame()
            url    = f"{ALPACA_DATA_URL}/v1beta3/crypto/us/bars"
            params = {"symbols": crypto_map[symbol], "timeframe": "1Day", "start": start}
            r      = requests.get(url, headers=HEADERS, params=params, timeout=10)
            r.raise_for_status()
            bars = r.json().get("bars", {}).get(crypto_map[symbol], [])
        else:
            url    = f"{ALPACA_DATA_URL}/v2/stocks/{symbol}/bars"
            params = {"timeframe": "1Day", "start": start}
            r      = requests.get(url, headers=HEADERS, params=params, timeout=10)
            r.raise_for_status()
            bars = r.json().get("bars", [])
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars)
        df.rename(columns={"c": "close", "o": "open", "h": "high", "l": "low", "v": "volume"}, inplace=True)
        return df
    except Exception as e:
        log.debug(f"fetch_bars {symbol}: {e}")
        return pd.DataFrame()


_CRYPTO_LIST = ["BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC","BCH",
                "DOT","UNI","AAVE","MATIC","ATOM","ALGO","NEAR","FTM","SAND",
                "MANA","CRV","GRT","SUSHI","MKR","SNX","PEPE","SHIB","FLOKI",
                "WIF","BONK","FET","RNDR","TAO"]


def get_market_data(symbols: list) -> dict:
    """Build rich market context for all symbols."""
    data = {}
    for sym in symbols:
        df = fetch_bars(sym)
        if df.empty:
            data[sym] = {"price": 0, "error": "no data"}
            continue
        closes = df["close"].astype(float)

        # RSI
        delta = closes.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        rsi   = (100 - (100 / (1 + rs))).iloc[-1]

        ma20 = closes.rolling(20).mean().iloc[-1] if len(closes) >= 20 else None
        ma50 = closes.rolling(50).mean().iloc[-1] if len(closes) >= 50 else None

        is_crypto = sym in _CRYPTO_LIST or (PORTFOLIO.get(sym, {}).get("type") == "crypto")

        data[sym] = {
            "price":        round(float(closes.iloc[-1]), 4),
            "change_1d":    round(float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100), 2) if len(closes) >= 2 else 0,
            "change_7d":    round(float((closes.iloc[-1] - closes.iloc[-7]) / closes.iloc[-7] * 100), 2) if len(closes) >= 7 else 0,
            "rsi":          round(float(rsi), 1) if not pd.isna(rsi) else None,
            "above_ma20":   bool(closes.iloc[-1] > ma20) if ma20 else None,
            "above_ma50":   bool(closes.iloc[-1] > ma50) if ma50 else None,
            "currency":     "AUD" if is_crypto else "USD",
        }
    return data


def get_news(symbols: list) -> list:
    try:
        stock_syms = [s for s in symbols if s not in _CRYPTO_LIST and PORTFOLIO.get(s, {}).get("type") != "crypto"]
        if not stock_syms:
            return []
        r = requests.get(
            f"{ALPACA_DATA_URL}/v1beta1/news",
            headers=HEADERS,
            params={"symbols": ",".join(stock_syms), "limit": 8},
            timeout=10
        )
        r.raise_for_status()
        return [{"headline": a["headline"], "symbols": a.get("symbols", [])}
                for a in r.json().get("news", [])[:6]]
    except Exception as e:
        log.warning(f"News fetch failed: {e}")
        return []


# ─── Position helpers ─────────────────────────────────────────────────────────

def _position_age_hours(pos: dict) -> float:
    """How long has this position been open, in hours."""
    opened = pos.get("opened_at") or pos.get("created_at")
    if not opened:
        return 0.0
    try:
        # Supabase returns ISO strings
        if isinstance(opened, str):
            opened = opened.replace("Z", "+00:00")
            dt = datetime.fromisoformat(opened)
        else:
            dt = opened
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.utcnow()
        return (now - dt).total_seconds() / 3600
    except Exception:
        return 0.0


def _should_force_exit(sym: str, pos: dict) -> tuple[bool, str]:
    """Mechanical exits that fire regardless of what Claude says.
    Returns (should_exit, reason)."""
    pnl_pct = pos.get("pnl_pct", 0) or 0
    is_crypto = pos.get("market") == "coinspot"

    stop = CRYPTO_STOP_LOSS_PCT if is_crypto else STOCK_STOP_LOSS_PCT
    take = CRYPTO_TAKE_PROFIT_PCT if is_crypto else STOCK_TAKE_PROFIT_PCT

    if pnl_pct <= -stop:
        return True, f"Stop-loss at {pnl_pct:.2%}"
    if pnl_pct >= take:
        return True, f"Take-profit at {pnl_pct:.2%}"

    # Trailing stop for crypto winners
    if is_crypto:
        peak = pos.get("peak_pnl_pct", pnl_pct)
        if peak >= CRYPTO_TRAIL_TRIGGER and pnl_pct < CRYPTO_TRAIL_FLOOR:
            return True, f"Trailing stop — peaked at {peak:.2%}, locked in exit"

        # Time exit for stagnant crypto positions
        age = _position_age_hours(pos)
        if age >= CRYPTO_TIME_EXIT_HOURS and abs(pnl_pct) < 0.015:
            return True, f"Time exit after {age:.1f}hr — no movement ({pnl_pct:+.2%})"

    return False, ""


# ─── Claude calls ─────────────────────────────────────────────────────────────

def _call_claude(system: str, user: str, max_tokens: int = 1500) -> dict | None:
    try:
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}]
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        log.error(f"Claude JSON parse error: {e}")
        return None
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return None


def evening_briefing(positions: dict, trade_history: list, signal_weights: dict) -> dict:
    """
    Full nightly analysis with market scanner.
    Picks 3-6 intraday setups for tonight — both US stocks for overnight session
    and crypto for 24/7 trading.
    """
    from bot.scanner import run_full_scan
    aud_usd = get_aud_usd()
    log.info("Running market scan for evening briefing...")
    scan        = run_full_scan()
    stock_opps  = scan["stock_opportunities"]
    crypto_opps = scan["crypto_opportunities"]
    news        = scan["news"]

    position_context = {
        sym: {"entry_price": pos.get("entry_price"), "pnl_pct": pos.get("pnl_pct"),
              "aud_amount": pos.get("aud_amount"), "market": pos.get("market")}
        for sym, pos in positions.items()
    }
    recent = [{"symbol": t["symbol"], "action": t["action"],
               "pnl_pct": t.get("pnl_pct"), "date": t.get("created_at", "")[:10]}
              for t in (trade_history or [])[-15:]]

    # Calculate available cash
    deployed_aud    = sum(p.get("aud_amount", 0) for p in position_context.values())
    crypto_deployed = sum(p.get("aud_amount", 0) for p in position_context.values() if p.get("market") == "coinspot")
    stock_deployed  = sum(p.get("aud_amount", 0) for p in position_context.values() if p.get("market") == "alpaca")
    total_available = max(0, 5000 - deployed_aud - MIN_CASH_RESERVE)
    crypto_available = max(0, CRYPTO_MAX_DEPLOYED - crypto_deployed)
    stock_available  = max(0, STOCK_MAX_DEPLOYED - stock_deployed)

    system = f"""You are RivX, an autonomous intraday trading system managing $5,000 AUD paper capital.
You are a DAY TRADER, not a swing trader. You take many small trades targeting 3-8% gains.
You are NOT limited to fixed stocks — pick the best opportunities from the scanner.

STRATEGY — MODERATE-HOT INTRADAY:
- Target 3-10 trades per day across crypto + stocks
- Crypto: {CRYPTO_TAKE_PROFIT_PCT:.0%} take-profit, {CRYPTO_STOP_LOSS_PCT:.1%} stop-loss, sold after {CRYPTO_TIME_EXIT_HOURS}hr if stagnant
- Stocks: {STOCK_TAKE_PROFIT_PCT:.0%} take-profit, {STOCK_STOP_LOSS_PCT:.1%} stop-loss, all closed before US market close
- Once up +{CRYPTO_TRAIL_TRIGGER:.0%}, crypto positions trail to lock in +{CRYPTO_TRAIL_FLOOR:.1%} minimum

POSITION RULES:
- Max {CRYPTO_MAX_POSITIONS} crypto + {STOCK_MAX_POSITIONS} stock positions concurrently
- Position size ~$400-600 AUD (scales with confidence)
- Max ${CRYPTO_MAX_DEPLOYED} deployed in crypto, ${STOCK_MAX_DEPLOYED} in stocks
- ${MIN_CASH_RESERVE} cash reserve is mandatory — you cannot override this
- You are told exactly how much is available — don't exceed it

ENTRY BAR:
- Confidence 55%+ for new entries (lower than swing-trade bar — we want more trades)
- CoinSpot charges ~0.1% per trade — {CRYPTO_TAKE_PROFIT_PCT:.0%} target easily clears that
- Alpaca stocks commission-free — small target is fine
- Cash is still a valid position — don't force trades if nothing's moving

Respond ONLY with valid JSON."""

    user = f"""Date: {datetime.now().strftime('%A %d %B %Y')} -- Evening briefing
AUD/USD: {aud_usd:.4f}
Portfolio: $5,000 AUD | Available for new trades: ${total_available:.0f} AUD
Deployed: ${deployed_aud:.0f} AUD ({len(position_context)} positions)
Crypto budget remaining: ${crypto_available:.0f} | Stock budget remaining: ${stock_available:.0f}
Crypto positions: {crypto_deployed and len([p for p in position_context.values() if p.get('market')=='coinspot']) or 0}/{CRYPTO_MAX_POSITIONS}
Stock positions: {stock_deployed and len([p for p in position_context.values() if p.get('market')=='alpaca']) or 0}/{STOCK_MAX_POSITIONS}
Stocks scanned: {scan['stocks_scanned']} | Crypto scanned: {scan['crypto_scanned']}

TOP STOCK OPPORTUNITIES (ranked):
{json.dumps(stock_opps, indent=2)}

TOP CRYPTO OPPORTUNITIES:
{json.dumps(crypto_opps, indent=2)}

MARKET NEWS:
{json.dumps(news, indent=2)}

CURRENT OPEN POSITIONS:
{json.dumps(position_context, indent=2)}

RECENT TRADE HISTORY:
{json.dumps(recent, indent=2)}

SIGNAL WEIGHTS (learned from past trades):
{json.dumps(signal_weights, indent=2)}

Pick 3-6 intraday setups for tonight. Prefer coins/stocks with clear momentum and recent volume spike.
Size each trade within budget. Don't exceed position counts.

Return this exact JSON:
{{
  "decisions": {{
    "SYMBOL": {{"action": "BUY|HOLD|SELL", "confidence": 0.0, "reasoning": "max 2 sentences",
               "aud_amount": 500, "stop_loss_pct": {CRYPTO_STOP_LOSS_PCT}, "take_profit_pct": {CRYPTO_TAKE_PROFIT_PCT},
               "market": "alpaca|coinspot", "type": "stock|crypto"}}
  }},
  "market_summary": "2-3 sentence overall market view for tonight",
  "risk_level": "LOW|MEDIUM|HIGH",
  "portfolio_health": "one sentence",
  "cash_held_aud": 0,
  "watch_for_overnight": "what to watch for tonight",
  "scanner_highlights": "2 sentences on what the scanner found most interesting"
}}"""

    result = _call_claude(system, user, max_tokens=2000)
    if result:
        result["scan_data"] = scan
        # Enforce stops/targets regardless of what Claude returned
        for sym, dec in result.get("decisions", {}).items():
            is_crypto = dec.get("market") == "coinspot" or dec.get("type") == "crypto"
            dec["stop_loss_pct"]   = CRYPTO_STOP_LOSS_PCT if is_crypto else STOCK_STOP_LOSS_PCT
            dec["take_profit_pct"] = CRYPTO_TAKE_PROFIT_PCT if is_crypto else STOCK_TAKE_PROFIT_PCT
    return result or _hold_all()


def intraday_check(positions: dict, approved_plan: dict) -> dict:
    """
    Every 2 mins during US market hours.
    Aggressive day-trader: snap winners, cut losers fast, exit all stocks before close.
    """
    if not positions and not approved_plan:
        return {"actions": [], "reasoning": "No positions and no approved plan"}

    actions = []
    reasons = []

    # 1. Mechanical exits first (fire regardless of Claude)
    for sym, pos in positions.items():
        should_exit, reason = _should_force_exit(sym, pos)
        if should_exit:
            actions.append({"symbol": sym, "action": "SELL", "reason": reason, "urgency": "immediate"})
            reasons.append(f"{sym}: {reason}")

    # 2. Near-close exit for any open stocks
    now_aest = datetime.now(AEST)
    minutes_to_close = None
    # US close is 6:00am AEST (roughly, daylight savings shifts it by an hour)
    # We aim to close at 5:45am AEST
    if now_aest.hour == 5 and now_aest.minute >= (60 - STOCK_EXIT_ALL_BEFORE_CLOSE_MIN):
        for sym, pos in positions.items():
            if pos.get("market") == "alpaca" and not any(a["symbol"] == sym for a in actions):
                actions.append({
                    "symbol": sym, "action": "SELL",
                    "reason": "End-of-session exit — never hold stocks overnight",
                    "urgency": "immediate"
                })
                reasons.append(f"{sym}: EOD exit")

    # 3. Ask Claude about anything not mechanically forced
    open_syms = [s for s in positions.keys() if not any(a["symbol"] == s for a in actions)]
    plan_buys = [s for s, d in (approved_plan.get("decisions") or {}).items() if d.get("action") == "BUY"]
    query_syms = list(set(open_syms + plan_buys)) or list(positions.keys())

    if query_syms:
        market_data = get_market_data(query_syms)
        news        = get_news(query_syms)

        position_summary = {}
        for sym in open_syms:
            pos     = positions[sym]
            current = market_data.get(sym, {}).get("price", 0)
            entry   = pos.get("entry_price", current)
            pnl_pct = ((current - entry) / entry) if entry > 0 else 0
            is_crypto = pos.get("market") == "coinspot"
            position_summary[sym] = {
                "entry_price":   entry,
                "current_price": current,
                "pnl_pct":       round(pnl_pct * 100, 2),
                "stop_loss_pct": (CRYPTO_STOP_LOSS_PCT if is_crypto else STOCK_STOP_LOSS_PCT) * 100,
                "take_profit_pct": (CRYPTO_TAKE_PROFIT_PCT if is_crypto else STOCK_TAKE_PROFIT_PCT) * 100,
                "age_hours":     round(_position_age_hours(pos), 1),
                "market":        pos.get("market"),
            }

        system = f"""You are RivX intraday monitor. Day-trading mode.
You've already had mechanical stops/targets applied. Your job now: spot discretionary exits
or new entries from the approved plan that make sense RIGHT NOW.

Rules:
- Don't HOLD if position is flat and >2hrs old during active session
- Pick new BUY entries only if approved plan said BUY AND setup still valid
- Flag any position showing sudden momentum change (news, volume spike)
Respond ONLY with valid JSON."""

        user = f"""Intraday check — {datetime.utcnow().strftime('%H:%M UTC')}

OPEN POSITIONS (mechanical exits already handled):
{json.dumps(position_summary, indent=2)}

CURRENT MARKET DATA:
{json.dumps(market_data, indent=2)}

BREAKING NEWS:
{json.dumps(news, indent=2)}

TONIGHT'S APPROVED PLAN (BUY entries we haven't taken yet):
{json.dumps({s: approved_plan.get("decisions", {}).get(s) for s in plan_buys if s not in positions}, indent=2)}

Any discretionary actions to take?

Return this exact JSON:
{{
  "actions": [
    {{
      "symbol": "NVDA",
      "action": "SELL|BUY|HOLD",
      "reason": "brief reason",
      "urgency": "immediate|normal"
    }}
  ],
  "reasoning": "overall assessment in one sentence"
}}"""

        claude_result = _call_claude(system, user, max_tokens=800)
        if claude_result:
            for act in claude_result.get("actions", []):
                if act.get("action") in ("BUY", "SELL"):
                    actions.append(act)
            reasons.append(claude_result.get("reasoning", ""))

    return {
        "actions": actions,
        "reasoning": " | ".join(r for r in reasons if r) or "No action needed"
    }


def crypto_check(positions: dict, approved_plan: dict) -> dict:
    """
    Every 5 mins, 24/7.
    Active day-trader: snap winners, cut losers, take new setups aggressively.
    """
    from bot.scanner import get_crypto_movers
    crypto_opps = get_crypto_movers()

    # Build market data for open positions only (performance)
    held_syms   = [s for s, p in positions.items() if p.get("market") == "coinspot"]
    market_data = get_market_data(held_syms + ["BTC", "ETH"]) if held_syms else get_market_data(["BTC", "ETH"])

    actions = []
    reasons = []

    # 1. Mechanical exits first
    for sym, pos in positions.items():
        if pos.get("market") != "coinspot":
            continue
        should_exit, reason = _should_force_exit(sym, pos)
        if should_exit:
            actions.append({"symbol": sym, "action": "SELL", "reason": reason, "urgency": "immediate"})
            reasons.append(f"{sym}: {reason}")

    # 2. Build position summary for Claude (only those NOT being force-exited)
    position_summary = {}
    for sym, pos in positions.items():
        if pos.get("market") != "coinspot":
            continue
        if any(a["symbol"] == sym for a in actions):
            continue
        current = market_data.get(sym, {}).get("price", 0)
        entry   = pos.get("entry_price", current)
        pnl_pct = ((current - entry) / entry) if entry > 0 else 0
        position_summary[sym] = {
            "entry_price":   entry,
            "current_price": current,
            "pnl_pct":       round(pnl_pct * 100, 2),
            "age_hours":     round(_position_age_hours(pos), 1),
            "take_profit_at": CRYPTO_TAKE_PROFIT_PCT * 100,
            "stop_loss_at":   -CRYPTO_STOP_LOSS_PCT * 100,
        }

    # 3. Budget check
    crypto_deployed = sum(p.get("aud_amount", 0) for s, p in positions.items() if p.get("market") == "coinspot")
    crypto_available = max(0, CRYPTO_MAX_DEPLOYED - crypto_deployed)
    open_crypto_count = len([p for p in positions.values() if p.get("market") == "coinspot"])

    # 4. Ask Claude about new entries + discretionary exits
    system = f"""You are RivX crypto day-trader. Running every 5 minutes, 24/7.

STRATEGY — ACTIVE INTRADAY:
- Target {CRYPTO_TAKE_PROFIT_PCT:.0%} gains per trade (stops auto-fire at -{CRYPTO_STOP_LOSS_PCT:.1%})
- Positions auto-exit after {CRYPTO_TIME_EXIT_HOURS}hr if stagnant
- Winners trail to lock in +{CRYPTO_TRAIL_FLOOR:.1%} once they hit +{CRYPTO_TRAIL_TRIGGER:.0%}

ENTRY SIGNALS (BUY when ANY two+ align):
- RSI 30-50 with positive 1h momentum (oversold bounce)
- Volume ratio >1.3x average AND price breaking 1h high (momentum)
- Opportunity score >2.0 with clear trend
- Confidence 55%+ (lower bar than swing trading)

POSITION LIMITS:
- Max {CRYPTO_MAX_POSITIONS} concurrent crypto positions
- ${CRYPTO_POSITION_SIZE} per trade default (300-600 AUD range by confidence)
- Max ${CRYPTO_MAX_DEPLOYED} total crypto exposure

DON'T:
- Chase pumps already >20% today (reversion risk)
- Enter if position count at max
- Force a trade — cash is fine

Respond ONLY with valid JSON."""

    user = f"""Crypto check — {datetime.utcnow().strftime('%H:%M UTC')}
Budget: ${crypto_available:.0f} AUD available | Open: {open_crypto_count}/{CRYPTO_MAX_POSITIONS} positions

TOP CRYPTO OPPORTUNITIES:
{json.dumps(crypto_opps[:20], indent=2)}

OPEN CRYPTO POSITIONS (mechanical exits already handled):
{json.dumps(position_summary, indent=2)}

What actions now? Look hard for new entries that match the signals above.

Return this exact JSON:
{{
  "actions": [
    {{"symbol": "SYM", "action": "BUY|SELL|HOLD", "reason": "one line", "urgency": "immediate|normal", "aud_amount": 500}}
  ],
  "reasoning": "one sentence on crypto market right now"
}}"""

    claude_result = _call_claude(system, user, max_tokens=800) or {"actions": [], "reasoning": "Claude unavailable"}
    reasons.append(claude_result.get("reasoning", ""))

    # 5. Validate Claude's new entries against budget
    for act in claude_result.get("actions", []):
        if act.get("action") != "BUY":
            if act.get("action") == "SELL":
                actions.append(act)
            continue
        # BUY validation
        requested = min(act.get("aud_amount", CRYPTO_POSITION_SIZE), 600)
        if requested > crypto_available:
            reasons.append(f"Skipped {act['symbol']} BUY — budget ${crypto_available:.0f} insufficient")
            continue
        if open_crypto_count >= CRYPTO_MAX_POSITIONS:
            reasons.append(f"Skipped {act['symbol']} BUY — at max {CRYPTO_MAX_POSITIONS} positions")
            continue
        act["aud_amount"] = requested
        actions.append(act)
        crypto_available -= requested
        open_crypto_count += 1

    return {
        "actions": actions,
        "reasoning": " | ".join(r for r in reasons if r) or "Monitoring",
        "opportunities": crypto_opps,
    }


def _hold_all() -> dict:
    return {
        "decisions": {},
        "market_summary": "Claude unavailable. All positions held as safety measure.",
        "risk_level": "LOW",
        "portfolio_health": "Safe mode — no trades until Claude responds.",
        "watch_for_overnight": "Nothing — holding all positions.",
        "market_data": {},
    }
