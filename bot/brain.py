"""
RivX brain.py — cost-controlled trading intelligence.

Design principles:
  1. MECHANICAL FIRST. Stops/targets/time exits are pure Python. No Claude.
  2. CLAUDE ONLY WHEN NEEDED. Skip the call if there's nothing to decide.
  3. CHEAP MODEL FOR MONITORING. Haiku ($1/$5) for crypto_check & intraday_check.
  4. GOOD MODEL FOR BRIEFING. Sonnet ($3/$15) for evening planning + Q&A only.
  5. HARD TOKEN BUDGET. Stops calling Claude if daily spend exceeds $2.

Three modes:
  evening_briefing  — daily plan (Sonnet, ~$0.05/call)
  intraday_check    — stock monitoring (Haiku, ~$0.003/call, skipped if nothing to decide)
  crypto_check      — crypto monitoring (Haiku, ~$0.003/call, skipped if nothing to decide)
"""

import json
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone, date
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

# ─── Model selection (cost control) ────────────────────────────────────────
MODEL_BRIEFING   = "claude-sonnet-4-5"      # Nightly plan — needs reasoning
MODEL_MONITORING = "claude-haiku-4-5"       # Crypto/intraday checks — cheap & fast
MODEL_QA         = "claude-sonnet-4-5"      # Your dashboard questions — needs reasoning

# Token prices (USD per million tokens) for budget tracking
PRICES = {
    "claude-sonnet-4-5": {"input": 3.00,  "output": 15.00},
    "claude-haiku-4-5":  {"input": 1.00,  "output": 5.00},
}

# Daily budget ceiling — if we hit this, no more Claude calls today.
# Mechanical stops still fire to protect positions.
DAILY_BUDGET_USD = 2.00

# ─── Strategy constants (moderate-hot day trading) ─────────────────────────
CRYPTO_TAKE_PROFIT_PCT = 0.05   # 5% — clears CoinSpot 0.1% fees comfortably
CRYPTO_STOP_LOSS_PCT   = 0.025  # 2.5%
CRYPTO_TIME_EXIT_HOURS = 4      # sell if stagnant
CRYPTO_TRAIL_TRIGGER   = 0.03   # once +3%, start trailing
CRYPTO_TRAIL_FLOOR     = 0.015  # lock in min +1.5%
CRYPTO_MAX_POSITIONS   = 6
CRYPTO_MAX_DEPLOYED    = 3000
CRYPTO_POSITION_SIZE   = 500

STOCK_TAKE_PROFIT_PCT  = 0.03
STOCK_STOP_LOSS_PCT    = 0.015
STOCK_MAX_POSITIONS    = 3
STOCK_MAX_DEPLOYED     = 1500
STOCK_POSITION_SIZE    = 500

MIN_CASH_RESERVE = 1000

# Close stocks 15 min before US close (Fri-only — weekdays we check futures)
STOCK_EXIT_BEFORE_CLOSE_MIN = 15

_CRYPTO_LIST = ["BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC","BCH",
                "DOT","UNI","AAVE","MATIC","ATOM","ALGO","NEAR","FTM","SAND",
                "MANA","CRV","GRT","SUSHI","MKR","SNX","PEPE","SHIB","FLOKI",
                "WIF","BONK","FET","RNDR","TAO"]


# ─── Budget tracking ───────────────────────────────────────────────────────

def _check_budget(db) -> tuple[bool, float]:
    """Returns (under_budget, spent_today_usd). Reads token_usage table."""
    try:
        today = date.today().isoformat()
        rows = db._get("token_usage", {"date": f"eq.{today}"})
        if not rows:
            return True, 0.0
        spent = float(rows[0].get("cost_usd", 0))
        return spent < DAILY_BUDGET_USD, spent
    except Exception:
        return True, 0.0  # on error, default to allowing calls


def _record_usage(db, model: str, input_tokens: int, output_tokens: int):
    """Upsert today's token usage in Supabase."""
    try:
        price = PRICES.get(model, PRICES["claude-sonnet-4-5"])
        cost = (input_tokens * price["input"] + output_tokens * price["output"]) / 1_000_000
        today = date.today().isoformat()
        existing = db._get("token_usage", {"date": f"eq.{today}"})
        if existing:
            row = existing[0]
            db._patch("token_usage",
                      {"input_tokens":  int(row.get("input_tokens", 0)) + input_tokens,
                       "output_tokens": int(row.get("output_tokens", 0)) + output_tokens,
                       "cost_usd":      round(float(row.get("cost_usd", 0)) + cost, 4),
                       "call_count":    int(row.get("call_count", 0)) + 1},
                      "date", today)
        else:
            db._post("token_usage", {
                "date": today,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_usd": round(cost, 4),
                "call_count": 1,
            })
    except Exception as e:
        log.warning(f"Token usage record failed: {e}")


# ─── Market data ───────────────────────────────────────────────────────────

def get_aud_usd() -> float:
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=AUD&to=USD", timeout=5)
        return r.json()["rates"]["USD"]
    except Exception:
        return AUD_USD_FALLBACK


def _fetch_bars(symbol: str, days: int = 30) -> pd.DataFrame:
    try:
        start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        is_crypto = symbol in _CRYPTO_LIST or PORTFOLIO.get(symbol, {}).get("type") == "crypto"

        if is_crypto:
            crypto_map = {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD",
                          "XRP": "XRP/USD", "AVAX": "AVAX/USD", "LINK": "LINK/USD",
                          "DOGE": "DOGE/USD", "LTC": "LTC/USD", "BCH": "BCH/USD"}
            if symbol not in crypto_map:
                return pd.DataFrame()
            r = requests.get(f"{ALPACA_DATA_URL}/v1beta3/crypto/us/bars",
                             headers=HEADERS,
                             params={"symbols": crypto_map[symbol], "timeframe": "1Day", "start": start},
                             timeout=10)
            r.raise_for_status()
            bars = r.json().get("bars", {}).get(crypto_map[symbol], [])
        else:
            r = requests.get(f"{ALPACA_DATA_URL}/v2/stocks/{symbol}/bars",
                             headers=HEADERS,
                             params={"timeframe": "1Day", "start": start},
                             timeout=10)
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


def get_market_data(symbols: list) -> dict:
    """Build market context for symbols. Returns price + RSI + trend."""
    data = {}
    for sym in symbols:
        df = _fetch_bars(sym)
        if df.empty:
            data[sym] = {"price": 0, "error": "no data"}
            continue
        closes = df["close"].astype(float)

        delta = closes.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        rsi   = (100 - (100 / (1 + rs))).iloc[-1]

        ma20 = closes.rolling(20).mean().iloc[-1] if len(closes) >= 20 else None
        ma50 = closes.rolling(50).mean().iloc[-1] if len(closes) >= 50 else None
        is_crypto = sym in _CRYPTO_LIST

        data[sym] = {
            "price":      round(float(closes.iloc[-1]), 4),
            "change_1d":  round(float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100), 2) if len(closes) >= 2 else 0,
            "change_7d":  round(float((closes.iloc[-1] - closes.iloc[-7]) / closes.iloc[-7] * 100), 2) if len(closes) >= 7 else 0,
            "rsi":        round(float(rsi), 1) if not pd.isna(rsi) else None,
            "above_ma20": bool(closes.iloc[-1] > ma20) if ma20 else None,
            "above_ma50": bool(closes.iloc[-1] > ma50) if ma50 else None,
            "currency":   "AUD" if is_crypto else "USD",
        }
    return data


def get_news(symbols: list) -> list:
    try:
        stock_syms = [s for s in symbols if s not in _CRYPTO_LIST]
        if not stock_syms:
            return []
        r = requests.get(f"{ALPACA_DATA_URL}/v1beta1/news",
                         headers=HEADERS,
                         params={"symbols": ",".join(stock_syms), "limit": 8},
                         timeout=10)
        r.raise_for_status()
        return [{"headline": a["headline"], "symbols": a.get("symbols", [])}
                for a in r.json().get("news", [])[:6]]
    except Exception as e:
        log.debug(f"News fetch failed: {e}")
        return []


# ─── Position helpers ──────────────────────────────────────────────────────

def _position_age_hours(pos: dict) -> float:
    opened = pos.get("opened_at") or pos.get("created_at")
    if not opened:
        return 0.0
    try:
        if isinstance(opened, str):
            opened = opened.replace("Z", "+00:00")
            dt = datetime.fromisoformat(opened)
        else:
            dt = opened
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.utcnow()
        return (now - dt).total_seconds() / 3600
    except Exception:
        return 0.0


def _should_force_exit(pos: dict) -> tuple[bool, str]:
    """Pure-Python mechanical exit rules. No Claude call needed."""
    pnl_pct = pos.get("pnl_pct", 0) or 0
    is_crypto = pos.get("market") == "coinspot"

    stop = CRYPTO_STOP_LOSS_PCT if is_crypto else STOCK_STOP_LOSS_PCT
    take = CRYPTO_TAKE_PROFIT_PCT if is_crypto else STOCK_TAKE_PROFIT_PCT

    if pnl_pct <= -stop:
        return True, f"Stop-loss at {pnl_pct:.2%}"
    if pnl_pct >= take:
        return True, f"Take-profit at {pnl_pct:.2%}"

    if is_crypto:
        peak = pos.get("peak_pnl_pct", pnl_pct)
        if peak >= CRYPTO_TRAIL_TRIGGER and pnl_pct < CRYPTO_TRAIL_FLOOR:
            return True, f"Trailing stop — peaked at {peak:.2%}"
        age = _position_age_hours(pos)
        if age >= CRYPTO_TIME_EXIT_HOURS and abs(pnl_pct) < 0.015:
            return True, f"Time exit after {age:.1f}hr — no movement ({pnl_pct:+.2%})"

    return False, ""


# ─── Claude wrapper (budget-aware) ─────────────────────────────────────────

def _call_claude(db, system: str, user: str, model: str, max_tokens: int = 1000) -> dict | None:
    """Returns parsed JSON or None. Records token usage. Skips if over budget."""
    under_budget, spent = _check_budget(db)
    if not under_budget:
        log.warning(f"Skipping Claude call — daily budget hit (${spent:.2f}/{DAILY_BUDGET_USD})")
        return None

    try:
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}]
        )
        # Record usage BEFORE parsing — even if JSON parse fails, we still spent the tokens
        try:
            _record_usage(db, model, response.usage.input_tokens, response.usage.output_tokens)
        except Exception:
            pass

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
        log.error(f"Claude API error ({model}): {e}")
        return None


# ─── Evening briefing (Sonnet — daily, ~$0.05/call) ────────────────────────

def evening_briefing(db, positions: dict, trade_history: list, signal_weights: dict) -> dict:
    """Full nightly plan. Called once per day at 8pm AEST."""
    from bot.scanner import run_full_scan

    aud_usd = get_aud_usd()
    log.info("Evening briefing: running market scan...")
    scan        = run_full_scan()
    stock_opps  = (scan or {}).get("stock_opportunities", [])[:15]  # trim to control tokens
    crypto_opps = (scan or {}).get("crypto_opportunities", [])[:15]
    news        = (scan or {}).get("news", [])[:5]

    position_context = {
        sym: {"entry_price": pos.get("entry_price"), "pnl_pct": pos.get("pnl_pct"),
              "aud_amount": pos.get("aud_amount"), "market": pos.get("market")}
        for sym, pos in positions.items()
    }

    deployed_aud    = sum(p.get("aud_amount", 0) for p in position_context.values())
    crypto_deployed = sum(p.get("aud_amount", 0) for p in position_context.values() if p.get("market") == "coinspot")
    stock_deployed  = sum(p.get("aud_amount", 0) for p in position_context.values() if p.get("market") == "alpaca")
    crypto_available = max(0, CRYPTO_MAX_DEPLOYED - crypto_deployed)
    stock_available  = max(0, STOCK_MAX_DEPLOYED - stock_deployed)

    system = f"""You are RivX, an autonomous INTRADAY day trader managing $5,000 AUD paper capital.

STRATEGY:
- Many small trades, 3-8% target gains
- Crypto: +{CRYPTO_TAKE_PROFIT_PCT:.0%} take / -{CRYPTO_STOP_LOSS_PCT:.1%} stop, time-exit after {CRYPTO_TIME_EXIT_HOURS}hr
- Stocks: +{STOCK_TAKE_PROFIT_PCT:.0%} take / -{STOCK_STOP_LOSS_PCT:.1%} stop, close by US close
- Max {CRYPTO_MAX_POSITIONS} crypto + {STOCK_MAX_POSITIONS} stock positions
- Position size $300-600 AUD, confidence 55%+

RULES:
- Never exceed available budget given below
- ${MIN_CASH_RESERVE} cash reserve mandatory
- Cash is a valid position — don't force trades

Respond with valid JSON only, no prose."""

    user = f"""Evening briefing — {datetime.now().strftime('%A %d %b %Y')}
AUD/USD: {aud_usd:.4f}
Budget: ${crypto_available:.0f} crypto / ${stock_available:.0f} stocks available

TOP STOCKS:
{json.dumps(stock_opps, indent=1)}

TOP CRYPTO:
{json.dumps(crypto_opps, indent=1)}

NEWS:
{json.dumps(news, indent=1)}

OPEN POSITIONS:
{json.dumps(position_context, indent=1)}

Pick 3-6 trades for tonight. Return:
{{
  "decisions": {{
    "SYMBOL": {{"action":"BUY|HOLD|SELL","confidence":0.0,"reasoning":"one sentence",
               "aud_amount":500,"market":"coinspot|alpaca","type":"crypto|stock"}}
  }},
  "market_summary":"2 sentences",
  "risk_level":"LOW|MEDIUM|HIGH",
  "portfolio_health":"one sentence",
  "watch_for_overnight":"one sentence"
}}"""

    result = _call_claude(db, system, user, MODEL_BRIEFING, max_tokens=2000)
    if result:
        result["scan_data"] = scan
        # Enforce stops/targets regardless of what Claude says
        for sym, dec in result.get("decisions", {}).items():
            is_crypto = dec.get("market") == "coinspot" or dec.get("type") == "crypto"
            dec["stop_loss_pct"]   = CRYPTO_STOP_LOSS_PCT if is_crypto else STOCK_STOP_LOSS_PCT
            dec["take_profit_pct"] = CRYPTO_TAKE_PROFIT_PCT if is_crypto else STOCK_TAKE_PROFIT_PCT
    return result or _hold_all()


# ─── Intraday check (Haiku — cheap, skipped if nothing to decide) ──────────

def intraday_check(db, positions: dict, approved_plan: dict) -> dict:
    """Stock monitoring during US hours. Mechanical first, Claude only if needed."""
    actions = []
    reasons = []

    # 1. Mechanical stops/targets (no Claude call)
    for sym, pos in positions.items():
        if pos.get("market") != "alpaca":
            continue
        should_exit, reason = _should_force_exit(pos)
        if should_exit:
            actions.append({"symbol": sym, "action": "SELL", "reason": reason, "urgency": "immediate"})
            reasons.append(f"{sym}: {reason}")

    # 2. Friday end-of-day stock close (simple rule — weekend coming)
    now_aest = datetime.now(AEST)
    is_friday = now_aest.weekday() == 4
    approaching_close = (now_aest.hour == 5 and now_aest.minute >= (60 - STOCK_EXIT_BEFORE_CLOSE_MIN))
    if is_friday and approaching_close:
        for sym, pos in positions.items():
            if pos.get("market") == "alpaca" and not any(a["symbol"] == sym for a in actions):
                actions.append({"symbol": sym, "action": "SELL",
                               "reason": "Friday EOD — no weekend stock exposure",
                               "urgency": "immediate"})
                reasons.append(f"{sym}: Friday EOD exit")

    # 3. Only call Claude if there's something discretionary to decide
    open_stocks = [s for s, p in positions.items() if p.get("market") == "alpaca"
                   and not any(a["symbol"] == s for a in actions)]
    plan_buys = [s for s, d in (approved_plan.get("decisions") or {}).items()
                 if d.get("action") == "BUY" and s not in positions]

    if not open_stocks and not plan_buys:
        return {"actions": actions, "reasoning": " | ".join(reasons) or "No stock positions, no buys pending"}

    market_data = get_market_data(open_stocks + plan_buys)
    position_summary = {}
    for sym in open_stocks:
        pos = positions[sym]
        current = market_data.get(sym, {}).get("price", 0)
        entry = pos.get("entry_price", current)
        pnl_pct = ((current - entry) / entry) if entry > 0 else 0
        position_summary[sym] = {
            "pnl_pct": round(pnl_pct * 100, 2),
            "age_hours": round(_position_age_hours(pos), 1),
            "current_price": current,
        }

    system = "You are RivX intraday stock monitor. Mechanical exits already handled. Only flag discretionary exits or new entries with clear momentum change. Valid JSON only."
    user = f"""US intraday check — {datetime.utcnow().strftime('%H:%M UTC')}

OPEN STOCKS:
{json.dumps(position_summary, indent=1)}

MARKET DATA:
{json.dumps(market_data, indent=1)}

PLAN BUYS NOT YET TAKEN:
{json.dumps({s: approved_plan.get('decisions', {}).get(s) for s in plan_buys}, indent=1)}

Return:
{{"actions":[{{"symbol":"X","action":"BUY|SELL|HOLD","reason":"brief","urgency":"immediate|normal"}}],
  "reasoning":"one sentence"}}"""

    result = _call_claude(db, system, user, MODEL_MONITORING, max_tokens=400)
    if result:
        for act in result.get("actions", []):
            if act.get("action") in ("BUY", "SELL"):
                actions.append(act)
        reasons.append(result.get("reasoning", ""))

    return {"actions": actions, "reasoning": " | ".join(r for r in reasons if r) or "Monitoring"}


# ─── Crypto check (Haiku — cheap, skipped if nothing to decide) ────────────

def crypto_check(db, positions: dict, approved_plan: dict) -> dict:
    """24/7 crypto monitoring. Mechanical first, Claude only if scanner found setups."""
    from bot.scanner import get_crypto_movers

    actions = []
    reasons = []
    crypto_positions = {s: p for s, p in positions.items() if p.get("market") == "coinspot"}

    # 1. Mechanical exits (no Claude call)
    for sym, pos in crypto_positions.items():
        should_exit, reason = _should_force_exit(pos)
        if should_exit:
            actions.append({"symbol": sym, "action": "SELL", "reason": reason, "urgency": "immediate"})
            reasons.append(f"{sym}: {reason}")

    # 2. Get scanner output (cheap — no Claude)
    crypto_opps = []
    try:
        crypto_opps = get_crypto_movers() or []
    except Exception as e:
        log.warning(f"Scanner failed: {e}")

    # 3. Decide if we should ask Claude at all
    crypto_deployed = sum(p.get("aud_amount", 0) for p in crypto_positions.values())
    crypto_available = max(0, CRYPTO_MAX_DEPLOYED - crypto_deployed)
    open_count = len(crypto_positions)

    # Filter to high-conviction opportunities only
    strong_opps = [o for o in crypto_opps if o.get("opportunity_score", 0) >= 2.5][:10]

    at_max_positions = open_count >= CRYPTO_MAX_POSITIONS
    no_budget = crypto_available < 300
    nothing_new = len(strong_opps) == 0
    open_non_exiting = [s for s, p in crypto_positions.items()
                       if not any(a["symbol"] == s for a in actions)]

    # Skip Claude if: nothing to buy AND nothing to manage
    if (at_max_positions or no_budget or nothing_new) and not open_non_exiting:
        return {
            "actions": actions,
            "reasoning": f"No Claude call needed. {open_count}/{CRYPTO_MAX_POSITIONS} positions, "
                         f"${crypto_available:.0f} budget, {len(strong_opps)} strong setups. "
                         + (" | ".join(reasons) if reasons else ""),
            "opportunities": crypto_opps,
        }

    # 4. Claude call — but with Haiku, and trimmed context
    position_summary = {}
    for sym in open_non_exiting:
        pos = crypto_positions[sym]
        position_summary[sym] = {
            "pnl_pct": round((pos.get("pnl_pct", 0) or 0) * 100, 2),
            "age_hours": round(_position_age_hours(pos), 1),
        }

    system = f"""You are RivX crypto trader. Mechanical exits already applied.
Decide on new BUYs from opportunities, or discretionary SELLs on open positions.
Budget: ${crypto_available:.0f} AUD. Slots: {CRYPTO_MAX_POSITIONS - open_count}/{CRYPTO_MAX_POSITIONS} free.
Entry bar: opportunity_score >= 2.5, confidence 55%+. Cash is fine if nothing stands out.
Valid JSON only."""

    user = f"""Crypto check — {datetime.utcnow().strftime('%H:%M UTC')}

OPEN POSITIONS:
{json.dumps(position_summary, indent=1)}

STRONG OPPORTUNITIES:
{json.dumps(strong_opps, indent=1)}

Return:
{{"actions":[{{"symbol":"X","action":"BUY|SELL|HOLD","reason":"brief","aud_amount":500}}],
  "reasoning":"one sentence"}}"""

    result = _call_claude(db, system, user, MODEL_MONITORING, max_tokens=500)
    if result:
        reasons.append(result.get("reasoning", ""))
        for act in result.get("actions", []):
            if act.get("action") == "BUY":
                requested = min(act.get("aud_amount", CRYPTO_POSITION_SIZE), 600)
                if requested > crypto_available:
                    reasons.append(f"Skipped {act.get('symbol')} — budget")
                    continue
                if open_count >= CRYPTO_MAX_POSITIONS:
                    reasons.append(f"Skipped {act.get('symbol')} — max positions")
                    continue
                act["aud_amount"] = requested
                actions.append(act)
                crypto_available -= requested
                open_count += 1
            elif act.get("action") == "SELL":
                actions.append(act)

    return {
        "actions": actions,
        "reasoning": " | ".join(r for r in reasons if r) or "Monitoring",
        "opportunities": crypto_opps,
    }


def _hold_all() -> dict:
    return {
        "decisions": {},
        "market_summary": "Claude unavailable or over budget. No trades tonight.",
        "risk_level": "LOW",
        "portfolio_health": "Safe mode — no new trades.",
        "watch_for_overnight": "Nothing — mechanical stops still active.",
        "scan_data": {},
    }
