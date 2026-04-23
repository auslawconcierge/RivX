"""
RivX brain.py — Claude is the trading intelligence.

Three modes:
  evening_briefing  — full analysis, generates tonight's approved plan
  intraday_check    — quick scan of open positions, adapt or take profit
  crypto_check      — 24/7 crypto monitoring, RSI bounces and momentum
"""

import json
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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


# ─── Market data ──────────────────────────────────────────────────────────────

def get_aud_usd() -> float:
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=AUD&to=USD", timeout=5)
        return r.json()["rates"]["USD"]
    except Exception:
        return AUD_USD_FALLBACK


def fetch_bars(symbol: str, days: int = 30) -> pd.DataFrame:
    try:
        start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        if PORTFOLIO[symbol]["type"] == "crypto":
            crypto_map = {"BTC": "BTC/USD", "ETH": "ETH/USD"}
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
        log.error(f"fetch_bars {symbol}: {e}")
        return pd.DataFrame()


def get_market_data(symbols: list) -> dict:
    """Build rich market context for all symbols."""
    data = {}
    for sym in symbols:
        df = fetch_bars(sym)
        if df.empty:
            data[sym] = {"error": "no data"}
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

        data[sym] = {
            "price":        round(float(closes.iloc[-1]), 4),
            "change_1d":    round(float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100), 2) if len(closes) >= 2 else 0,
            "change_7d":    round(float((closes.iloc[-1] - closes.iloc[-7]) / closes.iloc[-7] * 100), 2) if len(closes) >= 7 else 0,
            "rsi":          round(float(rsi), 1) if not pd.isna(rsi) else None,
            "above_ma20":   bool(closes.iloc[-1] > ma20) if ma20 else None,
            "above_ma50":   bool(closes.iloc[-1] > ma50) if ma50 else None,
            "currency":     "AUD" if PORTFOLIO[sym]["type"] == "crypto" else "USD",
        }
    return data


def get_news(symbols: list) -> list:
    try:
        stock_syms = [s for s in symbols if PORTFOLIO[s]["type"] != "crypto"]
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
    Scans 50+ stocks and 9 crypto for the best opportunities.
    Claude picks trades from real movers — not a fixed list.
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

    system = """You are RivX, an autonomous AI trading system managing a $5,000 AUD portfolio.
You are a GENERAL MARKET SCANNER. You scan the whole market for the best opportunities tonight.
You are NOT limited to fixed stocks. Pick the best opportunities from what is actually moving.

PORTFOLIO RULES:
- Total: $5,000 AUD. Never put more than $600 in any single position.
- ETFs (SPY, QQQ etc): max $2,500 total, 7% stop-loss, 5% take-profit
- Individual stocks: max $1,500 total, 7% stop-loss, 8% take-profit
- Crypto: max $1,000 total, 10% stop-loss, 12% take-profit
- Only trade what you are confident in. Hold cash if nothing looks compelling.
Respond ONLY with valid JSON."""

    user = f"""Date: {datetime.now().strftime('%A %d %B %Y')} -- Evening briefing
AUD/USD: {aud_usd:.4f} | Portfolio: $5,000 AUD
Stocks scanned: {scan['stocks_scanned']} | Crypto scanned: {scan['crypto_scanned']}

TOP STOCK OPPORTUNITIES (ranked by opportunity score):
{json.dumps(stock_opps, indent=2)}

TOP CRYPTO OPPORTUNITIES:
{json.dumps(crypto_opps, indent=2)}

MARKET NEWS:
{json.dumps(news, indent=2)}

CURRENT OPEN POSITIONS:
{json.dumps(position_context, indent=2)}

RECENT TRADE HISTORY:
{json.dumps(recent, indent=2)}

ADAPTIVE SIGNAL WEIGHTS:
{json.dumps(signal_weights, indent=2)}

Select the best 3-6 trading opportunities for tonight from the scanner results.
You can trade ANY stock or crypto from the lists. Size within portfolio rules.

Return this exact JSON:
{{
  "decisions": {{
    "SYMBOL": {{"action": "BUY|HOLD|SELL", "confidence": 0.0, "reasoning": "max 2 sentences",
               "aud_amount": 500, "stop_loss_pct": 0.07, "take_profit_pct": 0.08,
               "market": "alpaca", "type": "stock"}}
  }},
  "market_summary": "2-3 sentence overall market view",
  "risk_level": "LOW|MEDIUM|HIGH",
  "portfolio_health": "one sentence",
  "cash_held_aud": 0,
  "watch_for_overnight": "what to watch for tonight",
  "scanner_highlights": "2 sentences on what the scanner found most interesting"
}}"""

    result = _call_claude(system, user, max_tokens=2000)
    if result:
        result["scan_data"] = scan
    return result or _hold_all()


def intraday_check(positions: dict, approved_plan: dict) -> dict:
    """
    Quick check every 2 mins during US market hours.
    Looks for: take profit opportunities, stop loss breaches,
    better opportunities than approved plan, breaking news.
    """
    if not positions and not approved_plan:
        return {"actions": [], "reasoning": "No positions and no approved plan"}

    symbols      = list(positions.keys()) + [
        s for s, d in (approved_plan.get("decisions") or {}).items()
        if d.get("action") == "BUY"
    ]
    symbols      = list(set(symbols)) or list(PORTFOLIO.keys())
    market_data  = get_market_data(symbols)
    news         = get_news(symbols)

    position_summary = {}
    for sym, pos in positions.items():
        current = market_data.get(sym, {}).get("price", 0)
        entry   = pos.get("entry_price", current)
        pnl_pct = ((current - entry) / entry) if entry > 0 else 0
        position_summary[sym] = {
            "entry_price":   entry,
            "current_price": current,
            "pnl_pct":       round(pnl_pct * 100, 2),
            "stop_loss":     PORTFOLIO[sym]["stop_loss_pct"] * 100,
            "take_profit":   PORTFOLIO[sym]["take_profit_pct"] * 100,
        }

    system = """You are RivX intraday monitor. Check every 2 minutes during US market hours.
Your job: protect profits, cut losses, and opportunistically improve the approved plan.
Be decisive but not trigger-happy. Only act when there's a clear reason.
Respond ONLY with valid JSON."""

    user = f"""Intraday check — {datetime.utcnow().strftime('%H:%M UTC')}

OPEN POSITIONS:
{json.dumps(position_summary, indent=2)}

CURRENT MARKET DATA:
{json.dumps(market_data, indent=2)}

BREAKING NEWS:
{json.dumps(news, indent=2)}

TONIGHT'S APPROVED PLAN:
{json.dumps(approved_plan.get('decisions', {}), indent=2)}

Should any action be taken right now?

Return this exact JSON:
{{
  "actions": [
    {{
      "symbol": "NVDA",
      "action": "SELL|BUY|HOLD",
      "reason": "take profit at +8.2%",
      "urgency": "immediate|normal"
    }}
  ],
  "reasoning": "overall assessment in one sentence",
  "plan_change": "any change to tonight's approved plan, or null"
}}"""

    return _call_claude(system, user, max_tokens=800) or {"actions": [], "reasoning": "Claude unavailable"}


def crypto_check(positions: dict, approved_plan: dict) -> dict:
    """
    Runs every 5 mins, 24/7 for BTC and ETH.
    Looks for RSI bounces, momentum, stop-loss breaches.
    """
    crypto_syms = ["BTC", "ETH"]
    market_data = get_market_data(crypto_syms)

    position_summary = {}
    for sym in crypto_syms:
        if sym in positions:
            current = market_data.get(sym, {}).get("price", 0)
            entry   = positions[sym].get("entry_price", current)
            pnl_pct = ((current - entry) / entry) if entry > 0 else 0
            position_summary[sym] = {
                "held":          True,
                "entry_price":   entry,
                "current_price": current,
                "pnl_pct":       round(pnl_pct * 100, 2),
                "stop_loss":     PORTFOLIO[sym]["stop_loss_pct"] * 100,
                "take_profit":   PORTFOLIO[sym]["take_profit_pct"] * 100,
            }
        else:
            position_summary[sym] = {
                "held":          False,
                "current_price": market_data.get(sym, {}).get("price", 0),
                "rsi":           market_data.get(sym, {}).get("rsi"),
                "approved_action": approved_plan.get("decisions", {}).get(sym, {}).get("action", "HOLD"),
            }

    system = """You are RivX crypto monitor running 24/7.
Monitor BTC and ETH only. React to RSI extremes, momentum shifts, stop-loss breaches.
Crypto is volatile — be comfortable with wider swings but protect against crashes.
Only suggest action when there is a clear signal. Respond ONLY with valid JSON."""

    user = f"""Crypto check — {datetime.utcnow().strftime('%H:%M UTC')}

CRYPTO STATUS:
{json.dumps(position_summary, indent=2)}

MARKET DATA:
{json.dumps(market_data, indent=2)}

Return this exact JSON:
{{
  "actions": [
    {{
      "symbol": "BTC",
      "action": "BUY|SELL|HOLD",
      "reason": "RSI at 28, oversold bounce likely",
      "urgency": "immediate|normal"
    }}
  ],
  "reasoning": "one sentence summary"
}}"""

    return _call_claude(system, user, max_tokens=600) or {"actions": [], "reasoning": "Claude unavailable"}


def _hold_all() -> dict:
    return {
        "decisions": {
            sym: {"action": "HOLD", "confidence": 0.0,
                  "reasoning": "Claude unavailable — safety hold",
                  "intraday_target_pct": cfg["take_profit_pct"]}
            for sym, cfg in PORTFOLIO.items()
        },
        "market_summary": "Claude unavailable. All positions held as safety measure.",
        "risk_level": "LOW",
        "portfolio_health": "Safe mode — no trades until Claude responds.",
        "watch_for_overnight": "Nothing — holding all positions.",
        "market_data": {},
    }
