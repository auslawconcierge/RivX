"""
RivX scanner.py — universe builder using CoinSpot + CoinPaprika + Alpaca.

CoinGecko has been REMOVED from this scanner. Data sources are split by job:

  Tradeable universe + spot AUD prices  → CoinSpot /pubapi/v2/latest
    No auth, no rate-limit, returns every symbol the bot can actually buy.

  24h ranking (change, volume, market cap) → CoinPaprika /v1/tickers
    Free tier ~25k calls/month, no auth. Drop-in replacement for the
    CoinGecko volume_desc/gainers/losers calls we used to make.

  OHLCV / RSI / MACD                     → Alpaca crypto bars (in brain.py)
    Auth'd via your Alpaca paper account, no rate cliff.

  Stocks                                 → Alpaca screener + bars (unchanged)

Why this combo: CoinSpot is the source of truth for what we can trade and at
what AUD price (it's our exchange). CoinPaprika gives us the global "what's
pumping today" intelligence which CoinSpot's public API doesn't. Alpaca gives
us the deep historical bars for technical scoring. Each does what it's best
at, no overlap, no rate-limit storms.
"""

import os
import time
import json
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta
from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_DATA_URL

log = logging.getLogger(__name__)

ALPACA_HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

# ── Cache (file-based, survives restarts) ───────────────────────────────────
CACHE_DIR = Path(os.environ.get("RIVX_CACHE_DIR", "/tmp/rivx_cache"))
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    CACHE_DIR = Path("/tmp")

CACHE_TTL_FRESH = 600     # 10 min — return as-fresh
CACHE_TTL_STALE = 86400   # 24 h  — usable as last-resort fallback


def _cache_get(key: str, max_age: int):
    p = CACHE_DIR / f"{key}.json"
    if not p.exists():
        return None
    if time.time() - p.stat().st_mtime > max_age:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _cache_set(key: str, data) -> None:
    try:
        (CACHE_DIR / f"{key}.json").write_text(json.dumps(data))
    except Exception as e:
        log.debug(f"Cache write failed for {key}: {e}")


def _stale_or_empty(key: str) -> list:
    stale = _cache_get(key, CACHE_TTL_STALE)
    if stale:
        log.warning(f"All sources failed — returning stale {key} ({len(stale)} items)")
        return stale
    log.error(f"All sources failed and no cache for {key}")
    return []


# ── Static fallback universe (used only if CoinSpot's public API is down) ──
COINSPOT_FALLBACK = {
    "BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC","BCH","DOT",
    "UNI","AAVE","MATIC","ATOM","ALGO","NEAR","FTM","SAND","MANA","CRV",
    "GRT","SUSHI","MKR","SNX","PEPE","SHIB","FLOKI","WIF","BONK","FET",
    "RNDR","TAO","TRX","TON","APT","SUI","SEI","TIA","INJ","ICP","ARB",
    "OP","HBAR","FIL","VET","STX","IMX","RUNE","JUP","PYTH","JTO","WLD","ENA",
}

STOCK_STAPLES = ["SPY", "QQQ", "IWM", "NVDA", "AAPL", "MSFT", "META", "TSLA",
                 "AMD", "GOOGL", "AMZN", "NFLX"]


# ─── CoinSpot universe + spot AUD prices ──────────────────────────────────

def _coinspot_universe_with_prices() -> dict:
    """
    Returns {SYM: {"last_aud": float, "bid_aud": float, "ask_aud": float}}
    for everything CoinSpot lists. 5-min cache (prices need to be fresher
    than the 10-min ranking data).
    """
    cached = _cache_get("coinspot_full", 300)
    if cached:
        return cached

    for url in [
        "https://www.coinspot.com.au/pubapi/v2/latest",
        "https://www.coinspot.com.au/pubapi/latest",
    ]:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            prices = data.get("prices") or data
            if not isinstance(prices, dict):
                continue

            out = {}
            for sym_raw, entry in prices.items():
                sym = sym_raw.upper()
                if isinstance(entry, dict):
                    try:
                        out[sym] = {
                            "last_aud": float(entry.get("last") or 0),
                            "bid_aud":  float(entry.get("bid")  or 0),
                            "ask_aud":  float(entry.get("ask")  or 0),
                        }
                    except (TypeError, ValueError):
                        pass
                elif isinstance(entry, (int, float, str)):
                    try:
                        out[sym] = {"last_aud": float(entry), "bid_aud": 0, "ask_aud": 0}
                    except (TypeError, ValueError):
                        pass

            if len(out) > 20:
                log.info(f"CoinSpot universe: {len(out)} symbols + prices from {url}")
                _cache_set("coinspot_full", out)
                return out
        except Exception as e:
            log.debug(f"CoinSpot {url} failed: {e}")

    # Fallback: stale cache, then bare set
    stale = _cache_get("coinspot_full", CACHE_TTL_STALE)
    if stale:
        log.warning(f"CoinSpot live unavailable — using stale cache ({len(stale)} symbols)")
        return stale
    log.warning(f"CoinSpot endpoints unavailable — using fallback set ({len(COINSPOT_FALLBACK)} symbols, no prices)")
    return {s: {"last_aud": 0, "bid_aud": 0, "ask_aud": 0} for s in COINSPOT_FALLBACK}


# ─── CoinPaprika ranking ──────────────────────────────────────────────────

def _coinpaprika_top(limit: int = 300):
    """
    Returns list sorted by rank: [{symbol, name, price_usd, change_24h,
    volume_24h, market_cap}, ...]

    Free tier: 25k calls/month, no auth. Cached 10 min.
    """
    cached = _cache_get("paprika_top", CACHE_TTL_FRESH)
    if cached:
        return cached

    try:
        r = requests.get("https://api.coinpaprika.com/v1/tickers",
                         params={"limit": limit}, timeout=15)
        r.raise_for_status()
        rows = r.json() or []
        out = []
        for row in rows:
            q = (row.get("quotes") or {}).get("USD") or {}
            sym = (row.get("symbol") or "").upper()
            if not sym:
                continue
            out.append({
                "symbol":     sym,
                "name":       row.get("name"),
                "price_usd":  float(q.get("price") or 0),
                "change_24h": float(q.get("percent_change_24h") or 0),
                "volume_24h": float(q.get("volume_24h") or 0),
                "market_cap": float(q.get("market_cap") or 0),
            })
        log.info(f"CoinPaprika: {len(out)} symbols ranked")
        _cache_set("paprika_top", out)
        return out
    except Exception as e:
        log.warning(f"CoinPaprika fetch failed: {e}")
        return None


# ─── Crypto scan ──────────────────────────────────────────────────────────

def get_crypto_movers() -> list:
    """
    Build crypto opportunity list. CoinSpot defines the universe (only what
    we can buy), CoinPaprika provides ranking signals (24h change, volume,
    market cap). We score and filter, return top 50.
    """
    cached = _cache_get("crypto_movers", CACHE_TTL_FRESH)
    if cached:
        log.info(f"Crypto scan: {len(cached)} candidates (cached)")
        return cached

    cs = _coinspot_universe_with_prices()
    if not cs:
        return _stale_or_empty("crypto_movers")

    ranking = _coinpaprika_top()
    if not ranking:
        return _stale_or_empty("crypto_movers")

    # Intersect: only consider what we can actually buy
    cs_universe = set(cs.keys())
    candidates = []
    for r in ranking:
        sym = r["symbol"]
        if sym not in cs_universe:
            continue
        candidates.append({
            "symbol":     sym,
            "name":       r["name"],
            "price_usd":  r["price_usd"],
            "price_aud":  cs[sym].get("last_aud") or 0,
            "change_24h": r["change_24h"],
            "volume_24h": r["volume_24h"],
            "market_cap": r["market_cap"],
        })

    scored = []
    for c in candidates:
        c["opportunity_score"] = round(_score_crypto(c), 2)
        c["reasons"] = _crypto_reasons(c)
        c["tier"]    = _crypto_tier(c)
        scored.append(c)

    scored.sort(key=lambda x: x["opportunity_score"], reverse=True)
    result = scored[:50]
    log.info(f"Crypto scan: {len(result)} candidates after filtering "
             f"(of {len(cs_universe)} tradeable, {len(ranking)} globally ranked)")
    _cache_set("crypto_movers", result)
    return result


def _score_crypto(c: dict) -> float:
    score = 0.0
    chg = c.get("change_24h", 0)
    vol = c.get("volume_24h", 0)
    mc  = c.get("market_cap", 0)

    # Momentum
    if 2 < chg < 15:        score += 1.5
    elif 15 <= chg < 30:    score += 1.0
    elif -10 < chg < -3:    score += 1.2
    elif chg >= 30:         score += 0.3
    elif chg <= -20:        score += 0.5

    # Volume
    if vol > 50_000_000:    score += 1.0
    elif vol > 10_000_000:  score += 0.5

    # Market cap
    if mc > 1_000_000_000:  score += 0.5
    elif mc < 10_000_000:   score -= 1.0

    return max(0, score)


def _crypto_reasons(c: dict) -> list:
    reasons = []
    chg = c.get("change_24h", 0)
    vol = c.get("volume_24h", 0)
    if chg > 5:
        reasons.append(f"+{chg:.1f}% 24h")
    elif chg < -5:
        reasons.append(f"{chg:.1f}% 24h — reversal watch")
    if vol > 100_000_000:
        reasons.append(f"High volume ${vol/1e6:.0f}M")
    elif vol < 1_000_000:
        reasons.append("Low liquidity")
    return reasons


def _crypto_tier(c: dict) -> str:
    mc = c.get("market_cap", 0)
    if mc > 100_000_000_000:  return "blue_chip"
    elif mc > 10_000_000_000: return "large_cap"
    elif mc > 1_000_000_000:  return "mid_cap"
    elif mc > 100_000_000:    return "small_cap"
    else:                     return "micro_cap"


# ─── Stock scan (unchanged) ────────────────────────────────────────────────

def get_stock_movers() -> list:
    cached = _cache_get("stock_movers", CACHE_TTL_FRESH)
    if cached:
        log.info(f"Stock scan: {len(cached)} candidates (cached)")
        return cached

    tickers = set(STOCK_STAPLES)
    try:
        r = requests.get(f"{ALPACA_DATA_URL}/v1beta1/screener/stocks/most-actives",
                         headers=ALPACA_HEADERS, params={"top": 30}, timeout=10)
        if r.status_code == 200:
            for a in r.json().get("most_actives", []):
                sym = a.get("symbol")
                if sym:
                    tickers.add(sym)
    except Exception as e:
        log.warning(f"Alpaca most-actives failed: {e}")

    opportunities = []
    for sym in list(tickers)[:40]:
        try:
            r = requests.get(f"{ALPACA_DATA_URL}/v2/stocks/{sym}/bars",
                             headers=ALPACA_HEADERS,
                             params={"timeframe": "1Day",
                                     "start": (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")},
                             timeout=8)
            if r.status_code != 200:
                continue
            bars = r.json().get("bars", [])
            if len(bars) < 2:
                continue
            closes = [b["c"] for b in bars]
            vols   = [b["v"] for b in bars]
            price  = closes[-1]
            chg_1d = (closes[-1] - closes[-2]) / closes[-2] * 100
            chg_5d = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else 0
            vol_ratio = vols[-1] / (sum(vols[:-1]) / max(1, len(vols) - 1))
            score = 0.0
            if 1 < chg_1d < 8: score += 1.0
            elif chg_1d > 8: score += 0.3
            elif -5 < chg_1d < -1: score += 0.8
            if vol_ratio > 1.5: score += 1.0
            if chg_5d > 0 and chg_1d > 0: score += 0.5
            opportunities.append({
                "symbol": sym, "price_usd": round(price, 2),
                "change_1d_pct": round(chg_1d, 2),
                "change_5d_pct": round(chg_5d, 2),
                "volume_ratio":  round(vol_ratio, 2),
                "opportunity_score": round(score, 2),
            })
        except Exception as e:
            log.debug(f"Stock {sym}: {e}")
            continue
        time.sleep(0.1)

    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    result = opportunities[:20]
    if not result:
        return _stale_or_empty("stock_movers")
    _cache_set("stock_movers", result)
    return result


# ─── News + entry point (unchanged) ────────────────────────────────────────

def get_market_news() -> list:
    try:
        r = requests.get(f"{ALPACA_DATA_URL}/v1beta1/news",
                         headers=ALPACA_HEADERS, params={"limit": 10}, timeout=10)
        if r.status_code == 200:
            return [{"headline": a.get("headline", ""),
                     "symbols": a.get("symbols", [])[:3]}
                    for a in r.json().get("news", [])[:6]]
    except Exception as e:
        log.debug(f"News fetch failed: {e}")
    return []


def run_full_scan() -> dict:
    log.info("run_full_scan: starting")
    stocks = get_stock_movers()
    log.info(f"Stocks scanned: {len(stocks)}")
    crypto = get_crypto_movers()
    log.info(f"Crypto scanned: {len(crypto)}")
    news = get_market_news()
    return {
        "stock_opportunities":  stocks,
        "crypto_opportunities": crypto,
        "news":                 news,
        "stocks_scanned":       len(stocks),
        "crypto_scanned":       len(crypto),
        "scanned_at":           datetime.utcnow().isoformat(),
    }
