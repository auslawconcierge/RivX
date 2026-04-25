"""
RivX scanner.py — dynamic universe builder for stocks + crypto.

Data sources, in priority order for crypto:
  1. CoinGecko /coins/markets (free tier, rate-limited — RESULTS CACHED 10 MIN)
  2. CoinPaprika /v1/tickers   (fallback — 25k calls/month free, no auth needed)
  3. Last cached scan          (graceful degradation if both fail)

The previous version was hammered by CoinGecko's free-tier rate limit and
returned 0 candidates whenever it got 429'd. That blocked every crypto buy
because crypto_check filters on score >= 2.5 and an empty universe filters
to nothing. Cache + fallback fixes the data starvation.
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

CG_BASE  = "https://api.coingecko.com/api/v3"
CGD_KEY  = os.environ.get("COINGECKO_API_KEY", "")  # optional — Demo plan = 30/min
CP_BASE  = "https://api.coinpaprika.com/v1"

# ── Cache ───────────────────────────────────────────────────────────────────
# File cache survives bot restarts; survives 429 storms; survives CoinGecko outages.
CACHE_DIR = Path(os.environ.get("RIVX_CACHE_DIR", "/tmp/rivx_cache"))
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    CACHE_DIR = Path("/tmp")
CACHE_TTL_FRESH = 600    # 10 min — return as-fresh
CACHE_TTL_STALE = 86400  # 24 h  — usable as last-resort fallback

def _cache_get(key: str, max_age: int) -> dict | list | None:
    p = CACHE_DIR / f"{key}.json"
    if not p.exists():
        return None
    age = time.time() - p.stat().st_mtime
    if age > max_age:
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


# ── Fallback universe + staples ─────────────────────────────────────────────
COINSPOT_FALLBACK = {
    "BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC","BCH","DOT",
    "UNI","AAVE","MATIC","ATOM","ALGO","NEAR","FTM","SAND","MANA","CRV",
    "GRT","SUSHI","MKR","SNX","PEPE","SHIB","FLOKI","WIF","BONK","FET",
    "RNDR","TAO","TRX","TON","APT","SUI","SEI","TIA","INJ","ICP","ARB",
    "OP","HBAR","FIL","VET","STX","IMX","RUNE","JUP","PYTH","JTO","WLD","ENA"
}

STOCK_STAPLES = ["SPY", "QQQ", "IWM", "NVDA", "AAPL", "MSFT", "META", "TSLA",
                 "AMD", "GOOGL", "AMZN", "NFLX"]


# ── CoinGecko with retry/backoff/cache ─────────────────────────────────────
def _cg_get(endpoint: str, params: dict, label: str = "") -> list | dict | None:
    """GET from CoinGecko with backoff. Demo key used if available."""
    headers = {"x-cg-demo-api-key": CGD_KEY} if CGD_KEY else {}
    for attempt in range(4):
        try:
            r = requests.get(f"{CG_BASE}{endpoint}",
                             params=params, headers=headers, timeout=20)
            if r.status_code == 429:
                wait = 8 * (attempt + 1)
                log.warning(f"CoinGecko {label}: 429 rate-limit, waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 3:
                log.warning(f"CoinGecko {label} failed after retries: {e}")
                return None
            time.sleep(4 * (attempt + 1))
    return None


# ── CoinPaprika fallback ────────────────────────────────────────────────────
def _coinpaprika_top(limit: int = 200) -> list | None:
    """
    CoinPaprika's /tickers — generous free tier, no auth. Returns top-N coins
    sorted by market cap. We adapt to the same shape CoinGecko produces so the
    rest of the scanner doesn't need to change.
    """
    try:
        r = requests.get(f"{CP_BASE}/tickers", params={"limit": limit}, timeout=15)
        r.raise_for_status()
        rows = r.json() or []
        out = []
        for row in rows[:limit]:
            q = (row.get("quotes") or {}).get("USD") or {}
            out.append({
                "id":            row.get("id"),
                "symbol":        (row.get("symbol") or "").upper(),
                "name":          row.get("name"),
                "current_price": q.get("price"),
                "price_change_percentage_24h": q.get("percent_change_24h"),
                "total_volume":  q.get("volume_24h"),
                "market_cap":    q.get("market_cap"),
            })
        return out
    except Exception as e:
        log.warning(f"CoinPaprika fetch failed: {e}")
        return None


# ── CoinSpot tradeable universe ─────────────────────────────────────────────
def _get_coinspot_universe() -> set:
    cached = _cache_get("coinspot_universe", CACHE_TTL_FRESH)
    if cached:
        return set(cached)

    for url in [
        "https://www.coinspot.com.au/pubapi/v2/latest",
        "https://www.coinspot.com.au/pubapi/latest",
    ]:
        try:
            r = requests.get(url, timeout=8)
            if r.status_code == 200:
                data = r.json()
                prices = data.get("prices") or data
                if isinstance(prices, dict):
                    syms = {k.upper() for k in prices.keys() if isinstance(k, str)}
                    if len(syms) > 20:
                        log.info(f"CoinSpot universe: {len(syms)} symbols from {url}")
                        _cache_set("coinspot_universe", list(syms))
                        return syms
        except Exception as e:
            log.debug(f"CoinSpot {url}: {e}")

    # Last-ditch stale cache, then hard fallback
    stale = _cache_get("coinspot_universe", CACHE_TTL_STALE)
    if stale:
        log.warning(f"CoinSpot endpoints unavailable — using stale cache ({len(stale)} symbols)")
        return set(stale)

    log.warning(f"CoinSpot endpoints unavailable — using fallback set ({len(COINSPOT_FALLBACK)} symbols)")
    return COINSPOT_FALLBACK


# ── Crypto scan ─────────────────────────────────────────────────────────────
def get_crypto_movers() -> list:
    """
    Build crypto opportunity list. Cache-first: if we have a fresh scan from
    the last 10 min, return that without hitting any external API. This is
    what fixes the rate-limit starvation problem.
    """
    cached = _cache_get("crypto_movers", CACHE_TTL_FRESH)
    if cached:
        log.info(f"Crypto scan: {len(cached)} candidates (cached)")
        return cached

    cs_universe = _get_coinspot_universe()
    candidates = {}

    def absorb(coins):
        if not coins:
            return
        for c in coins:
            sym = (c.get("symbol") or "").upper()
            if sym not in cs_universe:
                continue
            if sym in candidates:
                continue
            candidates[sym] = {
                "symbol":       sym,
                "coingecko_id": c.get("id"),
                "name":         c.get("name"),
                "price_usd":    float(c.get("current_price") or 0),
                "change_24h":   float(c.get("price_change_percentage_24h") or 0),
                "volume_24h":   float(c.get("total_volume") or 0),
                "market_cap":   float(c.get("market_cap") or 0),
            }

    # Pass 1: CoinGecko top by volume
    data = _cg_get("/coins/markets",
                   {"vs_currency":"usd", "order":"volume_desc", "per_page":200, "page":1,
                    "price_change_percentage":"24h"}, "volume")
    absorb(data or [])

    # If CG completely failed, try CoinPaprika
    if not candidates:
        log.warning("CoinGecko returned nothing — trying CoinPaprika fallback")
        absorb(_coinpaprika_top(limit=200) or [])

    # If we still have nothing, return STALE cache (better than empty)
    if not candidates:
        stale = _cache_get("crypto_movers", CACHE_TTL_STALE)
        if stale:
            log.warning(f"All live sources failed — returning stale crypto scan ({len(stale)} candidates)")
            return stale
        log.error("Crypto scan: ALL data sources failed and no cache — returning empty")
        return []

    # Optional extra passes — only if pass 1 succeeded and we have time
    if 0 < len(candidates) < 100:
        time.sleep(8)
        data = _cg_get("/coins/markets",
                       {"vs_currency":"usd", "order":"price_change_percentage_24h_desc",
                        "per_page":50, "page":1, "price_change_percentage":"24h"}, "gainers")
        absorb(data or [])
        time.sleep(8)
        data = _cg_get("/coins/markets",
                       {"vs_currency":"usd", "order":"price_change_percentage_24h_asc",
                        "per_page":30, "page":1, "price_change_percentage":"24h"}, "losers")
        absorb(data or [])

    # Score
    scored = []
    for c in candidates.values():
        score = _score_crypto(c)
        c["opportunity_score"] = round(score, 2)
        c["reasons"] = _crypto_reasons(c)
        c["tier"]    = _crypto_tier(c)
        scored.append(c)
    scored.sort(key=lambda x: x["opportunity_score"], reverse=True)
    result = scored[:50]

    log.info(f"Crypto scan: {len(result)} candidates after filtering")
    _cache_set("crypto_movers", result)
    return result


def _score_crypto(c: dict) -> float:
    score = 0.0
    chg = c.get("change_24h", 0)
    vol = c.get("volume_24h", 0)
    mc  = c.get("market_cap", 0)
    if 2 < chg < 15: score += 1.5
    elif 15 <= chg < 30: score += 1.0
    elif -10 < chg < -3: score += 1.2
    elif chg >= 30: score += 0.3
    elif chg <= -20: score += 0.5
    if vol > 50_000_000: score += 1.0
    elif vol > 10_000_000: score += 0.5
    if mc > 1_000_000_000: score += 0.5
    elif mc < 10_000_000: score -= 1.0
    return max(0, score)


def _crypto_reasons(c: dict) -> list:
    reasons = []
    chg = c.get("change_24h", 0)
    vol = c.get("volume_24h", 0)
    if chg > 5: reasons.append(f"+{chg:.1f}% 24h")
    elif chg < -5: reasons.append(f"{chg:.1f}% 24h — reversal watch")
    if vol > 100_000_000: reasons.append(f"High volume ${vol/1e6:.0f}M")
    elif vol < 1_000_000: reasons.append("Low liquidity")
    return reasons


def _crypto_tier(c: dict) -> str:
    mc = c.get("market_cap", 0)
    if mc > 100_000_000_000: return "blue_chip"
    elif mc > 10_000_000_000: return "large_cap"
    elif mc > 1_000_000_000:  return "mid_cap"
    elif mc > 100_000_000:    return "small_cap"
    else: return "micro_cap"


# ── Stock scan ──────────────────────────────────────────────────────────────
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
        stale = _cache_get("stock_movers", CACHE_TTL_STALE)
        if stale:
            log.warning(f"Stock scan failed live — returning stale cache ({len(stale)})")
            return stale

    _cache_set("stock_movers", result)
    return result


# ── News (best-effort) ──────────────────────────────────────────────────────
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
