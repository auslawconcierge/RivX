"""
RivX scanner.py — dynamic universe builder for stocks + crypto.

Data sources:
  - CoinGecko (crypto prices, 24h movers) — free tier, rate-limited, respected
  - CoinSpot public coin list (tradeable universe filter) — optional
  - Alpaca (stock most-actives) — free with account
"""

import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_DATA_URL

log = logging.getLogger(__name__)

ALPACA_HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

CG_BASE = "https://api.coingecko.com/api/v3"

# Fallback crypto universe if CoinSpot endpoint fails
COINSPOT_FALLBACK = {
    "BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC","BCH","DOT",
    "UNI","AAVE","MATIC","ATOM","ALGO","NEAR","FTM","SAND","MANA","CRV",
    "GRT","SUSHI","MKR","SNX","PEPE","SHIB","FLOKI","WIF","BONK","FET",
    "RNDR","TAO","TRX","TON","APT","SUI","SEI","TIA","INJ","ICP","ARB",
    "OP","HBAR","FIL","VET","STX","IMX","RUNE"
}

# Stock staples — always included regardless of most-actives
STOCK_STAPLES = ["SPY", "QQQ", "IWM", "NVDA", "AAPL", "MSFT", "META", "TSLA",
                 "AMD", "GOOGL", "AMZN", "NFLX"]


# ─── CoinGecko with retry/backoff ──────────────────────────────────────────

def _cg_get(endpoint: str, params: dict, label: str = "") -> list | dict | None:
    """GET from CoinGecko with exponential backoff on 429. Returns None on failure."""
    for attempt in range(4):
        try:
            r = requests.get(f"{CG_BASE}{endpoint}", params=params, timeout=20)
            if r.status_code == 429:
                wait = 8 * (attempt + 1)  # 8, 16, 24, 32 seconds
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


# ─── CoinSpot tradeable universe ───────────────────────────────────────────

def _get_coinspot_universe() -> set:
    """Return set of symbols CoinSpot actually lists. Falls back to hardcoded set."""
    for url in [
        "https://www.coinspot.com.au/pubapi/v2/latest",       # has all coin prices
        "https://www.coinspot.com.au/pubapi/latest",          # v1 fallback
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
                        return syms
        except Exception as e:
            log.debug(f"CoinSpot {url}: {e}")

    log.warning(f"CoinSpot endpoints unavailable — using fallback set ({len(COINSPOT_FALLBACK)} symbols)")
    return COINSPOT_FALLBACK


# ─── Crypto scan ───────────────────────────────────────────────────────────

def get_crypto_movers() -> list:
    """Build crypto opportunity list. Returns [] if all sources fail."""
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
                "symbol":     sym,
                "coingecko_id": c.get("id"),
                "name":       c.get("name"),
                "price_usd":  float(c.get("current_price") or 0),
                "change_24h": float(c.get("price_change_percentage_24h") or 0),
                "volume_24h": float(c.get("total_volume") or 0),
                "market_cap": float(c.get("market_cap") or 0),
            }

    # Pass 1: top 200 by volume — this single pass gives us most of what we need
    data = _cg_get("/coins/markets",
                   {"vs_currency":"usd", "order":"volume_desc", "per_page":200, "page":1,
                    "price_change_percentage":"24h"}, "volume")
    absorb(data or [])

    # Only run extra passes if Pass 1 succeeded AND we have time
    # (skip gainers/losers if we already have 100+ candidates — it's overkill)
    if candidates and len(candidates) < 100:
        time.sleep(8)  # gentler spacing
        data = _cg_get("/coins/markets",
                       {"vs_currency":"usd", "order":"price_change_percentage_24h_desc",
                        "per_page":50, "page":1, "price_change_percentage":"24h"}, "gainers")
        absorb(data or [])

        time.sleep(8)
        data = _cg_get("/coins/markets",
                       {"vs_currency":"usd", "order":"price_change_percentage_24h_asc",
                        "per_page":30, "page":1, "price_change_percentage":"24h"}, "losers")
        absorb(data or [])

    # Score each candidate
    scored = []
    for c in candidates.values():
        score = _score_crypto(c)
        c["opportunity_score"] = round(score, 2)
        c["reasons"] = _crypto_reasons(c)
        c["tier"] = _crypto_tier(c)
        scored.append(c)

    # Sort by score, highest first
    scored.sort(key=lambda x: x["opportunity_score"], reverse=True)
    log.info(f"Crypto scan: {len(scored)} candidates after filtering")
    return scored[:50]  # cap returned list


def _score_crypto(c: dict) -> float:
    score = 0.0
    chg = c.get("change_24h", 0)
    vol = c.get("volume_24h", 0)
    mc = c.get("market_cap", 0)

    # Momentum
    if 2 < chg < 15: score += 1.5      # healthy gainer
    elif 15 <= chg < 30: score += 1.0  # strong but watch for FOMO top
    elif -10 < chg < -3: score += 1.2  # pullback reversal setup
    elif chg >= 30: score += 0.3        # chasing risk
    elif chg <= -20: score += 0.5       # falling knife

    # Volume signal
    if vol > 50_000_000: score += 1.0
    elif vol > 10_000_000: score += 0.5

    # Market cap preference — avoid dead coins
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
    elif mc > 1_000_000_000: return "mid_cap"
    elif mc > 100_000_000: return "small_cap"
    else: return "micro_cap"


# ─── Stock scan ────────────────────────────────────────────────────────────

def get_stock_movers() -> list:
    """Get Alpaca most-actives blended with staples."""
    tickers = set(STOCK_STAPLES)

    # Most-actives from Alpaca
    try:
        r = requests.get(f"{ALPACA_DATA_URL}/v1beta1/screener/stocks/most-actives",
                         headers=ALPACA_HEADERS,
                         params={"top": 30}, timeout=10)
        if r.status_code == 200:
            actives = r.json().get("most_actives", [])
            for a in actives:
                sym = a.get("symbol")
                if sym:
                    tickers.add(sym)
    except Exception as e:
        log.warning(f"Alpaca most-actives failed: {e}")

    # Get quotes for all
    opportunities = []
    for sym in list(tickers)[:40]:  # cap at 40 to avoid too many API calls
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
            vols = [b["v"] for b in bars]
            price = closes[-1]
            chg_1d = (closes[-1] - closes[-2]) / closes[-2] * 100
            chg_5d = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else 0
            vol_ratio = vols[-1] / (sum(vols[:-1]) / max(1, len(vols) - 1))

            score = 0.0
            if 1 < chg_1d < 8: score += 1.0
            elif chg_1d > 8: score += 0.3
            elif -5 < chg_1d < -1: score += 0.8  # dip
            if vol_ratio > 1.5: score += 1.0
            if chg_5d > 0 and chg_1d > 0: score += 0.5

            opportunities.append({
                "symbol": sym,
                "price_usd": round(price, 2),
                "change_1d_pct": round(chg_1d, 2),
                "change_5d_pct": round(chg_5d, 2),
                "volume_ratio": round(vol_ratio, 2),
                "opportunity_score": round(score, 2),
            })
        except Exception as e:
            log.debug(f"Stock {sym}: {e}")
            continue
        time.sleep(0.1)  # gentle on Alpaca

    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    return opportunities[:20]


# ─── News (best-effort) ────────────────────────────────────────────────────

def get_market_news() -> list:
    try:
        r = requests.get(f"{ALPACA_DATA_URL}/v1beta1/news",
                         headers=ALPACA_HEADERS,
                         params={"limit": 10}, timeout=10)
        if r.status_code == 200:
            return [{"headline": a.get("headline", ""),
                     "symbols": a.get("symbols", [])[:3]}
                    for a in r.json().get("news", [])[:6]]
    except Exception as e:
        log.debug(f"News fetch failed: {e}")
    return []


# ─── Main entry point ──────────────────────────────────────────────────────

def run_full_scan() -> dict:
    """Run stocks + crypto scan. Used by evening_briefing."""
    log.info("run_full_scan: starting")
    stocks = get_stock_movers()
    log.info(f"Stocks scanned: {len(stocks)}")
    crypto = get_crypto_movers()
    log.info(f"Crypto scanned: {len(crypto)}")
    news = get_market_news()

    return {
        "stock_opportunities": stocks,
        "crypto_opportunities": crypto,
        "news": news,
        "stocks_scanned": len(stocks),
        "crypto_scanned": len(crypto),
        "scanned_at": datetime.utcnow().isoformat(),
    }
