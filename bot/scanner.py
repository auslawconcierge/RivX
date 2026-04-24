"""
scanner.py — Broad market opportunity scanner.

Data sources:
- Stocks/ETFs: Alpaca (prices, bars) + Alpaca's "most active" feed for dynamic universe
- Crypto data: CoinGecko free API (prices, volumes, 24h changes, historical bars for RSI)
- Crypto universe: CoinSpot's public coins endpoint (only surface coins we can actually trade)
- News: Alpaca news API

Crypto scan strategy (dual-pass):
  1. Top 200 CoinSpot-tradeable coins by 24h volume — consistent coverage
  2. Today's top gainers + top losers from CoinGecko — catches the movers
  Union deduped, scored, ranked. Typically ~220-260 coins per scan.

Stock scan strategy:
  - Alpaca's most-active endpoint for dynamic daily universe (top 50)
  - Fallback to a curated list if the endpoint fails
"""

import logging
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_DATA_URL, ALPACA_BASE_URL

log = logging.getLogger(__name__)

ALPACA_HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

CG_BASE       = "https://api.coingecko.com/api/v3"
COINSPOT_BASE = "https://www.coinspot.com.au/pubapi/v2"

# ─── Fallback lists (used only if dynamic fetches fail) ────────────────────
FALLBACK_STOCK_UNIVERSE = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","SMCI","ARM",
    "AVGO","QCOM","MU","INTC","PLTR","SNOW","NET","CRWD","ZS","DDOG","MDB",
    "MSTR","COIN","HOOD","RKLB","IONQ","RGTI","MRNA","BNTX","NVAX",
    "SPY","QQQ","ARKK","SOXL","TQQQ","XOM","CVX","SLB","JPM","GS","BAC",
    "NFLX","DIS","ABNB","UBER","LYFT",
]

# ─── Universe fetchers ─────────────────────────────────────────────────────

def get_coinspot_universe() -> set:
    """
    Fetch the set of coin symbols CoinSpot currently lists.
    Returns a set of uppercase ticker symbols like {'BTC', 'ETH', 'PEPE', ...}
    Falls back to a known-good set if the endpoint fails.
    """
    try:
        r = requests.get(f"{COINSPOT_BASE}/coins", timeout=10)
        r.raise_for_status()
        data = r.json()
        # CoinSpot response shape: {"status":"ok","coins":{"BTC":{...},"ETH":{...},...}}
        coins = data.get("coins") or {}
        if isinstance(coins, dict) and coins:
            symbols = {sym.upper() for sym in coins.keys()}
            log.info(f"CoinSpot universe: {len(symbols)} tradeable coins")
            return symbols
        # Some versions return a list — handle that too
        if isinstance(coins, list):
            symbols = {c.get("symbol","").upper() for c in coins if c.get("symbol")}
            if symbols:
                log.info(f"CoinSpot universe: {len(symbols)} tradeable coins")
                return symbols
        raise ValueError("unexpected shape")
    except Exception as e:
        log.warning(f"CoinSpot universe fetch failed ({e}) — using fallback set")
        return {
            "BTC","ETH","XRP","LTC","DOGE","ADA","DOT","LINK","SOL","MATIC",
            "AVAX","UNI","ATOM","ALGO","BCH","XLM","VET","TRX","ETC","FIL",
            "NEAR","ICP","APT","ARB","OP","INJ","SUI","TIA","SEI","PEPE",
            "SHIB","FLOKI","WIF","BONK","FET","RNDR","TAO","AAVE","MKR","SNX",
            "CRV","COMP","LDO","GRT","SAND","MANA","AXS","APE","ENJ","CHZ",
        }


def get_stock_universe() -> list:
    """
    Dynamic stock universe — pulls Alpaca's most-active feed if available,
    falls back to a curated list of high-beta names.
    """
    try:
        r = requests.get(
            f"{ALPACA_DATA_URL}/v1beta1/screener/stocks/most-actives",
            headers=ALPACA_HEADERS,
            params={"by": "volume", "top": 50},
            timeout=10,
        )
        if r.ok:
            movers = r.json().get("most_actives", [])
            syms = [m.get("symbol") for m in movers if m.get("symbol")]
            if syms:
                # Blend in some staples so we don't drift too far from what we know
                staples = ["SPY","QQQ","NVDA","TSLA","AAPL","META","AMD","MSTR","COIN","PLTR"]
                merged = list(dict.fromkeys(syms + staples))[:60]
                log.info(f"Stock universe: {len(merged)} (dynamic movers + staples)")
                return merged
    except Exception as e:
        log.warning(f"Alpaca most-actives fetch failed ({e}) — using fallback universe")
    log.info(f"Stock universe: {len(FALLBACK_STOCK_UNIVERSE)} (fallback)")
    return list(FALLBACK_STOCK_UNIVERSE)


def get_crypto_candidates(coinspot_symbols: set) -> list:
    """
    Build the crypto candidate list from two CoinGecko passes:
      1. Top 200 coins by 24h volume
      2. Top 30 gainers and top 30 losers by 24h % change
    Both filtered to coins CoinSpot actually lists.
    Returns a list of dicts with: id, symbol, price_usd, change_24h, volume, market_cap
    """
    candidates = {}

    def absorb(rows):
        for c in rows or []:
            sym = (c.get("symbol") or "").upper()
            if not sym or sym not in coinspot_symbols:
                continue
            if sym in candidates:
                continue
            candidates[sym] = {
                "id":          c.get("id"),
                "symbol":      sym,
                "price_usd":   float(c.get("current_price") or 0),
                "change_24h":  float(c.get("price_change_percentage_24h") or 0),
                "volume_24h":  float(c.get("total_volume") or 0),
                "market_cap":  float(c.get("market_cap") or 0),
            }

    # Pass 1 — top 200 by volume
    try:
        r = requests.get(
            f"{CG_BASE}/coins/markets",
            params={"vs_currency":"usd", "order":"volume_desc",
                    "per_page":250, "page":1, "price_change_percentage":"24h"},
            timeout=15,
        )
        r.raise_for_status()
        absorb(r.json())
    except Exception as e:
        log.warning(f"CoinGecko volume pass failed: {e}")

    # Pass 2 — top gainers (by 24h % change)
    try:
        r = requests.get(
            f"{CG_BASE}/coins/markets",
            params={"vs_currency":"usd", "order":"price_change_percentage_24h_desc",
                    "per_page":50, "page":1, "price_change_percentage":"24h"},
            timeout=15,
        )
        r.raise_for_status()
        absorb(r.json())
    except Exception as e:
        log.warning(f"CoinGecko gainers pass failed: {e}")

    # Pass 3 — top losers (reversals are also opportunities)
    try:
        r = requests.get(
            f"{CG_BASE}/coins/markets",
            params={"vs_currency":"usd", "order":"price_change_percentage_24h_asc",
                    "per_page":30, "page":1, "price_change_percentage":"24h"},
            timeout=15,
        )
        r.raise_for_status()
        absorb(r.json())
    except Exception as e:
        log.warning(f"CoinGecko losers pass failed: {e}")

    result = list(candidates.values())
    log.info(f"Crypto candidates after CoinSpot filter: {len(result)}")
    return result


# ─── Scoring helpers ───────────────────────────────────────────────────────

def _calc_rsi(closes: pd.Series, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    delta = closes.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    val   = rsi.iloc[-1]
    return float(val) if not pd.isna(val) else None


def _cg_hourly_closes(coin_id: str, hours: int = 48) -> pd.Series:
    """Fetch hourly closes from CoinGecko for RSI calculation. Returns empty Series on fail."""
    try:
        days = max(2, int(hours / 24) + 1)
        r = requests.get(
            f"{CG_BASE}/coins/{coin_id}/market_chart",
            params={"vs_currency":"usd", "days":days},
            timeout=10,
        )
        r.raise_for_status()
        prices = r.json().get("prices") or []
        if not prices:
            return pd.Series(dtype=float)
        closes = pd.Series([p[1] for p in prices], dtype=float)
        return closes.tail(hours * 2)  # a bit extra for RSI warmup
    except Exception:
        return pd.Series(dtype=float)


def _fetch_bars(symbol: str, days: int = 20) -> pd.DataFrame:
    start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    url   = f"{ALPACA_DATA_URL}/v2/stocks/{symbol}/bars"
    r     = requests.get(url, headers=ALPACA_HEADERS,
                         params={"timeframe":"1Day", "start":start}, timeout=8)
    r.raise_for_status()
    bars = r.json().get("bars", [])
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df.rename(columns={"c":"close","o":"open","h":"high","l":"low","v":"volume"}, inplace=True)
    return df


# ─── Scoring ───────────────────────────────────────────────────────────────

def _score_stock(symbol: str) -> dict | None:
    df = _fetch_bars(symbol)
    if df.empty or len(df) < 5:
        return None
    closes  = df["close"].astype(float)
    volumes = df["volume"].astype(float)
    price   = float(closes.iloc[-1])
    if price < 3:  # skip sub-$3 penny stocks
        return None

    rsi           = _calc_rsi(closes)
    raw_change_1d = float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100) if len(closes) >= 2 else 0
    change_1d     = raw_change_1d if abs(raw_change_1d) <= 25 else 0
    change_5d     = float((closes.iloc[-1] - closes.iloc[-5]) / closes.iloc[-5] * 100) if len(closes) >= 5 else 0
    avg_vol       = float(volumes.rolling(10).mean().iloc[-1]) if len(volumes) >= 10 else float(volumes.mean())
    vol_ratio     = float(volumes.iloc[-1] / avg_vol) if avg_vol > 0 else 1
    ma5           = float(closes.rolling(5).mean().iloc[-1]) if len(closes) >= 5 else price
    ma20          = float(closes.rolling(20).mean().iloc[-1]) if len(closes) >= 20 else price

    score = 0.0
    reasons = []
    if rsi:
        if rsi < 30:   score += 3.0; reasons.append(f"RSI oversold ({rsi:.0f})")
        elif rsi < 40: score += 1.5; reasons.append(f"RSI low ({rsi:.0f})")
        elif rsi > 70: score += 1.0; reasons.append(f"RSI momentum ({rsi:.0f})")
    if vol_ratio > 2.0: score += 2.0; reasons.append(f"Volume spike {vol_ratio:.1f}x")
    elif vol_ratio > 1.5: score += 1.0; reasons.append(f"Above avg volume {vol_ratio:.1f}x")
    if abs(change_1d) > 3: score += 1.5; reasons.append(f"Strong move {change_1d:+.1f}% today")
    if abs(change_5d) > 8: score += 1.0; reasons.append(f"5d trend {change_5d:+.1f}%")
    if price > ma5 > ma20: score += 1.0; reasons.append("Uptrend (price > MA5 > MA20)")
    elif price < ma5 < ma20: score += 0.5; reasons.append("Downtrend — reversal watch")

    if score < 1.0:
        return None
    return {
        "symbol": symbol, "price": round(price, 2),
        "change_1d_pct": round(change_1d, 2), "change_5d_pct": round(change_5d, 2),
        "rsi": round(rsi, 1) if rsi else None, "volume_ratio": round(vol_ratio, 2),
        "opportunity_score": round(score, 2), "reasons": reasons, "type": "stock",
    }


def _score_crypto(candidate: dict, fetch_rsi: bool = True) -> dict | None:
    """
    Score a crypto candidate from CoinGecko market data.
    RSI fetched only for coins that already look interesting on cheap signals,
    to stay well under CoinGecko rate limits.
    """
    sym         = candidate["symbol"]
    price       = candidate["price_usd"]
    change_24h  = candidate["change_24h"]
    volume_24h  = candidate["volume_24h"]
    market_cap  = candidate["market_cap"]
    coin_id     = candidate["id"]

    # Classify risk tier for the reasoning — no filtering, just labelling
    if market_cap >= 10_000_000_000:   tier = "blue_chip"
    elif market_cap >= 1_000_000_000:  tier = "large_cap"
    elif market_cap >= 100_000_000:    tier = "mid_cap"
    elif market_cap >= 10_000_000:     tier = "small_cap"
    else:                              tier = "micro_cap"

    score = 0.0
    reasons = []

    # Cheap scoring first — don't call RSI endpoint for every coin
    if abs(change_24h) >= 15:  score += 3.0; reasons.append(f"Big move {change_24h:+.1f}% 24h")
    elif abs(change_24h) >= 8: score += 2.0; reasons.append(f"Strong move {change_24h:+.1f}% 24h")
    elif abs(change_24h) >= 4: score += 1.0; reasons.append(f"Move {change_24h:+.1f}% 24h")

    # Volume sanity — a mover on $100k daily volume is a trap, score it down
    if volume_24h >= 50_000_000:   score += 1.0; reasons.append("High liquidity")
    elif volume_24h >= 5_000_000:  score += 0.3
    elif volume_24h < 500_000:     score -= 2.0; reasons.append("Very thin volume — risky")

    # Small caps with genuine moves get a bonus (upside potential)
    if tier in ("small_cap","mid_cap") and abs(change_24h) >= 8:
        score += 1.0; reasons.append(f"{tier} mover")
    if tier == "micro_cap" and abs(change_24h) >= 15:
        score += 0.5; reasons.append("Micro-cap momentum — high risk")

    # Only fetch hourly bars for coins already scoring interesting
    rsi = None
    if fetch_rsi and score >= 1.5 and coin_id:
        closes = _cg_hourly_closes(coin_id)
        if len(closes) >= 15:
            rsi = _calc_rsi(closes)
            if rsi is not None:
                if rsi < 30:   score += 2.0; reasons.append(f"RSI oversold ({rsi:.0f})")
                elif rsi < 40: score += 1.0; reasons.append(f"RSI low ({rsi:.0f})")
                elif rsi > 70: score += 1.0; reasons.append(f"RSI overbought ({rsi:.0f})")
        time.sleep(0.3)  # polite throttle so we don't trip CoinGecko rate limits

    if score < 1.0:
        return None

    return {
        "symbol":             sym,
        "price_usd":          round(price, 6) if price < 1 else round(price, 4),
        "change_1d_pct":      round(change_24h, 2),
        "change_7d_pct":      0,  # not pulled in this pass to save API calls
        "rsi":                round(rsi, 1) if rsi else None,
        "volume_24h_usd":     round(volume_24h, 0),
        "market_cap_usd":     round(market_cap, 0),
        "tier":               tier,
        "opportunity_score":  round(score, 2),
        "reasons":            reasons,
        "type":               "crypto",
    }


# ─── Public entry points ───────────────────────────────────────────────────

def get_stock_movers() -> list:
    universe = get_stock_universe()
    log.info(f"Scanning {len(universe)} stocks for opportunities...")
    opps = []
    for sym in universe:
        try:
            s = _score_stock(sym)
            if s:
                opps.append(s)
        except Exception as e:
            log.debug(f"Skip {sym}: {e}")
    opps.sort(key=lambda x: x["opportunity_score"], reverse=True)
    top = opps[:10]
    log.info(f"Top stock opportunities: {[o['symbol'] for o in top]}")
    return top


def get_crypto_movers() -> list:
    """
    Broad crypto scan: CoinSpot universe × CoinGecko data.
    Two passes (top volume + today's movers), deduped, scored, ranked.
    """
    coinspot = get_coinspot_universe()
    candidates = get_crypto_candidates(coinspot)

    if not candidates:
        log.warning("Crypto scan: no candidates returned, check CoinGecko/CoinSpot endpoints")
        return []

    log.info(f"Scanning {len(candidates)} crypto pairs for opportunities...")
    opps = []
    for cand in candidates:
        try:
            s = _score_crypto(cand)
            if s:
                opps.append(s)
        except Exception as e:
            log.debug(f"Skip {cand.get('symbol')}: {e}")

    opps.sort(key=lambda x: x["opportunity_score"], reverse=True)
    top = opps[:12]
    log.info(f"Top crypto opportunities: {[o['symbol'] for o in top]}")
    return top


def get_market_news(symbols: list) -> list:
    try:
        stock_syms = [s for s in symbols if "/" not in s][:15]
        if not stock_syms:
            return []
        r = requests.get(
            f"{ALPACA_DATA_URL}/v1beta1/news",
            headers=ALPACA_HEADERS,
            params={"symbols": ",".join(stock_syms), "limit": 15, "sort": "desc"},
            timeout=10,
        )
        r.raise_for_status()
        return [
            {
                "headline":  a.get("headline",""),
                "symbols":   a.get("symbols",[]),
                "summary":   (a.get("summary","") or "")[:200],
                "published": a.get("created_at","")[:10],
            }
            for a in r.json().get("news", [])[:10]
        ]
    except Exception as e:
        log.warning(f"News fetch failed: {e}")
        return []


def run_full_scan() -> dict:
    log.info("Running full market scan...")
    stock_opps  = get_stock_movers()
    crypto_opps = get_crypto_movers()
    all_symbols = [o["symbol"] for o in stock_opps + crypto_opps]
    news        = get_market_news(all_symbols)
    log.info(f"Scan complete: {len(stock_opps)} stock opps, {len(crypto_opps)} crypto opps")
    return {
        "stock_opportunities":  stock_opps,
        "crypto_opportunities": crypto_opps,
        "news":                 news,
        "scan_time":            datetime.utcnow().isoformat(),
        "stocks_scanned":       len(get_stock_universe()),
        "crypto_scanned":       len(crypto_opps) if crypto_opps else 0,
    }
