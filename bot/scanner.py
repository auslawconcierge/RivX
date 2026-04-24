"""
scanner.py — Market opportunity scanner.

Every evening before the briefing, this scans:
- Top 50 most active/volatile US stocks on Nasdaq/NYSE
- Top 20 crypto by volume
- News sentiment across all assets
- Unusual volume and momentum signals

Feeds the best opportunities to Claude who then picks
tonight's actual trading targets — different every night.
"""

import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_DATA_URL

log = logging.getLogger(__name__)

HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

# Universe of stocks to scan — broad market coverage
# Mix of high-beta tech, volatile growth, ETFs, and sector plays
SCAN_UNIVERSE = [
    # Mega cap tech
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
    # High beta / volatile growth
    "AMD", "SMCI", "ARM", "AVGO", "QCOM", "MU", "INTC",
    # AI / cloud plays
    "PLTR", "SNOW", "NET", "CRWD", "ZS", "DDOG", "MDB",
    # High momentum stocks
    "MSTR", "COIN", "HOOD", "RKLB", "IONQ", "RGTI",
    # Biotech / volatile
    "MRNA", "BNTX", "NVAX",
    # ETFs for broad exposure
    "SPY", "QQQ", "ARKK", "SOXL", "TQQQ",
    # Energy / commodities
    "XOM", "CVX", "SLB",
    # Financials
    "JPM", "GS", "BAC",
    # Consumer
    "NFLX", "DIS", "ABNB", "UBER", "LYFT",
]

# Crypto universe — all pairs available on Alpaca crypto data
# Scanner will rank these by opportunity score and Claude picks the best
# CoinSpot will execute the actual trades in AUD
CRYPTO_UNIVERSE = {
    "BTC": "BTC/USD",
    "ETH": "ETH/USD",
    "SOL": "SOL/USD",
    "XRP": "XRP/USD",
    "ADA": "ADA/USD",
    "AVAX": "AVAX/USD",
    "DOGE": "DOGE/USD",
    "LINK": "LINK/USD",
    "LTC":  "LTC/USD",
    "BCH":  "BCH/USD",
    "DOT":  "DOT/USD",
    "UNI":  "UNI/USD",
    "AAVE": "AAVE/USD",
    "MATIC":"MATIC/USD",
    "ATOM": "ATOM/USD",
    "ALGO": "ALGO/USD",
    "NEAR": "NEAR/USD",
    "FTM":  "FTM/USD",
    "SAND": "SAND/USD",
    "MANA": "MANA/USD",
    "CRV":  "CRV/USD",
    "GRT":  "GRT/USD",
    "SUSHI":"SUSHI/USD",
    "MKR":  "MKR/USD",
    "SNX":  "SNX/USD",
}


def get_stock_movers() -> list:
    """
    Scan the stock universe for the best opportunities today.
    Returns top 10 stocks ranked by opportunity score.
    """
    opportunities = []
    log.info(f"Scanning {len(SCAN_UNIVERSE)} stocks for opportunities...")

    for symbol in SCAN_UNIVERSE:
        try:
            score_data = _score_stock(symbol)
            if score_data:
                opportunities.append(score_data)
        except Exception as e:
            log.debug(f"Skip {symbol}: {e}")

    # Sort by opportunity score descending
    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    top = opportunities[:10]
    log.info(f"Top stock opportunities: {[o['symbol'] for o in top]}")
    return top


def get_crypto_movers() -> list:
    """
    Scan ALL crypto pairs on Alpaca for momentum and RSI opportunities.
    Returns top 8 crypto ranked by opportunity score.
    No predefined winners — everything gets scanned.
    """
    opportunities = []
    log.info(f"Scanning {len(CRYPTO_UNIVERSE)} crypto pairs for opportunities...")

    for coin, pair in CRYPTO_UNIVERSE.items():
        try:
            score_data = _score_crypto(coin, pair)
            if score_data:
                opportunities.append(score_data)
        except Exception as e:
            log.debug(f"Skip {coin}: {e}")

    # Always return top results even if scores are low
    # In quiet markets everything may score low but we still need data
    if not opportunities:
        # Force include top coins with basic data even if no signal
        for coin, pair in list(CRYPTO_UNIVERSE.items())[:5]:
            try:
                df = _fetch_crypto_bars(pair, days=5)
                if not df.empty:
                    closes = df["close"].astype(float)
                    opportunities.append({
                        "symbol": coin,
                        "price_usd": round(float(closes.iloc[-1]), 4),
                        "change_1d_pct": round(float((closes.iloc[-1]-closes.iloc[-2])/closes.iloc[-2]*100), 2) if len(closes)>=2 else 0,
                        "change_7d_pct": 0,
                        "rsi": None,
                        "volume_ratio": 1.0,
                        "opportunity_score": 0.0,
                        "reasons": ["Low volatility — monitoring"],
                        "type": "crypto",
                    })
            except Exception:
                pass

    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    top = opportunities[:8]
    log.info(f"Top crypto opportunities: {[o['symbol'] for o in top]}")
    return top


def get_market_news(symbols: list) -> list:
    """Fetch news for a list of symbols."""
    try:
        stock_syms = [s for s in symbols if "/" not in s][:15]
        r = requests.get(
            f"{ALPACA_DATA_URL}/v1beta1/news",
            headers=HEADERS,
            params={"symbols": ",".join(stock_syms), "limit": 15, "sort": "desc"},
            timeout=10
        )
        r.raise_for_status()
        return [
            {
                "headline": a.get("headline", ""),
                "symbols":  a.get("symbols", []),
                "summary":  a.get("summary", "")[:200],
                "published": a.get("created_at", "")[:10],
            }
            for a in r.json().get("news", [])[:10]
        ]
    except Exception as e:
        log.warning(f"News fetch failed: {e}")
        return []


def _fetch_bars(symbol: str, days: int = 20) -> pd.DataFrame:
    start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    url   = f"{ALPACA_DATA_URL}/v2/stocks/{symbol}/bars"
    r     = requests.get(url, headers=HEADERS,
                         params={"timeframe": "1Day", "start": start}, timeout=8)
    r.raise_for_status()
    bars = r.json().get("bars", [])
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df.rename(columns={"c":"close","o":"open","h":"high","l":"low","v":"volume"}, inplace=True)
    return df


def _fetch_crypto_bars(pair: str, days: int = 20) -> pd.DataFrame:
    start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = requests.get(
        f"{ALPACA_DATA_URL}/v1beta3/crypto/us/bars",
        headers=HEADERS,
        params={"symbols": pair, "timeframe": "1Day", "start": start},
        timeout=8
    )
    r.raise_for_status()
    bars = r.json().get("bars", {}).get(pair, [])
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df.rename(columns={"c":"close","o":"open","h":"high","l":"low","v":"volume"}, inplace=True)
    return df


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


def _score_stock(symbol: str) -> dict | None:
    """Score a single stock for trading opportunity."""
    df = _fetch_bars(symbol)
    if df.empty or len(df) < 5:
        return None

    closes  = df["close"].astype(float)
    volumes = df["volume"].astype(float)
    price   = float(closes.iloc[-1])

    # Skip penny stocks
    if price < 5:
        return None

    # Indicators
    rsi        = _calc_rsi(closes)
    raw_change_1d = float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100) if len(closes) >= 2 else 0
    change_1d  = raw_change_1d if abs(raw_change_1d) <= 15 else 0  # Cap bad data
    change_5d  = float((closes.iloc[-1] - closes.iloc[-5]) / closes.iloc[-5] * 100) if len(closes) >= 5 else 0
    avg_vol    = float(volumes.rolling(10).mean().iloc[-1]) if len(volumes) >= 10 else float(volumes.mean())
    vol_ratio  = float(volumes.iloc[-1] / avg_vol) if avg_vol > 0 else 1
    ma5        = float(closes.rolling(5).mean().iloc[-1]) if len(closes) >= 5 else price
    ma20       = float(closes.rolling(20).mean().iloc[-1]) if len(closes) >= 20 else price

    # Opportunity scoring
    score = 0.0
    reasons = []

    # RSI signals
    if rsi:
        if rsi < 30:
            score += 3.0
            reasons.append(f"RSI oversold ({rsi:.0f})")
        elif rsi < 40:
            score += 1.5
            reasons.append(f"RSI low ({rsi:.0f})")
        elif rsi > 70:
            score += 1.0  # momentum continuation
            reasons.append(f"RSI momentum ({rsi:.0f})")

    # Volume spike
    if vol_ratio > 2.0:
        score += 2.0
        reasons.append(f"Volume spike {vol_ratio:.1f}x")
    elif vol_ratio > 1.5:
        score += 1.0
        reasons.append(f"Above avg volume {vol_ratio:.1f}x")

    # Price momentum
    if abs(change_1d) > 3:
        score += 1.5
        reasons.append(f"Strong move {change_1d:+.1f}% today")
    if abs(change_5d) > 8:
        score += 1.0
        reasons.append(f"5d trend {change_5d:+.1f}%")

    # MA setup
    if price > ma5 > ma20:
        score += 1.0
        reasons.append("Uptrend (price > MA5 > MA20)")
    elif price < ma5 < ma20:
        score += 0.5
        reasons.append("Downtrend — reversal watch")

    if score < 1.0:  # Lower threshold
        return None  # Not interesting enough

    return {
        "symbol":            symbol,
        "price":             round(price, 2),
        "change_1d_pct":     round(change_1d, 2),
        "change_5d_pct":     round(change_5d, 2),
        "rsi":               round(rsi, 1) if rsi else None,
        "volume_ratio":      round(vol_ratio, 2),
        "opportunity_score": round(score, 2),
        "reasons":           reasons,
        "type":              "stock",
    }


def _score_crypto(coin: str, pair: str) -> dict | None:
    """Score a single crypto for trading opportunity."""
    df = _fetch_crypto_bars(pair)
    if df.empty or len(df) < 5:
        return None

    closes  = df["close"].astype(float)
    volumes = df["volume"].astype(float)
    price   = float(closes.iloc[-1])

    rsi       = _calc_rsi(closes)
    # Validate price changes - discard if data looks wrong (>15% in a day is suspicious for daily bars)
    raw_change_1d = float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100) if len(closes) >= 2 else 0
    change_1d = raw_change_1d if abs(raw_change_1d) <= 15 else 0  # Cap at 15% - anything more is bad data
    change_7d = float((closes.iloc[-1] - closes.iloc[-7]) / closes.iloc[-7] * 100) if len(closes) >= 7 else 0
    avg_vol   = float(volumes.rolling(7).mean().iloc[-1]) if len(volumes) >= 7 else float(volumes.mean())
    vol_ratio = float(volumes.iloc[-1] / avg_vol) if avg_vol > 0 else 1

    score   = 0.0
    reasons = []

    if rsi:
        if rsi < 30:
            score += 3.5
            reasons.append(f"RSI oversold ({rsi:.0f})")
        elif rsi < 40:
            score += 2.0
            reasons.append(f"RSI low ({rsi:.0f})")
        elif rsi > 70:
            score += 1.5
            reasons.append(f"RSI momentum ({rsi:.0f})")

    if vol_ratio > 2.0:
        score += 2.5
        reasons.append(f"Volume spike {vol_ratio:.1f}x")
    elif vol_ratio > 1.5:
        score += 1.0
        reasons.append(f"Above avg volume")

    if abs(change_1d) > 5:
        score += 2.0
        reasons.append(f"Big move {change_1d:+.1f}% today")
    if abs(change_7d) > 15:
        score += 1.0
        reasons.append(f"7d trend {change_7d:+.1f}%")

    if score < 1.0:  # Lower threshold - show more opportunities
        return None

    return {
        "symbol":            coin,
        "price_usd":         round(price, 4),
        "change_1d_pct":     round(change_1d, 2),
        "change_7d_pct":     round(change_7d, 2),
        "rsi":               round(rsi, 1) if rsi else None,
        "volume_ratio":      round(vol_ratio, 2),
        "opportunity_score": round(score, 2),
        "reasons":           reasons,
        "type":              "crypto",
    }


def run_full_scan() -> dict:
    """
    Run the complete market scan.
    Returns everything Claude needs to pick tonight's trades.
    """
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
        "stocks_scanned":       len(SCAN_UNIVERSE),
        "crypto_scanned":       len(CRYPTO_UNIVERSE),
    }
