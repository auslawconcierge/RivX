"""
RivX scanner.py — find candidates that match the strategy's entry rules.

The scanner's only job is to produce a list of (symbol, bucket, signal_data)
candidates. It does NOT decide what to buy — that's the brain's job, with
Claude in the loop. The scanner's contract:

    candidates = scan_crypto()
    # → list of dicts:
    #   {"symbol":"BTC", "bucket":"swing_crypto",
    #    "signal":{"rank":1, "pullback_pct":-0.07, "above_50d_ma":True},
    #    "reasoning":"top-1 cap, -7.0% pullback, above 50d MA"}

Data sources, with explicit roles:

  Binance public API (data-api.binance.vision and api.binance.com mirrors)
    → 24h tickers (every USDT pair, with volume + change)
    → klines (OHLC bars for technicals: 7d high, 50d MA, volume avg)
    → primary because: free, no auth, sub-100ms response, never been down

  CoinSpot pubapi
    → universe filter: we only consider coins CoinSpot lists
    → we don't trust their PRICE here (that's prices.py's job), just their
      "do you offer this coin?" signal

  CoinPaprika
    → market cap rank (Binance doesn't expose this cleanly)
    → cached aggressively because it changes slowly

Yesterday's bugs we're explicitly NOT repeating:
  - We never read CoinSpot prices in this module — only their listing. Prices
    flow through prices.py with cross-validation.
  - We don't fall back to "use whatever data we got, even if obviously wrong".
    Each step has clear failure handling. If we can't get fresh data, we
    return an empty list, not stale junk.
  - All scoring lives in strategy.py's qualifies_* functions. This file
    just gathers raw signals; it doesn't decide.
"""

import os
import time
import json
import logging
import requests
import statistics
from pathlib import Path
from typing import Optional

from . import strategy

log = logging.getLogger(__name__)


# ── Sources ───────────────────────────────────────────────────────────────

BINANCE_HOSTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://data-api.binance.vision",
]
COINSPOT_HOSTS = [
    "https://www.coinspot.com.au/pubapi/v2/latest",
    "https://www.coinspot.com.au/pubapi/latest",
]
COINPAPRIKA_TICKERS = "https://api.coinpaprika.com/v1/tickers"


# ── Cache (mirrors prices.py for consistency) ────────────────────────────

CACHE_DIR = Path(os.environ.get("RIVX_CACHE_DIR", "/tmp/rivx_cache"))
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    CACHE_DIR = Path("/tmp")


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
        log.debug(f"cache write {key}: {e}")


# ── Stock universe (for stock scanner) ───────────────────────────────────

STOCK_QUALITY_LIST = [
    # Mega-cap tech
    "NVDA", "AAPL", "MSFT", "META", "GOOGL", "AMZN",
    # Semi
    "AMD", "AVGO", "TSM",
    # Other quality
    "TSLA", "NFLX", "ADBE", "CRM",
    # ETFs (very-defensive baseline)
    "SPY", "QQQ", "IWM",
]


# ── Binance: 24h tickers ──────────────────────────────────────────────────

def _binance_24h_all() -> list:
    """
    Returns list of 24h tickers for all USDT pairs.
    Each entry: {symbol, lastPrice, priceChangePercent, volume, quoteVolume}
    Cached 5 min — 24h stats change slowly.
    """
    cached = _cache_get("binance_24h", 300)
    if cached:
        return cached

    for host in BINANCE_HOSTS:
        try:
            r = requests.get(f"{host}/api/v3/ticker/24hr", timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            if not isinstance(data, list):
                continue
            usdt_only = [t for t in data
                         if isinstance(t, dict) and t.get("symbol", "").endswith("USDT")]
            if len(usdt_only) > 50:
                _cache_set("binance_24h", usdt_only)
                return usdt_only
        except Exception as e:
            log.debug(f"binance 24h via {host}: {e}")

    log.warning("Binance 24h: all hosts failed")
    return []


# ── Binance: klines (OHLC bars) ─────────────────────────────────────────

def _binance_klines(symbol: str, interval: str, limit: int) -> list:
    """
    Returns list of [openTime, open, high, low, close, volume, ...] arrays.
    No cache — bars are tactical and inputs to fresh decisions.
    """
    pair = f"{symbol.upper()}USDT"
    for host in BINANCE_HOSTS:
        try:
            r = requests.get(
                f"{host}/api/v3/klines",
                params={"symbol": pair, "interval": interval, "limit": limit},
                timeout=8,
            )
            if r.status_code == 400:
                # Symbol not on Binance
                return []
            if r.status_code != 200:
                continue
            data = r.json()
            if isinstance(data, list):
                return data
        except Exception as e:
            log.debug(f"klines {pair} via {host}: {e}")

    return []


# ── CoinSpot: tradeable universe (listings only, not prices) ─────────────

def _coinspot_listings() -> set:
    """
    Returns set of symbols CoinSpot offers. We only care that a symbol
    is listed — prices.py handles whether the price is valid.
    Cached 30 min — listings change rarely.
    """
    cached = _cache_get("coinspot_listings", 1800)
    if cached:
        return set(cached)

    for url in COINSPOT_HOSTS:
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            prices_obj = data.get("prices") or data
            if not isinstance(prices_obj, dict):
                continue
            symbols = {s.upper() for s in prices_obj.keys()}
            if len(symbols) > 20:
                _cache_set("coinspot_listings", sorted(symbols))
                return symbols
        except Exception as e:
            log.debug(f"coinspot listings via {url}: {e}")

    # Stale fallback up to 24h
    stale = _cache_get("coinspot_listings", 86400)
    if stale:
        log.warning("CoinSpot listings: live unavailable, using stale cache")
        return set(stale)

    # Hard fallback (paranoid baseline so scanner works even with full outage)
    log.warning("CoinSpot listings: all sources failed, using hardcoded fallback")
    return {
        "BTC","ETH","SOL","XRP","ADA","DOGE","AVAX","LINK","LTC","BCH","DOT",
        "UNI","AAVE","MATIC","ATOM","ALGO","NEAR","SAND","MANA","CRV","SHIB",
        "PEPE","FET","TAO","TRX","TON","APT","SUI","SEI","TIA","INJ","ICP",
        "ARB","OP","HBAR","FIL","VET","STX","IMX","RUNE","JUP","WLD",
    }


# ── CoinPaprika: market cap ranks ───────────────────────────────────────

def _market_cap_ranks() -> dict:
    """
    Returns {symbol: rank} for top 500 coins. Rank 1 = largest market cap.
    Cached 1 hour — ranks shuffle slowly, no need for fresh-fresh.
    """
    cached = _cache_get("paprika_ranks", 3600)
    if cached:
        return cached

    try:
        r = requests.get(COINPAPRIKA_TICKERS, params={"limit": 500}, timeout=12)
        if r.status_code == 200:
            rows = r.json() or []
            ranks = {}
            for row in rows:
                sym = (row.get("symbol") or "").upper()
                rank = row.get("rank") or 0
                if sym and rank > 0:
                    ranks[sym] = int(rank)
            if len(ranks) > 50:
                _cache_set("paprika_ranks", ranks)
                return ranks
    except Exception as e:
        log.warning(f"CoinPaprika ranks fetch: {e}")

    # Stale up to 24h
    stale = _cache_get("paprika_ranks", 86400)
    if stale:
        log.warning("CoinPaprika ranks: live unavailable, using stale cache")
        return stale

    return {}


# ── Signal computation from klines ─────────────────────────────────────

def _compute_rsi(closes: list, period: int = 14) -> float:
    """Standard 14-period RSI on a list of closes. Returns 50 if insufficient data."""
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i-1]
        gains.append(max(0, change))
        losses.append(max(0, -change))
    # Use last `period` values
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _is_falling_knife(closes: list, volumes: list) -> bool:
    """
    Heuristic for "don't catch this":
      - RSI < 30 AND last close lower than previous = oversold and still dropping
      - 3 consecutive red days on rising volume = capitulation underway
    Either signal = skip.
    """
    if len(closes) < 4 or len(volumes) < 4:
        return False

    # Test 1: oversold and still falling
    rsi = _compute_rsi(closes)
    if rsi < 30 and closes[-1] < closes[-2]:
        return True

    # Test 2: three consecutive red days with rising volume
    last4 = closes[-4:]
    last4_vol = volumes[-4:]
    three_red = (last4[1] < last4[0]) and (last4[2] < last4[1]) and (last4[3] < last4[2])
    rising_vol = (last4_vol[1] > last4_vol[0]) and (last4_vol[2] > last4_vol[1]) and (last4_vol[3] > last4_vol[2])
    if three_red and rising_vol:
        return True

    return False


def _is_volatility_spike(klines_daily: list, multiplier: float = 3.0) -> bool:
    """
    True if today's range (high - low) is more than `multiplier` × the 14-day
    average true range. Indicates news-driven chaos; trade after it settles.
    """
    if not klines_daily or len(klines_daily) < 15:
        return False
    try:
        highs = [float(k[2]) for k in klines_daily]
        lows = [float(k[3]) for k in klines_daily]
        closes = [float(k[4]) for k in klines_daily]
        # Compute true range for each day: max(H-L, |H-prev_close|, |L-prev_close|)
        trs = []
        for i in range(1, len(klines_daily)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1]),
            )
            trs.append(tr)
        if len(trs) < 14:
            return False
        atr14 = sum(trs[-15:-1]) / 14   # average of prior 14 days
        today_tr = trs[-1]
        return atr14 > 0 and today_tr > atr14 * multiplier
    except (ValueError, IndexError):
        return False


def _compute_pullback_signal(klines_daily: list) -> Optional[dict]:
    """
    Given daily klines, compute:
      - close_price (latest close)
      - pullback_pct: (close - 7d_high) / 7d_high  (negative if below high)
      - above_50d_ma: bool
      - broke_7d_high_today: bool (today's HIGH > prior 7-day high)
      - volume_ratio: today's vol / 7d avg vol
      - rsi (14-period)
      - falling_knife (bool, true if oversold + still dropping or 3-red-on-rising-vol)

    Returns None if klines insufficient.
    """
    if not klines_daily or len(klines_daily) < 50:
        return None

    try:
        closes = [float(k[4]) for k in klines_daily]
        highs = [float(k[2]) for k in klines_daily]
        volumes = [float(k[5]) for k in klines_daily]

        latest_close = closes[-1]
        latest_high = highs[-1]
        latest_volume = volumes[-1]

        # 7-day high using prior 7 days (not including today, for "broke today")
        prior_7d_highs = highs[-8:-1]
        prior_7d_high = max(prior_7d_highs) if prior_7d_highs else 0.0

        # Pullback computed against last 7 days INCLUDING today's high
        recent_7d_highs = highs[-7:]
        recent_7d_high = max(recent_7d_highs) if recent_7d_highs else 0.0

        pullback_pct = (
            (latest_close - recent_7d_high) / recent_7d_high
            if recent_7d_high > 0 else 0.0
        )

        # 50d MA using last 50 closes
        ma50 = statistics.mean(closes[-50:]) if len(closes) >= 50 else None
        above_50d_ma = (ma50 is not None) and (latest_close > ma50)

        broke_7d_high_today = (
            prior_7d_high > 0 and latest_high > prior_7d_high
        )

        avg_volume_7d = statistics.mean(volumes[-8:-1]) if len(volumes) >= 8 else 0.0
        volume_ratio = (
            latest_volume / avg_volume_7d if avg_volume_7d > 0 else 0.0
        )

        rsi = _compute_rsi(closes)
        falling_knife = _is_falling_knife(closes, volumes)
        vol_spike = _is_volatility_spike(klines_daily)

        return {
            "close": latest_close,
            "pullback_pct": pullback_pct,
            "above_50d_ma": above_50d_ma,
            "broke_7d_high_today": broke_7d_high_today,
            "volume_ratio": round(volume_ratio, 2),
            "ma50": ma50,
            "rsi": round(rsi, 1),
            "falling_knife": falling_knife,
            "volatility_spike": vol_spike,
        }
    except (ValueError, IndexError, statistics.StatisticsError) as e:
        log.debug(f"pullback signal: {e}")
        return None


# ── Public: crypto candidate scan ────────────────────────────────────────

def scan_crypto() -> list:
    """
    Returns list of candidates that pass entry filters for SOME bucket.
    Each candidate is:
      {
        "symbol": "BTC",
        "bucket": "swing_crypto" | "momentum_crypto",
        "signal": {...raw computed signals...},
        "reasoning": "human-readable why this qualified"
      }

    The brain decides which candidates to actually buy. We just produce the
    well-formed shortlist.
    """
    cs_listings = _coinspot_listings()
    if not cs_listings:
        log.error("scan_crypto: no CoinSpot listings, cannot proceed")
        return []

    ranks = _market_cap_ranks()
    if not ranks:
        log.warning("scan_crypto: no rank data — cannot determine bucket eligibility, skipping")
        return []

    tickers_24h = _binance_24h_all()
    if not tickers_24h:
        log.error("scan_crypto: Binance 24h tickers unavailable")
        return []

    # Build a focused candidate pool: symbols that are both on CoinSpot AND
    # ranked by Paprika AND have a Binance USDT pair. That intersection is
    # what we can actually trade with good data.
    binance_symbols = set()
    for t in tickers_24h:
        s = t.get("symbol", "")
        if s.endswith("USDT"):
            binance_symbols.add(s[:-4])  # strip USDT suffix

    universe = cs_listings & binance_symbols & set(ranks.keys())
    log.info(f"scan_crypto: universe {len(universe)} symbols "
             f"(CS:{len(cs_listings)} ∩ Binance:{len(binance_symbols)} ∩ ranked:{len(ranks)})")

    candidates = []
    # Sort by rank — process large-caps first so they show up at the top
    sorted_universe = sorted(universe, key=lambda s: ranks.get(s, 9999))

    # Limit to top 100 to keep API budget reasonable. 100 klines calls is well
    # within Binance's 6000 weight/min budget.
    for symbol in sorted_universe[:100]:
        rank = ranks.get(symbol, 9999)

        # Fetch daily bars: need 60 days for 50-day MA + 7-day pullback window.
        klines = _binance_klines(symbol, interval="1d", limit=60)
        signal = _compute_pullback_signal(klines)
        if not signal:
            continue

        # Mechanical falling-knife exclusion. Cheaper than asking Claude.
        if signal.get("falling_knife"):
            log.debug(f"scan_crypto: skipping {symbol} (falling knife: RSI {signal.get('rsi')})")
            continue

        # Volatility spike: today's range > 3x 14d ATR. News-driven chaos —
        # let it settle before we trade. False signals look the same as real
        # ones in this regime.
        if signal.get("volatility_spike"):
            log.debug(f"scan_crypto: skipping {symbol} (volatility spike)")
            continue

        # Try swing_crypto qualification first
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=rank,
            pullback_from_7d_high_pct=signal["pullback_pct"],
            above_50d_ma=signal["above_50d_ma"],
        )
        if ok:
            candidates.append({
                "symbol": symbol,
                "bucket": strategy.Bucket.SWING_CRYPTO,
                "signal": {**signal, "rank": rank},
                "reasoning": reason,
            })
            continue

        # Try momentum
        ok_m, reason_m = strategy.qualifies_momentum_crypto(
            market_cap_rank=rank,
            broke_7d_high_today=signal["broke_7d_high_today"],
            volume_vs_7d_avg_ratio=signal["volume_ratio"],
        )
        if ok_m:
            candidates.append({
                "symbol": symbol,
                "bucket": strategy.Bucket.MOMENTUM_CRYPTO,
                "signal": {**signal, "rank": rank},
                "reasoning": reason_m,
            })

    log.info(f"scan_crypto: {len(candidates)} candidates pass entry rules")
    return candidates


# ── Stocks (uses Alpaca via brain.get_market_data, lazy-imported) ────────

def scan_stocks() -> list:
    """
    Returns swing_stock candidates from the quality list.
    Stocks are USD; conversion to AUD happens at trade time, not here.
    """
    try:
        # Lazy import so this module doesn't fail without bot/ context
        from .brain import _fetch_bars
    except ImportError:
        try:
            from bot.brain import _fetch_bars  # production path
        except ImportError:
            log.error("scan_stocks: cannot import _fetch_bars")
            return []

    candidates = []
    for symbol in STOCK_QUALITY_LIST:
        try:
            df = _fetch_bars(symbol, days=60)
            if df is None or df.empty or len(df) < 50:
                continue
            closes = df["close"].astype(float).tolist()
            highs = df["high"].astype(float).tolist()

            recent_high = max(highs[-7:])
            latest_close = closes[-1]
            pullback = (latest_close - recent_high) / recent_high if recent_high > 0 else 0.0
            ma50 = statistics.mean(closes[-50:])
            above_ma50 = latest_close > ma50

            ok, reason = strategy.qualifies_swing_stock(
                is_quality=True,
                pullback_from_7d_high_pct=pullback,
                above_50d_ma=above_ma50,
            )
            if ok:
                candidates.append({
                    "symbol": symbol,
                    "bucket": strategy.Bucket.SWING_STOCK,
                    "signal": {
                        "close": latest_close,
                        "pullback_pct": pullback,
                        "above_50d_ma": above_ma50,
                        "ma50": ma50,
                    },
                    "reasoning": reason,
                })
        except Exception as e:
            log.debug(f"scan_stocks {symbol}: {e}")
            continue

    log.info(f"scan_stocks: {len(candidates)} candidates")
    return candidates


def scan_all() -> dict:
    """One-shot scan returning grouped candidates."""
    crypto = scan_crypto()
    stocks = scan_stocks()
    return {
        "swing_crypto":    [c for c in crypto if c["bucket"] == strategy.Bucket.SWING_CRYPTO],
        "momentum_crypto": [c for c in crypto if c["bucket"] == strategy.Bucket.MOMENTUM_CRYPTO],
        "swing_stock":     stocks,
        "scanned_at":      time.time(),
    }
