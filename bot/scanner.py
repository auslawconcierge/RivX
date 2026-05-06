# RIVX_VERSION: v3.0-momentum-5d-1.5x-2026-05-07
"""
RivX scanner.py — find candidates that match the strategy's entry rules.

The scanner's only job is to produce a list of (symbol, bucket, signal_data)
candidates. It does NOT decide what to buy — that's the brain's job, with
Claude in the loop.

═══════════════════════════════════════════════════════════════════════════
v3.0 changes (2026-05-07)
═══════════════════════════════════════════════════════════════════════════

  Momentum bucket detection switched from 7-day high break / 7-day volume
  avg to 5-day high break / 5-day volume avg. Combined with strategy.py's
  threshold drop from 2x to 1.5x, this should roughly double candidate
  flow into the momentum bucket.

  Signal field renames:
    broke_7d_high_today → broke_5d_high_today
    volume_ratio is now computed against 5-day avg (previously 7)

  Pullback computation is unchanged (still 7-day window). Swing buckets
  use the wider 7-day pullback context. Only the momentum breakout window
  shortened.

═══════════════════════════════════════════════════════════════════════════
v2.9.1 (2026-05-04) — CoinSpot listings fix preserved
═══════════════════════════════════════════════════════════════════════════

  CoinSpot listings was failing every scan from Render's Singapore IP,
  falling back to a 42-symbol hardcoded list. Three fixes:
    1. Browser User-Agent on CoinSpot fetches
    2. Failure logs promoted to WARNING with status + body snippet
    3. CoinPaprika top-200 fallback before hardcoded
    4. Hardcoded fallback expanded 42 → 140 symbols

Data sources unchanged: Binance for OHLC + 24h tickers, CoinSpot for the
tradeable universe filter, CoinPaprika for market cap rank.
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

COINSPOT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


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


# ── Hardcoded CoinSpot fallback (v2.9.1) ─────────────────────────────────

COINSPOT_HARDCODED_FALLBACK = {
    # Top 30 large-caps
    "BTC", "ETH", "USDT", "USDC", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX",
    "AVAX", "SHIB", "TON", "DOT", "LINK", "BCH", "NEAR", "MATIC", "LTC", "ICP",
    "UNI", "APT", "XLM", "ETC", "HBAR", "FIL", "ATOM", "CRO", "OKB", "KAS",
    # Rank 30-100 mid-caps
    "RUNE", "AAVE", "MKR", "INJ", "ALGO", "EOS", "MANA", "SAND", "FLOW", "TIA",
    "LDO", "GRT", "FET", "PEPE", "JUP", "TAO", "IMX", "WLD", "OP", "ARB",
    "RNDR", "FTM", "EGLD", "GALA", "AXS", "CHZ", "KAVA", "NEO", "IOTA", "GMT",
    "COMP", "KSM", "GMX", "DASH", "ZEC", "ENS", "JASMY", "SUI", "SEI", "STX",
    "BLUR", "DYDX", "ONE", "QTUM", "ENJ", "BAT", "BAL", "CRV", "MINA", "ORDI",
    "BSV", "PYTH", "CFX", "RON", "AKT", "ROSE", "AR", "THETA", "XTZ",
    # Rank 100-200 small-caps where momentum breakouts cluster
    "SUSHI", "1INCH", "LRC", "ANKR", "MASK", "OCEAN", "AGIX", "GLM", "CELO",
    "METIS", "RPL", "COTI", "SKL", "ASTR", "GAL", "CKB", "GNO", "ZIL", "FXS",
    "KNC", "IOST", "OMG", "SNX", "REN", "PERP", "TFUEL", "RAY", "SXP", "BAND",
    "GHST", "HOT", "RVN", "WAVES", "ANT", "JOE", "HIVE", "STORJ", "AUDIO",
    "API3", "BNT", "CTSI", "DGB", "ICX", "POWR", "REQ", "OGN", "ZEN", "NMR",
    "NKN", "RLC", "SC", "DENT", "WIN", "MTL", "ARDR", "SYS", "FLOKI", "BONK",
    "WIF", "BOME", "MEW", "POPCAT", "BRETT", "MOG", "TURBO", "ETHFI", "ENA",
    "NOT", "JTO", "ONDO", "DYM", "BLAST", "AERO", "USDE", "OM", "PYR", "HOOK",
}


# ── Binance: 24h tickers ──────────────────────────────────────────────────

def _binance_24h_all() -> list:
    """
    Returns list of 24h tickers for all USDT pairs.
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
    """v2.9.1: browser headers + CoinPaprika fallback + expanded hardcoded list."""
    cached = _cache_get("coinspot_listings", 1800)
    if cached:
        return set(cached)

    last_status = None
    last_body_snippet = ""
    for url in COINSPOT_HOSTS:
        try:
            r = requests.get(url, headers=COINSPOT_HEADERS, timeout=8)
            last_status = r.status_code
            if r.status_code != 200:
                last_body_snippet = (r.text or "")[:200].replace("\n", " ")
                log.warning(
                    f"coinspot listings via {url}: HTTP {r.status_code} "
                    f"body={last_body_snippet!r}"
                )
                continue
            data = r.json()
            prices_obj = data.get("prices") or data
            if not isinstance(prices_obj, dict):
                log.warning(
                    f"coinspot listings via {url}: unexpected shape "
                    f"keys={list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
                )
                continue
            symbols = {s.upper() for s in prices_obj.keys()}
            if len(symbols) > 20:
                log.info(f"coinspot listings: {len(symbols)} symbols (live)")
                _cache_set("coinspot_listings", sorted(symbols))
                return symbols
            log.warning(f"coinspot listings via {url}: only {len(symbols)} symbols, skipping")
        except Exception as e:
            log.warning(f"coinspot listings via {url}: {type(e).__name__}: {e}")

    stale = _cache_get("coinspot_listings", 86400)
    if stale and len(stale) > 20:
        log.warning(
            f"CoinSpot listings: live unavailable "
            f"(last status {last_status}), using stale cache ({len(stale)} symbols)"
        )
        return set(stale)

    try:
        ranks = _market_cap_ranks()
        if ranks:
            top_200 = {sym for sym, rank in ranks.items() if rank <= 200}
            if len(top_200) > 50:
                log.warning(
                    f"CoinSpot listings: live + stale failed, using CoinPaprika "
                    f"top 200 fallback ({len(top_200)} symbols)"
                )
                return top_200
    except Exception as e:
        log.debug(f"coinpaprika fallback failed: {e}")

    log.warning(
        f"CoinSpot listings: ALL sources failed, using hardcoded fallback "
        f"({len(COINSPOT_HARDCODED_FALLBACK)} symbols)"
    )
    return set(COINSPOT_HARDCODED_FALLBACK)


# ── CoinPaprika: market cap ranks ───────────────────────────────────────

def _market_cap_ranks() -> dict:
    """Returns {symbol: rank} for top 500 coins. Cached 1 hour."""
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
    """
    if len(closes) < 4 or len(volumes) < 4:
        return False

    rsi = _compute_rsi(closes)
    if rsi < 30 and closes[-1] < closes[-2]:
        return True

    last4 = closes[-4:]
    last4_vol = volumes[-4:]
    three_red = (last4[1] < last4[0]) and (last4[2] < last4[1]) and (last4[3] < last4[2])
    rising_vol = (last4_vol[1] > last4_vol[0]) and (last4_vol[2] > last4_vol[1]) and (last4_vol[3] > last4_vol[2])
    if three_red and rising_vol:
        return True

    return False


def _is_volatility_spike(klines_daily: list, multiplier: float = 3.0) -> bool:
    """True if today's range is more than 3x the 14-day ATR. News-driven chaos."""
    if not klines_daily or len(klines_daily) < 15:
        return False
    try:
        highs = [float(k[2]) for k in klines_daily]
        lows = [float(k[3]) for k in klines_daily]
        closes = [float(k[4]) for k in klines_daily]
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
        atr14 = sum(trs[-15:-1]) / 14
        today_tr = trs[-1]
        return atr14 > 0 and today_tr > atr14 * multiplier
    except (ValueError, IndexError):
        return False


def _compute_pullback_signal(klines_daily: list) -> Optional[dict]:
    """
    Given daily klines, compute:
      - close (latest close)
      - pullback_pct: vs 7-day high (used by swing buckets, unchanged)
      - above_50d_ma: bool
      - broke_5d_high_today: bool (v3.0: was 7-day; tightened for momentum)
      - volume_ratio: today vs 5-day avg (v3.0: was 7-day)
      - rsi (14-period)
      - falling_knife (bool)
      - volatility_spike (bool)

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

        # ── Swing buckets: 7-day pullback context (unchanged) ─────────────
        recent_7d_highs = highs[-7:]
        recent_7d_high = max(recent_7d_highs) if recent_7d_highs else 0.0
        pullback_pct = (
            (latest_close - recent_7d_high) / recent_7d_high
            if recent_7d_high > 0 else 0.0
        )

        # ── Momentum bucket: v3.0 — 5-day breakout, 5-day volume avg ──────
        prior_5d_highs = highs[-6:-1]   # 5 days excluding today
        prior_5d_high = max(prior_5d_highs) if prior_5d_highs else 0.0
        broke_5d_high_today = (
            prior_5d_high > 0 and latest_high > prior_5d_high
        )

        avg_volume_5d = (
            statistics.mean(volumes[-6:-1]) if len(volumes) >= 6 else 0.0
        )
        volume_ratio = (
            latest_volume / avg_volume_5d if avg_volume_5d > 0 else 0.0
        )

        # ── Trend filter (unchanged) ──────────────────────────────────────
        ma50 = statistics.mean(closes[-50:]) if len(closes) >= 50 else None
        above_50d_ma = (ma50 is not None) and (latest_close > ma50)

        rsi = _compute_rsi(closes)
        falling_knife = _is_falling_knife(closes, volumes)
        vol_spike = _is_volatility_spike(klines_daily)

        return {
            "close": latest_close,
            "pullback_pct": pullback_pct,
            "above_50d_ma": above_50d_ma,
            "broke_5d_high_today": broke_5d_high_today,
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

    binance_symbols = set()
    for t in tickers_24h:
        s = t.get("symbol", "")
        if s.endswith("USDT"):
            binance_symbols.add(s[:-4])

    universe = cs_listings & binance_symbols & set(ranks.keys())
    log.info(f"scan_crypto: universe {len(universe)} symbols "
             f"(CS:{len(cs_listings)} ∩ Binance:{len(binance_symbols)} ∩ ranked:{len(ranks)})")

    candidates = []
    sorted_universe = sorted(universe, key=lambda s: ranks.get(s, 9999))

    for symbol in sorted_universe[:100]:
        rank = ranks.get(symbol, 9999)

        klines = _binance_klines(symbol, interval="1d", limit=60)
        signal = _compute_pullback_signal(klines)
        if not signal:
            continue

        if signal.get("falling_knife"):
            log.debug(f"scan_crypto: skipping {symbol} (falling knife: RSI {signal.get('rsi')})")
            continue

        if signal.get("volatility_spike"):
            log.debug(f"scan_crypto: skipping {symbol} (volatility spike)")
            continue

        # Try swing_crypto first
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

        # v3.0: momentum uses 5d breakout + 5d volume avg
        ok_m, reason_m = strategy.qualifies_momentum_crypto(
            market_cap_rank=rank,
            broke_5d_high_today=signal["broke_5d_high_today"],
            volume_vs_5d_avg_ratio=signal["volume_ratio"],
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


# ── Stocks (uses Alpaca via brain._fetch_bars) ───────────────────────────

def scan_stocks() -> list:
    """
    Returns swing_stock candidates from the quality list.
    v3.0: pullback window widened to 3-12% (handled in strategy.qualifies_swing_stock).
    """
    try:
        from .brain import _fetch_bars
    except ImportError:
        try:
            from bot.brain import _fetch_bars
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
