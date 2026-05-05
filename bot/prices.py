# RIVX_VERSION: v2.9.2-multi-source-prices-2026-05-04
"""
RivX prices.py — single source of truth for crypto pricing.

v2.9.2: CoinSpot's bulk pubapi gutted to ~17 symbols, blocking every
mid-cap crypto trade via "price not validated". This version tries four
price sources in order until one returns a number, then validates against
Binance. Validation rule (5% disagreement = refuse trade) is unchanged.

Sources tried for CoinSpot AUD price, in order:
  1. /pubapi/v2/latest/{coin}     per-coin v2  (preferred)
  2. /pubapi/latest/{coin}        per-coin v1  (legacy fallback)
  3. /pubapi/v2/latest            bulk v2      (currently broken but cheap to try)
  4. CoinPaprika USD price × FX   external     (last resort, AUD-implied)

Source 4 is new. If CoinSpot is fully unreachable for a coin, we use
CoinPaprika's USD price converted to AUD via Frankfurter FX. CoinPaprika
is already used elsewhere in the bot for ranks, so it's a known-good source.
The trade-off: CoinPaprika gives us a global market price, not CoinSpot's
actual quote. The 5% Binance-vs-secondary validation still applies, so a
real CoinSpot quote that disagrees with global will still be caught. The
risk that remains: CoinSpot's actual fill could be a bit worse than the
CoinPaprika-derived price suggests. In paper mode this is fine. In live
mode the 1% CoinSpot fee + 1-2% spread already provides margin.
"""

import os
import time
import json
import logging
import requests
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────

PRICE_DISAGREEMENT_TOLERANCE = 0.05   # 5% — agreed in strategy session

BINANCE_HOSTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://data-api.binance.vision",
]

COINSPOT_BASE = "https://www.coinspot.com.au"
COINSPOT_PER_COIN_PATHS = [
    "/pubapi/v2/latest/{coin}",
    "/pubapi/latest/{coin}",
]
COINSPOT_BULK_PATHS = [
    "/pubapi/v2/latest",
    "/pubapi/latest",
]

# Cloudflare-friendly headers — same as scanner.py
COINSPOT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

COINPAPRIKA_TICKERS = "https://api.coinpaprika.com/v1/tickers"
FRANKFURTER_URL = "https://api.frankfurter.app/latest?from=USD&to=AUD"
USD_AUD_FALLBACK = 1.55

CACHE_DIR = Path(os.environ.get("RIVX_CACHE_DIR", "/tmp/rivx_cache"))
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    CACHE_DIR = Path("/tmp")

PRICE_CACHE_TTL = 60
FX_CACHE_TTL    = 3600
PAPRIKA_CACHE_TTL = 300   # 5 min — paprika tickers move fast enough


# ── Data class ────────────────────────────────────────────────────────────

@dataclass
class PriceQuote:
    symbol: str
    aud: float
    usd: float
    source: str
    cs_aud: float
    validated: bool
    disagreement_pct: float
    fx_rate: float
    fetched_at: float

    def to_dict(self) -> dict:
        return asdict(self)


# ── Cache helpers ─────────────────────────────────────────────────────────

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
        log.debug(f"Cache write failed {key}: {e}")


# ── FX rate (USD → AUD) ───────────────────────────────────────────────────

def get_usd_aud_rate() -> float:
    cached = _cache_get("fx_usd_aud", FX_CACHE_TTL)
    if cached and isinstance(cached, dict):
        rate = cached.get("rate", 0)
        if 0.5 < rate < 3.0:
            return rate
    try:
        r = requests.get(FRANKFURTER_URL, timeout=5)
        if r.status_code == 200:
            data = r.json()
            rate = float(data.get("rates", {}).get("AUD", 0))
            if 0.5 < rate < 3.0:
                _cache_set("fx_usd_aud", {"rate": rate, "fetched": time.time()})
                return rate
    except Exception as e:
        log.warning(f"Frankfurter FX fetch failed: {e}")
    p = CACHE_DIR / "fx_usd_aud.json"
    if p.exists():
        try:
            data = json.loads(p.read_text())
            rate = float(data.get("rate", 0))
            if 0.5 < rate < 3.0:
                log.warning(f"Using stale FX rate: {rate}")
                return rate
        except Exception:
            pass
    log.error(f"All FX sources failed, using hardcoded {USD_AUD_FALLBACK}")
    return USD_AUD_FALLBACK


# ── Binance USD price ─────────────────────────────────────────────────────

def _binance_price_usd(symbol: str) -> tuple[float, str]:
    pair = f"{symbol.upper()}USDT"
    for host in BINANCE_HOSTS:
        try:
            r = requests.get(
                f"{host}/api/v3/ticker/price",
                params={"symbol": pair},
                timeout=5,
            )
            if r.status_code == 200:
                data = r.json()
                price = float(data.get("price", 0))
                if price > 0:
                    return price, host
            elif r.status_code == 400:
                log.debug(f"Binance: {pair} not listed (400)")
                return 0, ""
        except Exception as e:
            log.debug(f"Binance {host} failed: {e}")
            continue
    log.warning(f"All Binance hosts failed for {symbol}")
    return 0, ""


# ── CoinPaprika USD price (new in v2.9.2 as price source) ────────────────

def _paprika_all_tickers() -> dict:
    """Returns {SYMBOL: {price_usd: ...}} for top 500 coins. Cached 5 min."""
    cached = _cache_get("paprika_tickers", PAPRIKA_CACHE_TTL)
    if cached:
        return cached
    try:
        r = requests.get(COINPAPRIKA_TICKERS, params={"limit": 500}, timeout=10)
        if r.status_code == 200:
            rows = r.json() or []
            out = {}
            for row in rows:
                sym = (row.get("symbol") or "").upper()
                quotes = row.get("quotes") or {}
                usd = (quotes.get("USD") or {}).get("price")
                if sym and usd and usd > 0:
                    out[sym] = {"usd": float(usd), "rank": row.get("rank") or 9999}
            if len(out) > 50:
                _cache_set("paprika_tickers", out)
                return out
    except Exception as e:
        log.warning(f"CoinPaprika tickers fetch: {e}")
    stale = _cache_get("paprika_tickers", 86400)
    if stale:
        log.warning("CoinPaprika tickers: live unavailable, using stale cache")
        return stale
    return {}


def _paprika_price_aud(symbol: str, fx_rate: float) -> float:
    """Returns CoinPaprika-derived AUD price for symbol, or 0 if not found."""
    tickers = _paprika_all_tickers()
    entry = tickers.get(symbol.upper())
    if not entry:
        return 0.0
    usd = entry.get("usd", 0)
    return usd * fx_rate if usd > 0 and fx_rate > 0 else 0.0


# ── CoinSpot AUD price ────────────────────────────────────────────────────

def _extract_last_from_payload(data, sym: str) -> float:
    if not isinstance(data, dict):
        return 0.0
    prices = data.get("prices")
    if isinstance(prices, dict) and "last" in prices:
        try:
            return float(prices["last"])
        except (TypeError, ValueError):
            pass
    if isinstance(prices, (str, int, float)):
        try:
            return float(prices)
        except (TypeError, ValueError):
            pass
    if isinstance(prices, dict):
        entry = prices.get(sym.upper()) or prices.get(sym.lower())
        if isinstance(entry, dict) and "last" in entry:
            try:
                return float(entry["last"])
            except (TypeError, ValueError):
                pass
        if isinstance(entry, (str, int, float)):
            try:
                return float(entry)
            except (TypeError, ValueError):
                pass
    return 0.0


def _coinspot_price_aud(symbol: str) -> tuple[float, str]:
    """
    Returns (aud_price, source_label). source_label tells us which path won
    so we can debug. (0.0, "") if all paths fail.
    """
    sym = symbol.upper().strip()
    if not sym:
        return 0.0, ""

    cache_key = f"cs_price_{sym}"
    cached = _cache_get(cache_key, PRICE_CACHE_TTL)
    if cached and isinstance(cached, dict):
        try:
            v = float(cached.get("aud", 0))
            src = cached.get("source", "cache")
            if v > 0:
                return v, src
        except (TypeError, ValueError):
            pass

    last_status, last_body = None, ""

    # Tier 1: per-coin endpoints
    for path in COINSPOT_PER_COIN_PATHS:
        url = f"{COINSPOT_BASE}{path.format(coin=sym)}"
        try:
            r = requests.get(url, headers=COINSPOT_HEADERS, timeout=5)
            last_status = r.status_code
            if r.status_code != 200:
                last_body = (r.text or "")[:120].replace("\n", " ")
                continue
            data = r.json()
            if isinstance(data, dict) and data.get("status") == "error":
                continue
            price = _extract_last_from_payload(data, sym)
            if price > 0:
                src = f"coinspot:{path}"
                _cache_set(cache_key, {"aud": price, "source": src, "fetched": time.time()})
                return price, src
        except Exception as e:
            log.debug(f"coinspot per-coin {sym} via {path}: {e}")

    # Tier 2: bulk endpoints
    for path in COINSPOT_BULK_PATHS:
        url = f"{COINSPOT_BASE}{path}"
        try:
            r = requests.get(url, headers=COINSPOT_HEADERS, timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            price = _extract_last_from_payload(data, sym)
            if price > 0:
                src = f"coinspot:{path}"
                _cache_set(cache_key, {"aud": price, "source": src, "fetched": time.time()})
                return price, src
        except Exception as e:
            log.debug(f"coinspot bulk {sym} via {path}: {e}")

    if last_status and last_status != 200:
        log.warning(
            f"coinspot price {sym}: all CoinSpot paths failed "
            f"(last status {last_status}, body={last_body!r})"
        )

    return 0.0, ""


# ── Public API: validated price quote ─────────────────────────────────────

def get_crypto_price(symbol: str) -> Optional[PriceQuote]:
    """
    v2.9.2: tries CoinSpot (4 endpoints), then CoinPaprika as a final
    fallback for the secondary price. Validation logic unchanged: Binance
    vs secondary must agree within 5% to allow a trade.
    """
    sym = symbol.upper().strip()

    binance_usd, binance_host = _binance_price_usd(sym)
    fx = get_usd_aud_rate()
    cs_aud, cs_source = _coinspot_price_aud(sym)

    # If CoinSpot has nothing, try CoinPaprika as the secondary source
    secondary_source = cs_source
    secondary_aud = cs_aud
    if cs_aud <= 0:
        paprika_aud = _paprika_price_aud(sym, fx)
        if paprika_aud > 0:
            secondary_aud = paprika_aud
            secondary_source = "coinpaprika"
            log.info(f"prices: {sym} secondary via CoinPaprika ${paprika_aud:.6f} AUD (CoinSpot unavailable)")

    # No price anywhere
    if binance_usd <= 0 and secondary_aud <= 0:
        log.warning(f"prices: no source has a price for {sym}")
        return None

    # Both Binance and a secondary available: cross-validate
    if binance_usd > 0 and secondary_aud > 0:
        implied_aud = binance_usd * fx
        bigger = max(implied_aud, secondary_aud)
        smaller = min(implied_aud, secondary_aud)
        disagreement = (bigger - smaller) / bigger if bigger > 0 else 1.0
        validated = disagreement <= PRICE_DISAGREEMENT_TOLERANCE

        if not validated:
            log.warning(
                f"prices: {sym} DISAGREEMENT {disagreement*100:.1f}% — "
                f"Binance ${binance_usd:.4f} USD = ${implied_aud:.4f} AUD vs "
                f"{secondary_source} ${secondary_aud:.4f} AUD"
            )
        else:
            log.info(
                f"prices: {sym} validated — Binance ${implied_aud:.4f} AUD vs "
                f"{secondary_source} ${secondary_aud:.4f} AUD ({disagreement*100:.1f}% disagree)"
            )

        return PriceQuote(
            symbol=sym,
            aud=secondary_aud if validated else 0.0,
            usd=binance_usd,
            source=binance_host,
            cs_aud=secondary_aud,
            validated=validated,
            disagreement_pct=round(disagreement * 100, 2),
            fx_rate=fx,
            fetched_at=time.time(),
        )

    # Only Binance (no CoinSpot, no CoinPaprika)
    if binance_usd > 0 and secondary_aud <= 0:
        log.info(f"prices: {sym} only on Binance (no secondary source)")
        return PriceQuote(
            symbol=sym,
            aud=binance_usd * fx,
            usd=binance_usd,
            source=binance_host,
            cs_aud=0.0,
            validated=False,
            disagreement_pct=0.0,
            fx_rate=fx,
            fetched_at=time.time(),
        )

    # Only secondary, no Binance — suspicious
    log.warning(f"prices: {sym} only on {secondary_source} — Binance doesn't list it. Treating as unvalidated.")
    return PriceQuote(
        symbol=sym,
        aud=secondary_aud,
        usd=secondary_aud / fx if fx > 0 else 0,
        source=secondary_source,
        cs_aud=secondary_aud,
        validated=False,
        disagreement_pct=0.0,
        fx_rate=fx,
        fetched_at=time.time(),
    )


def get_crypto_prices(symbols: list) -> dict:
    return {sym: get_crypto_price(sym) for sym in symbols}
