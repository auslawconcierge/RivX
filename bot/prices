"""
RivX prices.py — single source of truth for crypto pricing.

This module exists because yesterday's bot lost $300 on phantom ARB data
when CoinSpot returned $29.51 for a coin actually worth $0.40. That kind of
failure must be impossible going forward.

The contract this module provides:

  get_crypto_price(symbol) -> PriceQuote | None

Where PriceQuote is a validated price object with:
  - aud: float          (price in AUD, what we'd actually pay/receive)
  - usd: float          (Binance USD spot, the reference)
  - source: str         (which Binance host responded)
  - cs_aud: float       (CoinSpot AUD if available, else 0)
  - validated: bool     (True iff Binance and CoinSpot agree within tolerance)
  - disagreement_pct: float (how much the two sources differ, abs %)

If validated=False, the bot MUST NOT trade. The Telegram alert + log entry
are produced by the caller, not here — this module just measures truth.

Sources, in order:
  1. Binance USD spot price (primary "this is what the world says")
     - Multiple host fallbacks: api.binance.com, api1-3, data-api.binance.vision
     - Free, no auth, ~50ms response time, 99.99% uptime
  2. CoinSpot AUD spot price (secondary "this is what we'd actually trade at")
     - Used to compute the implied AUD price for execution
     - Failure here is non-fatal — we proceed with Binance USD × FX rate
  3. Frankfurter USD/AUD (for converting Binance USD to AUD when CoinSpot down)
     - Free, ECB-backed, no rate limit

Cross-validation rule: if both Binance and CoinSpot return prices, they must
agree within PRICE_DISAGREEMENT_TOLERANCE (default 5%). Otherwise validated=False.
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
COINSPOT_HOSTS = [
    "https://www.coinspot.com.au/pubapi/v2/latest",
    "https://www.coinspot.com.au/pubapi/latest",
]
FRANKFURTER_URL = "https://api.frankfurter.app/latest?from=USD&to=AUD"
USD_AUD_FALLBACK = 1.55  # used only if Frankfurter is also down

CACHE_DIR = Path(os.environ.get("RIVX_CACHE_DIR", "/tmp/rivx_cache"))
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    CACHE_DIR = Path("/tmp")

PRICE_CACHE_TTL = 60   # 1 min — prices are tactical, must be fresh
FX_CACHE_TTL    = 3600 # 1 hr — FX moves slowly enough


# ── Data class ────────────────────────────────────────────────────────────

@dataclass
class PriceQuote:
    symbol: str
    aud: float                    # price in AUD (what we trade at)
    usd: float                    # Binance USD reference
    source: str                   # which Binance host responded
    cs_aud: float                 # CoinSpot AUD (0 if unavailable)
    validated: bool               # both sources agreed within tolerance
    disagreement_pct: float       # 0.0 if only one source available
    fx_rate: float                # USD→AUD rate used for conversion
    fetched_at: float             # unix timestamp

    def to_dict(self) -> dict:
        return asdict(self)


# ── File cache helpers ────────────────────────────────────────────────────

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
    """Returns USD→AUD rate (i.e. 1 USD = X AUD). Cached 1 hour."""
    cached = _cache_get("fx_usd_aud", FX_CACHE_TTL)
    if cached and isinstance(cached, dict):
        rate = cached.get("rate", 0)
        if 0.5 < rate < 3.0:  # sanity bounds
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

    # Fallback to last cached value (even if stale) before hardcoded fallback
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
    """
    Returns (price_usd, host_used). Tries each host in order until one works.
    Returns (0, "") if all fail.

    Symbol convention: pass plain symbol like "BTC", we append "USDT".
    Binance's most liquid USD pair for everything is X-USDT.
    """
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
                # 400 = symbol doesn't exist on Binance (e.g. some Aussie-only coins).
                # No point trying other hosts, they all use the same exchange.
                log.debug(f"Binance: {pair} not listed (400)")
                return 0, ""
        except Exception as e:
            log.debug(f"Binance {host} failed: {e}")
            continue
    log.warning(f"All Binance hosts failed for {symbol}")
    return 0, ""


# ── CoinSpot AUD price ────────────────────────────────────────────────────

def _coinspot_universe() -> dict:
    """
    Returns {SYMBOL: aud_last_price} for everything CoinSpot lists.
    Cached 60s. Returns {} if CoinSpot is unreachable.
    """
    cached = _cache_get("coinspot_universe", PRICE_CACHE_TTL)
    if cached:
        return cached

    for url in COINSPOT_HOSTS:
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                continue
            data = r.json()
            prices_obj = data.get("prices") or data
            if not isinstance(prices_obj, dict):
                continue
            out = {}
            for sym_raw, entry in prices_obj.items():
                sym = sym_raw.upper()
                if isinstance(entry, dict):
                    try:
                        out[sym] = float(entry.get("last") or 0)
                    except (TypeError, ValueError):
                        pass
                elif isinstance(entry, (int, float, str)):
                    try:
                        out[sym] = float(entry)
                    except (TypeError, ValueError):
                        pass
            if len(out) > 0:
                _cache_set("coinspot_universe", out)
                return out
        except Exception as e:
            log.debug(f"CoinSpot {url} failed: {e}")

    # Stale cache fallback (up to 1 hour old)
    stale = _cache_get("coinspot_universe", 3600)
    if stale:
        log.warning("CoinSpot live unavailable, using stale (<1h) cache")
        return stale

    return {}


def _coinspot_price_aud(symbol: str) -> float:
    """Returns CoinSpot AUD spot for one symbol, or 0 if unavailable."""
    universe = _coinspot_universe()
    return float(universe.get(symbol.upper(), 0))


# ── Public API: validated price quote ─────────────────────────────────────

def get_crypto_price(symbol: str) -> Optional[PriceQuote]:
    """
    Returns a validated PriceQuote, or None if no source has a price for this
    symbol at all. The PriceQuote.validated flag indicates whether sources
    agree within tolerance.

    The bot's contract: if validated is False, do not trade. If returned None,
    do not trade. Only trade on a quote where validated=True.
    """
    sym = symbol.upper().strip()

    binance_usd, binance_host = _binance_price_usd(sym)
    cs_aud = _coinspot_price_aud(sym)
    fx = get_usd_aud_rate()

    # No price anywhere → nothing to validate, nothing to trade
    if binance_usd <= 0 and cs_aud <= 0:
        log.warning(f"prices: no source has a price for {sym}")
        return None

    # Both sources available: cross-validate
    if binance_usd > 0 and cs_aud > 0:
        implied_aud = binance_usd * fx
        # Disagreement as a fraction of the larger value (so symmetric)
        bigger = max(implied_aud, cs_aud)
        smaller = min(implied_aud, cs_aud)
        disagreement = (bigger - smaller) / bigger if bigger > 0 else 1.0
        validated = disagreement <= PRICE_DISAGREEMENT_TOLERANCE

        if not validated:
            log.warning(
                f"prices: {sym} DISAGREEMENT {disagreement*100:.1f}% — "
                f"Binance ${binance_usd:.4f} USD = ${implied_aud:.4f} AUD vs "
                f"CoinSpot ${cs_aud:.4f} AUD"
            )

        return PriceQuote(
            symbol=sym,
            # Trade execution happens on CoinSpot, so the "true" AUD price
            # we'd actually pay is CoinSpot's. Use that as the trade price
            # ONLY if validated. If not validated, caller must refuse.
            aud=cs_aud if validated else 0.0,
            usd=binance_usd,
            source=binance_host,
            cs_aud=cs_aud,
            validated=validated,
            disagreement_pct=round(disagreement * 100, 2),
            fx_rate=fx,
            fetched_at=time.time(),
        )

    # Only Binance available (CoinSpot down or doesn't list this coin)
    if binance_usd > 0 and cs_aud <= 0:
        # Can't validate. Report unvalidated. The caller decides whether to
        # trust a single source. For BUYS, we'll refuse. For HOLDS, we'll use
        # this as a fallback display price.
        log.info(f"prices: {sym} only on Binance (CoinSpot has no listing or is down)")
        return PriceQuote(
            symbol=sym,
            aud=binance_usd * fx,  # implied
            usd=binance_usd,
            source=binance_host,
            cs_aud=0.0,
            validated=False,        # single-source = not validated for buys
            disagreement_pct=0.0,
            fx_rate=fx,
            fetched_at=time.time(),
        )

    # Only CoinSpot available (Binance doesn't list this coin)
    # This is suspicious — most legitimate coins are on Binance. Don't trade.
    log.warning(f"prices: {sym} only on CoinSpot — Binance doesn't list it. Treating as unvalidated.")
    return PriceQuote(
        symbol=sym,
        aud=cs_aud,
        usd=cs_aud / fx if fx > 0 else 0,  # implied USD
        source="coinspot_only",
        cs_aud=cs_aud,
        validated=False,
        disagreement_pct=0.0,
        fx_rate=fx,
        fetched_at=time.time(),
    )


# ── Convenience: bulk price fetch ─────────────────────────────────────────

def get_crypto_prices(symbols: list) -> dict:
    """
    Returns {symbol: PriceQuote or None} for a list of symbols.
    Used by the snapshot loop to mark portfolio to market.
    """
    return {sym: get_crypto_price(sym) for sym in symbols}
