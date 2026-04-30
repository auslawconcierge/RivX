# RIVX_VERSION: v2.7-asx-analyser-2026-04-30
"""
RivX-ASX analyser.

Self-contained module that runs alongside RivX but never touches RivX's
portfolio, cash, slots, trades, or claude_decisions. Reads ASX 200 daily
+ intraday bars from Yahoo Finance and produces three setup types:

  - PULLBACK: 3-8% off recent high, above 50d MA, RSI 35-60, volume normal
  - BREAKOUT: today's high > 20-day high, volume >= 2x average
  - OVERSOLD BOUNCE: RSI <30, bullish reversal candle, above 200d MA
                    (capped at 70% confidence — speculative)

Outputs go to the `asx_signals` Supabase table and to Telegram tagged
"🇦🇺 RivX-ASX". Outcomes (target hit / stop hit / expired) get written
back to the same row over time so the dashboard can show hit rate.

Data source: yfinance bulk download. ~5s for the full ASX 200, so the
full scan is cheap. yfinance has no API key but rate-limits aggressively
on per-symbol calls — we use bulk where possible.

This module imports yfinance at top level. If it's missing, the whole
module fails to import — by design. The user's bot.py wraps the call
site in try/except so the rest of RivX continues working even if the
ASX module fails to load.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)


# ── ASX 200 universe ──────────────────────────────────────────────────────
# Maintained as a constant. yfinance uses .AX suffix for ASX tickers.
# This list is the ASX 200 as of late 2024 — close enough for now. Periodic
# refresh from https://www.asx.com.au/asx/research/ASXListedCompanies.csv
# would keep it tighter, but the names that matter (top 50) basically don't
# move. The tail of the list rotates a few times a year.

ASX_200 = [
    # Big 4 banks + insurance
    "CBA", "WBC", "NAB", "ANZ", "MQG", "QBE", "IAG", "SUN", "BEN", "BOQ",
    # Major resources
    "BHP", "RIO", "FMG", "S32", "MIN", "NCM", "EVN", "NST", "NEM", "PLS",
    "AKE", "IGO", "LYC", "WHC", "CRN", "NHC", "CIA", "SFR", "29M", "DEG",
    # Energy
    "WDS", "STO", "ORG", "AMP", "BPT",
    # Healthcare / biotech
    "CSL", "RMD", "COH", "SHL", "RHC", "FPH", "PME", "ANN", "EBO", "TLX",
    "NEU", "CUV", "CLE", "PNV",
    # Consumer staples / retail
    "WES", "WOW", "COL", "ALL", "TWE", "A2M", "MTS", "ELD", "ENA",
    # Discretionary retail
    "JBH", "HVN", "SUL", "PMV", "BBN", "LOV", "BAP", "AX1", "BRG",
    # Industrials / materials
    "TCL", "JHX", "BSL", "BLD", "ABC", "CWY", "BXB", "DOW", "LLC", "MND",
    "REH", "RWC", "GWA", "AMC", "ORI",
    # Tech
    "WTC", "XRO", "TNE", "NXT", "APX", "CDA", "ALU", "PME", "DTL", "MP1",
    "EML", "MPL", "NWL", "PNI", "HUB",
    # Telecoms / media
    "TLS", "TPG", "REA", "CAR", "SEK", "NEC", "NWS", "ARN", "OML",
    # REITs / property
    "GMG", "SCG", "VCX", "DXS", "MGR", "SGP", "GPT", "CHC", "CIP", "INA",
    "ABP", "BWP", "CQR", "CMW",
    # Utilities / infrastructure
    "AGL", "ORG", "APA", "AST", "MEZ", "SKI",
    # Transport / logistics
    "QAN", "ALX", "QUB", "AIA", "AZJ",
    # Financial services / wealth
    "MFG", "PPT", "IFL", "PNI", "GQG", "PDL", "CGF", "ASX", "JHG",
    # Smaller industrials / diversified
    "ALD", "ALQ", "AMI", "ARB", "BKL", "BPT", "CCP", "CGC", "CKF", "CNI",
    "CWN", "ECX", "FBU", "GNC", "GUD", "IPL", "LIC", "LNK", "MAH", "MFG",
    "MIN", "MMS", "MND", "MTS", "NHF", "NUF", "ORI", "OZL", "PPS", "PRU",
    "PTM", "QUB", "RBL", "RIC", "RMS", "RRL", "SBM", "SDF", "SGM", "SHV",
    "SIG", "SPK", "SVW", "TAH", "TPW", "VEA", "VOC", "WAF", "WGN", "ZIP",
]

# Strip duplicates while preserving order
_seen = set()
ASX_200 = [s for s in ASX_200 if not (s in _seen or _seen.add(s))]


# ── Setup tuning ──────────────────────────────────────────────────────────

# Pullback
PULLBACK_MIN_PCT = -0.08   # 8% off high — anything deeper is breakdown territory
PULLBACK_MAX_PCT = -0.03   # 3% off high — anything shallower isn't really a pullback
PULLBACK_RSI_MIN = 35
PULLBACK_RSI_MAX = 60

# Breakout
BREAKOUT_LOOKBACK_DAYS = 20
BREAKOUT_MIN_VOL_RATIO = 2.0   # today's vol >= 2x 20d avg

# Oversold bounce
OVERSOLD_RSI_MAX = 30.0
OVERSOLD_CONF_CAP = 0.70   # never exceeds 70% — speculative by nature

# High-conviction interrupt thresholds (used by intraday scan)
HIGH_CONV_CONF_MIN = 0.80
HIGH_CONV_VOL_RATIO_MIN = 3.0


# ── Data class ────────────────────────────────────────────────────────────

@dataclass
class AsxSignal:
    symbol: str
    setup_type: str           # 'pullback' | 'breakout' | 'oversold_bounce'
    confidence: float         # 0.0 to 1.0
    current_price: float
    entry_zone_low: float
    entry_zone_high: float
    stop_price: float
    target_price: float
    reasoning: str
    signal_strength: float    # composite for ranking
    volume_ratio: float
    rsi: float
    pullback_pct: float
    high_conviction: bool = False


# ── Yahoo Finance data fetcher ────────────────────────────────────────────

def _fetch_bulk_history(symbols: list, period: str = "6mo") -> dict:
    """
    Fetch daily bars for many ASX symbols at once via yfinance.download().
    Returns {symbol: pandas.DataFrame} with columns Open, High, Low, Close, Volume.
    Symbols are passed without the .AX suffix; we add it here.

    On total failure returns {} and the caller skips the scan.
    """
    try:
        import yfinance as yf
    except ImportError:
        log.error("ASX scan: yfinance not installed. Add 'yfinance' to requirements.txt.")
        return {}

    if not symbols:
        return {}

    # yfinance wants a space-separated string for multi-ticker download
    ax_tickers = [f"{s}.AX" for s in symbols]

    try:
        df = yf.download(
            tickers=" ".join(ax_tickers),
            period=period,
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        log.error(f"ASX bulk fetch failed: {e}")
        return {}

    out = {}
    if df is None or df.empty:
        return out

    # When multiple tickers, df has MultiIndex columns: (ticker, field)
    # When single ticker, df has flat columns. Normalize.
    if len(ax_tickers) == 1:
        sym = symbols[0]
        if not df.empty:
            out[sym] = df
        return out

    for sym, ax in zip(symbols, ax_tickers):
        try:
            if ax in df.columns.get_level_values(0):
                sub = df[ax].dropna(how="all")
                if not sub.empty and len(sub) >= 50:
                    out[sym] = sub
        except Exception as e:
            log.debug(f"ASX bulk slice {sym}: {e}")
            continue

    log.info(f"ASX bulk fetch: {len(out)}/{len(symbols)} symbols got usable data")
    return out


# ── Indicators ────────────────────────────────────────────────────────────

def _rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(0, ch))
        losses.append(max(0, -ch))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))


def _ma(values: list, period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return statistics.mean(values[-period:])


def _is_bullish_reversal_candle(opens: list, highs: list, lows: list,
                                 closes: list) -> bool:
    """
    Detects a bullish-reversal-style candle: today closes higher than
    today's open AND today's low is lower than yesterday's low (reversal),
    OR a hammer (long lower wick, small body near top).
    """
    if len(closes) < 2:
        return False
    o, c = opens[-1], closes[-1]
    h, l = highs[-1], lows[-1]
    prev_l = lows[-2]

    # Bullish engulfing-ish: green candle + new low broken + closed back up
    if c > o and l < prev_l and c > opens[-2]:
        return True

    # Hammer: long lower wick (>= 2× body), small upper wick, green-or-doji
    body = abs(c - o)
    upper_wick = h - max(c, o)
    lower_wick = min(c, o) - l
    if body > 0 and lower_wick >= 2 * body and upper_wick <= body:
        return True
    return False


# ── Setup qualification ───────────────────────────────────────────────────

def _qualify_pullback(*, closes, highs, volumes, rsi: float,
                      ma50: Optional[float]) -> tuple[bool, str, dict]:
    """Returns (qualifies, reason, extra_metrics)."""
    if ma50 is None:
        return False, "insufficient history for 50dMA", {}

    last_close = closes[-1]
    seven_d_high = max(highs[-7:])
    pullback_pct = (last_close - seven_d_high) / seven_d_high if seven_d_high else 0

    if pullback_pct > PULLBACK_MAX_PCT:
        return False, f"only {pullback_pct*100:.1f}% off 7d high — too shallow", {}
    if pullback_pct < PULLBACK_MIN_PCT:
        return False, f"{pullback_pct*100:.1f}% off 7d high — too deep, possible breakdown", {}
    if last_close <= ma50:
        return False, "below 50dMA — uptrend not intact", {}
    if rsi < PULLBACK_RSI_MIN:
        return False, f"RSI {rsi:.0f} below {PULLBACK_RSI_MIN}", {}
    if rsi > PULLBACK_RSI_MAX:
        return False, f"RSI {rsi:.0f} above {PULLBACK_RSI_MAX} — overbought", {}

    # Falling-knife filter: today red on volume spike = wait
    if len(closes) >= 2 and len(volumes) >= 8:
        avg_vol = statistics.mean(volumes[-8:-1])
        today_drop_pct = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] else 0
        if today_drop_pct < -0.02 and avg_vol > 0 and volumes[-1] > avg_vol * 1.5:
            return False, "today red >2% on heavy volume — wait for stabilisation", {}

    return True, (
        f"{pullback_pct*100:.1f}% pullback in uptrend, RSI {rsi:.0f}, above 50dMA"
    ), {"pullback_pct": pullback_pct}


def _qualify_breakout(*, closes, highs, volumes) -> tuple[bool, str, dict]:
    if len(highs) < BREAKOUT_LOOKBACK_DAYS + 1:
        return False, "insufficient history", {}
    prior_high = max(highs[-(BREAKOUT_LOOKBACK_DAYS + 1):-1])  # prior 20d, excluding today
    today_high = highs[-1]
    today_close = closes[-1]

    if today_high <= prior_high:
        return False, "no 20d-high break today", {}
    # Confirmation: close holding above the prior high (not faded back)
    if today_close < prior_high:
        return False, "broke 20d high but closed back below — fake breakout", {}

    # Volume confirmation
    if len(volumes) < 21:
        return False, "insufficient volume history", {}
    avg_vol = statistics.mean(volumes[-21:-1])
    if avg_vol == 0:
        return False, "zero avg volume", {}
    vol_ratio = volumes[-1] / avg_vol
    if vol_ratio < BREAKOUT_MIN_VOL_RATIO:
        return False, f"volume only {vol_ratio:.1f}x avg (need {BREAKOUT_MIN_VOL_RATIO}x+)", {}

    return True, (
        f"broke 20d high ${prior_high:.2f} on {vol_ratio:.1f}x volume, holding above"
    ), {"prior_high": prior_high, "vol_ratio": vol_ratio}


def _qualify_oversold(*, opens, highs, lows, closes, volumes,
                      rsi: float, ma200: Optional[float]) -> tuple[bool, str, dict]:
    if rsi >= OVERSOLD_RSI_MAX:
        return False, f"RSI {rsi:.0f} not oversold", {}
    if ma200 is None:
        return False, "insufficient history for 200dMA", {}
    last_close = closes[-1]
    if last_close <= ma200:
        return False, "below 200dMA — primary trend down, skip", {}
    if not _is_bullish_reversal_candle(opens, highs, lows, closes):
        return False, "no bullish reversal candle", {}
    return True, (
        f"RSI {rsi:.0f} oversold, bullish reversal candle, above 200dMA"
    ), {}


# ── Scoring ───────────────────────────────────────────────────────────────

def _score_pullback(pullback_pct: float, rsi: float, vol_ratio: float) -> float:
    """0..1 confidence score. Sweet-spot pullback = 4-6%."""
    score = 0.5  # base for qualifying
    abs_pull = abs(pullback_pct)
    if 0.04 <= abs_pull <= 0.06:
        score += 0.20
    elif 0.03 <= abs_pull <= 0.07:
        score += 0.10
    if 40 <= rsi <= 55:
        score += 0.10
    if 0.7 <= vol_ratio <= 1.3:   # normal volume = orderly pullback
        score += 0.10
    elif vol_ratio < 0.5:
        score -= 0.05  # very low volume = stale signal
    return min(0.95, max(0.0, score))


def _score_breakout(vol_ratio: float, breakout_strength_pct: float) -> float:
    """Breakout strength is how far today's high is above prior 20d high."""
    score = 0.55
    if vol_ratio >= 4.0:
        score += 0.20
    elif vol_ratio >= 3.0:
        score += 0.15
    elif vol_ratio >= 2.0:
        score += 0.05
    if breakout_strength_pct >= 0.02:
        score += 0.10  # broke high by 2%+
    return min(0.95, max(0.0, score))


def _score_oversold(rsi: float, vol_ratio: float) -> float:
    """Capped at OVERSOLD_CONF_CAP — speculative by nature."""
    score = 0.50
    if rsi <= 25:
        score += 0.10
    if rsi <= 20:
        score += 0.05
    if vol_ratio >= 1.5:
        score += 0.05  # capitulation volume on the bounce
    return min(OVERSOLD_CONF_CAP, max(0.0, score))


# ── Main scan ─────────────────────────────────────────────────────────────

def scan_asx(*, scan_event: str = "pre_open",
             intraday_volume_check: bool = False) -> list[AsxSignal]:
    """
    Run a full ASX 200 scan and return list of qualifying signals.

    Args:
      scan_event: tag stored on each signal — 'pre_open' | 'midday' | 'close' |
                  'high_conviction'. Just metadata for the dashboard / summary.
      intraday_volume_check: when True (used by the high-conviction polling
                  path during market hours), we also flag signals with
                  volume_ratio >= HIGH_CONV_VOL_RATIO_MIN as high_conviction=True.
                  Confidence cap of 0.80 still applies for those.
    """
    log.info(f"ASX scan starting ({scan_event}, {len(ASX_200)} symbols)")
    history = _fetch_bulk_history(ASX_200, period="6mo")
    if not history:
        log.warning("ASX scan: no data fetched, aborting")
        return []

    signals: list[AsxSignal] = []

    for sym, df in history.items():
        try:
            if df is None or df.empty or len(df) < 50:
                continue

            # Pull arrays we need (yfinance gives capitalised column names)
            opens   = df["Open"].astype(float).tolist()
            highs   = df["High"].astype(float).tolist()
            lows    = df["Low"].astype(float).tolist()
            closes  = df["Close"].astype(float).tolist()
            volumes = df["Volume"].astype(float).tolist()

            # Skip if any of those are degenerate
            if not closes or closes[-1] <= 0:
                continue

            rsi = _rsi(closes)
            ma50 = _ma(closes, 50)
            ma200 = _ma(closes, 200)
            current_price = closes[-1]

            avg_vol = (statistics.mean(volumes[-21:-1])
                       if len(volumes) >= 21 else 0)
            vol_ratio = (volumes[-1] / avg_vol) if avg_vol > 0 else 0

            seven_d_high = max(highs[-7:])

            # ── Try each setup type ──
            sig = _try_pullback(sym, opens, highs, lows, closes, volumes,
                                rsi, ma50, current_price, seven_d_high, vol_ratio)
            if sig:
                signals.append(sig)

            sig = _try_breakout(sym, opens, highs, lows, closes, volumes,
                                vol_ratio, current_price)
            if sig:
                signals.append(sig)

            sig = _try_oversold(sym, opens, highs, lows, closes, volumes,
                                rsi, ma200, current_price, vol_ratio)
            if sig:
                signals.append(sig)

        except Exception as e:
            log.debug(f"ASX scan {sym}: {e}")
            continue

    # Mark high-conviction (used by intraday polling)
    if intraday_volume_check:
        for s in signals:
            if (s.confidence >= HIGH_CONV_CONF_MIN
                    and s.volume_ratio >= HIGH_CONV_VOL_RATIO_MIN):
                s.high_conviction = True

    log.info(f"ASX scan complete: {len(signals)} signals "
             f"({sum(1 for s in signals if s.setup_type=='pullback')} pullback / "
             f"{sum(1 for s in signals if s.setup_type=='breakout')} breakout / "
             f"{sum(1 for s in signals if s.setup_type=='oversold_bounce')} oversold)")
    return signals


# ── Per-setup builder helpers ─────────────────────────────────────────────

def _try_pullback(sym, opens, highs, lows, closes, volumes,
                  rsi, ma50, current_price, seven_d_high, vol_ratio) -> Optional[AsxSignal]:
    ok, reason, extra = _qualify_pullback(
        closes=closes, highs=highs, volumes=volumes, rsi=rsi, ma50=ma50,
    )
    if not ok:
        return None
    pullback_pct = extra.get("pullback_pct", 0)
    conf = _score_pullback(pullback_pct, rsi, vol_ratio)

    # Suggested trade levels:
    # Entry zone: current price ± 0.5% (or down to 50dMA if close)
    # Stop: 1% below 50dMA OR 2% below entry low (whichever is tighter)
    # Target: prior 7d high (recover the pullback)
    entry_low  = round(current_price * 0.995, 4)
    entry_high = round(current_price * 1.005, 4)
    stop = max(round(ma50 * 0.99, 4), round(entry_low * 0.97, 4)) if ma50 else round(current_price * 0.95, 4)
    # We want stop BELOW entry — take the smaller of the two
    stop = min(round(ma50 * 0.99, 4) if ma50 else current_price * 0.95,
               round(entry_low * 0.97, 4))
    target = round(seven_d_high, 4)

    return AsxSignal(
        symbol=sym, setup_type="pullback", confidence=conf,
        current_price=current_price, entry_zone_low=entry_low,
        entry_zone_high=entry_high, stop_price=stop, target_price=target,
        reasoning=reason, signal_strength=conf * (1 + min(vol_ratio, 2)),
        volume_ratio=vol_ratio, rsi=rsi, pullback_pct=pullback_pct,
    )


def _try_breakout(sym, opens, highs, lows, closes, volumes,
                  vol_ratio, current_price) -> Optional[AsxSignal]:
    ok, reason, extra = _qualify_breakout(
        closes=closes, highs=highs, volumes=volumes,
    )
    if not ok:
        return None
    prior_high = extra.get("prior_high", current_price)
    breakout_strength = (current_price - prior_high) / prior_high if prior_high else 0
    conf = _score_breakout(vol_ratio, breakout_strength)

    # Levels: entry zone above prior high, stop below prior high, target = ATR-projected
    entry_low  = round(prior_high, 4)
    entry_high = round(current_price * 1.01, 4)
    stop       = round(prior_high * 0.97, 4)
    # Target: simple measured-move using last 20d range
    range_20d = max(highs[-20:]) - min(lows[-20:])
    target = round(current_price + range_20d * 0.5, 4)

    return AsxSignal(
        symbol=sym, setup_type="breakout", confidence=conf,
        current_price=current_price, entry_zone_low=entry_low,
        entry_zone_high=entry_high, stop_price=stop, target_price=target,
        reasoning=reason, signal_strength=conf * (1 + min(vol_ratio, 4)),
        volume_ratio=vol_ratio, rsi=_rsi(closes), pullback_pct=0.0,
    )


def _try_oversold(sym, opens, highs, lows, closes, volumes,
                  rsi, ma200, current_price, vol_ratio) -> Optional[AsxSignal]:
    ok, reason, extra = _qualify_oversold(
        opens=opens, highs=highs, lows=lows, closes=closes,
        volumes=volumes, rsi=rsi, ma200=ma200,
    )
    if not ok:
        return None
    conf = _score_oversold(rsi, vol_ratio)

    # Levels: entry near current, stop below today's low, target = 50dMA recovery
    entry_low  = round(current_price * 0.995, 4)
    entry_high = round(current_price * 1.01, 4)
    stop       = round(lows[-1] * 0.98, 4)
    ma50 = _ma(closes, 50) or current_price * 1.05
    target = round(ma50, 4)

    return AsxSignal(
        symbol=sym, setup_type="oversold_bounce", confidence=conf,
        current_price=current_price, entry_zone_low=entry_low,
        entry_zone_high=entry_high, stop_price=stop, target_price=target,
        reasoning=reason, signal_strength=conf,
        volume_ratio=vol_ratio, rsi=rsi, pullback_pct=0.0,
    )


# ── Outcome tracking ──────────────────────────────────────────────────────

def update_signal_outcomes(db, log_obj=None) -> int:
    """
    Walk all asx_signals where outcome='pending' and check current price
    against stop/target. Mark hit_target / hit_stop / expired (>30 days).

    Returns number of rows updated. Designed to be called once per scan.
    """
    if log_obj is None:
        log_obj = log
    try:
        pending = db._get("asx_signals", {
            "outcome": "eq.pending",
            "limit": "200",
        }) or []
    except Exception as e:
        log_obj.warning(f"outcome update: read failed: {e}")
        return 0

    if not pending:
        return 0

    # Group by symbol so we only fetch each ticker once
    symbols = list({r.get("symbol") for r in pending if r.get("symbol")})

    history = _fetch_bulk_history(symbols, period="3mo")
    if not history:
        log_obj.warning("outcome update: no fresh data, skipping")
        return 0

    updated = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for row in pending:
        try:
            sym = row.get("symbol")
            df = history.get(sym)
            if df is None or df.empty:
                continue

            highs = df["High"].astype(float).tolist()
            lows = df["Low"].astype(float).tolist()
            closes = df["Close"].astype(float).tolist()

            stop = float(row.get("stop_price") or 0)
            target = float(row.get("target_price") or 0)
            current_price = float(row.get("current_price") or 0)
            fired_at_iso = row.get("fired_at") or ""

            try:
                fired_at = datetime.fromisoformat(fired_at_iso.replace("Z", "+00:00"))
            except Exception:
                fired_at = datetime.now(timezone.utc)
            age_days = (datetime.now(timezone.utc) - fired_at).total_seconds() / 86400.0

            # Only look at bars after fired_at
            # yfinance df index is dates; we can't easily slice without converting.
            # Easier: just look at last 30 bars and check.
            # For 'hit_target' we need any high >= target since fire.
            # For 'hit_stop' we need any low <= stop since fire.
            # We approximate: look at the last min(30, age_in_days+1) bars.
            n_bars = max(1, min(30, int(age_days) + 2))
            recent_highs = highs[-n_bars:]
            recent_lows = lows[-n_bars:]
            current_close = closes[-1]

            outcome = None
            outcome_price = None

            # Stop check first (more conservative)
            if stop > 0 and any(l <= stop for l in recent_lows):
                outcome = "hit_stop"
                outcome_price = stop
            elif target > 0 and any(h >= target for h in recent_highs):
                outcome = "hit_target"
                outcome_price = target
            elif age_days >= 30:
                outcome = "expired"
                outcome_price = current_close

            if outcome:
                return_pct = 0.0
                if current_price > 0 and outcome_price:
                    return_pct = (outcome_price - current_price) / current_price

                ok = db._patch("asx_signals", {
                    "outcome": outcome,
                    "outcome_price": round(outcome_price, 4),
                    "outcome_at": now_iso,
                    "return_pct": round(return_pct, 4),
                }, "id", str(row.get("id")))
                if ok:
                    updated += 1

        except Exception as e:
            log_obj.debug(f"outcome update {row.get('symbol')}: {e}")
            continue

    log_obj.info(f"outcome update: {updated} signals resolved")
    return updated


# ── DB persistence ────────────────────────────────────────────────────────

def save_signals(db, signals: list[AsxSignal], scan_event: str) -> int:
    """Write signals into asx_signals table. Returns count saved."""
    saved = 0
    for s in signals:
        try:
            db._post("asx_signals", {
                "symbol": s.symbol,
                "setup_type": s.setup_type,
                "confidence": round(s.confidence, 4),
                "current_price": round(s.current_price, 4),
                "entry_zone_low": round(s.entry_zone_low, 4),
                "entry_zone_high": round(s.entry_zone_high, 4),
                "stop_price": round(s.stop_price, 4),
                "target_price": round(s.target_price, 4),
                "reasoning": (s.reasoning or "")[:500],
                "signal_strength": round(s.signal_strength, 4),
                "volume_ratio": round(s.volume_ratio, 4),
                "rsi": round(s.rsi, 2),
                "pullback_pct": round(s.pullback_pct, 4),
                "scan_event": scan_event,
                "high_conviction": bool(s.high_conviction),
            })
            saved += 1
        except Exception as e:
            log.debug(f"save_signal {s.symbol}: {e}")
    return saved
