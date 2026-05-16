# RIVX_VERSION: v3.1-momentum-loosened-2026-05-16
"""
RivX strategy.py — the trading rules.

This module owns "what we buy and when," "what we sell and why," and "how
much money goes where." It produces decisions, but it does NOT execute
them. Execution is in bot.py. Data fetching is in prices.py and scanner.py.
This file is pure logic — easy to test, easy to change.

═══════════════════════════════════════════════════════════════════════════
v3.1 changes from v3.0 (2026-05-16) — MOMENTUM LOOSENED
═══════════════════════════════════════════════════════════════════════════

Lesson from NEO and ENS on 2026-05-14: both opened on 2026-05-10, both
were momentum_crypto, both hit the 4-day hard exit at almost exactly
96 hours. NEO peaked at +6.77% and force-closed at -0.18%. ENS peaked
at +11.80% (a real building winner) and force-closed at +0.04%. Neither
hit the v3.0 +20% trail-arm threshold, and neither was given enough
runway to.

The previous config was internally inconsistent: it required +20% to
arm a trail, but only gave positions 96 hours to get there. Positions
trending up at +10-15% over 3-4 days were force-killed before they
could mature. ENS at peak +11.8% should have been protected, not
liquidated.

v3.1 momentum changes:
  - Trail arms at +15% peak (was +20%) — ENS would have armed at peak,
    and the trail floor would have been +10% (5% giveback).
  - Trail giveback tightened to 5% (was 7%) — once armed, lock in more.
  - Hard exit extended to 7 days (was 4) — gives positions a full week
    to convert, matching the cadence at which momentum setups typically
    play out. ENS and NEO would have had 3 more days to either arm the
    trail or hit the stop honestly.
  - Stop unchanged at -10%.

Net effect: positions like ENS get protected. Positions like INJ
(peaked at +30%, exited +21%) are unaffected — they would have armed
the trail at +15% just the same and the 5% giveback would actually
LOCK IN MORE on the way down. Positions that genuinely fail still
hit -10% stops within the 7-day window.

Worst case downside: capital tied up an extra 3 days in stinkers that
would have closed flat at day 4 under v3.0. Best case: 2-3 of every
10 momentum trades convert from flat-at-day-4 to real wins.

═══════════════════════════════════════════════════════════════════════════
v3.0 changes from v2 (2026-05-07) — kept for context
═══════════════════════════════════════════════════════════════════════════

Lesson from AMD on 2026-05-06: trend-following winners run past the target
threshold more often than they reverse from it. Half-take caps the runners.
Full-exit-at-target caps them entirely. Trail-only captures the full move
and only stops you out when the trend genuinely breaks.

AMD case study:
  - Trail fired at +20.49%. Realized ~$272 AUD gross.
  - Half-take counterfactual: half at +12%, half trails to ~+20%. ~$216 AUD.
  - Full-exit-at-target counterfactual: all at +12%. ~$160 AUD.
  - Trail won by ~$56 over half-take and ~$112 over full-target.

So v3.0 is fully trail-only. Target thresholds become trail-arming events,
not sell events. One rule, no hedge.

═══════════════════════════════════════════════════════════════════════════
Capital allocation ($10,000 total) — UNCHANGED in v3.1
═══════════════════════════════════════════════════════════════════════════

  Swing crypto    $4,000   up to 5 positions   $800 each      patient pile
  Momentum crypto $2,000   up to 4 positions   $500 each      aggressive pile
  Swing stocks    $3,500   up to 3 positions   ~$1,170 each   FX-cost aware
  Ops floor          $500   not deployed                       fees + FX buffer

═══════════════════════════════════════════════════════════════════════════
Entry rules (UNCHANGED from v3.0)
═══════════════════════════════════════════════════════════════════════════

  SWING CRYPTO — buying quality on pullbacks
    - Top 30 by market cap
    - Currently DOWN 4-13% from 7-day high
    - Above 50-day MA
    - 8 AM + 8 PM AEST scans (twice daily)

  MOMENTUM CRYPTO — catching the start of moves
    - Outside top 30, inside top 200
    - Just broke above 5-day high TODAY
    - Volume in last 24h > 1.5x its 5-day average
    - Every 2 hours, 24/7 (12 scans/day)

  SWING STOCKS — buying quality on pullbacks (US equities)
    - From quality list (NVDA, AAPL, MSFT, etc.)
    - Down 3-12% from 7-day high
    - Above 50-day MA
    - 11 PM + 3 AM AEST scans (US weekdays)

═══════════════════════════════════════════════════════════════════════════
Exit rules (v3.1 — TRAIL-ONLY, MOMENTUM LOOSENED)
═══════════════════════════════════════════════════════════════════════════

  SWING CRYPTO (unchanged)
    Stop:  -8% from entry
    Trail: arms once peak ≥ +10%, exits if price falls 5% below peak
    Time:  14-day review

  MOMENTUM CRYPTO (v3.1: trail-arm lowered, giveback tightened, time exit extended)
    Stop:  -10% from entry
    Trail: arms once peak ≥ +15% (was +20%), exits if price falls 5% below peak (was 7%)
    Time:  7-day hard exit (was 4)

  SWING STOCKS (unchanged)
    Stop:  -5% from entry
    Trail: arms once peak ≥ +8%, exits if price falls 4% below peak
    Time:  14-day review

  No target sells anywhere. Winners run until trail catches them.
"""

from dataclasses import dataclass
from typing import Optional


# ── Allocation constants (unchanged in v3.1) ──────────────────────────────

STARTING_CAPITAL_AUD = 10_000.0
OPS_FLOOR_AUD        = 500.0   # always-cash buffer for fees, FX

SWING_CRYPTO_BUDGET    = 4_000.0
SWING_CRYPTO_SLOTS     = 5
SWING_CRYPTO_SIZE      = SWING_CRYPTO_BUDGET / SWING_CRYPTO_SLOTS  # $800

MOMENTUM_CRYPTO_BUDGET = 2_000.0
MOMENTUM_CRYPTO_SLOTS  = 4
MOMENTUM_CRYPTO_SIZE   = MOMENTUM_CRYPTO_BUDGET / MOMENTUM_CRYPTO_SLOTS  # $500

SWING_STOCKS_BUDGET    = 3_500.0
SWING_STOCKS_SLOTS     = 3
SWING_STOCKS_SIZE      = SWING_STOCKS_BUDGET / SWING_STOCKS_SLOTS  # ~$1,167


# ── Exit rules (v3.1 — trail-only, momentum loosened) ─────────────────────

# Swing crypto (unchanged from v3.0)
SWING_CRYPTO_STOP_PCT       = -0.08    # -8%
SWING_CRYPTO_TRAIL_TRIGGER  = 0.10     # arm trail at +10%
SWING_CRYPTO_TRAIL_GIVEBACK = 0.05     # exit if 5% below peak
SWING_CRYPTO_REVIEW_DAYS    = 14

# Momentum crypto (v3.1: ALL three exit knobs adjusted)
MOMENTUM_STOP_PCT           = -0.10    # -10% (unchanged)
MOMENTUM_TRAIL_TRIGGER      = 0.15     # arm trail at +15% (v3.1: was +20%)
MOMENTUM_TRAIL_GIVEBACK     = 0.05     # 5% giveback (v3.1: was 7%)
MOMENTUM_MAX_DAYS           = 7        # 7-day hard exit (v3.1: was 4)

# Swing stocks (unchanged from v3.0)
SWING_STOCKS_STOP_PCT       = -0.05    # -5%
SWING_STOCKS_TRAIL_TRIGGER  = 0.08     # arm trail at +8%
SWING_STOCKS_TRAIL_GIVEBACK = 0.04     # 4% giveback
SWING_STOCKS_REVIEW_DAYS    = 14


# ── Bucket enum ───────────────────────────────────────────────────────────

class Bucket:
    SWING_CRYPTO    = "swing_crypto"
    MOMENTUM_CRYPTO = "momentum_crypto"
    SWING_STOCK     = "swing_stock"


# ── Entry decision ────────────────────────────────────────────────────────

@dataclass
class EntrySignal:
    """One candidate that passes the entry filter for a bucket."""
    symbol: str
    bucket: str
    size_aud: float
    reason: str        # short, human-readable: why this one


def qualifies_swing_crypto(
    *,
    market_cap_rank: int,                # 1 = biggest. None if unknown
    pullback_from_7d_high_pct: float,   # negative number, e.g. -0.08 = 8% off the high
    above_50d_ma: bool,
) -> tuple[bool, str]:
    """
    Returns (qualifies, reason).
    Quality + pullback + uptrend intact.
    v3.0: window widened from 5-15% to 4-13%.
    """
    if market_cap_rank is None or market_cap_rank > 30:
        return False, f"rank {market_cap_rank} outside top 30"

    # Pullback should be 4-13% off recent high (v3.0: was 5-15%)
    if pullback_from_7d_high_pct >= -0.04:
        return False, f"only {pullback_from_7d_high_pct*100:.1f}% off 7d high (need -4% to -13%)"
    if pullback_from_7d_high_pct < -0.13:
        return False, f"{pullback_from_7d_high_pct*100:.1f}% off 7d high (too deep, possible breakdown)"

    if not above_50d_ma:
        return False, "below 50-day MA — uptrend not intact"

    return True, (
        f"top-{market_cap_rank} cap, "
        f"{pullback_from_7d_high_pct*100:.1f}% pullback in uptrend"
    )


def qualifies_momentum_crypto(
    *,
    market_cap_rank: int,
    broke_5d_high_today: bool,             # v3.0: was broke_7d_high_today
    volume_vs_5d_avg_ratio: float,         # v3.0: was vs 7d avg
) -> tuple[bool, str]:
    """
    Catching the START of a breakout, not the middle.
    v3.0: 5d high break on 1.5x volume (was 7d on 2x). Loosened to roughly
    double the candidate flow into this bucket — momentum is supposed to be
    the high-frequency one and was firing as rarely as swing.
    """
    if market_cap_rank is None:
        return False, "no rank"
    if market_cap_rank <= 30:
        return False, f"rank {market_cap_rank} too big for momentum bucket"
    if market_cap_rank > 200:
        return False, f"rank {market_cap_rank} too obscure"

    if not broke_5d_high_today:
        return False, "no 5d-high breakout today"

    if volume_vs_5d_avg_ratio < 1.5:
        return False, f"volume only {volume_vs_5d_avg_ratio:.1f}x average (need ≥1.5x)"

    return True, (
        f"rank {market_cap_rank} broke 5d high today on "
        f"{volume_vs_5d_avg_ratio:.1f}x volume"
    )


def qualifies_swing_stock(
    *,
    is_quality: bool,                     # in staples list, or passes quality filter
    pullback_from_7d_high_pct: float,
    above_50d_ma: bool,
) -> tuple[bool, str]:
    """
    Same as swing crypto but with stock-appropriate thresholds.
    v3.0: pullback window widened from 3-8% to 3-12%.
    """
    if not is_quality:
        return False, "not in quality list"

    if pullback_from_7d_high_pct >= -0.03:
        return False, f"only {pullback_from_7d_high_pct*100:.1f}% off 7d high (need -3% to -12%)"
    if pullback_from_7d_high_pct < -0.12:
        return False, f"{pullback_from_7d_high_pct*100:.1f}% off 7d high (too deep)"

    if not above_50d_ma:
        return False, "below 50-day MA"

    return True, f"quality stock, {pullback_from_7d_high_pct*100:.1f}% pullback in uptrend"


# ── Slot accounting ───────────────────────────────────────────────────────

def slots_available(bucket: str, current_positions_in_bucket: int) -> int:
    """How many more positions can this bucket hold?"""
    cap = {
        Bucket.SWING_CRYPTO:    SWING_CRYPTO_SLOTS,
        Bucket.MOMENTUM_CRYPTO: MOMENTUM_CRYPTO_SLOTS,
        Bucket.SWING_STOCK:     SWING_STOCKS_SLOTS,
    }.get(bucket, 0)
    return max(0, cap - current_positions_in_bucket)


def position_size_for(bucket: str) -> float:
    """How much AUD goes into one position of this bucket type?"""
    return {
        Bucket.SWING_CRYPTO:    SWING_CRYPTO_SIZE,
        Bucket.MOMENTUM_CRYPTO: MOMENTUM_CRYPTO_SIZE,
        Bucket.SWING_STOCK:     SWING_STOCKS_SIZE,
    }.get(bucket, 0.0)


# ── Pre-score (deterministic ranking, used to cap candidates) ─────────────
# This is the score used to pick the top N candidates to send to Claude.
# It's NOT the qualification gate — qualifies_* already did that. This just
# orders the ones that already qualified, so we send the strongest first.

def prescore_swing_crypto(*, market_cap_rank: int, pullback_pct: float,
                          above_50d_ma: bool) -> float:
    """
    Higher = better setup. Used to pick top 8 of N qualified candidates.
    v3.0: sweet spot widened slightly to match the 4-13% window.
    """
    if not above_50d_ma:
        return 0.0
    score = 0.0
    # Rank: top 5 = 2.0, top 10 = 1.5, top 30 = 1.0, else 0
    if market_cap_rank <= 5:    score += 2.0
    elif market_cap_rank <= 10: score += 1.5
    elif market_cap_rank <= 30: score += 1.0
    # Pullback sweet spot: -6 to -10% (v3.0: was -7 to -10%)
    abs_pull = abs(pullback_pct)
    if 0.06 <= abs_pull <= 0.10:    score += 2.0
    elif 0.04 <= abs_pull <= 0.13:  score += 1.0
    return score


def prescore_momentum_crypto(*, market_cap_rank: int, broke_5d_high_today: bool,
                             volume_ratio: float) -> float:
    """
    Reward fresh breakouts on big volume in the right cap range.
    v3.0: scoring tiers adjusted for the new 1.5x minimum volume threshold.
    """
    if not broke_5d_high_today:
        return 0.0
    score = 1.0  # base for any breakout
    # Bigger volume = stronger conviction
    if volume_ratio >= 4.0:    score += 2.0
    elif volume_ratio >= 3.0:  score += 1.5
    elif volume_ratio >= 2.0:  score += 1.0
    elif volume_ratio >= 1.5:  score += 0.5
    # Mid-cap sweet spot (rank 30-100): biggest upside-with-floor
    if 30 < market_cap_rank <= 80:    score += 1.5
    elif 80 < market_cap_rank <= 150: score += 1.0
    return score


def prescore_swing_stock(*, pullback_pct: float, above_50d_ma: bool) -> float:
    """
    Same shape as crypto, tighter window.
    v3.0: sweet spot widened to match the 3-12% window.
    """
    if not above_50d_ma:
        return 0.0
    score = 1.0  # quality stock list = base
    abs_pull = abs(pullback_pct)
    if 0.04 <= abs_pull <= 0.07:    score += 2.0   # sweet spot
    elif 0.03 <= abs_pull <= 0.12:  score += 1.0   # acceptable
    return score


def cash_remaining_after_buy(
    *,
    current_cash_aud: float,
    intended_buy_aud: float,
) -> float:
    """How much cash would be left after this buy?"""
    return current_cash_aud - intended_buy_aud


def buy_respects_ops_floor(
    *,
    current_cash_aud: float,
    intended_buy_aud: float,
) -> tuple[bool, str]:
    """
    Don't deploy below the ops floor. The floor exists for fees + FX,
    not strategic dry powder.
    """
    after = cash_remaining_after_buy(
        current_cash_aud=current_cash_aud,
        intended_buy_aud=intended_buy_aud,
    )
    if after < OPS_FLOOR_AUD:
        return False, (
            f"would leave ${after:.0f} cash, below ops floor ${OPS_FLOOR_AUD:.0f}"
        )
    return True, f"${after:.0f} cash remains, floor ${OPS_FLOOR_AUD:.0f} respected"


# ── Exit decision ────────────────────────────────────────────────────────

@dataclass
class ExitDecision:
    """Result of applying exit rules to one open position."""
    should_exit: bool
    fraction: float       # 1.0 = full exit. v3.0+: always 1.0 (no half-take)
    reason: str
    new_peak_pnl_pct: float = 0.0   # what to update the trailing-stop watermark to


def decide_exit_swing_crypto(
    *,
    pnl_pct: float,                  # current unrealized P&L as fraction (0.05 = +5%)
    peak_pnl_pct: float,             # highest pnl_pct seen so far for trailing
    age_days: float,
) -> ExitDecision:
    """
    v3.0 trail-only: stop -8%, trail arms at +10% with 5% giveback, 14d review.
    No target firing — winners run until the trail catches them.
    """
    new_peak = max(peak_pnl_pct, pnl_pct)

    # Stop loss
    if pnl_pct <= SWING_CRYPTO_STOP_PCT:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"stop loss hit ({pnl_pct*100:.2f}% ≤ {SWING_CRYPTO_STOP_PCT*100:.0f}%)",
            new_peak_pnl_pct=new_peak,
        )

    # Trailing stop (only armed once peak hit +10%)
    if peak_pnl_pct >= SWING_CRYPTO_TRAIL_TRIGGER:
        if pnl_pct <= peak_pnl_pct - SWING_CRYPTO_TRAIL_GIVEBACK:
            return ExitDecision(
                should_exit=True,
                fraction=1.0,
                reason=f"trailing stop ({pnl_pct*100:.2f}% gave back "
                       f"{(peak_pnl_pct - pnl_pct)*100:.2f}% from peak {peak_pnl_pct*100:.2f}%)",
                new_peak_pnl_pct=new_peak,
            )

    # Time review at 14 days (v3.0: was 30)
    if age_days >= SWING_CRYPTO_REVIEW_DAYS:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"14-day review reached (age {age_days:.1f}d, pnl {pnl_pct*100:.2f}%)",
            new_peak_pnl_pct=new_peak,
        )

    # Hold
    return ExitDecision(
        should_exit=False,
        fraction=0.0,
        reason=f"holding ({pnl_pct*100:+.2f}%, peak {new_peak*100:+.2f}%, age {age_days:.1f}d)",
        new_peak_pnl_pct=new_peak,
    )


def decide_exit_momentum(
    *,
    pnl_pct: float,
    peak_pnl_pct: float,
    age_days: float,
) -> ExitDecision:
    """
    v3.1 trail-only LOOSENED: stop -10%, trail arms at +15% (was +20%) with
    5% giveback (was 7%), hard time exit at 7 days (was 4).

    Why all three knobs changed at once:
      The previous v3.0 config was internally inconsistent — it demanded
      a +20% peak to arm a trail but only gave 96 hours to get there.
      Positions trending up at +10-15% over 3-4 days were force-killed
      at flat (NEO peaked +6.77% → closed -0.18%; ENS peaked +11.80% →
      closed +0.04%). The trail-arm threshold and the time-exit window
      have to be in the same ballpark or one of them is useless.

      +15% / 5% / 7d is mutually consistent: a position that runs to
      +15% in a week arms the trail; the 5% giveback locks in +10%
      minimum; positions that fail still hit -10% stops within the
      window. Position like INJ (peaked +30% in 21 hours, exited +21%
      under v3.0) would arm the trail at the same point under v3.1 but
      the tighter 5% giveback would lock in MORE on the way down.

      Worst case: capital tied up 3 extra days in a position that would
      have closed flat at day 4. Acceptable.
    """
    new_peak = max(peak_pnl_pct, pnl_pct)

    # Stop loss (unchanged at -10%)
    if pnl_pct <= MOMENTUM_STOP_PCT:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"stop loss ({pnl_pct*100:.2f}% ≤ {MOMENTUM_STOP_PCT*100:.0f}%)",
            new_peak_pnl_pct=new_peak,
        )

    # v3.1: trail arms at +15% (was +20%), 5% giveback (was 7%)
    if peak_pnl_pct >= MOMENTUM_TRAIL_TRIGGER:
        if pnl_pct <= peak_pnl_pct - MOMENTUM_TRAIL_GIVEBACK:
            return ExitDecision(
                should_exit=True,
                fraction=1.0,
                reason=f"trailing stop ({pnl_pct*100:.2f}% gave back "
                       f"{(peak_pnl_pct - pnl_pct)*100:.2f}% from peak {peak_pnl_pct*100:.2f}%)",
                new_peak_pnl_pct=new_peak,
            )

    # v3.1: hard time exit at 7 days (was 4)
    if age_days >= MOMENTUM_MAX_DAYS:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"7-day momentum window expired (age {age_days:.1f}d, pnl {pnl_pct*100:+.2f}%)",
            new_peak_pnl_pct=new_peak,
        )

    return ExitDecision(
        should_exit=False,
        fraction=0.0,
        reason=f"holding ({pnl_pct*100:+.2f}%, peak {new_peak*100:+.2f}%, age {age_days:.1f}d)",
        new_peak_pnl_pct=new_peak,
    )


def decide_exit_swing_stock(
    *,
    pnl_pct: float,
    peak_pnl_pct: float,
    age_days: float,
) -> ExitDecision:
    """
    v3.0 trail-only: stop -5%, trail arms at +8% with 4% giveback, 14d review.
    Same shape as swing crypto, stock-tuned thresholds.
    """
    new_peak = max(peak_pnl_pct, pnl_pct)

    if pnl_pct <= SWING_STOCKS_STOP_PCT:
        return ExitDecision(
            should_exit=True, fraction=1.0,
            reason=f"stop loss ({pnl_pct*100:.2f}% ≤ {SWING_STOCKS_STOP_PCT*100:.0f}%)",
            new_peak_pnl_pct=new_peak,
        )

    if peak_pnl_pct >= SWING_STOCKS_TRAIL_TRIGGER:
        if pnl_pct <= peak_pnl_pct - SWING_STOCKS_TRAIL_GIVEBACK:
            return ExitDecision(
                should_exit=True, fraction=1.0,
                reason=f"trailing stop ({pnl_pct*100:.2f}% gave back "
                       f"{(peak_pnl_pct - pnl_pct)*100:.2f}% from peak {peak_pnl_pct*100:.2f}%)",
                new_peak_pnl_pct=new_peak,
            )

    if age_days >= SWING_STOCKS_REVIEW_DAYS:
        return ExitDecision(
            should_exit=True, fraction=1.0,
            reason=f"14-day review (age {age_days:.1f}d, pnl {pnl_pct*100:.2f}%)",
            new_peak_pnl_pct=new_peak,
        )

    return ExitDecision(
        should_exit=False, fraction=0.0,
        reason=f"holding ({pnl_pct*100:+.2f}%, peak {new_peak*100:+.2f}%, age {age_days:.1f}d)",
        new_peak_pnl_pct=new_peak,
    )
