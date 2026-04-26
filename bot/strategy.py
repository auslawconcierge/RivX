"""
RivX strategy.py — the trading rules.

This module owns "what we buy and when," "what we sell and why," and "how
much money goes where." It produces decisions, but it does NOT execute
them. Execution is in bot.py. Data fetching is in prices.py and scanner.py.
This file is pure logic — easy to test, easy to change.

Yesterday's lessons baked into these rules:

  - DON'T buy things that have already pumped 15%+ in 24h. That's chasing
    the top of a move. Yesterday's scoring rewarded that. We've inverted it.

  - DON'T auto-sell after 4 hours of "no movement." Sideways is normal.
    Most of the time, most assets are sideways. Selling on sideways
    guarantees turnover for no edge.

  - DO use wider stops. -2.5% gets stopped out by normal noise. Real
    swing trades need -8% room. Real momentum trades need -10%.

  - DO favor inaction. If nothing is a clean setup, buy nothing.
    Cash is a position. Empty slots are fine.

────────────────────────────────────────────────────────────────────────────
Capital allocation ($10,000 total)
────────────────────────────────────────────────────────────────────────────

  Swing crypto    $4,000   up to 5 positions   $800 each      patient pile
  Momentum crypto $2,000   up to 4 positions   $500 each      aggressive pile
  Swing stocks    $3,500   up to 3 positions   ~$1,170 each   FX-cost aware
  Ops floor          $500   not deployed                       fees + FX buffer

  Bot CAN deploy up to $9,500 if good setups exist. Doesn't HAVE to.

────────────────────────────────────────────────────────────────────────────
Entry rules
────────────────────────────────────────────────────────────────────────────

  SWING CRYPTO — buying quality on pullbacks
    - Top 30 by market cap (filter out micro-cap garbage)
    - Currently DOWN 5-15% from 7-day high (the pullback)
    - But still above 50-day moving average (the uptrend is intact)
    - Decision once daily at 8 AM AEST

  MOMENTUM CRYPTO — catching the start of moves
    - Outside top 30 (mid/small cap, more upside potential)
    - Just broke above 7-day high TODAY (not "already up 15%, late")
    - Volume in last 24h > 2x its 7-day average (real interest, not noise)
    - Decision twice daily, 8 AM and 4 PM AEST

  SWING STOCKS — buying quality on pullbacks (US equities)
    - From staples list (NVDA, AAPL, MSFT, etc.) or top Alpaca screener
    - Down 3-8% from 7-day high
    - Above 50-day MA
    - Decision once daily at 8 AM AEST during market hours window

────────────────────────────────────────────────────────────────────────────
Exit rules
────────────────────────────────────────────────────────────────────────────

  SWING (crypto + stocks)
    Stop: -8% from entry (crypto), -5% from entry (stocks)
    Target: +15% takes HALF, trailing stop on the rest (5% trail)
    Time: review at 30 days, no auto-exit

  MOMENTUM
    Stop: -10% from entry
    Target: +30% (no take-half — let it run or stop out)
    Time: max 7 days, then exit if not at target

  Removed entirely: 4-hour "no movement" rule. Sideways is fine.

────────────────────────────────────────────────────────────────────────────
"""

from dataclasses import dataclass
from typing import Optional


# ── Allocation constants ──────────────────────────────────────────────────

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


# ── Exit rules ────────────────────────────────────────────────────────────

# Crypto swing
SWING_CRYPTO_STOP_PCT       = -0.08    # -8%
SWING_CRYPTO_TARGET_PCT     = 0.15     # +15%
SWING_CRYPTO_TRAIL_TRIGGER  = 0.15     # arm trailing at +15%
SWING_CRYPTO_TRAIL_GIVEBACK = 0.05     # exit if 5% below trailing peak
SWING_CRYPTO_REVIEW_DAYS    = 30

# Crypto momentum
MOMENTUM_STOP_PCT     = -0.10    # -10%
MOMENTUM_TARGET_PCT   = 0.30     # +30% (no half-out — runs or stops)
MOMENTUM_MAX_DAYS     = 7

# Stocks (lower volatility, tighter rules)
SWING_STOCKS_STOP_PCT       = -0.05
SWING_STOCKS_TARGET_PCT     = 0.12
SWING_STOCKS_TRAIL_TRIGGER  = 0.12
SWING_STOCKS_TRAIL_GIVEBACK = 0.04
SWING_STOCKS_REVIEW_DAYS    = 30


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
    """
    if market_cap_rank is None or market_cap_rank > 30:
        return False, f"rank {market_cap_rank} outside top 30"

    # Pullback should be 5-15% off recent high (not too shallow, not crashing)
    if pullback_from_7d_high_pct >= -0.05:
        return False, f"only {pullback_from_7d_high_pct*100:.1f}% off 7d high (need -5% to -15%)"
    if pullback_from_7d_high_pct < -0.15:
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
    broke_7d_high_today: bool,            # True if today's high > prior 7d high
    volume_vs_7d_avg_ratio: float,       # 2.5 = 2.5x average volume
) -> tuple[bool, str]:
    """
    Catching the START of a breakout, not the middle.
    Outside top 30 (more upside), broke a recent high TODAY, on real volume.
    """
    if market_cap_rank is None:
        return False, "no rank"
    if market_cap_rank <= 30:
        return False, f"rank {market_cap_rank} too big for momentum bucket"
    if market_cap_rank > 200:
        return False, f"rank {market_cap_rank} too obscure"

    if not broke_7d_high_today:
        return False, "no 7d-high breakout today"

    if volume_vs_7d_avg_ratio < 2.0:
        return False, f"volume only {volume_vs_7d_avg_ratio:.1f}x average (need ≥2x)"

    return True, (
        f"rank {market_cap_rank} broke 7d high today on "
        f"{volume_vs_7d_avg_ratio:.1f}x volume"
    )


def qualifies_swing_stock(
    *,
    is_quality: bool,                     # in staples list, or passes quality filter
    pullback_from_7d_high_pct: float,
    above_50d_ma: bool,
) -> tuple[bool, str]:
    """Same as swing crypto but with stock-appropriate thresholds."""
    if not is_quality:
        return False, "not in quality list"

    if pullback_from_7d_high_pct >= -0.03:
        return False, f"only {pullback_from_7d_high_pct*100:.1f}% off 7d high (need -3% to -8%)"
    if pullback_from_7d_high_pct < -0.08:
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

    Reward:
      - higher market cap (lower rank number) = quality preference
      - pullback in the sweet spot (-7% to -10%) = better reward/risk
      - above 50d MA confirmed = uptrend
    """
    if not above_50d_ma:
        return 0.0
    score = 0.0
    # Rank: top 5 = 2.0, top 10 = 1.5, top 30 = 1.0, else 0
    if market_cap_rank <= 5:    score += 2.0
    elif market_cap_rank <= 10: score += 1.5
    elif market_cap_rank <= 30: score += 1.0
    # Pullback sweet spot: -8 to -10%
    abs_pull = abs(pullback_pct)
    if 0.07 <= abs_pull <= 0.10:    score += 2.0
    elif 0.05 <= abs_pull <= 0.13:  score += 1.0
    return score


def prescore_momentum_crypto(*, market_cap_rank: int, broke_7d_high_today: bool,
                             volume_ratio: float) -> float:
    """
    Reward fresh breakouts on big volume in the right cap range.
    """
    if not broke_7d_high_today:
        return 0.0
    score = 1.0  # base for any breakout
    # Bigger volume = stronger conviction
    if volume_ratio >= 4.0:    score += 2.0
    elif volume_ratio >= 3.0:  score += 1.5
    elif volume_ratio >= 2.0:  score += 1.0
    # Mid-cap sweet spot (rank 30-100): biggest upside-with-floor
    if 30 < market_cap_rank <= 80:    score += 1.5
    elif 80 < market_cap_rank <= 150: score += 1.0
    return score


def prescore_swing_stock(*, pullback_pct: float, above_50d_ma: bool) -> float:
    """Same shape as crypto, tighter window."""
    if not above_50d_ma:
        return 0.0
    score = 1.0  # quality stock list = base
    abs_pull = abs(pullback_pct)
    if 0.04 <= abs_pull <= 0.06:    score += 2.0
    elif 0.03 <= abs_pull <= 0.08:  score += 1.0
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
    fraction: float       # 1.0 = full exit, 0.5 = take half, 0.0 = keep
    reason: str
    new_peak_pnl_pct: float = 0.0   # what to update the trailing-stop watermark to


def decide_exit_swing_crypto(
    *,
    pnl_pct: float,                  # current unrealized P&L as fraction (0.05 = +5%)
    peak_pnl_pct: float,             # highest pnl_pct seen so far for trailing
    age_days: float,
) -> ExitDecision:
    """
    Stop: -8%. Target: +15% takes half, then trail with 5% give-back.
    No 4hr time-exit — review at 30 days only.
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

    # Trailing stop (only armed once we hit +15%)
    if peak_pnl_pct >= SWING_CRYPTO_TRAIL_TRIGGER:
        if pnl_pct <= peak_pnl_pct - SWING_CRYPTO_TRAIL_GIVEBACK:
            return ExitDecision(
                should_exit=True,
                fraction=1.0,
                reason=f"trailing stop ({pnl_pct*100:.2f}% gave back "
                       f"{(peak_pnl_pct - pnl_pct)*100:.2f}% from peak {peak_pnl_pct*100:.2f}%)",
                new_peak_pnl_pct=new_peak,
            )

    # Time review at 30 days
    if age_days >= SWING_CRYPTO_REVIEW_DAYS:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"30-day review reached (age {age_days:.1f}d, pnl {pnl_pct*100:.2f}%)",
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
    age_days: float,
) -> ExitDecision:
    """
    Stop: -10%. Target: +30% (no half-out, runs or stops).
    Hard time-exit at 7 days — momentum thesis is dead by then.
    """
    if pnl_pct <= MOMENTUM_STOP_PCT:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"stop loss ({pnl_pct*100:.2f}% ≤ {MOMENTUM_STOP_PCT*100:.0f}%)",
        )

    if pnl_pct >= MOMENTUM_TARGET_PCT:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"target reached ({pnl_pct*100:.2f}% ≥ {MOMENTUM_TARGET_PCT*100:.0f}%)",
        )

    if age_days >= MOMENTUM_MAX_DAYS:
        return ExitDecision(
            should_exit=True,
            fraction=1.0,
            reason=f"7-day momentum window expired (age {age_days:.1f}d, pnl {pnl_pct*100:+.2f}%)",
        )

    return ExitDecision(
        should_exit=False,
        fraction=0.0,
        reason=f"holding ({pnl_pct*100:+.2f}%, age {age_days:.1f}d)",
    )


def decide_exit_swing_stock(
    *,
    pnl_pct: float,
    peak_pnl_pct: float,
    age_days: float,
) -> ExitDecision:
    """Same shape as swing crypto with stock-tuned thresholds."""
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
            reason=f"30-day review (age {age_days:.1f}d, pnl {pnl_pct*100:.2f}%)",
            new_peak_pnl_pct=new_peak,
        )

    return ExitDecision(
        should_exit=False, fraction=0.0,
        reason=f"holding ({pnl_pct*100:+.2f}%, peak {new_peak*100:+.2f}%, age {age_days:.1f}d)",
        new_peak_pnl_pct=new_peak,
    )
