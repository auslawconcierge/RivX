# RIVX_VERSION: v3.0-buy-cap-15-2026-05-07
"""
RivX safety.py — circuit breakers and bot-level safeguards.

This module is the bot's emergency-stop system. It runs alongside the
trading logic but its only job is to halt the bot when something looks
wrong, before more damage happens.

Five guards:

  1. Drawdown circuit breaker
     If portfolio drops X% from its all-time peak, halt all new buys.
     Default: 5%. Existing positions can still be sold (stops still work).

  2. Single-trade max-loss guard
     If a sell would realize > X% loss, halt and require human review.
     Default: 15%. Catches "ARB at -99% from bad data" scenarios.

  3. Daily trade cap
     Maximum N buys per UTC day. Stops a runaway bot from blowing
     the budget on a bad day.

     v3.0: raised from 10 to 15. The new strategy targets ~120-180
     trades/year with looser entry rules, momentum every 2 hours, and
     trail-only exits (which fire more often than target+trail). On a
     strong-trend day the previous cap of 10 would block valid swing
     and stock buys after the morning momentum scans burned through it.
     15 buys × ~$700 avg = ~$10.5K — within total budget, so cap remains
     a runaway-bot guard rather than a strategic constraint.

     v2.9.0: bumped from 6 to 10 to match 12 momentum scans/day.

  4. Consecutive losses circuit breaker
     If the last N closed trades were all losses, halt. The strategy
     might be wrong for current market conditions. Default: 4.

     v3.0 NOTE: the trail-only strategy by design has a lower win rate
     than the old half-take + target rules. Expect more frequent stop-outs
     (which is the trade-off for letting winners run). 4-loss streaks
     should be more common in the first month of paper trading. If we
     hit consec_loss halts repeatedly without a clear edge case, we
     revisit this threshold in v3.1.

  5. Heartbeat / watchdog
     Bot writes timestamp every loop. External monitor (or the bot
     itself on next start) detects gaps and alerts. Doesn't prevent
     bugs but makes silent crashes visible.

Each guard returns a SafetyVerdict. The trading code's contract:
before any buy, call check_can_buy(). Before any sell, call
check_can_sell(). If verdict.allowed=False, refuse and log the reason.
"""

import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable

log = logging.getLogger(__name__)


# ── Tunable thresholds ────────────────────────────────────────────────────

DRAWDOWN_HALT_PCT       = 0.05   # 5% from peak halts new buys
DRAWDOWN_RESUME_PCT     = 0.03   # must recover to within 3% of peak to resume
SINGLE_LOSS_HALT_PCT    = 0.15   # >15% loss on a single sell halts bot
DAILY_BUY_CAP           = 15     # v3.0: raised from 10 to match new cadence
CONSECUTIVE_LOSS_HALT   = 4      # 4 losing closes in a row halts bot
HEARTBEAT_STALE_MINUTES = 10     # if heartbeat older than this, alert


@dataclass
class SafetyVerdict:
    """Returned by every check_* function."""
    allowed: bool
    reason: str          # human-readable explanation, even when allowed
    halt_kind: str = ""  # "" if allowed, else "drawdown" / "max_loss" / "daily_cap" / "consec_loss" / "manual"


# ── 1. Drawdown circuit breaker ───────────────────────────────────────────

def check_drawdown(
    current_total_aud: float,
    peak_total_aud: float,
) -> SafetyVerdict:
    """
    If portfolio is down >= DRAWDOWN_HALT_PCT from its peak, halt new buys.
    Existing positions still get managed (stops/trails fire).
    """
    if peak_total_aud <= 0:
        return SafetyVerdict(True, "no peak recorded yet")
    drop = (peak_total_aud - current_total_aud) / peak_total_aud
    if drop >= DRAWDOWN_HALT_PCT:
        return SafetyVerdict(
            allowed=False,
            reason=f"drawdown {drop*100:.1f}% from peak ${peak_total_aud:.0f} "
                   f"exceeds {DRAWDOWN_HALT_PCT*100:.0f}% halt threshold",
            halt_kind="drawdown",
        )
    return SafetyVerdict(True, f"drawdown {drop*100:.1f}% within tolerance")


def update_peak(current_total_aud: float, peak_total_aud: float) -> float:
    """Returns new peak (max of current vs stored)."""
    return max(current_total_aud, peak_total_aud)


# ── 2. Single-trade max-loss guard ────────────────────────────────────────

def check_sell_loss(
    entry_aud: float,
    exit_aud: float,
    symbol: str,
) -> SafetyVerdict:
    """
    Refuses a sell that would record a realized loss greater than
    SINGLE_LOSS_HALT_PCT.
    """
    if entry_aud <= 0:
        return SafetyVerdict(
            allowed=False,
            reason=f"{symbol}: entry_price is 0 — sell blocked, data issue",
            halt_kind="max_loss",
        )
    loss_pct = (entry_aud - exit_aud) / entry_aud
    if loss_pct >= SINGLE_LOSS_HALT_PCT:
        return SafetyVerdict(
            allowed=False,
            reason=f"{symbol}: sell would realize {loss_pct*100:.1f}% loss "
                   f"(entry ${entry_aud:.4f} → exit ${exit_aud:.4f}). "
                   f"Threshold {SINGLE_LOSS_HALT_PCT*100:.0f}%. Halt for review.",
            halt_kind="max_loss",
        )
    return SafetyVerdict(True, f"{symbol}: loss {loss_pct*100:.2f}% within tolerance")


# ── 3. Daily trade cap ────────────────────────────────────────────────────

def check_daily_cap(buys_today: int) -> SafetyVerdict:
    """Hard cap on buys per UTC day. Prevents runaway-bot scenarios."""
    if buys_today >= DAILY_BUY_CAP:
        return SafetyVerdict(
            allowed=False,
            reason=f"daily buy cap reached ({buys_today}/{DAILY_BUY_CAP}). "
                   f"Resets at UTC midnight.",
            halt_kind="daily_cap",
        )
    return SafetyVerdict(True, f"buys today {buys_today}/{DAILY_BUY_CAP}")


# ── 4. Consecutive losses circuit breaker ─────────────────────────────────

def check_consecutive_losses(consecutive_losses: int) -> SafetyVerdict:
    """If the last N closed trades were ALL losses, halt the strategy."""
    if consecutive_losses >= CONSECUTIVE_LOSS_HALT:
        return SafetyVerdict(
            allowed=False,
            reason=f"{consecutive_losses} consecutive losses — strategy halt. "
                   f"Review before resuming.",
            halt_kind="consec_loss",
        )
    return SafetyVerdict(True, f"consecutive losses {consecutive_losses}/{CONSECUTIVE_LOSS_HALT}")


def update_consecutive_losses(prior_count: int, last_trade_was_loss: bool) -> int:
    """If loss → increment. If win → reset to 0."""
    return prior_count + 1 if last_trade_was_loss else 0


# ── 5. Heartbeat / watchdog ───────────────────────────────────────────────

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_heartbeat_stale(last_heartbeat_iso: str) -> tuple[bool, int]:
    """Returns (is_stale, minutes_since)."""
    if not last_heartbeat_iso:
        return False, 0
    try:
        last = datetime.fromisoformat(last_heartbeat_iso.replace("Z", "+00:00"))
        delta_min = int((datetime.now(timezone.utc) - last).total_seconds() / 60)
        return delta_min >= HEARTBEAT_STALE_MINUTES, delta_min
    except Exception as e:
        log.warning(f"Bad heartbeat format: {last_heartbeat_iso}: {e}")
        return False, 0


# ── Composite checks called from trading code ─────────────────────────────

def check_can_buy(
    *,
    current_total_aud: float,
    peak_total_aud: float,
    buys_today: int,
    consecutive_losses: int,
    manual_kill: bool = False,
) -> SafetyVerdict:
    """Called BEFORE every buy. Combines all relevant guards."""
    if manual_kill:
        return SafetyVerdict(False, "kill switch ON (Telegram /pause or dashboard toggle)", "manual")

    v = check_drawdown(current_total_aud, peak_total_aud)
    if not v.allowed:
        return v

    v = check_consecutive_losses(consecutive_losses)
    if not v.allowed:
        return v

    v = check_daily_cap(buys_today)
    if not v.allowed:
        return v

    return SafetyVerdict(True, "all buy guards pass")


def check_can_sell(
    *,
    symbol: str,
    entry_aud: float,
    exit_aud: float,
    is_forced: bool = False,
) -> SafetyVerdict:
    """Called BEFORE every sell. is_forced=True bypasses max-loss check."""
    if is_forced:
        return SafetyVerdict(True, f"{symbol}: forced sell (human override)")
    return check_sell_loss(entry_aud, exit_aud, symbol)
