"""
RivX safety.py — circuit breakers and bot-level safeguards.

This module is the bot's emergency-stop system. It runs alongside the
trading logic but its only job is to halt the bot when something looks
wrong, before more damage happens.

Yesterday's lesson: an autonomous bot that can't stop itself will dig
holes faster than a human can patch them. The fixes we kept making
("don't sell on time-exit when entry is 0", "validate prices before
buy") were patches. This module is structural.

Five guards:

  1. Drawdown circuit breaker
     If portfolio drops X% from its all-time peak, halt all new buys.
     Default: 5%. Existing positions can still be sold (stops still work).

  2. Single-trade max-loss guard
     If a sell would realize > X% loss, halt and require human review.
     Default: 15%. Catches "ARB at -99% from bad data" scenarios.

  3. Daily trade cap
     Maximum N buys per UTC day. Stops a runaway bot from blowing
     the budget on a bad day. Default: 6 (covers $4 momentum + $2 swing
     daily volume estimates).

  4. Consecutive losses circuit breaker
     If the last N closed trades were all losses, halt. The strategy
     might be wrong for current market conditions. Default: 4.

  5. Heartbeat / watchdog
     Bot writes timestamp every loop. External monitor (or the bot
     itself on next start) detects gaps and alerts. Doesn't prevent
     bugs but makes silent crashes visible.

Each guard returns a SafetyVerdict. The trading code's contract:
before any buy, call check_can_buy(). Before any sell, call
check_can_sell(). If verdict.allowed=False, refuse and log the reason.

State persistence: drawdown peak and consecutive losses survive bot
restarts via Supabase flags (passed in as a callback). Daily counts
reset at UTC midnight.
"""

import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable

log = logging.getLogger(__name__)


# ── Tunable thresholds (overridable via env or config) ────────────────────

DRAWDOWN_HALT_PCT       = 0.05   # 5% from peak halts new buys
DRAWDOWN_RESUME_PCT     = 0.03   # must recover to within 3% of peak to resume
SINGLE_LOSS_HALT_PCT    = 0.15   # >15% loss on a single sell halts bot
DAILY_BUY_CAP           = 6      # max buys per UTC day
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

    Existing positions still get managed (stops/targets fire), so the bot
    can still exit losers — it just can't open new positions while
    underwater. Resume happens when drawdown recovers to <= DRAWDOWN_RESUME_PCT.
    """
    if peak_total_aud <= 0:
        # No peak recorded yet — first run. Allow.
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
    SINGLE_LOSS_HALT_PCT. Catches the ARB-style scenario where bad data
    creates a phantom -99% that auto-stops.

    The bot calls this BEFORE executing the sell. If allowed=False, the
    sell is held back and a Telegram alert fires. The user reviews and
    either confirms (force-sell) or investigates the price feed.
    """
    if entry_aud <= 0:
        # Can't compute % loss with no entry price. Refuse — symptom of
        # the entry-price-zero bug. User must investigate.
        return SafetyVerdict(
            allowed=False,
            reason=f"{symbol}: entry_price is 0 — sell blocked, data issue",
            halt_kind="max_loss",
        )
    loss_pct = (entry_aud - exit_aud) / entry_aud  # positive = loss
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
    """
    If the last N closed trades were ALL losses, halt the strategy.
    Almost certainly means the strategy is wrong for current conditions
    OR there's a systemic data issue.
    """
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
    """
    Returns (is_stale, minutes_since). Used by:
      a) Bot startup: detect that previous instance died silently
      b) External monitor (cron, dashboard): alert if bot stops pinging
    """
    if not last_heartbeat_iso:
        return False, 0  # no heartbeat at all = fresh deploy, not stale
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
    """
    Called BEFORE every buy. Combines all relevant guards.
    Returns the FIRST failing guard, with full reason.
    """
    if manual_kill:
        return SafetyVerdict(False, "kill switch ON (Telegram /pause or dashboard toggle)", "manual")

    # Order matters: report the most actionable failure first.
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
    """
    Called BEFORE every sell. Only the max-loss guard applies — drawdown
    and other guards intentionally don't block sells (we want the bot to
    be able to exit losers even when halted).

    is_forced=True (Telegram /sell or dashboard force-sell) bypasses the
    max-loss check, since the human has chosen to accept the realization.
    """
    if is_forced:
        return SafetyVerdict(True, f"{symbol}: forced sell (human override)")
    return check_sell_loss(entry_aud, exit_aud, symbol)
