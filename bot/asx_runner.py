# RIVX_VERSION: v2.7.1-asx-runner-2026-04-30
"""
RivX-ASX runner — orchestrates the ASX scans.

v2.7.1 fix: on first startup with empty flags, all four scheduled scans
fired at once because _at_or_past_today() returned True for every past
time. Now we seed the flags on first run so scans only fire at the
NEXT scheduled time.

bot.py calls into this with a tick on every main-loop iteration. This
module decides whether it's time for a scan and fires it if so. All
state (last-scan timestamps, list of pre-open symbols for midday
diff) is persisted via Supabase bot_flags so it survives restarts.

Schedule (AEST weekdays only, except outcome-update which runs daily):
  - 09:30  pre-open scan
  - 12:30  midday scan
  - 16:30  close-of-day scan
  - 10:00..16:00 every 15 min: high-conviction polling
  - 17:00  outcome update (resolve pending signals against today's bars)

Failures are caught and logged. ASX failures NEVER halt RivX.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

AEST = timezone(timedelta(hours=10))


# ── Schedule config ──────────────────────────────────────────────────────

PRE_OPEN_TIME      = "09:30"
MIDDAY_TIME        = "12:30"
CLOSE_TIME         = "16:30"
OUTCOME_TIME       = "17:00"

INTRADAY_START     = "10:00"
INTRADAY_END       = "16:00"
INTRADAY_POLL_MIN  = 15

# Bot-flags keys
FLAG_LAST_PRE_OPEN  = "asx_last_pre_open"
FLAG_LAST_MIDDAY    = "asx_last_midday"
FLAG_LAST_CLOSE     = "asx_last_close"
FLAG_LAST_OUTCOME   = "asx_last_outcome"
FLAG_LAST_INTRADAY  = "asx_last_intraday_poll"
FLAG_FIRST_BOOT     = "asx_first_boot_done"
FLAG_PRE_OPEN_SYMS  = "asx_pre_open_symbols_today"


# ── Time helpers ─────────────────────────────────────────────────────────

def _aest_now() -> datetime:
    return datetime.now(AEST)


def _is_weekday() -> bool:
    return _aest_now().weekday() < 5


def _at_or_past_today(target_hhmm: str, last_run_iso: str | None) -> bool:
    """True iff current AEST time is past target AND we haven't run today."""
    now = _aest_now()
    th, tm = map(int, target_hhmm.split(":"))
    target = now.replace(hour=th, minute=tm, second=0, microsecond=0)
    if now < target:
        return False
    if not last_run_iso:
        return True
    try:
        last = datetime.fromisoformat(last_run_iso.replace("Z", "+00:00"))
        last_aest = last.astimezone(AEST)
        return last_aest < target
    except Exception:
        return True


def _in_intraday_window() -> bool:
    now = _aest_now()
    sh, sm = map(int, INTRADAY_START.split(":"))
    eh, em = map(int, INTRADAY_END.split(":"))
    start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= now < end


def _should_intraday_poll(last_iso: str | None) -> bool:
    if not last_iso:
        return True
    try:
        last = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
        delta = (datetime.now(timezone.utc) - last).total_seconds() / 60
        return delta >= INTRADAY_POLL_MIN
    except Exception:
        return True


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _seed_flags_for_first_boot(db, log_obj):
    """
    On the very first boot, mark all past-time scans for today as 'already
    ran' so we don't avalanche. This means the first real scan will be
    tomorrow's 09:30 AEST pre-open (or today's if we boot before 09:30).
    """
    if db.get_flag(FLAG_FIRST_BOOT):
        return  # already seeded

    now = _aest_now()
    seeded = []

    # For each scheduled time, if we're already past it today, mark it as
    # done so it doesn't fire today. If we're before it, leave it empty
    # so it can fire normally at the scheduled time.
    schedule = [
        (PRE_OPEN_TIME, FLAG_LAST_PRE_OPEN),
        (MIDDAY_TIME,   FLAG_LAST_MIDDAY),
        (CLOSE_TIME,    FLAG_LAST_CLOSE),
        (OUTCOME_TIME,  FLAG_LAST_OUTCOME),
    ]
    for hhmm, flag_key in schedule:
        h, m = map(int, hhmm.split(":"))
        target = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if now >= target:
            db.set_flag(flag_key, _utc_now_iso())
            seeded.append(hhmm)

    # Also seed intraday so we don't immediately fire one
    if _in_intraday_window():
        db.set_flag(FLAG_LAST_INTRADAY, _utc_now_iso())
        seeded.append("intraday")

    db.set_flag(FLAG_FIRST_BOOT, _utc_now_iso())
    if seeded:
        log_obj.info(f"ASX first boot: seeded {seeded} as already-run for today. "
                     f"Next scan will be next scheduled time.")
    else:
        log_obj.info("ASX first boot: nothing to seed (booted before any scheduled scan)")


# ── Main tick (called from bot.py main loop) ─────────────────────────────

def tick(db, tg, log_obj=None):
    """
    Called once per main-loop iteration from bot.py.
    Cheap when nothing's due — does only a few DB reads per call.
    """
    if log_obj is None:
        log_obj = log

    try:
        # First-time setup: seed flags so we don't avalanche
        _seed_flags_for_first_boot(db, log_obj)

        if not _is_weekday():
            return

        last_pre_open = db.get_flag(FLAG_LAST_PRE_OPEN) or ""
        last_midday   = db.get_flag(FLAG_LAST_MIDDAY) or ""
        last_close    = db.get_flag(FLAG_LAST_CLOSE) or ""
        last_outcome  = db.get_flag(FLAG_LAST_OUTCOME) or ""
        last_intraday = db.get_flag(FLAG_LAST_INTRADAY) or ""

        if _at_or_past_today(PRE_OPEN_TIME, last_pre_open):
            _run_pre_open(db, tg, log_obj)
            db.set_flag(FLAG_LAST_PRE_OPEN, _utc_now_iso())
            return

        if _at_or_past_today(MIDDAY_TIME, last_midday):
            _run_midday(db, tg, log_obj)
            db.set_flag(FLAG_LAST_MIDDAY, _utc_now_iso())
            return

        if _at_or_past_today(CLOSE_TIME, last_close):
            _run_close(db, tg, log_obj)
            db.set_flag(FLAG_LAST_CLOSE, _utc_now_iso())
            return

        if _at_or_past_today(OUTCOME_TIME, last_outcome):
            _run_outcome_update(db, log_obj)
            db.set_flag(FLAG_LAST_OUTCOME, _utc_now_iso())
            return

        if _in_intraday_window() and _should_intraday_poll(last_intraday):
            _run_intraday_high_conviction(db, tg, log_obj)
            db.set_flag(FLAG_LAST_INTRADAY, _utc_now_iso())
            return

    except Exception as e:
        log_obj.warning(f"asx tick error: {e}")


# ── Scan implementations ────────────────────────────────────────────────

def _run_pre_open(db, tg, log_obj):
    log_obj.info("ASX: pre-open scan starting")
    try:
        from bot import asx_analyser, asx_telegram
    except ImportError as e:
        log_obj.warning(f"ASX modules not available: {e}")
        return

    signals = asx_analyser.scan_asx(scan_event="pre_open")
    if not signals:
        msg = asx_telegram.build_pre_open_message([])
        tg.send(msg)
        return

    asx_analyser.save_signals(db, signals, "pre_open")

    syms_today = ",".join(sorted({s.symbol for s in signals}))
    db.set_flag(FLAG_PRE_OPEN_SYMS, syms_today)

    sig_dicts = [_signal_as_dict(s) for s in signals]
    msg = asx_telegram.build_pre_open_message(sig_dicts)
    tg.send(msg)
    log_obj.info(f"ASX pre-open: {len(signals)} signals sent")


def _run_midday(db, tg, log_obj):
    log_obj.info("ASX: midday scan starting")
    try:
        from bot import asx_analyser, asx_telegram
    except ImportError as e:
        log_obj.warning(f"ASX modules not available: {e}")
        return

    signals = asx_analyser.scan_asx(scan_event="midday")

    prior_syms_str = db.get_flag(FLAG_PRE_OPEN_SYMS) or ""
    prior_syms = set(s.strip() for s in prior_syms_str.split(",") if s.strip())

    if signals:
        asx_analyser.save_signals(db, signals, "midday")

    sig_dicts = [_signal_as_dict(s) for s in signals]
    msg = asx_telegram.build_midday_message(sig_dicts, prior_syms)
    tg.send(msg)
    log_obj.info(f"ASX midday: {len(signals)} signals sent")


def _run_close(db, tg, log_obj):
    log_obj.info("ASX: close scan starting")
    try:
        from bot import asx_analyser, asx_telegram
    except ImportError as e:
        log_obj.warning(f"ASX modules not available: {e}")
        return

    try:
        asx_analyser.update_signal_outcomes(db, log_obj)
    except Exception as e:
        log_obj.warning(f"close-time outcome update: {e}")

    today_aest = _aest_now().date()
    midnight_aest = datetime.combine(today_aest, datetime.min.time(), tzinfo=AEST)
    midnight_utc = midnight_aest.astimezone(timezone.utc).isoformat()

    try:
        rows = db._get("asx_signals", {
            "fired_at": f"gte.{midnight_utc}",
            "order": "fired_at.asc",
            "limit": "200",
        }) or []
    except Exception as e:
        log_obj.warning(f"close: read today's signals: {e}")
        rows = []

    outcomes_by_sym = {r.get("symbol"): r.get("outcome", "pending") for r in rows}
    msg = asx_telegram.build_close_message(rows, outcomes_by_sym)
    tg.send(msg)
    log_obj.info(f"ASX close: recap sent ({len(rows)} signals today)")


def _run_outcome_update(db, log_obj):
    log_obj.info("ASX: outcome update starting")
    try:
        from bot import asx_analyser
    except ImportError as e:
        log_obj.warning(f"ASX modules not available: {e}")
        return
    try:
        asx_analyser.update_signal_outcomes(db, log_obj)
    except Exception as e:
        log_obj.warning(f"outcome update: {e}")


def _run_intraday_high_conviction(db, tg, log_obj):
    log_obj.info("ASX: intraday high-conviction poll")
    try:
        from bot import asx_analyser, asx_telegram
    except ImportError as e:
        log_obj.warning(f"ASX modules not available: {e}")
        return

    signals = asx_analyser.scan_asx(
        scan_event="high_conviction",
        intraday_volume_check=True,
    )
    hc_signals = [s for s in signals if s.high_conviction]
    if not hc_signals:
        return

    today_aest = _aest_now().date()
    midnight_aest = datetime.combine(today_aest, datetime.min.time(), tzinfo=AEST)
    midnight_utc = midnight_aest.astimezone(timezone.utc).isoformat()

    try:
        already = db._get("asx_signals", {
            "fired_at": f"gte.{midnight_utc}",
            "high_conviction": "eq.true",
            "limit": "200",
        }) or []
    except Exception:
        already = []
    already_syms = {r.get("symbol") for r in already}

    new_hc = [s for s in hc_signals if s.symbol not in already_syms]
    if not new_hc:
        return

    asx_analyser.save_signals(db, new_hc, "high_conviction")
    for s in new_hc:
        msg = asx_telegram.build_high_conviction_message(_signal_as_dict(s))
        tg.send(msg)
    log_obj.info(f"ASX intraday: {len(new_hc)} high-conviction alerts sent")


# ── Helpers ──────────────────────────────────────────────────────────────

def _signal_as_dict(s) -> dict:
    if isinstance(s, dict):
        return s
    return {
        "symbol": s.symbol,
        "setup_type": s.setup_type,
        "confidence": s.confidence,
        "current_price": s.current_price,
        "entry_zone_low": s.entry_zone_low,
        "entry_zone_high": s.entry_zone_high,
        "stop_price": s.stop_price,
        "target_price": s.target_price,
        "reasoning": s.reasoning,
        "signal_strength": s.signal_strength,
        "volume_ratio": s.volume_ratio,
        "rsi": s.rsi,
        "pullback_pct": s.pullback_pct,
        "high_conviction": s.high_conviction,
    }
