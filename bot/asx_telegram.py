# RIVX_VERSION: v2.7-asx-telegram-2026-04-30
"""
RivX-ASX Telegram formatter.

Builds the four message types and sends them via the existing
TelegramNotifier. All messages are clearly tagged 🇦🇺 RivX-ASX so they
never blur with regular RivX crypto/stock alerts.

Setups are sorted by confidence within each type. Pre-open and close
messages cap each section at 8 entries to stay readable on mobile.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

AEST = timezone(timedelta(hours=10))


def _aest_now() -> datetime:
    return datetime.now(AEST)


def _signed_pct(p: float) -> str:
    return f"+{p:.2f}%" if p >= 0 else f"{p:.2f}%"


def _fmt_signal_line(s: dict | object) -> str:
    """Render one signal as 2 lines.

    Accepts either an AsxSignal dataclass instance or a dict from Supabase.
    """
    def g(key, default=None):
        if isinstance(s, dict):
            return s.get(key, default)
        return getattr(s, key, default)

    sym = g("symbol", "?")
    price = float(g("current_price", 0))
    conf = float(g("confidence", 0))
    el = float(g("entry_zone_low", 0))
    eh = float(g("entry_zone_high", 0))
    stop = float(g("stop_price", 0))
    target = float(g("target_price", 0))
    reasoning = (g("reasoning", "") or "").strip()
    rsi = float(g("rsi", 0))

    # R:R calc
    risk = price - stop
    reward = target - price
    rr = (reward / risk) if risk > 0 else 0
    rr_str = f"R:R {rr:.1f}" if rr > 0 else "R:R n/a"

    # Truncate reasoning if very long
    if len(reasoning) > 90:
        reasoning = reasoning[:87].rstrip() + "…"

    line1 = f"  <b>{sym}</b> ${price:.2f} · {int(conf*100)}% conf · {rr_str}"
    line2 = f"    {reasoning}"
    line3 = f"    Entry ${el:.2f}–${eh:.2f} · Stop ${stop:.2f} · Target ${target:.2f}"
    return f"{line1}\n{line2}\n{line3}"


def _section(title: str, signals: list, emoji: str, max_entries: int = 8) -> list[str]:
    if not signals:
        return []
    lines = [f"\n{emoji} <b>{title}</b> ({len(signals)})"]
    sorted_sigs = sorted(
        signals,
        key=lambda s: (s.get("confidence", 0) if isinstance(s, dict)
                        else getattr(s, "confidence", 0)),
        reverse=True,
    )
    for s in sorted_sigs[:max_entries]:
        lines.append(_fmt_signal_line(s))
    if len(sorted_sigs) > max_entries:
        lines.append(f"  …and {len(sorted_sigs) - max_entries} more on dashboard")
    return lines


def build_pre_open_message(signals: list) -> str:
    """Pre-open report at 09:30 AEST."""
    now = _aest_now()
    pullbacks = [s for s in signals if _setup_of(s) == "pullback"]
    breakouts = [s for s in signals if _setup_of(s) == "breakout"]
    oversolds = [s for s in signals if _setup_of(s) == "oversold_bounce"]

    lines = [
        f"🇦🇺 <b>RivX-ASX PRE-OPEN</b>",
        f"{now.strftime('%a %d %b, %H:%M')} AEST · market opens 10:00",
    ]

    if not signals:
        lines.append("\nNo qualifying setups this morning. Quiet open.")
    else:
        lines += _section("PULLBACK SETUPS", pullbacks, "📈")
        lines += _section("BREAKOUT WATCH", breakouts, "🚀")
        lines += _section("OVERSOLD BOUNCE (speculative)", oversolds, "⚡", max_entries=5)

    lines.append("")
    lines.append("📊 Full analysis in dashboard · CommSec ready")
    return "\n".join(lines)


def build_midday_message(signals: list, prior_pre_open_symbols: set) -> str:
    """Midday update at 12:30 AEST.

    prior_pre_open_symbols: set of symbols that were in this morning's
    pre-open report (so we can flag NEW setups vs ones that have been
    available since open).
    """
    now = _aest_now()
    lines = [
        f"🇦🇺 <b>RivX-ASX MIDDAY</b>",
        f"{now.strftime('%a %d %b, %H:%M')} AEST · 2.5h to close",
    ]

    new_setups = [s for s in signals if _sym_of(s) not in prior_pre_open_symbols]
    still_valid = [s for s in signals if _sym_of(s) in prior_pre_open_symbols]

    if new_setups:
        lines += _section("NEW SETUPS THIS MORNING", new_setups, "🆕")
    else:
        lines.append("\n🆕 No new setups since open.")

    if still_valid:
        lines += _section("STILL VALID FROM PRE-OPEN", still_valid, "✅", max_entries=5)

    if not signals:
        lines.append("\nNo qualifying setups currently.")

    lines.append("")
    lines.append("📊 Dashboard for live data")
    return "\n".join(lines)


def build_close_message(signals_today: list, outcomes_today: dict) -> str:
    """End-of-day recap at 16:30 AEST.

    signals_today: all signals fired today.
    outcomes_today: {symbol: {hit_target|hit_stop|still_open|...}} for those
                    same signals.
    """
    now = _aest_now()
    lines = [
        f"🇦🇺 <b>RivX-ASX CLOSE</b>",
        f"{now.strftime('%a %d %b, %H:%M')} AEST · market closed",
    ]

    if not signals_today:
        lines.append("\nNo signals fired today.")
    else:
        n_total = len(signals_today)
        n_hit = sum(1 for o in outcomes_today.values() if o == "hit_target")
        n_stop = sum(1 for o in outcomes_today.values() if o == "hit_stop")
        n_open = sum(1 for o in outcomes_today.values() if o in (None, "pending", "still_open"))

        lines.append(f"\n<b>TODAY'S SIGNALS: {n_total}</b>")
        lines.append(f"  ✅ Hit target: {n_hit}")
        lines.append(f"  ❌ Hit stop: {n_stop}")
        lines.append(f"  ⏳ Still open: {n_open}")

        # Show top performers if any
        # We sort signals_today by current move vs entry
        ranked = []
        for s in signals_today:
            sym = _sym_of(s)
            ranked.append((sym, s))
        if ranked:
            lines.append(f"\n<b>SIGNALS:</b>")
            for sym, s in ranked[:10]:
                conf = float(_get(s, "confidence", 0))
                setup = _setup_of(s)
                outcome = outcomes_today.get(sym, "pending")
                emoji = {"hit_target": "🟢", "hit_stop": "🔴",
                         "pending": "⏳", "still_open": "⏳"}.get(outcome, "·")
                lines.append(
                    f"  {emoji} <b>{sym}</b> [{setup}] "
                    f"{int(conf*100)}% · {outcome}"
                )

    lines.append("")
    lines.append("📊 Full data + hit-rate stats in dashboard")
    return "\n".join(lines)


def build_high_conviction_message(signal) -> str:
    """Immediate alert when a high-conviction setup appears mid-session."""
    now = _aest_now()
    sym = _sym_of(signal)
    setup = _setup_of(signal)
    conf = float(_get(signal, "confidence", 0))
    vol_ratio = float(_get(signal, "volume_ratio", 0))
    price = float(_get(signal, "current_price", 0))
    el = float(_get(signal, "entry_zone_low", 0))
    eh = float(_get(signal, "entry_zone_high", 0))
    stop = float(_get(signal, "stop_price", 0))
    target = float(_get(signal, "target_price", 0))
    reasoning = (_get(signal, "reasoning", "") or "").strip()

    risk = price - stop
    reward = target - price
    rr = (reward / risk) if risk > 0 else 0

    return "\n".join([
        f"🇦🇺 ⚡ <b>HIGH-CONVICTION SETUP</b>",
        f"{now.strftime('%H:%M')} AEST",
        "",
        f"<b>{sym}</b> ${price:.2f} · {setup} setup · {int(conf*100)}% conf",
        f"{reasoning}",
        f"",
        f"Entry: ${el:.2f}–${eh:.2f}",
        f"Stop: ${stop:.2f} (risk ${risk:.2f}/sh)",
        f"Target: ${target:.2f} (reward ${reward:.2f}/sh)",
        f"R:R {rr:.1f} · Volume {vol_ratio:.1f}x avg",
        f"",
        f"Fired automatically: conf ≥ 80% AND volume ≥ 3x avg",
    ])


# ── Helpers that work with both dict and dataclass forms ────────────────

def _get(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _sym_of(obj) -> str:
    return _get(obj, "symbol", "?")


def _setup_of(obj) -> str:
    return _get(obj, "setup_type", "")
