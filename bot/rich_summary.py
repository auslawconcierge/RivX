# RIVX_VERSION: v2.5-rich-summary-2026-04-30
"""
Rich daily summary for RivX.

Replaces the thin run_daily_summary() in bot.py and adds a new
send_rich_summary() method to telegram_notify.py.

Reads from `positions` table directly (the `trades` table is currently
not being populated on closes — separate bug). Filters out phantom
cleanups (exit_price == entry_price AND pnl_pct == 0).

Designed for someone reading on mobile while away from desk: maximum
useful data, scannable structure, no fluff.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from bot import strategy


# ── Time helpers (duplicated from bot.py to keep this self-contained) ───

AEST = timezone(timedelta(hours=10))


def _aest_now() -> datetime:
    return datetime.now(AEST)


def _us_market_state() -> str:
    """Returns one of: open, premarket, afterhours, closed (weekend or other)."""
    now_aest = _aest_now()
    et_now = now_aest - timedelta(hours=14)  # rough EDT; off by 1hr in winter
    if et_now.weekday() >= 5:
        return "closed (weekend)"
    mins = et_now.hour * 60 + et_now.minute
    if 4 * 60 <= mins < 9 * 60 + 30:
        return "premarket"
    if 9 * 60 + 30 <= mins < 16 * 60:
        return "open"
    if 16 * 60 <= mins < 20 * 60:
        return "afterhours"
    return "closed"


def _next_scan_label() -> str:
    """
    Look at the schedule and return the next upcoming scan event in human form.
    Schedule:
      - Crypto scans: 8:00, 16:00 AEST every day
      - Stock scans: 23:00, 03:00 AEST weekdays (Mon-Fri AEST)
      - Daily summaries: 8:00, 20:00 AEST every day
    """
    now = _aest_now()
    candidates = []

    # Crypto scans (daily)
    for hh, mm in [(8, 0), (16, 0)]:
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        candidates.append((target, "crypto scan"))

    # Stock scans (weekdays only — Mon-Fri AEST)
    for hh, mm in [(23, 0), (3, 0)]:
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        # Skip weekends — bump forward
        while target.weekday() >= 5:
            target += timedelta(days=1)
        candidates.append((target, "stock scan"))

    # Summaries
    for hh, mm in [(8, 0), (20, 0)]:
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        candidates.append((target, "summary"))

    candidates.sort(key=lambda x: x[0])
    next_dt, label = candidates[0]
    delta = next_dt - now
    hours = int(delta.total_seconds() // 3600)
    mins = int((delta.total_seconds() % 3600) // 60)
    when = next_dt.strftime("%H:%M")
    if hours == 0:
        eta = f"in {mins}m"
    elif hours < 12:
        eta = f"in {hours}h{mins}m"
    else:
        eta = f"tomorrow {when}"
    return f"{when} AEST ({label}, {eta})"


def _bucket_of(position: dict) -> str:
    bucket = (position.get("bucket") or "").strip()
    if bucket in (strategy.Bucket.SWING_CRYPTO,
                  strategy.Bucket.MOMENTUM_CRYPTO,
                  strategy.Bucket.SWING_STOCK):
        return bucket
    market = (position.get("market") or "").lower()
    if market == "alpaca":
        return strategy.Bucket.SWING_STOCK
    return strategy.Bucket.SWING_CRYPTO


def _hold_duration(opened_iso: str, closed_iso: str | None = None) -> str:
    """Returns human-readable duration like '2d 4h' or '6h 30m'."""
    try:
        opened = datetime.fromisoformat((opened_iso or "").replace("Z", "+00:00"))
        end = (datetime.fromisoformat((closed_iso or "").replace("Z", "+00:00"))
               if closed_iso else datetime.now(timezone.utc))
        delta = end - opened
        total_mins = int(delta.total_seconds() // 60)
        days = total_mins // (24 * 60)
        hours = (total_mins % (24 * 60)) // 60
        mins = total_mins % 60
        if days > 0:
            return f"{days}d {hours}h"
        if hours > 0:
            return f"{hours}h {mins}m"
        return f"{mins}m"
    except Exception:
        return "?"


def _signed_dollar(amount: float) -> str:
    """+$12.34 or -$12.34"""
    if amount >= 0:
        return f"+${amount:.2f}"
    return f"-${abs(amount):.2f}"


def _signed_pct(pct: float) -> str:
    """+2.34% or -2.34%"""
    if pct >= 0:
        return f"+{pct:.2f}%"
    return f"{pct:.2f}%"


# ── The replacement run_daily_summary ────────────────────────────────────

def run_rich_daily_summary(db, tg, log):
    """
    Pull everything needed for a comprehensive summary, format it, send it.

    Args:
        db: SupabaseLogger instance
        tg: TelegramNotifier instance
        log: logger
    """
    try:
        now_aest = _aest_now()
        midnight_aest = now_aest.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc = midnight_aest.astimezone(timezone.utc)
        midnight_iso = midnight_utc.isoformat()

        # ── Pull data ───────────────────────────────────────────────────

        portfolio = db.get_portfolio_value() or {}
        positions = db.get_positions() or {}

        # Closed today — read from `positions` table directly because `trades`
        # isn't populated on close (separate bug). Filter phantom cleanups.
        try:
            closed_today = db._get("positions", {
                "status": "eq.closed",
                "closed_at": f"gte.{midnight_iso}",
                "order": "closed_at.desc",
            }) or []
        except Exception as e:
            log.warning(f"summary: closed positions read failed: {e}")
            closed_today = []

        # Filter out phantom cleanups (exit==entry AND pnl==0)
        real_closes = []
        for c in closed_today:
            try:
                entry = float(c.get("entry_price") or 0)
                exit_p = float(c.get("exit_price") or 0)
                pnl = float(c.get("pnl_pct") or 0)
                if abs(entry - exit_p) < 0.0001 and abs(pnl) < 0.0001:
                    continue  # phantom
                real_closes.append(c)
            except Exception:
                real_closes.append(c)

        # Claude decisions today
        try:
            decisions_today = db._get("claude_decisions", {
                "decided_at": f"gte.{midnight_iso}",
                "order": "decided_at.desc",
            }) or []
        except Exception as e:
            log.warning(f"summary: decisions read failed: {e}")
            decisions_today = []

        # Today's API cost
        try:
            cost_str = db.get_flag(
                f"claude_spend_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
            ) or "0"
            api_cost = float(cost_str)
        except Exception:
            api_cost = 0.0

        # ── Calculate numbers ───────────────────────────────────────────

        STARTING = float(strategy.STARTING_CAPITAL_AUD)
        total = float(portfolio.get("total_aud") or STARTING)
        cash = float(portfolio.get("cash_aud") or 0)
        deployed = float(portfolio.get("deployed_aud") or 0)
        day_pnl = float(portfolio.get("day_pnl") or 0)
        all_pnl = float(portfolio.get("total_pnl") or 0)
        all_pct = (all_pnl / STARTING) * 100 if STARTING else 0

        # Realised today (from real closes)
        realised_today = 0.0
        for c in real_closes:
            try:
                aud = float(c.get("aud_amount") or 0)
                pct = float(c.get("pnl_pct") or 0)
                realised_today += aud * pct
            except Exception:
                pass

        # Unrealised on open positions
        unrealised = 0.0
        for sym, p in positions.items():
            try:
                aud = float(p.get("aud_amount") or 0)
                pct = float(p.get("pnl_pct") or 0)
                unrealised += aud * pct
            except Exception:
                pass

        # ── Group open positions by bucket ──────────────────────────────

        groups = {
            strategy.Bucket.SWING_CRYPTO: {},
            strategy.Bucket.MOMENTUM_CRYPTO: {},
            strategy.Bucket.SWING_STOCK: {},
        }
        for sym, p in positions.items():
            groups[_bucket_of(p)][sym] = p

        # ── Decision breakdown ──────────────────────────────────────────

        buys_executed = []
        skips = []
        safety_blocks = []
        for d in decisions_today:
            action = (d.get("action") or "").lower()
            sym = d.get("symbol") or "?"
            bucket = d.get("bucket") or "?"
            reason = (d.get("reason") or "").strip()
            conf = d.get("confidence")
            executed = bool(d.get("executed"))

            if action == "buy" and executed:
                buys_executed.append({
                    "sym": sym, "bucket": bucket, "reason": reason, "conf": conf,
                })
            elif action == "rejected_by_safety":
                safety_blocks.append({
                    "sym": sym, "bucket": bucket, "reason": reason, "conf": conf,
                })
            elif action == "skip":
                skips.append({
                    "sym": sym, "bucket": bucket, "reason": reason, "conf": conf,
                })

        # ── Format the message ──────────────────────────────────────────

        lines = []

        # Header
        market_state = _us_market_state()
        head_emoji = "📈" if (day_pnl + realised_today) >= 0 else "📉"
        lines.append(
            f"{head_emoji} <b>RivX summary</b> · "
            f"{now_aest.strftime('%a %d %b, %H:%M')} AEST · "
            f"US mkt: {market_state}"
        )
        lines.append("")

        # Portfolio block
        lines.append("<b>📊 PORTFOLIO</b>")
        lines.append(f"Total: <b>${total:,.2f} AUD</b>")
        lines.append(
            f"All-time: {_signed_dollar(all_pnl)} ({_signed_pct(all_pct)})"
        )
        lines.append(
            f"Today: {_signed_dollar(realised_today)} realised · "
            f"{_signed_dollar(unrealised)} unrealised on open"
        )
        lines.append("")

        # Closed today
        if real_closes:
            lines.append(f"<b>✅ CLOSED TODAY ({len(real_closes)})</b>")
            for c in real_closes:
                sym = c.get("symbol") or "?"
                aud = float(c.get("aud_amount") or 0)
                pct = float(c.get("pnl_pct") or 0)
                dollar = aud * pct
                pct_disp = pct * 100
                hold = _hold_duration(c.get("created_at"), c.get("closed_at"))
                emoji = "🟢" if pct > 0 else "🔴"
                lines.append(
                    f"  {emoji} <b>{sym}</b>  {_signed_dollar(dollar)} "
                    f"({_signed_pct(pct_disp)}) · held {hold}"
                )
            net = sum(
                float(c.get("aud_amount") or 0) * float(c.get("pnl_pct") or 0)
                for c in real_closes
            )
            lines.append(f"  <b>Net realised: {_signed_dollar(net)}</b>")
            lines.append("")
        else:
            lines.append("<b>✅ CLOSED TODAY</b>: none")
            lines.append("")

        # Open positions
        n_open = len(positions)
        n_sw = len(groups[strategy.Bucket.SWING_CRYPTO])
        n_mo = len(groups[strategy.Bucket.MOMENTUM_CRYPTO])
        n_st = len(groups[strategy.Bucket.SWING_STOCK])
        lines.append(
            f"<b>📋 OPEN ({n_open})</b> · "
            f"{n_sw} swing crypto · {n_mo} momentum · {n_st} stocks"
        )
        if not positions:
            lines.append("  None — fully in cash")
        else:
            for label, key, slots in [
                ("Swing crypto", strategy.Bucket.SWING_CRYPTO,
                 strategy.SWING_CRYPTO_SLOTS),
                ("Momentum crypto", strategy.Bucket.MOMENTUM_CRYPTO,
                 strategy.MOMENTUM_CRYPTO_SLOTS),
                ("US stocks", strategy.Bucket.SWING_STOCK,
                 strategy.SWING_STOCKS_SLOTS),
            ]:
                group = groups[key]
                if not group:
                    continue
                lines.append(f"  <i>{label} ({len(group)}/{slots})</i>")
                for sym, p in group.items():
                    aud = float(p.get("aud_amount") or 0)
                    pct = float(p.get("pnl_pct") or 0)
                    dollar = aud * pct
                    pct_disp = pct * 100
                    age = _hold_duration(p.get("created_at"))
                    emoji = "🟢" if pct >= 0 else "🔴"
                    # Distance to stop/target depends on bucket
                    extra = ""
                    if key == strategy.Bucket.SWING_CRYPTO:
                        stop_dist = pct_disp - (-8.0)  # -8% stop
                        tgt_dist = 15.0 - pct_disp     # +15% target
                        extra = f" · stop {stop_dist:.1f}% away · tgt {tgt_dist:.1f}% to go"
                    elif key == strategy.Bucket.MOMENTUM_CRYPTO:
                        stop_dist = pct_disp - (-10.0)
                        tgt_dist = 30.0 - pct_disp
                        extra = f" · stop {stop_dist:.1f}% away · tgt {tgt_dist:.1f}% to go"
                    elif key == strategy.Bucket.SWING_STOCK:
                        stop_dist = pct_disp - (-5.0)
                        tgt_dist = 12.0 - pct_disp
                        extra = f" · stop {stop_dist:.1f}% away · tgt {tgt_dist:.1f}% to go"
                    lines.append(
                        f"    {emoji} <b>{sym}</b> {_signed_dollar(dollar)} "
                        f"({_signed_pct(pct_disp)}) · age {age}{extra}"
                    )
        lines.append("")

        # Cash & deployment
        SW_B = strategy.SWING_CRYPTO_BUDGET
        MO_B = strategy.MOMENTUM_CRYPTO_BUDGET
        ST_B = strategy.SWING_STOCKS_BUDGET
        sw_dep = sum(float(p.get("aud_amount") or 0)
                     for p in groups[strategy.Bucket.SWING_CRYPTO].values())
        mo_dep = sum(float(p.get("aud_amount") or 0)
                     for p in groups[strategy.Bucket.MOMENTUM_CRYPTO].values())
        st_dep = sum(float(p.get("aud_amount") or 0)
                     for p in groups[strategy.Bucket.SWING_STOCK].values())
        lines.append("<b>💰 CASH & DEPLOYMENT</b>")
        lines.append(
            f"  Cash: ${cash:,.0f} · Deployed: ${deployed:,.0f} of "
            f"${SW_B + MO_B + ST_B:,.0f} max"
        )
        lines.append(
            f"  Swing crypto: ${sw_dep:.0f}/{SW_B:.0f} "
            f"({n_sw}/{strategy.SWING_CRYPTO_SLOTS} slots)"
        )
        lines.append(
            f"  Momentum: ${mo_dep:.0f}/{MO_B:.0f} "
            f"({n_mo}/{strategy.MOMENTUM_CRYPTO_SLOTS} slots)"
        )
        lines.append(
            f"  Stocks: ${st_dep:.0f}/{ST_B:.0f} "
            f"({n_st}/{strategy.SWING_STOCKS_SLOTS} slots)"
        )
        lines.append("")

        # Activity today
        lines.append("<b>🤖 BOT ACTIVITY TODAY</b>")
        lines.append(
            f"  Decisions: {len(decisions_today)} "
            f"({len(buys_executed)} buys, {len(skips)} skips, "
            f"{len(safety_blocks)} safety blocks)"
        )

        if buys_executed:
            lines.append("  <i>Buys executed:</i>")
            for b in buys_executed:
                conf_str = (f" · conf {float(b['conf'])*100:.0f}%"
                            if b['conf'] is not None else "")
                reason = b['reason'][:80] + ("…" if len(b['reason']) > 80 else "")
                lines.append(
                    f"    📥 <b>{b['sym']}</b> [{b['bucket']}]{conf_str}\n"
                    f"       {reason}"
                )

        if safety_blocks:
            lines.append("  <i>Safety blocks:</i>")
            for s in safety_blocks:
                reason = s['reason'][:80] + ("…" if len(s['reason']) > 80 else "")
                lines.append(f"    🛑 <b>{s['sym']}</b>: {reason}")

        if skips:
            lines.append("  <i>Skipped (top 5):</i>")
            for s in skips[:5]:
                conf_str = (f" ({float(s['conf'])*100:.0f}%)"
                            if s['conf'] is not None else "")
                reason = s['reason'][:60] + ("…" if len(s['reason']) > 60 else "")
                lines.append(
                    f"    ⏭ <b>{s['sym']}</b>{conf_str}: {reason}"
                )
            if len(skips) > 5:
                lines.append(f"    …and {len(skips) - 5} more")

        if not decisions_today:
            lines.append("  No scans completed yet today")

        lines.append("")

        # Cost & next scan
        lines.append("<b>⏱ NEXT & COST</b>")
        lines.append(f"  API spend today: ${api_cost:.3f} of $2.00 cap")
        lines.append(f"  Next event: {_next_scan_label()}")
        lines.append("")

        # Footer
        lines.append("Reply <b>STOP ALL</b> to halt · /summary for fresh data")

        message = "\n".join(lines)
        tg.send(message)
        log.info(
            f"rich summary sent: {len(real_closes)} closes, "
            f"{n_open} open, {len(decisions_today)} decisions, "
            f"${api_cost:.3f} spend"
        )
    except Exception as e:
        import traceback
        log.error(f"rich summary failed: {e}")
        log.error(traceback.format_exc())
        # Fall back to a minimal message so user still gets something
        try:
            tg.send(f"⚠️ Daily summary error: {e}")
        except Exception:
            pass
