# RIVX_VERSION: v2.8-trail-aware-2026-05-16
"""
Rich daily summary for RivX.

v2.8 changes from v2.7 (2026-05-16):
  - Open position lines no longer say "tgt X% to go" — that was v2 wording
    from before trail-only exits. v3.0 strategy has no target sells, so the
    old "tgt 15.0% to go" line for a swing crypto position was misleading
    (15% wasn't even the current trail-arm threshold of 10%).
  - Each position now shows either "X% to trail-arm" (if the peak is below
    the bucket's arm threshold) or "trail floor Y% away (peak Z%)" (if the
    trail is live). Uses the stored peak_pnl_pct on the position row.
  - Stop distance line unchanged.

v2.7 changes (2026-05-09):
  - All displayed dollar amounts and percentages are now NET of estimated
    fees (using bot.fees model). Matches the dashboard's "Closed Positions"
    table and headline portfolio total to-the-cent.
  - Open positions still show stop/trail distance in GROSS pct (because
    strategy thresholds operate against gross). The leading dollar/pct
    on each row is net.

v2.6 changes:
  - Bot activity rewritten in plain English, grouped by scan event.
  - Headline portfolio computed locally (matches dashboard).
  - Phantom cleanups filtered.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from bot import strategy
from bot import fees as fee_calc


# ── Time helpers ─────────────────────────────────────────────────────────

AEST = timezone(timedelta(hours=10))


def _aest_now() -> datetime:
    return datetime.now(AEST)


def _us_market_state() -> str:
    now_aest = _aest_now()
    et_now = now_aest - timedelta(hours=14)
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
    now = _aest_now()
    candidates = []

    for hh, mm in [(8, 0), (16, 0)]:
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        candidates.append((target, "crypto scan"))

    for hh, mm in [(23, 0), (3, 0)]:
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        while target.weekday() >= 5:
            target += timedelta(days=1)
        candidates.append((target, "stock scan"))

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


def _bucket_label(bucket: str) -> str:
    return {
        strategy.Bucket.SWING_CRYPTO:    "swing crypto",
        strategy.Bucket.MOMENTUM_CRYPTO: "momentum crypto",
        strategy.Bucket.SWING_STOCK:     "US stocks",
    }.get(bucket, bucket)


def _hold_duration(opened_iso: str, closed_iso: str | None = None) -> str:
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
    if amount >= 0:
        return f"+${amount:.2f}"
    return f"-${abs(amount):.2f}"


def _signed_pct(pct: float) -> str:
    if pct >= 0:
        return f"+{pct:.2f}%"
    return f"{pct:.2f}%"


def _scan_window(decided_at_iso: str) -> str:
    try:
        dt = datetime.fromisoformat(decided_at_iso.replace("Z", "+00:00")).astimezone(AEST)
    except Exception:
        return "unknown scan"

    h = dt.hour
    if 7 <= h <= 9:
        return "8 AM crypto scan"
    if 15 <= h <= 17:
        return "4 PM crypto scan"
    if 22 <= h or h == 23:
        return "11 PM stock scan"
    if 2 <= h <= 4:
        return "3 AM stock scan"
    return f"{dt.strftime('%H:%M')} ad-hoc"


def _explain_signal(decision: dict, position_for_symbol: dict | None) -> str:
    sym = decision.get("symbol", "?")
    action = (decision.get("action") or "").lower()
    conf = decision.get("confidence")
    raw_reason = (decision.get("reason") or "").strip()
    executed = bool(decision.get("executed"))

    reason_clean = raw_reason
    for prefix in ["Clean breakout setup: ", "Clean ", "Setup: "]:
        if reason_clean.startswith(prefix):
            reason_clean = reason_clean[len(prefix):]
    reason_clean = reason_clean.rstrip(".…")

    if len(reason_clean) > 130:
        reason_clean = reason_clean[:127].rstrip() + "…"

    conf_str = f" ({float(conf)*100:.0f}% conf)" if conf is not None else ""

    if action == "buy" and executed:
        if position_for_symbol:
            if position_for_symbol.get("status") == "open":
                return f"BOUGHT <b>{sym}</b>{conf_str} — {reason_clean} → position now open"
            else:
                _, net_pct = fee_calc.net_dollar_pct_for_position(position_for_symbol)
                return (f"BOUGHT <b>{sym}</b>{conf_str} — {reason_clean} → "
                        f"already closed at {_signed_pct(net_pct)}")
        else:
            return (f"BOUGHT <b>{sym}</b>{conf_str} — {reason_clean} → "
                    f"<i>no matching position found (data anomaly)</i>")

    if action == "execution_failed":
        return f"<b>{sym}</b> BUY ATTEMPTED BUT FAILED — {reason_clean}"

    if action == "rejected_by_safety":
        return f"<b>{sym}</b> blocked by safety filter — {reason_clean}"

    if action == "skip":
        return f"Skipped <b>{sym}</b>{conf_str} — {reason_clean}"

    return f"<b>{sym}</b> {action}{conf_str} — {reason_clean}"


# ── Trail status for open positions (v2.8) ───────────────────────────────

def _trail_status_extra(bucket: str, gross_pct: float, peak_pct: float) -> str:
    """
    Returns the trailing portion of an open-position one-liner — distance
    to stop, plus either 'X% to trail-arm' (peak below arm threshold) or
    'trail floor Y% away (peak Z%)' (trail is live).

    All percentages are GROSS — same basis as the strategy thresholds.
    The leading dollar/pct on the position row is NET (handled elsewhere).
    """
    if bucket == strategy.Bucket.SWING_CRYPTO:
        stop_pct, arm_pct, give_pct = -8.0, 10.0, 5.0
    elif bucket == strategy.Bucket.MOMENTUM_CRYPTO:
        stop_pct, arm_pct, give_pct = -10.0, 20.0, 7.0
    elif bucket == strategy.Bucket.SWING_STOCK:
        stop_pct, arm_pct, give_pct = -5.0, 8.0, 4.0
    else:
        return ""

    stop_dist = gross_pct - stop_pct

    if peak_pct >= arm_pct:
        floor_pct = peak_pct - give_pct
        floor_dist = gross_pct - floor_pct
        return (f" · stop {stop_dist:.1f}% away · "
                f"trail floor {floor_dist:.1f}% away (peak {peak_pct:.1f}%)")
    else:
        arm_dist = arm_pct - peak_pct
        return (f" · stop {stop_dist:.1f}% away · "
                f"{arm_dist:.1f}% to trail-arm")


# ── Headline portfolio (NET of fees) ─────────────────────────────────────

def _compute_portfolio_headline(db) -> dict:
    """
    v2.7: net-of-fees portfolio math. Mirrors supabase_logger.get_portfolio_value
    so both the bot's internal view and Telegram's headline agree.
    """
    STARTING = strategy.STARTING_CAPITAL_AUD
    try:
        open_positions = db._get("positions", {"status": "eq.open"}) or []
    except Exception:
        open_positions = []
    try:
        closed_positions = db._get("positions", {"status": "eq.closed"}) or []
    except Exception:
        closed_positions = []

    deployed_entry   = 0.0
    buy_fees_open    = 0.0
    market_value_net = 0.0
    unrealised_net   = 0.0

    for p in open_positions:
        try:
            aud     = float(p.get("aud_amount") or 0)
            pnl_pct = float(p.get("pnl_pct") or 0)
        except (TypeError, ValueError):
            continue
        market = p.get("market")
        deployed_entry   += aud
        buy_fees_open    += fee_calc.buy_fee_paid(aud_amount=aud, market=market)
        unrealised_net   += fee_calc.realised_dollar_net(
            aud_amount=aud, pnl_pct=pnl_pct, market=market,
        )
        market_value_net += fee_calc.market_value_net_if_sold(
            aud_amount=aud, pnl_pct=pnl_pct, market=market,
        )

    realised_lifetime = 0.0
    for c in closed_positions:
        try:
            aud     = float(c.get("aud_amount") or 0)
            pnl_pct = float(c.get("pnl_pct") or 0)
            entry_p = float(c.get("entry_price") or 0)
            exit_p  = float(c.get("exit_price") or 0)
        except (TypeError, ValueError):
            continue
        if abs(entry_p - exit_p) < 0.0001 and abs(pnl_pct) < 0.0001:
            continue  # phantom
        realised_lifetime += fee_calc.realised_dollar_net(
            aud_amount=aud, pnl_pct=pnl_pct, market=c.get("market"),
        )

    cash = max(0, STARTING + realised_lifetime - deployed_entry - buy_fees_open)
    total = STARTING + realised_lifetime + unrealised_net

    try:
        snaps = db._get("snapshots",
                        {"order": "created_at.desc", "limit": "1"}) or []
        prev_total = float(snaps[0].get("total_aud", STARTING)) if snaps else STARTING
    except Exception:
        prev_total = STARTING

    return {
        "total_aud":         round(total, 2),
        "day_pnl":           round(total - prev_total, 2),
        "total_pnl":         round(total - STARTING, 2),
        "realised_lifetime": round(realised_lifetime, 2),
        "deployed_aud":      round(deployed_entry, 2),
        "market_value":      round(market_value_net, 2),
        "cash_aud":          round(cash, 2),
        "unrealised_net":    round(unrealised_net, 2),
    }


# ── Main entry point ────────────────────────────────────────────────────

def run_rich_daily_summary(db, tg, log):
    """Build and send a comprehensive daily summary to Telegram."""
    try:
        now_aest = _aest_now()
        midnight_aest = now_aest.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc = midnight_aest.astimezone(timezone.utc)
        midnight_iso = midnight_utc.isoformat()

        # ── Pull data ───────────────────────────────────────────────────

        positions = db.get_positions() or {}
        portfolio = _compute_portfolio_headline(db)

        try:
            closed_today = db._get("positions", {
                "status": "eq.closed",
                "closed_at": f"gte.{midnight_iso}",
                "order": "closed_at.desc",
            }) or []
        except Exception as e:
            log.warning(f"summary: closed-today read failed: {e}")
            closed_today = []

        real_closes = []
        for c in closed_today:
            try:
                entry_p = float(c.get("entry_price") or 0)
                exit_p  = float(c.get("exit_price") or 0)
                pct     = float(c.get("pnl_pct") or 0)
                if abs(entry_p - exit_p) < 0.0001 and abs(pct) < 0.0001:
                    continue
                real_closes.append(c)
            except Exception:
                real_closes.append(c)

        try:
            decisions_today = db._get("claude_decisions", {
                "decided_at": f"gte.{midnight_iso}",
                "order": "decided_at.asc",
            }) or []
        except Exception as e:
            log.warning(f"summary: decisions read failed: {e}")
            decisions_today = []

        try:
            all_positions_today = db._get("positions", {
                "created_at": f"gte.{midnight_iso}",
            }) or []
        except Exception:
            all_positions_today = []

        positions_by_symbol = {}
        for p in all_positions_today:
            positions_by_symbol[(p.get("symbol") or "").upper()] = p

        try:
            cost_str = db.get_flag(
                f"claude_spend_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
            ) or "0"
            api_cost = float(cost_str)
        except Exception:
            api_cost = 0.0

        # ── Calculate numbers (NET of fees) ─────────────────────────────

        STARTING = float(strategy.STARTING_CAPITAL_AUD)
        total       = float(portfolio.get("total_aud") or STARTING)
        cash        = float(portfolio.get("cash_aud") or 0)
        deployed    = float(portfolio.get("deployed_aud") or 0)
        all_pnl     = float(portfolio.get("total_pnl") or 0)
        realised_lt = float(portfolio.get("realised_lifetime") or 0)
        unrealised  = float(portfolio.get("unrealised_net") or 0)
        all_pct     = (all_pnl / STARTING) * 100 if STARTING else 0

        # Today's realised P&L (net of fees)
        realised_today = 0.0
        for c in real_closes:
            try:
                aud = float(c.get("aud_amount") or 0)
                pnl_pct = float(c.get("pnl_pct") or 0)
                realised_today += fee_calc.realised_dollar_net(
                    aud_amount=aud, pnl_pct=pnl_pct, market=c.get("market"),
                )
            except Exception:
                pass

        # ── Group open positions by bucket ──────────────────────────────

        groups = {
            strategy.Bucket.SWING_CRYPTO:    {},
            strategy.Bucket.MOMENTUM_CRYPTO: {},
            strategy.Bucket.SWING_STOCK:     {},
        }
        for sym, p in positions.items():
            groups[_bucket_of(p)][sym] = p

        # ── Group decisions by scan window ──────────────────────────────

        scan_groups = {}
        scan_summaries = {}
        for d in decisions_today:
            window = _scan_window(d.get("decided_at") or "")
            if d.get("symbol") == "_scan" and d.get("action") == "scan_summary":
                scan_summaries[window] = {
                    "bucket": d.get("bucket"),
                    "reason": d.get("reason") or "",
                    "decided_at": d.get("decided_at"),
                }
                continue
            scan_groups.setdefault(window, []).append(d)

        all_windows = sorted(
            set(list(scan_groups.keys()) + list(scan_summaries.keys())),
            key=lambda w: ({
                "8 AM crypto scan": 1,
                "4 PM crypto scan": 2,
                "11 PM stock scan": 3,
                "3 AM stock scan": 4,
            }.get(w, 99), w),
        )

        # ── Format the message ──────────────────────────────────────────

        lines = []

        market_state = _us_market_state()
        head_emoji = "📈" if (realised_today + unrealised) >= 0 else "📉"
        lines.append(
            f"{head_emoji} <b>RivX summary</b> · "
            f"{now_aest.strftime('%a %d %b, %H:%M')} AEST · "
            f"US mkt: {market_state}"
        )
        lines.append("")

        # Portfolio (all NET of est. fees)
        lines.append("📊 <b>PORTFOLIO</b> <i>(net of est. fees)</i>")
        lines.append(f"Total: <b>${total:,.2f} AUD</b>")
        lines.append(
            f"All-time: {_signed_dollar(all_pnl)} ({_signed_pct(all_pct)})"
        )
        lines.append(
            f"Today: {_signed_dollar(realised_today)} realised · "
            f"{_signed_dollar(unrealised)} unrealised on open"
        )
        if abs(realised_lt - realised_today) > 0.01:
            lines.append(
                f"Lifetime realised: {_signed_dollar(realised_lt)}"
            )
        lines.append("")

        # Closed today (NET, matches dashboard)
        if real_closes:
            lines.append(f"✅ <b>CLOSED TODAY ({len(real_closes)})</b>")
            net_total = 0.0
            for c in real_closes:
                sym = c.get("symbol") or "?"
                net_dollar, net_pct = fee_calc.net_dollar_pct_for_position(c)
                net_total += net_dollar
                hold = _hold_duration(c.get("created_at"), c.get("closed_at"))
                emoji = "🟢" if net_dollar > 0 else "🔴"
                lines.append(
                    f"  {emoji} <b>{sym}</b>  {_signed_dollar(net_dollar)} "
                    f"({_signed_pct(net_pct)}) · held {hold}"
                )
            lines.append(f"  <b>Net realised: {_signed_dollar(net_total)}</b>")
            lines.append("")
        else:
            lines.append("✅ <b>CLOSED TODAY</b>: none")
            lines.append("")

        # Open positions
        n_open = len(positions)
        n_sw = len(groups[strategy.Bucket.SWING_CRYPTO])
        n_mo = len(groups[strategy.Bucket.MOMENTUM_CRYPTO])
        n_st = len(groups[strategy.Bucket.SWING_STOCK])
        lines.append(
            f"📋 <b>OPEN ({n_open})</b> · "
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
                    # NET dollar + NET pct (matches dashboard)
                    net_dollar, net_pct = fee_calc.net_dollar_pct_for_position(p)
                    # GROSS pct used for stop / trail distance (those thresholds
                    # operate against gross)
                    gross_pct = float(p.get("pnl_pct") or 0) * 100

                    # v2.8: peak-aware trail status, replaces the v2 "tgt X%" line.
                    peak_raw = p.get("peak_pnl_pct")
                    try:
                        peak_pct = (
                            float(peak_raw) * 100 if peak_raw is not None
                            else gross_pct
                        )
                    except (TypeError, ValueError):
                        peak_pct = gross_pct
                    peak_pct = max(peak_pct, gross_pct)

                    age = _hold_duration(p.get("created_at"))
                    emoji = "🟢" if net_dollar >= 0 else "🔴"
                    extra = _trail_status_extra(key, gross_pct, peak_pct)
                    lines.append(
                        f"    {emoji} <b>{sym}</b> {_signed_dollar(net_dollar)} "
                        f"({_signed_pct(net_pct)}) · age {age}{extra}"
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
        lines.append("💰 <b>CASH &amp; DEPLOYMENT</b>")
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

        # ── Bot activity ────────────────────────────────────────────────
        lines.append("🤖 <b>SCANS &amp; DECISIONS TODAY</b>")

        if not all_windows:
            lines.append("  No scans completed yet today")
        else:
            for window in all_windows:
                lines.append("")
                lines.append(f"  <b>{window}</b>")

                marker = scan_summaries.get(window)
                if marker:
                    bucket = marker.get("bucket") or ""
                    bucket_lbl = _bucket_label(bucket) if bucket else ""
                    reason = marker.get("reason") or ""
                    if bucket_lbl:
                        lines.append(f"    Looked for {bucket_lbl} setups · {reason}")
                    else:
                        lines.append(f"    {reason}")

                window_decisions = scan_groups.get(window, [])
                if not window_decisions:
                    if marker:
                        lines.append("    No qualified setups → no decisions made")
                    continue

                for d in window_decisions:
                    sym = (d.get("symbol") or "").upper()
                    pos_for_sym = positions_by_symbol.get(sym)
                    explanation = _explain_signal(d, pos_for_sym)
                    lines.append(f"    • {explanation}")

        lines.append("")

        lines.append("⏱ <b>NEXT &amp; COST</b>")
        lines.append(f"  API spend today: ${api_cost:.3f} of $2.00 cap")
        lines.append(f"  Next event: {_next_scan_label()}")
        lines.append("")

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
        try:
            tg.send(f"⚠️ Daily summary error: {e}")
        except Exception:
            pass
