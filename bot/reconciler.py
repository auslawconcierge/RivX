# RIVX_VERSION: v3.0.1-reconciler-env-url-2026-05-12
"""
RivX reconciler — keeps Alpaca and Supabase in agreement on US stock state.

ARCHITECTURE:
  Two-phase rollout. Phase 1 (this version) is READ-ONLY: every 5 minutes
  during/around US market hours we pull Alpaca's actual state (positions
  + open orders) and compare to Supabase. Mismatches get written to the
  `reconciliation_log` table and a single Telegram warning per day.

  Phase 2 (later, after we trust phase 1) will add automatic healing for
  specific mismatch classes. Not in this version.

WHAT WE CHECK:
  A. Symbols held in Alpaca but not open in Supabase  (orphan-in-alpaca)
  B. Symbols open in Supabase but not held in Alpaca (orphan-in-supabase)
  C. Quantity mismatches between the two systems     (qty-mismatch)
  D. Pending sell orders in Alpaca for symbols already closed in Supabase
     (zombie-sell)
  E. Pending buy orders in Alpaca for symbols already open in Supabase
     (duplicate-buy)

NOT IN SCOPE:
  - Crypto reconciliation (CoinSpot is read-only, no positions to reconcile)
  - Automatic fixing (phase 2)
  - Rate / FX checks (separate concern)

CALL SITE: bot.py main loop calls reconciler.tick(db, alpaca, tg, log)
once per snapshot cycle (every 5 min). Cheap when nothing diverges.

v3.0.1 change:
  Switched two hardcoded paper-api.alpaca.markets URLs to ALPACA_BASE_URL
  from config, so the same code path works against paper or live without
  edits. Required for live Alpaca rollout 2026-05-12.
"""

from __future__ import annotations

import logging
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)

# How often to actually run the reconciliation check (independent of how
# often tick() is called). 10 minutes is plenty — orders settle on minutes
# scale, not seconds.
RECONCILE_INTERVAL_SEC = 600

# Bot-flag keys
FLAG_LAST_RECONCILE = "reconcile_last_run"
FLAG_LAST_WARN_DAY = "reconcile_last_warn_day"


def tick(db, alpaca, tg, log_obj=None):
    """
    Called from bot.py main loop. Cheap most of the time — only does real
    work every RECONCILE_INTERVAL_SEC seconds.
    """
    if log_obj is None:
        log_obj = log

    try:
        last = db.get_flag(FLAG_LAST_RECONCILE)
        if last:
            try:
                last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                age_sec = (datetime.now(timezone.utc) - last_dt).total_seconds()
                if age_sec < RECONCILE_INTERVAL_SEC:
                    return
            except Exception:
                pass

        run_reconciliation(db, alpaca, tg, log_obj)
        db.set_flag(FLAG_LAST_RECONCILE, datetime.now(timezone.utc).isoformat())

    except Exception as e:
        log_obj.warning(f"reconciler tick error (non-fatal): {e}")


def run_reconciliation(db, alpaca, tg, log_obj) -> dict:
    """
    Fetch state from both systems, compare, log mismatches.
    Returns a summary dict.
    """
    # ── Pull Supabase view ──
    sb_positions = db.get_positions() or {}
    sb_stocks = {sym: p for sym, p in sb_positions.items()
                 if (p.get("market") or "").lower() == "alpaca"}

    # ── Pull Alpaca view ──
    try:
        alpaca_positions = _fetch_alpaca_positions(alpaca)
    except Exception as e:
        log_obj.warning(f"reconcile: alpaca position fetch failed: {e}")
        return {"error": str(e)}

    try:
        alpaca_open_orders = _fetch_alpaca_open_orders(alpaca)
    except Exception as e:
        log_obj.warning(f"reconcile: alpaca order fetch failed: {e}")
        alpaca_open_orders = []

    # ── Compute mismatches ──
    mismatches = []

    sb_syms = set(sb_stocks.keys())
    alp_syms = set(alpaca_positions.keys())

    # A. In Alpaca but not in Supabase (orphan-in-alpaca)
    for sym in alp_syms - sb_syms:
        ap = alpaca_positions[sym]
        mismatches.append({
            "kind": "orphan_in_alpaca",
            "symbol": sym,
            "detail": f"Alpaca holds {ap['qty']:.4f} sh @ avg ${ap['avg_entry']:.2f}, "
                      f"Supabase has no open position",
            "alpaca_qty": ap["qty"],
            "alpaca_avg_entry": ap["avg_entry"],
            "supabase_qty": 0,
        })

    # B. In Supabase but not in Alpaca (orphan-in-supabase)
    for sym in sb_syms - alp_syms:
        sp = sb_stocks[sym]
        mismatches.append({
            "kind": "orphan_in_supabase",
            "symbol": sym,
            "detail": f"Supabase has open position (${float(sp.get('aud_amount') or 0):.0f} AUD), "
                      f"Alpaca holds nothing",
            "alpaca_qty": 0,
            "supabase_qty": float(sp.get("qty") or 0),
        })

    # C. Quantity mismatch
    for sym in sb_syms & alp_syms:
        sb_qty = float(sb_stocks[sym].get("qty") or 0)
        alp_qty = alpaca_positions[sym]["qty"]
        # Allow small rounding diff (fractional shares)
        if abs(sb_qty - alp_qty) > 0.001:
            mismatches.append({
                "kind": "qty_mismatch",
                "symbol": sym,
                "detail": f"Supabase qty {sb_qty:.4f} vs Alpaca qty {alp_qty:.4f} "
                          f"(diff {(alp_qty - sb_qty):+.4f})",
                "alpaca_qty": alp_qty,
                "supabase_qty": sb_qty,
            })

    # D & E. Open orders that conflict with current Supabase state
    for order in alpaca_open_orders:
        sym = order["symbol"]
        side = order["side"]  # "buy" or "sell"
        in_sb = sym in sb_syms

        if side == "sell" and not in_sb:
            mismatches.append({
                "kind": "zombie_sell",
                "symbol": sym,
                "detail": f"Alpaca has pending SELL order (qty {order['qty']:.4f}, "
                          f"submitted {order['submitted_at']}) but Supabase shows position closed",
                "order_id": order["id"],
            })
        elif side == "buy" and in_sb:
            mismatches.append({
                "kind": "duplicate_buy",
                "symbol": sym,
                "detail": f"Alpaca has pending BUY order (qty {order['qty']:.4f}) "
                          f"and Supabase already has open position",
                "order_id": order["id"],
            })

    # ── Persist to reconciliation_log ──
    if mismatches:
        _write_reconciliation_log(db, mismatches, log_obj)

        # Telegram warn — at most once per day
        _maybe_warn_telegram(db, tg, mismatches, log_obj)
    else:
        log_obj.info("reconcile: all good — Alpaca and Supabase agree")

    return {
        "ok": True,
        "mismatch_count": len(mismatches),
        "alpaca_positions": len(alpaca_positions),
        "supabase_positions": len(sb_stocks),
        "alpaca_open_orders": len(alpaca_open_orders),
    }


def _fetch_alpaca_positions(alpaca) -> dict:
    """Returns {symbol: {qty, avg_entry, current_price}}."""
    import requests
    from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    base = ALPACA_BASE_URL.rstrip("/")

    r = requests.get(
        f"{base}/v2/positions",
        headers=headers, timeout=10,
    )
    r.raise_for_status()
    data = r.json() or []

    out = {}
    for p in data:
        try:
            sym = (p.get("symbol") or "").upper()
            if not sym:
                continue
            qty = float(p.get("qty") or 0)
            if qty <= 0:
                continue
            out[sym] = {
                "qty": qty,
                "avg_entry": float(p.get("avg_entry_price") or 0),
                "current_price": float(p.get("current_price") or 0),
                "market_value": float(p.get("market_value") or 0),
            }
        except Exception:
            continue

    return out


def _fetch_alpaca_open_orders(alpaca) -> list:
    """Returns list of {symbol, side, qty, status, id, submitted_at}."""
    import requests
    from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    base = ALPACA_BASE_URL.rstrip("/")

    r = requests.get(
        f"{base}/v2/orders",
        headers=headers,
        params={"status": "open", "limit": "100"},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json() or []

    out = []
    for o in data:
        try:
            out.append({
                "id": o.get("id"),
                "symbol": (o.get("symbol") or "").upper(),
                "side": (o.get("side") or "").lower(),
                "qty": float(o.get("qty") or 0),
                "status": o.get("status") or "",
                "submitted_at": (o.get("submitted_at") or "")[:19],
            })
        except Exception:
            continue
    return out


def _write_reconciliation_log(db, mismatches: list, log_obj):
    """Write each mismatch as a row in reconciliation_log."""
    now_iso = datetime.now(timezone.utc).isoformat()
    for m in mismatches:
        try:
            db._post("reconciliation_log", {
                "kind": m["kind"],
                "symbol": m["symbol"],
                "detail": m["detail"][:500],
                "data_json": json.dumps(m),
                "detected_at": now_iso,
                "resolved": False,
            })
        except Exception as e:
            log_obj.debug(f"reconciliation_log write {m['symbol']}: {e}")
    log_obj.warning(f"reconcile: {len(mismatches)} mismatches detected and logged")


def _maybe_warn_telegram(db, tg, mismatches: list, log_obj):
    """Send at most one Telegram warn per day, summarising current mismatches."""
    today = datetime.now(timezone.utc).date().isoformat()
    last_warn = db.get_flag(FLAG_LAST_WARN_DAY) or ""
    if last_warn == today:
        return

    by_kind = {}
    for m in mismatches:
        by_kind.setdefault(m["kind"], []).append(m["symbol"])

    lines = ["⚠️ <b>RivX reconciliation alert</b>", ""]
    lines.append(f"Found {len(mismatches)} Alpaca↔Supabase mismatches:")
    lines.append("")

    kind_labels = {
        "orphan_in_alpaca": "Held in Alpaca, missing from RivX",
        "orphan_in_supabase": "Open in RivX, missing from Alpaca",
        "qty_mismatch": "Quantity disagreement",
        "zombie_sell": "Pending sell for closed position",
        "duplicate_buy": "Pending buy for already-open position",
    }
    for kind, syms in by_kind.items():
        label = kind_labels.get(kind, kind)
        lines.append(f"<b>{label}:</b> {', '.join(syms)}")

    lines.append("")
    lines.append("<i>Read-only mode — no automatic fixes. "
                 "Check dashboard reconciliation_log for details.</i>")

    try:
        tg.send("\n".join(lines))
        db.set_flag(FLAG_LAST_WARN_DAY, today)
        log_obj.info(f"reconcile: telegram warning sent ({len(mismatches)} mismatches)")
    except Exception as e:
        log_obj.warning(f"reconcile: telegram warn failed: {e}")
