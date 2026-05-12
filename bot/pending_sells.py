# RIVX_VERSION: v3.0.1-pending-sells-env-url-2026-05-12
"""
Pending sells tracker.

v3.0.1 change (2026-05-12):
  Switched hardcoded paper-api.alpaca.markets URL in _fetch_alpaca_order
  to ALPACA_BASE_URL from config. Critical fix for live Alpaca rollout:
  without this, the post-submit poll for fill confirmation would hit
  paper-api with live keys, get 401s, never confirm fills, and leave
  positions stuck in pending_close until the 48h timeout reverted them.

v2.9.4 fix from v2.8:
  Two bugs, plus a subtle interaction.

  BUG 1: submit_sell_for_stock didn't check the return value of db._patch.
  db._patch returns False on HTTP errors without raising, so any failure
  silently flowed through and submit_sell still returned True.

  BUG 2 (the one that actually broke AMD on 2026-05-06): _finalise_close
  called db.close_position. close_position queries
    SELECT * FROM positions WHERE symbol=X AND status='open'
  But by the time _finalise_close runs, the position is status='pending_close'
  — submit_sell_for_stock put it there. The query returns nothing. The
  close silently no-ops. _finalise_close then nulls pending_order_id and
  returns True with "confirmed fill @ ...".

  Five minutes later, resolve_pending_closes finds AMD with status='pending_close'
  and pending_order_id=null, hits the "no order_id, reverting to open"
  branch, sets status='open'. The next manage cycle fires the exit rule
  again. execute_sell calls alpaca.get_position — Alpaca has no position
  (the original sell already filled). Returns False with "alpaca reports
  no live position." Telegram alert. Loop forever.

  FIX: _finalise_close does the close PATCH directly using the row id,
  in one atomic update that sets status='closed' AND clears pending fields.
  Does not touch close_position at all. Works regardless of whether the
  row is in status='open' or status='pending_close'.

  ALSO: submit_sell_for_stock now checks the PATCH return value. If the
  PATCH to pending_close fails, it logs critically and stashes the order
  id in bot_flags so bot.py's self-heal path (v2.9.4) can recover.

ORIGINAL v2.8 DOCSTRING:

Problem this solves:
  When the bot decides to sell a stock outside US market hours (e.g. at the
  11 PM AEST scan, or at 8 PM AEST), Alpaca accepts the order but it sits
  in 'new' / 'accepted' / 'pending_new' status until the next market open.

  The OLD execute_sell flow called db.close_position() immediately after
  alpaca.sell() returned a non-None response. This meant Supabase showed
  the position as closed (with whatever exit_price the bot guessed),
  but Alpaca was still holding the actual shares with a pending order.

  Result: the next scanner pass would see the symbol as "available again"
  and try to buy it back. Alpaca rejects with a wash-trade error because
  it has both an open position AND a pending opposite-side order.

Fix:
  Two-phase close. Phase 1 — order submitted, mark Supabase position with
  status='pending_close' (NEW status, not 'closed'). Phase 2 — every
  snapshot, the resolver polls Alpaca for the order id; once status is
  'filled' with a real fill price, Supabase position moves to status='closed'
  with the actual exit price and the trade row gets written.

  If the order ends up cancelled, expired, or rejected, we revert
  status back to 'open' and log the event so the bot can try again later.

DB SCHEMA REQUIREMENTS:
  - positions.status accepts a new value 'pending_close'
  - positions has columns: pending_order_id (text), pending_since (timestamptz)
  These are added in a migration the user runs once.

The reconciler (reconciler.py) is independent — it observes that Alpaca
and Supabase agree, but doesn't drive the close lifecycle. This module
drives the close lifecycle so the reconciler stays clean.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)


def submit_sell_for_stock(*, symbol: str, position: dict,
                          db, alpaca, log_obj=None) -> tuple[bool, str]:
    """
    Submit a sell order to Alpaca and mark the Supabase position as
    pending_close. Does NOT close the position in Supabase yet.

    Returns (ok, message). ok=True means the order was submitted to
    Alpaca AND Supabase was updated to pending_close. If Supabase
    update fails after Alpaca accepts the order, returns False with
    the order id stashed in bot_flags as a recovery breadcrumb — the
    self-heal path in bot.py (v2.9.4) will close the orphan on the
    next manage cycle.
    """
    if log_obj is None:
        log_obj = log

    try:
        res = alpaca.sell(symbol)
    except Exception as e:
        return False, f"alpaca sell error: {e}"
    if not res:
        return False, "alpaca sell returned None"

    order_id = res.get("id") or ""
    fill_price = float(res.get("filled_avg_price") or 0)
    fill_qty = float(res.get("filled_qty") or 0)
    status = (res.get("status") or "").lower()

    # If the order is already filled at submission time (rare but possible
    # during market hours), close immediately.
    if fill_price > 0 and fill_qty > 0 and status == "filled":
        return _finalise_close(
            symbol=symbol, position=position, fill_price=fill_price,
            db=db, log_obj=log_obj, reason="submit-time fill",
        )

    # Otherwise: mark pending_close. v2.9.4: ACTUALLY CHECK THE RETURN.
    full_data = {
        "status": "pending_close",
        "pending_order_id": order_id,
        "pending_since": datetime.now(timezone.utc).isoformat(),
    }
    ok = db._patch("positions", full_data, "symbol", symbol)

    if not ok:
        # Alpaca order is live, Supabase failed. Record breadcrumb;
        # bot.py's self-heal will close on the next manage cycle.
        log_obj.error(f"CRITICAL: {symbol} sell order {order_id} submitted "
                      f"to Alpaca but pending_close PATCH failed. Self-heal "
                      f"will close on next manage cycle.")
        try:
            db.set_flag(f"orphan_sell_{symbol}", order_id)
        except Exception:
            pass
        return False, (f"alpaca order {order_id[:8]} submitted but supabase "
                       f"update failed — auto-heal will close on next cycle")

    log_obj.info(f"SELL {symbol}: order {order_id} submitted, status={status}, "
                 f"marked pending_close")
    return True, f"submitted, awaiting fill (order {order_id[:8]})"


def resolve_pending_closes(db, alpaca, log_obj=None) -> int:
    """
    Walk all positions where status='pending_close' and check Alpaca for
    the order's current status. Returns number of positions resolved
    (closed or reverted-to-open).

    Called from bot.py snapshot loop every 5 minutes.
    """
    if log_obj is None:
        log_obj = log

    try:
        pending = db._get("positions", {
            "status": "eq.pending_close",
            "limit": "50",
        }) or []
    except Exception as e:
        log_obj.debug(f"resolve_pending_closes read failed: {e}")
        return 0

    if not pending:
        return 0

    resolved = 0
    for pos in pending:
        sym = pos.get("symbol")
        order_id = pos.get("pending_order_id") or ""

        if not order_id:
            log_obj.warning(f"pending_close on {sym} has no order_id, reverting to open")
            try:
                db._patch("positions", {"status": "open", "pending_order_id": None,
                                         "pending_since": None}, "symbol", sym)
            except Exception:
                pass
            continue

        try:
            order = _fetch_alpaca_order(order_id)
        except Exception as e:
            log_obj.debug(f"order fetch {order_id} failed: {e}")
            continue

        if not order:
            continue

        status = (order.get("status") or "").lower()
        fill_price = float(order.get("filled_avg_price") or 0)
        fill_qty = float(order.get("filled_qty") or 0)

        if status == "filled" and fill_price > 0:
            ok, msg = _finalise_close(
                symbol=sym, position=pos, fill_price=fill_price,
                db=db, log_obj=log_obj,
                reason=f"async fill (order {order_id[:8]})",
            )
            if ok:
                # Clear breadcrumb if present
                try:
                    db.set_flag(f"orphan_sell_{sym}", "")
                except Exception:
                    pass
                resolved += 1

        elif status in ("canceled", "cancelled", "expired", "rejected"):
            # Order died without filling — revert to open so it can be
            # re-evaluated by the exit logic next cycle.
            log_obj.warning(f"SELL {sym}: order {order_id[:8]} ended with status "
                            f"'{status}' without filling — reverting to open")
            try:
                db._patch("positions", {
                    "status": "open",
                    "pending_order_id": None,
                    "pending_since": None,
                }, "symbol", sym)
                try:
                    db.set_flag(f"orphan_sell_{sym}", "")
                except Exception:
                    pass
                resolved += 1
            except Exception as e:
                log_obj.warning(f"revert-to-open for {sym} failed: {e}")

        # else: still pending (new, accepted, partially_filled, etc) — leave alone

    if resolved > 0:
        log_obj.info(f"resolve_pending_closes: {resolved} positions resolved")

    # Also: time out any pending_close older than 48h (something is wrong)
    _timeout_stale_pending(db, log_obj)

    return resolved


def _finalise_close(*, symbol: str, position: dict, fill_price: float,
                    db, log_obj, reason: str) -> tuple[bool, str]:
    """
    Close the position by direct PATCH. v2.9.4: does NOT call
    db.close_position because that function only matches status='open'
    and we may be transitioning out of status='pending_close'. Instead
    we PATCH the row by id with status='closed' and cleared pending
    fields, all in one atomic update.
    """
    avg_entry = float(position.get("entry_price") or 0)
    if avg_entry <= 0:
        # Try to derive from aud_amount and qty
        qty = float(position.get("qty") or 0)
        aud_amount = float(position.get("aud_amount") or 0)
        if qty > 0 and aud_amount > 0:
            avg_entry = aud_amount / qty
        else:
            avg_entry = fill_price  # last resort, marks position as flat

    pnl_pct = (fill_price - avg_entry) / avg_entry if avg_entry > 0 else 0

    # Find the row id. The position dict from resolve_pending_closes already
    # contains id (it came from a _get result). If not present (paranoid
    # case), look it up by symbol matching either open or pending_close.
    pos_id = position.get("id")
    if not pos_id:
        try:
            existing = db._get("positions", {
                "symbol": f"eq.{symbol}",
                "status": "in.(open,pending_close)",
                "order": "created_at.desc",
                "limit": "1",
            })
            if existing:
                pos_id = existing[0].get("id")
        except Exception as e:
            return False, f"close lookup failed: {e}"

    if not pos_id:
        return False, f"no open/pending_close row found for {symbol}"

    close_data = {
        "status": "closed",
        "exit_price": fill_price,
        "pnl_pct": pnl_pct,
        "closed_at": datetime.now(timezone.utc).isoformat(),
        "pending_order_id": None,
        "pending_since": None,
    }

    ok = db._patch("positions", close_data, "id", str(pos_id))

    if not ok:
        # Retry without the pending_* nulls in case those columns somehow
        # weren't migrated. Belt and braces — we know they exist now, but
        # this avoids a hard failure if the schema diverges in the future.
        retry_data = {
            "status": "closed",
            "exit_price": fill_price,
            "pnl_pct": pnl_pct,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        ok = db._patch("positions", retry_data, "id", str(pos_id))
        if not ok:
            return False, f"close PATCH failed for {symbol} (id {pos_id})"

    log_obj.info(f"SELL {symbol}: confirmed fill @ ${fill_price:.4f} "
                 f"({pnl_pct*100:+.2f}%) — {reason}")

    return True, f"closed @ ${fill_price:.4f} ({pnl_pct*100:+.2f}%)"


def _timeout_stale_pending(db, log_obj):
    """
    If a pending_close is older than 48 hours, something has gone wrong.
    Log it loudly and revert to open so the bot can try again.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    try:
        stale = db._get("positions", {
            "status": "eq.pending_close",
            "pending_since": f"lt.{cutoff}",
            "limit": "20",
        }) or []
    except Exception:
        return

    for pos in stale:
        sym = pos.get("symbol")
        log_obj.warning(f"pending_close on {sym} is older than 48h — reverting to open. "
                        f"Manual investigation needed.")
        try:
            db._patch("positions", {
                "status": "open",
                "pending_order_id": None,
                "pending_since": None,
            }, "symbol", sym)
        except Exception as e:
            log_obj.warning(f"stale revert {sym} failed: {e}")


def _fetch_alpaca_order(order_id: str) -> Optional[dict]:
    """Get current state of one order from Alpaca."""
    import requests
    from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }
    base = ALPACA_BASE_URL.rstrip("/")

    r = requests.get(
        f"{base}/v2/orders/{order_id}",
        headers=headers, timeout=8,
    )
    if r.status_code != 200:
        return None
    return r.json()
