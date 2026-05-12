# RIVX_VERSION: v3.0.1-scanner-exclusions-env-url-2026-05-12
"""
Scanner pre-filter for blocked symbols.

Problem this solves:
  Even with the pending_sells fix, there's a window where Alpaca has
  closed a position but the system might be considering re-buying it
  immediately. Or — more importantly — there may be Alpaca-side state
  the bot doesn't know about (manual orders, stuck orders).

  This filter is the belt-and-braces: before the scanner returns a
  candidate, we ask "does Alpaca have ANY open order for this symbol
  right now?" If yes, skip — even if Supabase thinks we have no position.

USAGE:
  scanner.scan_stocks() in scanner.py imports this and filters its
  candidate list through filter_blocked_symbols() before returning.

  Cheap: one HTTP call per scan event, cached for 60 seconds.

v3.0.1 change:
  Switched hardcoded paper-api.alpaca.markets URL to ALPACA_BASE_URL from
  config, so the same code path works against paper or live without edits.
"""
from __future__ import annotations
import logging
import time
from typing import Iterable

log = logging.getLogger(__name__)

# Cache to avoid hammering Alpaca on repeated scanner calls
_BLOCKED_CACHE = {"data": None, "fetched_at": 0.0}
_CACHE_TTL_SEC = 60


def get_blocked_symbols(force_refresh: bool = False) -> set:
    """
    Returns a set of UPPERCASE symbols that have any open order in Alpaca
    right now (buy or sell). These should be excluded from any new
    buy candidate list.
    """
    now = time.time()
    if (not force_refresh
            and _BLOCKED_CACHE["data"] is not None
            and now - _BLOCKED_CACHE["fetched_at"] < _CACHE_TTL_SEC):
        return _BLOCKED_CACHE["data"]
    try:
        blocked = _fetch_blocked_from_alpaca()
        _BLOCKED_CACHE["data"] = blocked
        _BLOCKED_CACHE["fetched_at"] = now
        return blocked
    except Exception as e:
        log.warning(f"get_blocked_symbols failed: {e}, using cached or empty set")
        return _BLOCKED_CACHE["data"] or set()


def filter_blocked_symbols(candidates: list, log_obj=None) -> list:
    """
    Given a list of candidate dicts (each with 'symbol' key), return only
    those whose symbol is NOT currently blocked by an open Alpaca order.

    Logs each exclusion at info level so you can see in Render why a
    candidate was dropped.
    """
    if log_obj is None:
        log_obj = log
    blocked = get_blocked_symbols()
    if not blocked:
        return candidates
    out = []
    for c in candidates:
        sym = (c.get("symbol") or "").upper()
        if sym in blocked:
            log_obj.info(f"scanner: skipping {sym} — open Alpaca order exists "
                         f"(would cause wash-trade rejection)")
            continue
        out.append(c)
    return out


def _fetch_blocked_from_alpaca() -> set:
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
        timeout=8,
    )
    r.raise_for_status()
    data = r.json() or []
    out = set()
    for o in data:
        sym = (o.get("symbol") or "").upper()
        if sym:
            out.add(sym)
    return out
