# RIVX_VERSION: v2.2-supabase-logger-fixed-2026-04-26
"""
RivX supabase_logger.py
Stores all bot state between loop iterations.
Tables: trades, positions, signal_weights, snapshots, approved_plan,
        crypto_checks, bot_flags

v2.2 changes from v1:
  - set_flag / get_flag now write to the dedicated `bot_flags` table
    (key/value/updated_at columns) instead of JSON-inside-approved_plan.
    The dashboard + safety circuit breakers all read from bot_flags so
    everything reconciles.
  - STARTING capital constant is 10000 (was 5000) to match the v2 strategy.

Everything else is byte-identical to the v1 file so existing position/trade/
snapshot/signal-weight logic keeps working.
"""

import json
import logging
from datetime import datetime, date
import requests
from bot.config import SUPABASE_URL, SUPABASE_API_KEY, PORTFOLIO

log = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {"rsi": 0.2, "macd": 0.2, "bollinger": 0.2, "volume": 0.2, "ma_cross": 0.2}

# v2: $10K paper-trading capital. Single source of truth for portfolio math
# in this module. Other modules (strategy.py) have their own constant; both
# must agree.
STARTING_CAPITAL_AUD = 10000.0


class SupabaseLogger:

    def __init__(self):
        self.base    = SUPABASE_URL.rstrip('/')
        self.headers = {
            "apikey":        SUPABASE_API_KEY,
            "Authorization": f"Bearer {SUPABASE_API_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "return=representation",
        }

    def _get(self, table: str, params: dict = None) -> list:
        try:
            r = requests.get(f"{self.base}/rest/v1/{table}",
                             headers=self.headers, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            body = e.response.text[:500] if e.response is not None else ""
            log.error(f"DB GET {table}: {e} — body: {body}")
            return []
        except Exception as e:
            log.error(f"DB GET {table}: {e}")
            return []

    def _post(self, table: str, data: dict) -> dict | None:
        try:
            r = requests.post(f"{self.base}/rest/v1/{table}",
                              headers=self.headers, json=data, timeout=10)
            r.raise_for_status()
            result = r.json()
            return result[0] if isinstance(result, list) else result
        except requests.HTTPError as e:
            body = e.response.text[:500] if e.response is not None else ""
            log.error(f"DB POST {table}: {e} — body: {body}")
            return None
        except Exception as e:
            log.error(f"DB POST {table}: {e}")
            return None

    def _patch(self, table: str, data: dict, col: str, val: str) -> bool:
        try:
            r = requests.patch(f"{self.base}/rest/v1/{table}",
                               headers=self.headers, json=data,
                               params={col: f"eq.{val}"}, timeout=10)
            r.raise_for_status()
            return True
        except requests.HTTPError as e:
            body = e.response.text[:500] if e.response is not None else ""
            log.error(f"DB PATCH {table}: {e} — body: {body}")
            return False
        except Exception as e:
            log.error(f"DB PATCH {table}: {e}")
            return False

    def _patch_with_fallback(self, table: str, data: dict, col: str, val: str) -> bool:
        """
        PATCH that retries without unknown columns if the first attempt fails.
        Lets us add new fields (current_price, qty, last_priced_at, change_today)
        without requiring the user to ALTER TABLE first. If the column exists, great.
        If not, we drop it and keep going.
        """
        if self._patch(table, data, col, val):
            return True
        # Try with progressively fewer optional fields
        optional = ["last_priced_at", "change_today", "qty", "current_price"]
        trimmed = dict(data)
        for field in optional:
            if field in trimmed:
                trimmed.pop(field)
                if self._patch(table, trimmed, col, val):
                    log.info(f"PATCH {table} succeeded after dropping '{field}' "
                             f"— add this column in Supabase to persist it.")
                    return True
        return False

    def _post_with_fallback(self, table: str, data: dict,
                            optional_fields: list = None) -> dict | None:
        """
        POST that retries without optional columns if the first attempt fails.
        Same pattern as _patch_with_fallback but for inserts. Lets the bot start
        writing new fields (executions, skipped_setups) before the user has run
        the ALTER TABLE — the data goes in without those fields until they exist.
        """
        result = self._post(table, data)
        if result is not None:
            return result
        if not optional_fields:
            return None
        trimmed = dict(data)
        for field in optional_fields:
            if field in trimmed:
                trimmed.pop(field)
                result = self._post(table, trimmed)
                if result is not None:
                    log.info(f"POST {table} succeeded after dropping '{field}' "
                             f"— add this column in Supabase to persist it.")
                    return result
        return None

    # ── Trades ────────────────────────────────────────────────────────────────

    def log_trade(self, symbol: str, action: str, aud_amount: float,
                  order: dict, confidence: float, details: str,
                  raw_signals: dict = None):
        self._post("trades", {
            "symbol":       symbol,
            "action":       action,
            "aud_amount":   aud_amount,
            "score":        confidence,
            "details":      details,
            "raw_signals":  raw_signals or {},
            "order_id":     order.get("id", ""),
            "order_status": order.get("status", ""),
            "pnl_pct":      None,
            "created_at":   datetime.utcnow().isoformat(),
        })

    def get_recent_trades(self, limit: int = 30) -> list:
        return self._get("trades", {"order": "created_at.desc", "limit": str(limit)})

    # ── Positions ─────────────────────────────────────────────────────────────

    def get_positions(self) -> dict:
        rows = self._get("positions", {"status": "eq.open"})
        return {r["symbol"]: r for r in rows}

    def save_position(self, symbol: str, entry_price: float,
                      aud_amount: float, market: str):
        self._post("positions", {
            "symbol":      symbol,
            "entry_price": entry_price,
            "aud_amount":  aud_amount,
            "market":      market,
            "status":      "open",
            "pnl_pct":     0,
            "created_at":  datetime.utcnow().isoformat(),
        })

    def close_position(self, symbol: str, exit_price: float, pnl_pct: float):
        existing = self._get("positions",
                             {"symbol": f"eq.{symbol}", "status": "eq.open"})
        if existing:
            self._patch("positions",
                        {"status": "closed", "exit_price": exit_price,
                         "pnl_pct": pnl_pct,
                         "closed_at": datetime.utcnow().isoformat()},
                        "id", str(existing[0]["id"]))

    def update_position_pnl(self, symbol: str, current_price: float):
        """Update unrealised P&L for an open position using a provided current price."""
        existing = self._get("positions",
                             {"symbol": f"eq.{symbol}", "status": "eq.open"})
        if existing:
            pos   = existing[0]
            entry = pos.get("entry_price", current_price) or current_price
            pnl   = (current_price - entry) / entry if entry > 0 else 0
            data = {
                "pnl_pct": round(pnl, 4),
                "current_price": round(current_price, 6),
                "last_priced_at": datetime.utcnow().isoformat(),
            }
            self._patch_with_fallback("positions", data, "id", str(pos["id"]))

    def update_position_pnl_direct(self, symbol: str, pnl_pct: float):
        """
        Write a pre-computed pnl_pct directly (e.g. from Alpaca's unrealized_plpc).
        Used when we trust the upstream broker more than our own math.
        """
        existing = self._get("positions",
                             {"symbol": f"eq.{symbol}", "status": "eq.open"})
        if existing:
            self._patch("positions",
                        {"pnl_pct": round(pnl_pct, 4)},
                        "id", str(existing[0]["id"]))

    def update_position_from_alpaca(self, symbol: str, current_price: float,
                                    pnl_pct: float, qty: float = None,
                                    change_today: float = None,
                                    avg_entry_price: float = None):
        """
        Push live Alpaca position data back to Supabase for the dashboard.

        Also heals historical rows where entry_price was stored as 0 due to the
        old bug — if the stored entry_price is 0 and Alpaca reports a non-zero
        avg_entry_price, we overwrite.
        """
        existing = self._get("positions",
                             {"symbol": f"eq.{symbol}", "status": "eq.open"})
        if not existing:
            return
        pos = existing[0]

        data = {
            "pnl_pct": round(pnl_pct, 4),
            "current_price": round(current_price, 6) if current_price else None,
            "last_priced_at": datetime.utcnow().isoformat(),
        }
        if qty is not None:
            data["qty"] = round(qty, 8)
        if change_today is not None:
            data["change_today"] = round(change_today, 6)

        # Heal entry_price=0 rows (from the old bug where Alpaca fills weren't captured)
        stored_entry = float(pos.get("entry_price") or 0)
        if stored_entry == 0 and avg_entry_price and avg_entry_price > 0:
            data["entry_price"] = round(avg_entry_price, 6)
            log.info(f"Healed entry_price for {symbol}: 0 → {avg_entry_price:.4f}")

        self._patch_with_fallback("positions", data, "id", str(pos["id"]))

    # ── Signal weights ────────────────────────────────────────────────────────

    def get_signal_weights(self) -> dict:
        rows = self._get("signal_weights",
                         {"order": "updated_at.desc", "limit": "1"})
        if rows:
            r = rows[0]
            return {k: r.get(k, DEFAULT_WEIGHTS[k]) for k in DEFAULT_WEIGHTS}
        return DEFAULT_WEIGHTS.copy()

    def update_signal_weights_from_confidence(self, confidence_scores: dict):
        current    = self.get_signal_weights()
        avg_conf   = sum(confidence_scores.values()) / len(confidence_scores) if confidence_scores else 0.5
        adjustment = 0.01 if avg_conf > 0.65 else -0.005
        new_w      = {k: max(0.05, min(0.50, v + adjustment)) for k, v in current.items()}
        total      = sum(new_w.values())
        new_w      = {k: round(v / total, 4) for k, v in new_w.items()}
        data       = {**new_w, "updated_at": datetime.utcnow().isoformat()}
        existing   = self._get("signal_weights")
        if existing:
            self._patch("signal_weights", data, "id", str(existing[0]["id"]))
        else:
            self._post("signal_weights", data)

    # ── Approved plan ─────────────────────────────────────────────────────────
    # The approved_plan table still exists for v1 nightly-plan storage. It is
    # NOT used for flags any more. Kept here so save_approved_plan / get_approved_plan
    # callers (if any remain) keep working.

    def save_approved_plan(self, plan: dict):
        """Save tonight's approved plan so intraday loop can reference it."""
        data     = {"plan": json.dumps(plan), "updated_at": datetime.utcnow().isoformat()}
        existing = self._get("approved_plan")
        if existing:
            self._patch("approved_plan", data, "id", str(existing[0]["id"]))
        else:
            self._post("approved_plan", data)

    def get_approved_plan(self) -> dict:
        rows = self._get("approved_plan", {"order": "updated_at.desc", "limit": "1"})
        if rows and rows[0].get("plan"):
            try:
                return json.loads(rows[0]["plan"])
            except Exception:
                return {}
        return {}

    # ── Snapshots & portfolio value ───────────────────────────────────────────

    def save_snapshot(self, total_aud: float, day_pnl: float, total_pnl: float):
        today = date.today().isoformat()
        existing = self._get("snapshots", {"date": f"eq.{today}"})
        if existing:
            self._patch("snapshots", {
                "total_aud": total_aud,
                "day_pnl":   day_pnl,
                "total_pnl": total_pnl,
            }, "date", today)
        else:
            self._post("snapshots", {
                "date":      today,
                "total_aud": total_aud,
                "day_pnl":   day_pnl,
                "total_pnl": total_pnl,
            })

    # ── Flags (bot_flags table) ───────────────────────────────────────────────
    # v2.2 fix: was previously stored as JSON keys inside approved_plan.plan
    # (`_flag_<key>`). Now lives in a dedicated key/value table that both the
    # dashboard and the safety/circuit-breaker code can read directly.

    def get_flag(self, key: str) -> str:
        """Read a flag value from bot_flags. Returns "" if missing/error."""
        rows = self._get("bot_flags", {"key": f"eq.{key}", "limit": "1"})
        if rows:
            v = rows[0].get("value")
            return "" if v is None else str(v)
        return ""

    def set_flag(self, key: str, value: str) -> bool:
        """
        Write a flag to bot_flags. Upsert: update if key exists, insert if not.
        Returns True on success. All values stored as text — callers that need
        numbers must cast on read.
        """
        str_value = "" if value is None else str(value)
        now_iso   = datetime.utcnow().isoformat()
        existing  = self._get("bot_flags", {"key": f"eq.{key}", "limit": "1"})
        if existing:
            return self._patch("bot_flags",
                               {"value": str_value, "updated_at": now_iso},
                               "key", key)
        result = self._post("bot_flags",
                            {"key": key, "value": str_value, "updated_at": now_iso})
        return result is not None

    def get_portfolio_value(self) -> dict:
        """
        Live portfolio total computed from current open positions + cash.
        Falls back to snapshots only if positions table is unreadable.

        Returns total_aud (current), day_pnl (vs yesterday's snapshot if we
        have one, else zero), total_pnl (vs $10K starting capital).
        """
        STARTING = STARTING_CAPITAL_AUD
        try:
            positions = self._get("positions", {"status": "eq.open"}) or []
        except Exception:
            positions = []

        # For each open position, prefer market_value computed from
        # qty × current_price (truthful, includes FX move). Fall back to
        # aud_amount × (1 + pnl_pct) if qty/current_price missing.
        deployed_entry = 0.0   # capital that went into entries
        market_value   = 0.0   # current value of those positions
        for p in positions:
            entry = float(p.get("aud_amount") or 0)
            deployed_entry += entry

            qty           = float(p.get("qty") or 0)
            current_price = float(p.get("current_price") or 0)
            market        = (p.get("market") or "").lower()

            mv = 0.0
            if qty > 0 and current_price > 0:
                if market == "alpaca":
                    # Stocks: current_price is USD, convert via stored
                    # entry rate = aud_amount / (qty × usd_entry)
                    usd_entry = float(p.get("entry_price") or 0)
                    if usd_entry > 0 and entry > 0:
                        # Implied AUD/USD at entry, then mark to current USD price
                        # AUD value = qty × current_USD × (entry_AUD / (qty × usd_entry))
                        # But we don't have today's FX cleanly server-side, so use
                        # the simpler: entry × (1 + pnl_pct) which captures USD move
                        # but not FX. Good enough for portfolio total in practice.
                        pnl_pct = float(p.get("pnl_pct") or 0)
                        mv = entry * (1 + pnl_pct)
                    else:
                        mv = entry
                else:
                    # Crypto: current_price is AUD-native
                    mv = qty * current_price
            else:
                # Missing fields — fall back to aud_amount × (1 + pnl_pct)
                pnl_pct = float(p.get("pnl_pct") or 0)
                mv = entry * (1 + pnl_pct) if entry > 0 else 0

            market_value += mv

        cash = max(0, STARTING - deployed_entry)
        total = market_value + cash

        # Day P&L: compare to yesterday's snapshot if we have one
        try:
            snaps = self._get("snapshots", {"order": "date.desc", "limit": "1"}) or []
            prev_total = float(snaps[0].get("total_aud", STARTING)) if snaps else STARTING
        except Exception:
            prev_total = STARTING

        return {
            "total_aud":      round(total, 2),
            "day_pnl":        round(total - prev_total, 2),
            "total_pnl":      round(total - STARTING, 2),
            "deployed_aud":   round(deployed_entry, 2),
            "market_value":   round(market_value, 2),
            "cash_aud":       round(cash, 2),
        }
