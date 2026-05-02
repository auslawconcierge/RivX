# RIVX_VERSION: v2.8.1-token-usage-2026-05-02
"""
RivX supabase_logger.py
Stores all bot state between loop iterations.
Tables: trades, positions, signal_weights, snapshots, approved_plan,
        crypto_checks, bot_flags, token_usage

v2.8.1 changes from v2.6:
  - Added record_token_usage() method. Upserts one row per day in the
    token_usage table, incrementing totals as Claude calls happen.
    Powers the Cost tab on the dashboard. Was previously broken because
    nothing ever wrote to this table — bot only wrote daily spend to a
    bot_flags entry.

v2.6 fix: get_portfolio_value() previously only counted open positions +
cash. When all positions were closed, total_aud snapped back to $10,000
even though closed positions had realised gains/losses. Now includes
sum of realised P&L from `positions` table where status='closed'.
This means: total = cash + open_market_value + realised_pnl_lifetime,
which is what the dashboard math reconciles to.

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
        """
        v2.3: Write a new row every call for time-series charting.
        The dashboard chart reads `created_at` to plot the line.
        Old behavior was upsert-by-date which only kept one row per day.
        """
        self._post("snapshots", {
            "date":       date.today().isoformat(),
            "total_aud":  round(float(total_aud), 2),
            "day_pnl":    round(float(day_pnl), 2),
            "total_pnl":  round(float(total_pnl), 2),
            "created_at": datetime.utcnow().isoformat(),
        })

    # ── Token usage (Cost tab) ────────────────────────────────────────────────
    # v2.8.1: track Claude API spend per day. One row per date — increment if
    # the row exists, insert if it doesn't.

    def record_token_usage(self, input_tokens: int, output_tokens: int,
                           cost_usd: float):
        """
        Upsert today's token_usage row.

        token_usage schema (all NOT NULL):
          date         date PK
          input_tokens bigint
          output_tokens bigint
          cost_usd     numeric
          call_count   integer

        If today's row exists, increment all four counters. If not, create
        the row with these as the starting values.
        """
        today = date.today().isoformat()
        try:
            existing = self._get("token_usage",
                                  {"date": f"eq.{today}", "limit": "1"})
        except Exception as e:
            log.warning(f"token_usage read failed: {e}")
            return

        if existing:
            row = existing[0]
            new_data = {
                "input_tokens":  int(row.get("input_tokens") or 0) + int(input_tokens or 0),
                "output_tokens": int(row.get("output_tokens") or 0) + int(output_tokens or 0),
                "cost_usd":      float(row.get("cost_usd") or 0) + float(cost_usd or 0),
                "call_count":    int(row.get("call_count") or 0) + 1,
            }
            ok = self._patch("token_usage", new_data, "date", today)
            if not ok:
                log.warning(f"token_usage PATCH failed for {today}")
        else:
            new_row = {
                "date":          today,
                "input_tokens":  int(input_tokens or 0),
                "output_tokens": int(output_tokens or 0),
                "cost_usd":      float(cost_usd or 0),
                "call_count":    1,
            }
            result = self._post("token_usage", new_row)
            if result is None:
                log.warning(f"token_usage INSERT failed for {today}")

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
        Live portfolio total = cash + market value of open positions + lifetime
        realised P&L from closed positions.

        v2.6 fix: previously this only counted cash + market_value of open
        positions. When everything was closed, total_aud snapped back to the
        starting $10,000 even though realised gains/losses had moved the real
        balance. Now sums realised P&L (aud_amount × pnl_pct) across all
        closed positions and adds it in.

        Cash modelling: starting capital minus capital deployed into currently-
        OPEN positions. Closed positions don't tie up cash — their realised P&L
        becomes part of cash automatically (via the realised_lifetime add).
        """
        STARTING = STARTING_CAPITAL_AUD
        try:
            open_positions = self._get("positions", {"status": "eq.open"}) or []
        except Exception:
            open_positions = []

        try:
            closed_positions = self._get("positions", {"status": "eq.closed"}) or []
        except Exception:
            closed_positions = []

        # ── Open positions: market value + capital deployed ─────────────
        deployed_entry = 0.0   # capital that went into entries (still locked up)
        market_value   = 0.0   # current value of those positions
        for p in open_positions:
            entry = float(p.get("aud_amount") or 0)
            deployed_entry += entry

            qty           = float(p.get("qty") or 0)
            current_price = float(p.get("current_price") or 0)
            market        = (p.get("market") or "").lower()

            mv = 0.0
            if qty > 0 and current_price > 0:
                if market == "alpaca":
                    # Stocks: current_price is USD. We don't have today's FX
                    # cleanly server-side, so use entry × (1 + pnl_pct) which
                    # captures the USD move via Alpaca's unrealized_plpc.
                    pnl_pct = float(p.get("pnl_pct") or 0)
                    mv = entry * (1 + pnl_pct) if entry > 0 else 0
                else:
                    # Crypto: current_price is AUD-native
                    mv = qty * current_price
            else:
                # Missing fields — fall back to aud_amount × (1 + pnl_pct)
                pnl_pct = float(p.get("pnl_pct") or 0)
                mv = entry * (1 + pnl_pct) if entry > 0 else 0

            market_value += mv

        # ── Closed positions: lifetime realised P&L ─────────────────────
        # Filter out phantom cleanups (entry==exit AND pnl_pct==0). Those are
        # rows we manually marked closed without a real sell — they shouldn't
        # count toward realised gains.
        realised_lifetime = 0.0
        for c in closed_positions:
            try:
                aud = float(c.get("aud_amount") or 0)
                pct = float(c.get("pnl_pct") or 0)
                entry_p = float(c.get("entry_price") or 0)
                exit_p  = float(c.get("exit_price") or 0)
                # Phantom cleanup detection: entry == exit AND pnl == 0
                if abs(entry_p - exit_p) < 0.0001 and abs(pct) < 0.0001:
                    continue
                realised_lifetime += aud * pct
            except (TypeError, ValueError):
                continue

        # ── Cash & total ────────────────────────────────────────────────
        # Cash = starting capital + lifetime realised gains − capital still
        # locked up in open positions. When you close a winner, your cash
        # goes up by the entry amount + realised gain. This formula captures that.
        cash = STARTING + realised_lifetime - deployed_entry
        cash = max(0, cash)

        total = market_value + cash

        # ── Day P&L: vs prior snapshot (kept simple) ────────────────────
        try:
            snaps = self._get("snapshots", {"order": "created_at.desc",
                                             "limit": "1"}) or []
            prev_total = float(snaps[0].get("total_aud", STARTING)) if snaps else STARTING
        except Exception:
            prev_total = STARTING

        return {
            "total_aud":         round(total, 2),
            "day_pnl":           round(total - prev_total, 2),
            "total_pnl":         round(total - STARTING, 2),
            "realised_lifetime": round(realised_lifetime, 2),
            "deployed_aud":      round(deployed_entry, 2),
            "market_value":      round(market_value, 2),
            "cash_aud":          round(cash, 2),
        }
