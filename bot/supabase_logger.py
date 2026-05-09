# RIVX_VERSION: v3.0.2-fee-adjusted-portfolio-2026-05-09
"""
RivX supabase_logger.py
Stores all bot state between loop iterations.

v3.0.2 changes from v3.0.1 (2026-05-09):
  - get_portfolio_value() now uses bot.fees to compute net-of-fees
    realised, unrealised, market value, and cash. Telegram and dashboard
    now agree to the cent on closed-trade P&L.
  - "total_aud" is the post-liquidation total: STARTING +
    realised_lifetime_net + unrealised_net.
  - "cash_aud" subtracts buy fees paid on currently-open positions.

v3.0.1: save_position accepts qty parameter, written at insert time.
v2.8.1: record_token_usage()
v2.6:   get_portfolio_value() includes realised P&L from closed positions
v2.2:   set_flag/get_flag use bot_flags table
"""

import json
import logging
from datetime import datetime, date
import requests
from bot.config import SUPABASE_URL, SUPABASE_API_KEY, PORTFOLIO

log = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {"rsi": 0.2, "macd": 0.2, "bollinger": 0.2, "volume": 0.2, "ma_cross": 0.2}

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
        if self._patch(table, data, col, val):
            return True
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
                      aud_amount: float, market: str,
                      qty: float = None):
        """v3.0.1: qty optional, written at insert time when provided."""
        row = {
            "symbol":      symbol,
            "entry_price": entry_price,
            "aud_amount":  aud_amount,
            "market":      market,
            "status":      "open",
            "pnl_pct":     0,
            "created_at":  datetime.utcnow().isoformat(),
        }
        if qty is not None and qty > 0:
            row["qty"] = round(float(qty), 8)

        self._post_with_fallback("positions", row, optional_fields=["qty"])

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

    def save_approved_plan(self, plan: dict):
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
        self._post("snapshots", {
            "date":       date.today().isoformat(),
            "total_aud":  round(float(total_aud), 2),
            "day_pnl":    round(float(day_pnl), 2),
            "total_pnl":  round(float(total_pnl), 2),
            "created_at": datetime.utcnow().isoformat(),
        })

    # ── Token usage ───────────────────────────────────────────────────────────

    def record_token_usage(self, input_tokens: int, output_tokens: int,
                           cost_usd: float):
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

    # ── Flags ─────────────────────────────────────────────────────────────────

    def get_flag(self, key: str) -> str:
        rows = self._get("bot_flags", {"key": f"eq.{key}", "limit": "1"})
        if rows:
            v = rows[0].get("value")
            return "" if v is None else str(v)
        return ""

    def set_flag(self, key: str, value: str) -> bool:
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

    # ── Portfolio value (NET of estimated fees) ───────────────────────────────

    def get_portfolio_value(self) -> dict:
        """
        v3.0.2 — net-of-fees portfolio math, matches dashboard to-the-cent.

        Returns:
          deployed_aud      — gross capital deployed (no buy fees)
          realised_lifetime — net realised P&L on closed positions
          unrealised        — net unrealised P&L on opens (if liquidated)
          market_value      — net liquidation value of open positions
          cash_aud          — STARTING + realised_lifetime
                              - deployed_aud - buy_fees_open
          total_aud         — STARTING + realised_lifetime + unrealised
                            == cash_aud + market_value
        """
        from bot import fees

        STARTING = STARTING_CAPITAL_AUD

        try:
            open_positions = self._get("positions", {"status": "eq.open"}) or []
        except Exception:
            open_positions = []
        try:
            closed_positions = self._get("positions", {"status": "eq.closed"}) or []
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
            buy_fees_open    += fees.buy_fee_paid(aud_amount=aud, market=market)
            unrealised_net   += fees.realised_dollar_net(
                aud_amount=aud, pnl_pct=pnl_pct, market=market,
            )
            market_value_net += fees.market_value_net_if_sold(
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
            # phantom cleanup
            if abs(entry_p - exit_p) < 0.0001 and abs(pnl_pct) < 0.0001:
                continue
            realised_lifetime += fees.realised_dollar_net(
                aud_amount=aud, pnl_pct=pnl_pct, market=c.get("market"),
            )

        cash = STARTING + realised_lifetime - deployed_entry - buy_fees_open
        cash = max(0, cash)
        total = STARTING + realised_lifetime + unrealised_net

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
            "market_value":      round(market_value_net, 2),
            "cash_aud":          round(cash, 2),
        }
