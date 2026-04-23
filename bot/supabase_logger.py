"""
RivX supabase_logger.py
Stores all bot state between loop iterations.
Tables: trades, positions, signal_weights, snapshots, approved_plan
"""

import json
import logging
from datetime import datetime, date
import requests
from bot.config import SUPABASE_URL, SUPABASE_API_KEY, PORTFOLIO

log = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {"rsi": 0.2, "macd": 0.2, "bollinger": 0.2, "volume": 0.2, "ma_cross": 0.2}


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
        except Exception as e:
            log.error(f"DB PATCH {table}: {e}")
            return False

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
        """Update unrealised P&L for an open position."""
        existing = self._get("positions",
                             {"symbol": f"eq.{symbol}", "status": "eq.open"})
        if existing:
            pos   = existing[0]
            entry = pos.get("entry_price", current_price)
            pnl   = (current_price - entry) / entry if entry > 0 else 0
            self._patch("positions", {"pnl_pct": round(pnl, 4)},
                        "id", str(pos["id"]))

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

    def get_flag(self, key: str) -> str:
        """Get a persistent flag — survives redeploys."""
        rows = self._get("approved_plan", {"order": "updated_at.desc", "limit": "1"})
        if rows:
            try:
                plan = json.loads(rows[0].get("plan", "{}"))
                return plan.get(f"_flag_{key}", "")
            except Exception:
                return ""
        return ""

    def set_flag(self, key: str, value: str):
        """Set a persistent flag — survives redeploys."""
        rows = self._get("approved_plan")
        try:
            plan = json.loads(rows[0].get("plan", "{}")) if rows else {}
        except Exception:
            plan = {}
        plan[f"_flag_{key}"] = value
        data = {"plan": json.dumps(plan), "updated_at": datetime.utcnow().isoformat()}
        if rows:
            self._patch("approved_plan", data, "id", str(rows[0]["id"]))
        else:
            self._post("approved_plan", data)

    def get_portfolio_value(self) -> dict:
        snaps = self._get("snapshots", {"order": "date.desc", "limit": "2"})
        today = snaps[0] if snaps else {}
        prev  = snaps[1] if len(snaps) > 1 else {}
        total = today.get("total_aud", 5000)
        prev_total = prev.get("total_aud", 5000)
        return {
            "total_aud": total,
            "day_pnl":   round(total - prev_total, 2),
            "total_pnl": round(total - 5000, 2),
        }
