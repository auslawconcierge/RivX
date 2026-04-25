"""
telegram_notify.py — Sends alerts to your Telegram and waits for CONFIRM/CANCEL.

Kill switch architecture:
  - Persistent flag in Supabase: flags['kill_switch']
  - To halt: send "STOP ALL" in Telegram OR set the flag manually
  - To resume: DELETE FROM flags WHERE key = 'kill_switch'  THEN restart Render
  - On startup, ALL existing Telegram messages are acknowledged so old STOP ALLs
    don't keep retriggering
  - One alert at activation, then silence. No spam.
"""

import logging
import time
import requests
from bot.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, PAPER_MODE

log = logging.getLogger(__name__)

BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


class TelegramNotifier:

    def __init__(self):
        self.chat_id = TELEGRAM_CHAT_ID
        self._last_seen_update_id = None
        self._kill_switch_announced = False
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("Telegram not configured — alerts disabled")

    def send(self, message: str) -> bool:
        if not TELEGRAM_TOKEN:
            log.info(f"[TELEGRAM DISABLED] {message}")
            return True
        try:
            resp = requests.post(
                f"{BASE}/sendMessage",
                json={"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"},
                timeout=10
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            log.error(f"Telegram send failed: {e}")
            return False

    def send_and_wait(self, message: str, timeout_seconds: int = 300) -> bool:
        if PAPER_MODE:
            log.info(f"[PAPER MODE] Would send: {message}")
            return True

        full_msg = f"{message}\n\n⏱ Auto-confirming in {timeout_seconds // 60} mins if no reply."
        self.send(full_msg)

        last_id = self._get_latest_update_id() or 0
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            time.sleep(5)
            updates = self._get_updates(offset=last_id + 1)
            for update in updates:
                last_id = max(last_id, update.get("update_id", 0))
                text = (update.get("message", {}).get("text", "") or "").strip().upper()
                chat = str(update.get("message", {}).get("chat", {}).get("id", ""))
                if chat != str(self.chat_id):
                    continue
                if "CONFIRM" in text or text in ("YES", "Y"):
                    self.send("✓ Confirmed — executing trade now.")
                    return True
                elif "CANCEL" in text or text in ("NO", "N"):
                    self.send("✗ Cancelled — trade aborted.")
                    return False

        self.send("⏱ No response — auto-confirming and executing trade.")
        return True

    def send_daily_summary(self, total_aud: float, day_pnl: float,
                           total_pnl: float, actions: list):
        pnl_emoji = "📈" if day_pnl >= 0 else "📉"
        action_str = "\n".join(f"  • {a}" for a in actions) if actions else "  • No trades today"
        self.send(
            f"{pnl_emoji} <b>Daily summary</b>\n\n"
            f"Portfolio: <b>${total_aud:,.2f} AUD</b>\n"
            f"Today: {'+' if day_pnl >= 0 else ''}${day_pnl:.2f}\n"
            f"Total P&L: {'+' if total_pnl >= 0 else ''}${total_pnl:.2f}\n\n"
            f"Actions:\n{action_str}\n\n"
            f"Reply <b>STOP ALL</b> to halt the bot."
        )

    # ─── Telegram polling ──────────────────────────────────────────────────

    def _get_updates(self, offset: int = None) -> list:
        """Raw getUpdates. Pass offset to acknowledge updates with id < offset."""
        params = {"timeout": 1}
        if offset:
            params["offset"] = offset
        try:
            resp = requests.get(f"{BASE}/getUpdates", params=params, timeout=5)
            resp.raise_for_status()
            return resp.json().get("result", []) or []
        except Exception as e:
            log.debug(f"Telegram getUpdates failed: {e}")
            return []

    def _get_latest_update_id(self) -> int | None:
        updates = self._get_updates()
        return updates[-1]["update_id"] if updates else None

    # ─── Kill switch ───────────────────────────────────────────────────────

    def check_kill_switch(self, db=None) -> bool:
        """
        Halt-check called by main loop. Two layers:
          1. Persistent Supabase flag — once set, stays set across restarts
          2. Fresh Telegram STOP ALL during this run — sets the flag, halts

        Args:
          db: SupabaseLogger — required for persistent flag
        """
        # Layer 1: Supabase persistent flag
        if db is not None:
            try:
                if db.get_flag("kill_switch") == "1":
                    if not self._kill_switch_announced:
                        self._kill_switch_announced = True
                        self.send("🛑 Kill switch is active. Bot halted. To resume: run "
                                 "<code>DELETE FROM flags WHERE key = 'kill_switch';</code> "
                                 "in Supabase, then restart Render.")
                        log.warning("Kill switch flag is set — bot halted")
                    return True
            except Exception as e:
                log.debug(f"Kill switch flag read failed: {e}")

        # Layer 2: Telegram polling — STARTUP, drain everything
        if self._last_seen_update_id is None:
            try:
                updates = self._get_updates()
                if updates:
                    # ACK all existing updates so they're consumed and never returned again.
                    # ANY STOP ALL sitting in history is dropped here.
                    last_id = updates[-1]["update_id"]
                    # The +1 offset tells Telegram "I've seen everything ≤ last_id"
                    self._get_updates(offset=last_id + 1)
                    self._last_seen_update_id = last_id
                    log.info(f"Kill switch startup: drained {len(updates)} old Telegram message(s). "
                             f"Watermark = {last_id}")
                else:
                    self._last_seen_update_id = 0
                    log.info("Kill switch startup: no old messages")
            except Exception as e:
                log.warning(f"Kill switch startup drain failed: {e}")
                self._last_seen_update_id = 0
            # Never halt on startup, only on fresh messages thereafter
            return False

        # Layer 2: Telegram polling — RUNTIME, only fresh messages
        try:
            updates = self._get_updates(offset=self._last_seen_update_id + 1)
        except Exception:
            return False

        for update in updates:
            uid = update.get("update_id", 0)
            if uid > self._last_seen_update_id:
                self._last_seen_update_id = uid

            msg = update.get("message", {})
            text = (msg.get("text", "") or "").strip().upper()
            chat = str(msg.get("chat", {}).get("id", ""))

            if chat != str(self.chat_id):
                continue
            if text != "STOP ALL":
                continue

            log.warning(f"Fresh STOP ALL received (update_id {uid}) — setting flag")
            if db is not None:
                try:
                    db.set_flag("kill_switch", "1")
                except Exception as e:
                    log.error(f"Could not persist kill switch flag: {e}")

            if not self._kill_switch_announced:
                self._kill_switch_announced = True
                self.send("🛑 Kill switch activated. Bot halted. To resume: run "
                         "<code>DELETE FROM flags WHERE key = 'kill_switch';</code> "
                         "in Supabase, then restart Render.")
            return True

        return False
