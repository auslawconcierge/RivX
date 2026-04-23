"""
telegram_notify.py — Sends alerts to your Telegram and waits for CONFIRM/CANCEL.
This is the human-in-the-loop layer before every real money trade.
You have a configurable window (default 5 mins) to cancel any trade.
If you don't respond in time, the bot proceeds automatically.
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
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("Telegram not configured — alerts disabled")

    def send(self, message: str) -> bool:
        """Send a plain text message to your Telegram."""
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
        """
        Send a trade alert and wait for the user to reply CONFIRM or CANCEL.
        Returns True if confirmed (or timed out — default is to proceed).
        Returns False only if the user explicitly replies CANCEL.

        In paper mode, always returns True without waiting.
        """
        if PAPER_MODE:
            log.info(f"[PAPER MODE] Would send: {message}")
            return True

        full_msg = f"{message}\n\n⏱ Auto-confirming in {timeout_seconds // 60} mins if no reply."
        self.send(full_msg)

        # Poll for a reply
        last_update_id = self._get_last_update_id()
        deadline = time.time() + timeout_seconds
        poll_interval = 5  # check every 5 seconds

        while time.time() < deadline:
            time.sleep(poll_interval)
            updates = self._get_updates(offset=last_update_id + 1 if last_update_id else None)
            for update in updates:
                last_update_id = update["update_id"]
                text = update.get("message", {}).get("text", "").strip().upper()
                chat = str(update.get("message", {}).get("chat", {}).get("id", ""))
                if chat == str(self.chat_id):
                    if "CONFIRM" in text or "YES" in text or "Y" == text:
                        self.send("✓ Confirmed — executing trade now.")
                        return True
                    elif "CANCEL" in text or "NO" in text or "N" == text:
                        self.send("✗ Cancelled — trade aborted.")
                        return False

        # Timed out — auto-confirm
        self.send("⏱ No response — auto-confirming and executing trade.")
        return True

    def send_stop_loss_alert(self, symbol: str, pnl_pct: float):
        """Stop-loss alerts fire immediately — no confirmation needed."""
        self.send(
            f"⚠️ <b>Stop-loss triggered — {symbol}</b>\n"
            f"Position down {pnl_pct:.1%}. Selling now to protect capital.\n"
            f"This trade executed automatically."
        )

    def send_daily_summary(self, total_aud: float, day_pnl: float,
                           total_pnl: float, actions: list):
        """End-of-day summary message."""
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

    def _get_updates(self, offset: int = None) -> list:
        params = {"timeout": 1}
        if offset:
            params["offset"] = offset
        try:
            resp = requests.get(f"{BASE}/getUpdates", params=params, timeout=5)
            resp.raise_for_status()
            return resp.json().get("result", [])
        except Exception:
            return []

    def _get_last_update_id(self) -> int | None:
        updates = self._get_updates()
        if updates:
            return updates[-1]["update_id"]
        return None

    def check_kill_switch(self) -> bool:
        """
        Check if the user has sent 'STOP ALL' in the last few messages.
        Call this at the start of each bot run.
        """
        updates = self._get_updates()
        for update in updates[-10:]:  # check last 10 messages
            text = update.get("message", {}).get("text", "").strip().upper()
            chat = str(update.get("message", {}).get("chat", {}).get("id", ""))
            if chat == str(self.chat_id) and "STOP ALL" in text:
                self.send("🛑 Kill switch activated. Bot halted. No more trades will execute.")
                return True
        return False
