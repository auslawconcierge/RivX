"""
telegram_notify.py — Sends alerts to your Telegram and waits for CONFIRM/CANCEL.

Kill switch architecture:
  - Persistent flag in Supabase: flags['kill_switch']
  - To halt: send "STOP ALL" or /pause in Telegram, OR toggle on dashboard
  - To resume: send /resume, OR toggle on dashboard
  - On startup, ALL existing Telegram messages are acknowledged so old STOP ALLs
    don't keep retriggering
  - One alert at activation, then silence. No spam.

Slash commands (added):
  /summary, /positions, /cash, /pause, /resume, /sell SYMBOL, /help
  Each command runs inside the same message-consumption loop as the kill switch
  so there's no second poller fighting over the update watermark.
"""

import logging
import time
import json
import requests
from datetime import datetime
from bot.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, PAPER_MODE

log = logging.getLogger(__name__)

BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Allocation constants (mirrored from bot/brain.py — duplicated here so we don't
# create a circular import). If you change them in brain.py, change them here.
STARTING_CAPITAL_AUD  = 5000
STOCK_MAX_DEPLOYED    = 1500
STOCK_MAX_POSITIONS   = 3
CRYPTO_MAX_DEPLOYED   = 3000
CRYPTO_MAX_POSITIONS  = 6


def _truthy_flag(v: str) -> bool:
    """Accept multiple sentinel values for kill switch flag."""
    return (v or "").lower() in ("on", "1", "true", "yes")


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

    # ─── Kill switch + command dispatch (single message-consumption pass) ──

    def check_kill_switch(self, db=None) -> bool:
        """
        Halt-check called every main-loop tick. Three things happen:
          1. Check Supabase flag — if on, return True (halted)
          2. Drain old Telegram messages on first call (suppresses STOP ALLs
             sent before this run started)
          3. Process fresh messages — STOP ALL → halt, /command → dispatch

        Slash commands run inside this same loop so we never spawn a second
        poller (which would fight over `_last_seen_update_id` and could
        accidentally double-respond).
        """
        halted = False

        # ── Layer 1: persistent flag ──────────────────────────────────────
        if db is not None:
            try:
                flag = db.get_flag("kill_switch")
                if _truthy_flag(flag):
                    halted = True
                    if not self._kill_switch_announced:
                        self._kill_switch_announced = True
                        self.send("🛑 Kill switch active. Bot halted. "
                                  "Send /resume to re-enable trading.")
                        log.warning("Kill switch flag is set — bot halted")
                else:
                    # Flag cleared (e.g. /resume) — reset so re-pause re-announces
                    self._kill_switch_announced = False
            except Exception as e:
                log.debug(f"Kill switch flag read failed: {e}")

        # ── Layer 2: startup drain — DO NOT ACT on old messages ───────────
        if self._last_seen_update_id is None:
            try:
                updates = self._get_updates()
                if updates:
                    last_id = updates[-1]["update_id"]
                    self._get_updates(offset=last_id + 1)
                    self._last_seen_update_id = last_id
                    log.info(f"Telegram startup: drained {len(updates)} old "
                             f"message(s). Watermark = {last_id}")
                else:
                    self._last_seen_update_id = 0
                    log.info("Telegram startup: no old messages")
            except Exception as e:
                log.warning(f"Telegram startup drain failed: {e}")
                self._last_seen_update_id = 0
            return halted

        # ── Layer 3: process fresh messages ───────────────────────────────
        try:
            updates = self._get_updates(offset=self._last_seen_update_id + 1)
        except Exception:
            return halted

        for update in updates:
            uid = update.get("update_id", 0)
            if uid > self._last_seen_update_id:
                self._last_seen_update_id = uid

            msg = update.get("message", {})
            text_raw = (msg.get("text", "") or "").strip()
            text_upper = text_raw.upper()
            chat = str(msg.get("chat", {}).get("id", ""))

            if chat != str(self.chat_id):
                continue
            if not text_raw:
                continue

            # STOP ALL — kill switch trigger (existing behaviour)
            if text_upper == "STOP ALL":
                log.warning(f"Fresh STOP ALL received (update_id {uid}) — setting flag")
                if db is not None:
                    try:
                        db.set_flag("kill_switch", "on")
                    except Exception as e:
                        log.error(f"Could not persist kill switch flag: {e}")
                if not self._kill_switch_announced:
                    self._kill_switch_announced = True
                    self.send("🛑 Bot halted. Send /resume to re-enable trading.")
                halted = True
                continue

            # Slash commands
            if text_raw.startswith("/"):
                try:
                    self._handle_command(text_raw, db)
                except Exception as e:
                    log.error(f"Command handling failed for '{text_raw}': {e}",
                              exc_info=True)
                continue

            # Anything else — silent ignore. Do NOT reply (avoids loops).

        # Re-check flag in case /pause just set it
        if not halted and db is not None:
            try:
                if _truthy_flag(db.get_flag("kill_switch")):
                    halted = True
            except Exception:
                pass

        return halted

    # ─── Slash-command dispatch ────────────────────────────────────────────

    def _handle_command(self, text: str, db) -> None:
        parts = text.strip().split()
        if not parts:
            return
        # Strip leading slash and any @botname suffix (Telegram convention)
        cmd = parts[0].lstrip("/").lower()
        if "@" in cmd:
            cmd = cmd.split("@", 1)[0]
        args = parts[1:]

        if cmd in ("summary", "s"):
            self._cmd_summary(db)
        elif cmd in ("positions", "pos", "p"):
            self._cmd_positions(db)
        elif cmd == "cash":
            self._cmd_cash(db)
        elif cmd in ("pause", "stop", "halt"):
            self._cmd_pause(db)
        elif cmd in ("resume", "start", "go"):
            self._cmd_resume(db)
        elif cmd == "sell":
            self._cmd_sell(args, db)
        elif cmd in ("help", "commands", "h"):
            self._cmd_help()
        # Unknown commands: silent ignore (avoids reply storms if someone
        # forwards a message starting with "/" by accident)

    def _cmd_summary(self, db) -> None:
        if db is None:
            self.send("Cannot read state — db unavailable")
            return
        try:
            portfolio = db.get_portfolio_value()
            positions = db.get_positions()
            recent    = db.get_recent_trades(limit=50) or []
            kill      = (db.get_flag("kill_switch") or "").lower() in ("on", "1", "true")
        except Exception as e:
            self.send(f"Could not load summary: {e}")
            return

        from datetime import datetime, timezone, timedelta
        aest = timezone(timedelta(hours=10))
        now_aest = datetime.now(aest)

        # ── Portfolio header ─────────────────────────────────────────────
        total    = float(portfolio.get("total_aud", 5000) or 5000)
        day_pnl  = float(portfolio.get("day_pnl", 0) or 0)
        all_pnl  = float(portfolio.get("total_pnl", 0) or 0)
        day_pct  = (day_pnl / max(1, total - day_pnl)) * 100
        all_pct  = (all_pnl / 5000) * 100
        head_emoji = "📈" if day_pnl >= 0 else "📉"

        # ── Capacity (deployed vs available, per market) ─────────────────
        stock_pos  = {s: p for s, p in positions.items() if (p.get("market") or "").lower() == "alpaca"}
        crypto_pos = {s: p for s, p in positions.items() if (p.get("market") or "").lower() == "coinspot"}

        stock_deployed  = sum(float(p.get("aud_amount") or 0) for p in stock_pos.values())
        crypto_deployed = sum(float(p.get("aud_amount") or 0) for p in crypto_pos.values())
        total_deployed  = stock_deployed + crypto_deployed
        # Hard-coded budgets — match brain.py constants
        STOCK_BUDGET, CRYPTO_BUDGET, TOTAL_BUDGET = 1500, 3000, 4500
        STOCK_SLOTS, CRYPTO_SLOTS = 3, 6
        cash_avail = max(0, 5000 - total_deployed)

        # ── Today's activity (trades since AEST midnight) ────────────────
        midnight_aest = now_aest.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc  = midnight_aest.astimezone(timezone.utc)
        todays_trades = []
        for t in recent:
            try:
                ts = datetime.fromisoformat((t.get("created_at") or "").replace("Z", "+00:00"))
                if ts >= midnight_utc:
                    todays_trades.append(t)
            except Exception:
                pass
        bought = [t for t in todays_trades if (t.get("action") or "").upper() == "BUY"]
        sold   = [t for t in todays_trades if (t.get("action") or "").upper() == "SELL"]

        # ── Top movers (best/worst by pnl_pct) ───────────────────────────
        movers = []
        for sym, p in positions.items():
            pct = float(p.get("pnl_pct") or 0) * 100
            aud = float(p.get("aud_amount") or 0)
            dollar = aud * (pct / 100)
            movers.append((sym, pct, dollar))
        movers.sort(key=lambda x: x[1], reverse=True)
        winners = [m for m in movers if m[1] > 0][:2]
        losers  = sorted([m for m in movers if m[1] < 0], key=lambda x: x[1])[:2]

        # ── Position list, grouped, with $/% ─────────────────────────────
        def _fmt_group(label, group, budget, slots):
            if not group:
                return f"<b>{label}</b> (0/{slots} slots) — none\n"
            net_pct_sum = sum(float(p.get("aud_amount") or 0) * float(p.get("pnl_pct") or 0)
                              for p in group.values())
            lines = [f"<b>{label}</b> ({len(group)}/{slots} slots) — net {'+' if net_pct_sum >= 0 else '-'}${abs(net_pct_sum):.2f}"]
            # Sort by abs P&L $ desc so biggest movers first
            sorted_items = sorted(group.items(),
                                  key=lambda kv: abs(float(kv[1].get("aud_amount") or 0) *
                                                     float(kv[1].get("pnl_pct") or 0)),
                                  reverse=True)
            for sym, p in sorted_items:
                pct = float(p.get("pnl_pct") or 0) * 100
                aud = float(p.get("aud_amount") or 0)
                dollar = aud * (pct / 100)
                lines.append(f"  • <b>{sym}</b>: ${aud:.0f} → {'+' if pct >= 0 else ''}{pct:.2f}% "
                             f"({'+' if dollar >= 0 else '-'}${abs(dollar):.2f})")
            return "\n".join(lines) + "\n"

        # ── Build the message ────────────────────────────────────────────
        parts = []
        parts.append(
            f"{head_emoji} <b>RivX summary</b> — {now_aest.strftime('%H:%M')} AEST\n\n"
            f"Portfolio: <b>${total:,.2f} AUD</b>  "
            f"({'+' if day_pnl >= 0 else '-'}${abs(day_pnl):.2f}, "
            f"{'+' if day_pct >= 0 else ''}{day_pct:.2f}% today)\n"
            f"All-time: {'+' if all_pnl >= 0 else '-'}${abs(all_pnl):.2f} "
            f"({'+' if all_pct >= 0 else ''}{all_pct:.2f}%)\n"
        )

        parts.append(
            f"\n<b>Capital</b>\n"
            f"  Deployed: ${total_deployed:.0f} / ${TOTAL_BUDGET} "
            f"({total_deployed/TOTAL_BUDGET*100:.0f}%)\n"
            f"  Cash: ${cash_avail:.0f}\n"
            f"  Stocks: ${stock_deployed:.0f} / ${STOCK_BUDGET}  "
            f"({len(stock_pos)}/{STOCK_SLOTS} slots)\n"
            f"  Crypto: ${crypto_deployed:.0f} / ${CRYPTO_BUDGET}  "
            f"({len(crypto_pos)}/{CRYPTO_SLOTS} slots)\n"
        )

        parts.append(
            f"\n<b>Today</b>\n"
            f"  Bought: {len(bought)}"
            + (f" ({', '.join(t.get('symbol','?') for t in bought[:6])}{'...' if len(bought)>6 else ''})" if bought else "")
            + f"\n  Sold: {len(sold)}"
            + (f" ({', '.join(t.get('symbol','?') for t in sold[:6])})" if sold else "")
            + "\n"
        )

        if winners or losers:
            parts.append("\n<b>Top movers</b>\n")
            for sym, pct, dollar in winners:
                parts.append(f"  🟢 {sym}  +{pct:.2f}% (+${dollar:.2f})\n")
            for sym, pct, dollar in losers:
                parts.append(f"  🔴 {sym}  {pct:.2f}% (-${abs(dollar):.2f})\n")

        parts.append(f"\n<b>Open ({len(positions)})</b>\n")
        parts.append(_fmt_group("US stocks", stock_pos, STOCK_BUDGET, STOCK_SLOTS))
        parts.append(_fmt_group("Crypto",    crypto_pos, CRYPTO_BUDGET, CRYPTO_SLOTS))

        status_emoji = "🔴 paused" if kill else "🟢 trading active"
        parts.append(f"\nBot: {status_emoji}")

        self.send("".join(parts))

    def _cmd_positions(self, db) -> None:
        if db is None:
            self.send("Cannot read positions — db unavailable")
            return
        try:
            positions = db.get_positions()
        except Exception as e:
            self.send(f"Could not load positions: {e}")
            return

        if not positions:
            self.send("No open positions.")
            return

        # Group by market for clarity
        stocks  = {s: p for s, p in positions.items() if (p.get("market") or "").lower() == "alpaca"}
        crypto  = {s: p for s, p in positions.items() if (p.get("market") or "").lower() == "coinspot"}
        other   = {s: p for s, p in positions.items() if s not in stocks and s not in crypto}

        sections = []
        for label, group in [("US stocks", stocks), ("Crypto", crypto), ("Other", other)]:
            if not group:
                continue
            lines = [f"<b>{label}</b>"]
            for sym, p in group.items():
                pnl_pct = (p.get("pnl_pct") or 0) * 100
                aud = p.get("aud_amount") or 0
                lines.append(f"  • {sym}: ${aud:.0f} → {pnl_pct:+.2f}%")
            sections.append("\n".join(lines))

        self.send("📋 <b>Open positions</b>\n\n" + "\n\n".join(sections))

    def _cmd_cash(self, db) -> None:
        if db is None:
            self.send("Cannot read state — db unavailable")
            return
        try:
            positions = db.get_positions()
        except Exception as e:
            self.send(f"Could not load: {e}")
            return

        stock_pos = [p for p in positions.values() if (p.get("market") or "").lower() == "alpaca"]
        crypto_pos = [p for p in positions.values() if (p.get("market") or "").lower() == "coinspot"]
        stock_dep = sum((p.get("aud_amount") or 0) for p in stock_pos)
        crypto_dep = sum((p.get("aud_amount") or 0) for p in crypto_pos)
        free_cash = max(0, STARTING_CAPITAL_AUD - stock_dep - crypto_dep)

        def status(dep, free, slots, max_dep, max_pos, min_size=300):
            if slots <= 0:
                return f"⛔ at position cap ({max_pos}/{max_pos})"
            if free < min_size:
                return f"⚠ budget full (${free:.0f} left)"
            return f"✅ ${free:.0f} budget + {slots} slots free"

        stock_free  = STOCK_MAX_DEPLOYED  - stock_dep
        crypto_free = CRYPTO_MAX_DEPLOYED - crypto_dep
        stock_slots  = STOCK_MAX_POSITIONS  - len(stock_pos)
        crypto_slots = CRYPTO_MAX_POSITIONS - len(crypto_pos)

        msg = (
            f"💰 <b>Cash &amp; capacity</b>\n\n"
            f"Free cash: <b>${free_cash:,.2f}</b> of ${STARTING_CAPITAL_AUD}\n\n"
            f"<b>Stocks</b>: ${stock_dep:.0f}/{STOCK_MAX_DEPLOYED} · "
            f"{len(stock_pos)}/{STOCK_MAX_POSITIONS} pos\n"
            f"  → {status(stock_dep, stock_free, stock_slots, STOCK_MAX_DEPLOYED, STOCK_MAX_POSITIONS)}\n\n"
            f"<b>Crypto</b>: ${crypto_dep:.0f}/{CRYPTO_MAX_DEPLOYED} · "
            f"{len(crypto_pos)}/{CRYPTO_MAX_POSITIONS} pos\n"
            f"  → {status(crypto_dep, crypto_free, crypto_slots, CRYPTO_MAX_DEPLOYED, CRYPTO_MAX_POSITIONS)}"
        )
        self.send(msg)

    def _cmd_pause(self, db) -> None:
        if db is None:
            self.send("Cannot pause — db unavailable")
            return
        try:
            db.set_flag("kill_switch", "on")
        except Exception as e:
            self.send(f"Could not pause: {e}")
            return
        # Suppress layer-1 re-announcement so the user only sees this one reply
        self._kill_switch_announced = True
        self.send("⏸ <b>Trading paused</b>\n\n"
                  "Snapshots and force-sells still work. "
                  "Send /resume to re-enable trading.")

    def _cmd_resume(self, db) -> None:
        if db is None:
            self.send("Cannot resume — db unavailable")
            return
        try:
            db.set_flag("kill_switch", "off")
        except Exception as e:
            self.send(f"Could not resume: {e}")
            return
        self._kill_switch_announced = False
        self.send("▶ <b>Trading resumed.</b>")

    def _cmd_sell(self, args, db) -> None:
        if not args:
            self.send("Usage: <code>/sell SYMBOL</code>\nExample: <code>/sell APLD</code>")
            return
        if db is None:
            self.send("Cannot queue sell — db unavailable")
            return

        sym = args[0].upper()
        try:
            positions = db.get_positions()
        except Exception as e:
            self.send(f"Could not check positions: {e}")
            return

        if sym not in positions:
            open_list = ", ".join(positions.keys()) or "(none)"
            self.send(f"No open position for <b>{sym}</b>.\nOpen: {open_list}")
            return

        market = positions[sym].get("market")
        try:
            result = db._post("manual_orders", {
                "symbol":       sym,
                "action":       "SELL",
                "market":       market,
                "requested_at": datetime.utcnow().isoformat(),
                "status":       "pending",
            })
        except Exception as e:
            self.send(f"Sell queue failed: {e}")
            return

        if result:
            self.send(f"📥 Force-sell queued for <b>{sym}</b>. "
                      f"Will execute on next cycle (~30 sec).")
        else:
            self.send(f"Could not queue sell for {sym} — does the "
                      f"<code>manual_orders</code> table exist?")

    def _cmd_help(self) -> None:
        self.send(
            "<b>RivX commands</b>\n\n"
            "/summary — portfolio overview\n"
            "/positions — open positions list\n"
            "/cash — free cash &amp; budget headroom\n"
            "/pause — pause trading\n"
            "/resume — resume trading\n"
            "/sell SYMBOL — force-sell a position\n"
            "/help — this list\n\n"
            "Or send <b>STOP ALL</b> to halt the bot completely."
        )
