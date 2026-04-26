# RIVX_VERSION: v2.1-render-fixed-2026-04-26
"""
telegram_notify.py — Sends alerts to your Telegram and waits for CONFIRM/CANCEL.

v2 changes from yesterday's old strategy:
  - $10K starting capital (was $5K)
  - Three buckets (was two): swing crypto, momentum crypto, swing stocks
  - $500 ops floor (always-cash buffer)
  - Reads slot/budget constants from bot.strategy directly so they can never
    drift from what the bot actually uses

Slash commands:
  /summary, /positions, /cash, /pause, /resume, /sell SYMBOL, /help
"""

import logging
import time
import json
import requests
from datetime import datetime, timezone, timedelta
from bot.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, PAPER_MODE
from bot import strategy

log = logging.getLogger(__name__)

BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


def _truthy_flag(v: str) -> bool:
    return (v or "").lower() in ("on", "1", "true", "yes")


def _bucket_of(position: dict) -> str:
    """
    Return bucket name for a position. v2 positions have a `bucket` field;
    legacy positions don't, so we infer:
      - market=alpaca → swing_stock
      - market=coinspot, no bucket → swing_crypto (most conservative default)
    """
    bucket = (position.get("bucket") or "").strip()
    if bucket in (strategy.Bucket.SWING_CRYPTO,
                  strategy.Bucket.MOMENTUM_CRYPTO,
                  strategy.Bucket.SWING_STOCK):
        return bucket
    market = (position.get("market") or "").lower()
    if market == "alpaca":
        return strategy.Bucket.SWING_STOCK
    return strategy.Bucket.SWING_CRYPTO


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

    # ─── Telegram polling ─────────────────────────────────────────────────

    def _get_updates(self, offset: int = None) -> list:
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

    # ─── Kill switch + command dispatch ───────────────────────────────────

    def check_kill_switch(self, db=None) -> bool:
        halted = False

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
                    self._kill_switch_announced = False
            except Exception as e:
                log.debug(f"Kill switch flag read failed: {e}")

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

            if text_raw.startswith("/"):
                try:
                    self._handle_command(text_raw, db)
                except Exception as e:
                    log.error(f"Command handling failed for '{text_raw}': {e}",
                              exc_info=True)
                continue

        if not halted and db is not None:
            try:
                if _truthy_flag(db.get_flag("kill_switch")):
                    halted = True
            except Exception:
                pass

        return halted

    # ─── Slash-command dispatch ───────────────────────────────────────────

    def _handle_command(self, text: str, db) -> None:
        parts = text.strip().split()
        if not parts:
            return
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

    # ─── /summary ─────────────────────────────────────────────────────────

    def _cmd_summary(self, db) -> None:
        if db is None:
            self.send("Cannot read state — db unavailable")
            return
        try:
            portfolio = db.get_portfolio_value() or {}
            positions = db.get_positions() or {}
            recent    = db.get_recent_trades(limit=50) or []
            kill      = _truthy_flag(db.get_flag("kill_switch") or "")
        except Exception as e:
            self.send(f"Could not load summary: {e}")
            return

        aest = timezone(timedelta(hours=10))
        now_aest = datetime.now(aest)

        def _us_market_state(now):
            et_now = now - timedelta(hours=14)
            if et_now.weekday() >= 5:
                return "closed"
            mins = et_now.hour * 60 + et_now.minute
            if 4*60 <= mins < 9*60 + 30:
                return "premarket"
            if 9*60 + 30 <= mins < 16*60:
                return "open"
            if 16*60 <= mins < 20*60:
                return "afterhours"
            return "closed"

        market_state = _us_market_state(now_aest)
        market_label = {
            "open":       "",
            "premarket":  " (pre-market)",
            "afterhours": " (after-hours)",
            "closed":     " (market closed)",
        }[market_state]

        STARTING = float(strategy.STARTING_CAPITAL_AUD)
        total    = float(portfolio.get("total_aud") or STARTING)
        day_pnl  = float(portfolio.get("day_pnl") or 0)
        all_pnl  = float(portfolio.get("total_pnl") or 0)
        day_pct  = (day_pnl / max(1, total - day_pnl)) * 100
        all_pct  = (all_pnl / STARTING) * 100
        head_emoji = "📈" if day_pnl >= 0 else "📉"

        # Group positions by bucket (v2: three buckets)
        buckets = {
            strategy.Bucket.SWING_CRYPTO:    {},
            strategy.Bucket.MOMENTUM_CRYPTO: {},
            strategy.Bucket.SWING_STOCK:     {},
        }
        for sym, p in positions.items():
            buckets[_bucket_of(p)][sym] = p

        sw_crypto = buckets[strategy.Bucket.SWING_CRYPTO]
        mo_crypto = buckets[strategy.Bucket.MOMENTUM_CRYPTO]
        stocks    = buckets[strategy.Bucket.SWING_STOCK]

        sw_dep = sum(float(p.get("aud_amount") or 0) for p in sw_crypto.values())
        mo_dep = sum(float(p.get("aud_amount") or 0) for p in mo_crypto.values())
        st_dep = sum(float(p.get("aud_amount") or 0) for p in stocks.values())
        total_deployed = sw_dep + mo_dep + st_dep
        cash_avail = max(0, STARTING - total_deployed)

        SW_BUDGET = strategy.SWING_CRYPTO_BUDGET
        MO_BUDGET = strategy.MOMENTUM_CRYPTO_BUDGET
        ST_BUDGET = strategy.SWING_STOCKS_BUDGET
        DEPLOYABLE = SW_BUDGET + MO_BUDGET + ST_BUDGET
        SW_SLOTS = strategy.SWING_CRYPTO_SLOTS
        MO_SLOTS = strategy.MOMENTUM_CRYPTO_SLOTS
        ST_SLOTS = strategy.SWING_STOCKS_SLOTS

        midnight_aest = now_aest.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc  = midnight_aest.astimezone(timezone.utc)
        todays_trades = []
        for t in recent:
            try:
                ts_str = (t.get("created_at") or "").replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_str)
                if ts >= midnight_utc:
                    todays_trades.append(t)
            except Exception:
                pass
        bought = [t for t in todays_trades if (t.get("action") or "").upper() == "BUY"]
        sold   = [t for t in todays_trades if (t.get("action") or "").upper() == "SELL"]

        movers = []
        for sym, p in positions.items():
            pct = float(p.get("pnl_pct") or 0) * 100
            aud = float(p.get("aud_amount") or 0)
            dollar = aud * (pct / 100)
            movers.append((sym, pct, dollar))
        movers.sort(key=lambda x: x[1], reverse=True)
        winners = [m for m in movers if m[1] > 0][:2]
        losers  = sorted([m for m in movers if m[1] < 0], key=lambda x: x[1])[:2]

        def _fmt_group(label, group, slots):
            if not group:
                return f"<b>{label}</b> (0/{slots} slots) — none\n"
            net_dollar = sum(float(p.get("aud_amount") or 0) * float(p.get("pnl_pct") or 0)
                             for p in group.values())
            lines = [f"<b>{label}</b> ({len(group)}/{slots} slots) — net "
                     f"{'+' if net_dollar >= 0 else '-'}${abs(net_dollar):.2f}"]
            sorted_items = sorted(
                group.items(),
                key=lambda kv: abs(float(kv[1].get("aud_amount") or 0) *
                                   float(kv[1].get("pnl_pct") or 0)),
                reverse=True,
            )
            for sym, p in sorted_items:
                pct = float(p.get("pnl_pct") or 0) * 100
                aud = float(p.get("aud_amount") or 0)
                dollar = aud * (pct / 100)
                lines.append(f"  • <b>{sym}</b>: ${aud:.0f} → "
                             f"{'+' if pct >= 0 else ''}{pct:.2f}% "
                             f"({'+' if dollar >= 0 else '-'}${abs(dollar):.2f})")
            return "\n".join(lines) + "\n"

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
            f"  Cash: ${cash_avail:.0f}\n"
            f"  Deployed: ${total_deployed:.0f} / ${DEPLOYABLE:.0f} "
            f"({total_deployed/DEPLOYABLE*100:.0f}%)\n"
            f"  Ops floor: ${strategy.OPS_FLOOR_AUD:.0f}\n"
        )

        parts.append(
            f"\n<b>Buckets</b>\n"
            f"  Swing crypto:    ${sw_dep:.0f} / ${SW_BUDGET:.0f}  "
            f"({len(sw_crypto)}/{SW_SLOTS} slots, ${strategy.SWING_CRYPTO_SIZE:.0f}/buy)\n"
            f"  Momentum crypto: ${mo_dep:.0f} / ${MO_BUDGET:.0f}  "
            f"({len(mo_crypto)}/{MO_SLOTS} slots, ${strategy.MOMENTUM_CRYPTO_SIZE:.0f}/buy)\n"
            f"  Swing stocks:    ${st_dep:.0f} / ${ST_BUDGET:.0f}  "
            f"({len(stocks)}/{ST_SLOTS} slots, ${strategy.SWING_STOCKS_SIZE:.0f}/buy)\n"
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
                tag = market_label if sym in stocks else ""
                parts.append(f"  🟢 {sym}  +{pct:.2f}% (+${dollar:.2f}){tag}\n")
            for sym, pct, dollar in losers:
                tag = market_label if sym in stocks else ""
                parts.append(f"  🔴 {sym}  {pct:.2f}% (-${abs(dollar):.2f}){tag}\n")

        parts.append(f"\n<b>Open ({len(positions)})</b>\n")
        parts.append(_fmt_group("Swing crypto",    sw_crypto, SW_SLOTS))
        parts.append(_fmt_group("Momentum crypto", mo_crypto, MO_SLOTS))
        parts.append(_fmt_group(f"US stocks{market_label}", stocks, ST_SLOTS))

        status_emoji = "🔴 paused" if kill else "🟢 trading active"
        parts.append(f"\nBot: {status_emoji}")

        self.send("".join(parts))

    # ─── /positions ───────────────────────────────────────────────────────

    def _cmd_positions(self, db) -> None:
        if db is None:
            self.send("Cannot read positions — db unavailable")
            return
        try:
            positions = db.get_positions() or {}
        except Exception as e:
            self.send(f"Could not load positions: {e}")
            return

        if not positions:
            self.send("No open positions.")
            return

        groups = {
            strategy.Bucket.SWING_CRYPTO:    {},
            strategy.Bucket.MOMENTUM_CRYPTO: {},
            strategy.Bucket.SWING_STOCK:     {},
        }
        for sym, p in positions.items():
            groups[_bucket_of(p)][sym] = p

        sections = []
        for label, bucket_key in [
            ("Swing crypto",    strategy.Bucket.SWING_CRYPTO),
            ("Momentum crypto", strategy.Bucket.MOMENTUM_CRYPTO),
            ("US stocks",       strategy.Bucket.SWING_STOCK),
        ]:
            group = groups[bucket_key]
            if not group:
                continue
            lines = [f"<b>{label}</b>"]
            for sym, p in group.items():
                pnl_pct = float(p.get("pnl_pct") or 0) * 100
                aud = float(p.get("aud_amount") or 0)
                lines.append(f"  • {sym}: ${aud:.0f} → {pnl_pct:+.2f}%")
            sections.append("\n".join(lines))

        self.send("📋 <b>Open positions</b>\n\n" + "\n\n".join(sections))

    # ─── /cash ────────────────────────────────────────────────────────────

    def _cmd_cash(self, db) -> None:
        if db is None:
            self.send("Cannot read state — db unavailable")
            return
        try:
            positions = db.get_positions() or {}
        except Exception as e:
            self.send(f"Could not load: {e}")
            return

        STARTING = float(strategy.STARTING_CAPITAL_AUD)
        SW_B = strategy.SWING_CRYPTO_BUDGET
        MO_B = strategy.MOMENTUM_CRYPTO_BUDGET
        ST_B = strategy.SWING_STOCKS_BUDGET
        OPS = strategy.OPS_FLOOR_AUD
        SW_N = strategy.SWING_CRYPTO_SLOTS
        MO_N = strategy.MOMENTUM_CRYPTO_SLOTS
        ST_N = strategy.SWING_STOCKS_SLOTS

        groups = {
            strategy.Bucket.SWING_CRYPTO:    {},
            strategy.Bucket.MOMENTUM_CRYPTO: {},
            strategy.Bucket.SWING_STOCK:     {},
        }
        for sym, p in positions.items():
            groups[_bucket_of(p)][sym] = p

        sw = groups[strategy.Bucket.SWING_CRYPTO]
        mo = groups[strategy.Bucket.MOMENTUM_CRYPTO]
        st = groups[strategy.Bucket.SWING_STOCK]

        sw_dep = sum(float(p.get("aud_amount") or 0) for p in sw.values())
        mo_dep = sum(float(p.get("aud_amount") or 0) for p in mo.values())
        st_dep = sum(float(p.get("aud_amount") or 0) for p in st.values())
        total_dep = sw_dep + mo_dep + st_dep
        free_cash = max(0, STARTING - total_dep)

        def status(deployed, budget, used_slots, total_slots):
            free_in_bucket = budget - deployed
            slots_left = total_slots - used_slots
            if slots_left <= 0:
                return f"⛔ at slot cap ({total_slots}/{total_slots})"
            if free_in_bucket < 200:
                return f"⚠ budget nearly full (${free_in_bucket:.0f} left)"
            return f"✅ ${free_in_bucket:.0f} headroom + {slots_left} slot(s) free"

        msg = (
            f"💰 <b>Cash &amp; capacity</b>\n\n"
            f"Free cash: <b>${free_cash:,.2f}</b> of ${STARTING:.0f}\n"
            f"Ops floor: ${OPS:.0f}\n\n"
            f"<b>Swing crypto</b>: ${sw_dep:.0f}/{SW_B:.0f} · "
            f"{len(sw)}/{SW_N} pos · ${strategy.SWING_CRYPTO_SIZE:.0f}/buy\n"
            f"  → {status(sw_dep, SW_B, len(sw), SW_N)}\n\n"
            f"<b>Momentum crypto</b>: ${mo_dep:.0f}/{MO_B:.0f} · "
            f"{len(mo)}/{MO_N} pos · ${strategy.MOMENTUM_CRYPTO_SIZE:.0f}/buy\n"
            f"  → {status(mo_dep, MO_B, len(mo), MO_N)}\n\n"
            f"<b>Swing stocks</b>: ${st_dep:.0f}/{ST_B:.0f} · "
            f"{len(st)}/{ST_N} pos · ${strategy.SWING_STOCKS_SIZE:.0f}/buy\n"
            f"  → {status(st_dep, ST_B, len(st), ST_N)}"
        )
        self.send(msg)

    # ─── /pause /resume ───────────────────────────────────────────────────

    def _cmd_pause(self, db) -> None:
        if db is None:
            self.send("Cannot pause — db unavailable")
            return
        try:
            db.set_flag("kill_switch", "on")
        except Exception as e:
            self.send(f"Could not pause: {e}")
            return
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

    # ─── /sell ────────────────────────────────────────────────────────────

    def _cmd_sell(self, args, db) -> None:
        if not args:
            self.send("Usage: <code>/sell SYMBOL</code>\nExample: <code>/sell BTC</code>")
            return
        if db is None:
            self.send("Cannot queue sell — db unavailable")
            return

        sym = args[0].upper()
        try:
            positions = db.get_positions() or {}
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
                "action":       "sell",
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
            self.send(f"Could not queue sell for {sym}.")

    # ─── /help ────────────────────────────────────────────────────────────

    def _cmd_help(self) -> None:
        self.send(
            "<b>RivX commands</b>\n\n"
            "/summary — portfolio overview\n"
            "/positions — open positions list\n"
            "/cash — free cash &amp; bucket headroom\n"
            "/pause — pause trading\n"
            "/resume — resume trading\n"
            "/sell SYMBOL — force-sell a position\n"
            "/help — this list\n\n"
            "Or send <b>STOP ALL</b> to halt the bot completely."
        )
