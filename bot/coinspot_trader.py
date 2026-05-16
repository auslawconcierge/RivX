# RIVX_VERSION: v3.1.0-balance-checks-2026-05-16
"""
coinspot_trader.py — Executes crypto trades via CoinSpot API.
Paper mode never calls authenticated endpoints.

v3.1.0 (2026-05-16): Pre-trade balance checks against the exchange.
  - buy() now queries CoinSpot AUD balance before placing the order and
    skips (returns None) if balance < requested trade size. Fixes ADA/BTC
    buys getting rejected with HTTP 400 'Insufficient funds' when the
    bot's internal cash model drifted ahead of actual CoinSpot AUD.
  - sell() now queries CoinSpot's actual coin balance and clamps the
    sell qty to min(supabase_qty, actual_balance). Fixes STX trail-stop
    sells spinning on 'Insufficient funds' because tiny rounding/fee
    deductions at buy time left Supabase qty slightly greater than the
    coins actually held on CoinSpot.
  - Added _available_balance() helper that returns the available balance
    for an asset (AUD or any coin) as a float, or None if the CoinSpot
    balances call failed. None vs 0.0 distinguishes 'cannot verify'
    (abort the trade) from 'genuinely zero' (also abort, but a different
    log line).
  - sell() now echoes the (possibly-clamped) coin_amount into the result
    dict so callers can record the actual sold qty in Supabase rather
    than the requested qty.

v3.0.8 (2026-05-10): amount and rate sent to CoinSpot order endpoints
  must be JSON strings, not numbers. CoinSpot rejected numeric values
  with 'Valid amount type required' (HTTP 400). Cast to str().

v3.0.7 (2026-05-10): _post now reads response body BEFORE raise_for_status,
  so HTTP 4xx errors include CoinSpot's actual rejection reason in the
  log instead of a generic 'Bad Request' message.

v3.0.6 (2026-05-10): buy() accepts price_hint kwarg. When CoinSpot's
  public price endpoints return 0 (their listings API has been degraded
  for weeks), we use the validated hint from prices.py rather than
  refusing the trade.

v3.0.1 (2026-05-09): live sell REFUSES if coin_amount is None, so the
  bot can never accidentally dump non-bot holdings sitting in the same
  CoinSpot account. Bot must pass the exact qty it bought.
"""

import hmac
import hashlib
import json
import logging
import time
import requests
from bot.config import COINSPOT_API_KEY, COINSPOT_SECRET_KEY, PAPER_MODE

log = logging.getLogger(__name__)

COINSPOT_BASE = "https://www.coinspot.com.au"


class CoinSpotTrader:
    def __init__(self):
        self.mode = "PAPER" if PAPER_MODE else "LIVE"
        log.info(f"CoinSpotTrader initialised — {self.mode} mode")

    def _sign(self, payload: dict) -> tuple[str, str]:
        payload_str = json.dumps(payload, separators=(",", ":"))
        signature = hmac.new(
            COINSPOT_SECRET_KEY.encode("utf-8"),
            payload_str.encode("utf-8"),
            hashlib.sha512
        ).hexdigest()
        return payload_str, signature

    def _post(self, endpoint: str, data: dict) -> dict | None:
        data["nonce"] = int(time.time() * 1000)
        payload_str, signature = self._sign(data)
        headers = {
            "Content-Type": "application/json",
            "key": COINSPOT_API_KEY,
            "sign": signature,
        }
        try:
            resp = requests.post(f"{COINSPOT_BASE}{endpoint}",
                                data=payload_str, headers=headers, timeout=10)
            # v3.0.7: read body BEFORE raise_for_status. CoinSpot returns the
            # actual rejection reason in the JSON body even on 4xx, but
            # raise_for_status throws away the body. Log it so we can see why.
            body_snippet = (resp.text or "")[:400].replace("\n", " ")
            if resp.status_code >= 400:
                log.error(
                    f"CoinSpot HTTP {resp.status_code} on {endpoint}: "
                    f"body={body_snippet!r}"
                )
                return None
            result = resp.json()
            if result.get("status") != "ok":
                log.error(f"CoinSpot error on {endpoint}: {result}")
                return None
            return result
        except Exception as e:
            log.error(f"CoinSpot request failed on {endpoint}: {e}")
            return None

    def get_latest_price(self, coin: str) -> float:
        """Try multiple CoinSpot endpoints — they return different shapes for different coins."""
        sym = coin.upper()

        try:
            resp = requests.get(f"{COINSPOT_BASE}/pubapi/v2/latest/{sym}", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data.get("prices"), dict) and "last" in data["prices"]:
                    return float(data["prices"]["last"])
                if isinstance(data.get("prices"), (str, int, float)):
                    return float(data["prices"])
        except Exception as e:
            log.debug(f"v2/latest/{sym} failed: {e}")

        try:
            resp = requests.get(f"{COINSPOT_BASE}/pubapi/v2/latest", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                prices = data.get("prices", {})
                if isinstance(prices, dict):
                    entry = prices.get(sym) or prices.get(sym.lower())
                    if isinstance(entry, dict) and "last" in entry:
                        return float(entry["last"])
                    if isinstance(entry, (str, int, float)):
                        return float(entry)
        except Exception as e:
            log.debug(f"v2/latest list failed: {e}")

        try:
            resp = requests.get(f"{COINSPOT_BASE}/pubapi/latest", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                prices = data.get("prices", {})
                if isinstance(prices, dict):
                    entry = prices.get(sym.lower()) or prices.get(sym)
                    if isinstance(entry, dict) and "last" in entry:
                        return float(entry["last"])
        except Exception as e:
            log.debug(f"v1 fallback failed: {e}")

        try:
            from bot.brain import get_market_data
            md = get_market_data([sym])
            p = (md.get(sym) or {}).get("price")
            if p and float(p) > 0:
                return float(p)
        except Exception as e:
            log.debug(f"CoinGecko fallback failed: {e}")

        log.warning(f"Price unavailable for {sym} on CoinSpot — coin may not be tradeable")
        return 0.0

    def _available_balance(self, asset: str) -> float | None:
        """
        v3.1.0: Live-mode lookup of available balance for `asset`
        ('AUD' or any coin ticker like 'STX', 'BTC').

        Returns:
          - float >= 0 — the available balance from CoinSpot.
          - 0.0 — asset is not in the account at all (e.g. never traded).
          - None — the balances API call failed or returned an unparseable
            shape. Callers MUST treat None as 'cannot verify, abort trade'.

        Reuses _get_balances() so we inherit the list-or-dict response
        shape handling that's already proven in production.

        Prefers the 'available' field over 'balance' to exclude coins
        locked in open orders. With market-only orders this rarely
        matters, but it's the correct conservative choice.
        """
        if PAPER_MODE:
            return None
        balances = self._get_balances()
        if not balances:
            # _get_balances() returns {} on POST failure OR on a truly empty
            # response. In practice the live account always has at least an
            # AUD entry, so {} reliably means the API call failed.
            return None
        entry = balances.get(asset.upper())
        if not isinstance(entry, dict):
            # Asset is not in the balances response — never traded / not held.
            return 0.0
        for field in ("available", "balance"):
            val = entry.get(field)
            if val is None:
                continue
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
        return 0.0

    def buy(self, symbol: str, aud_amount: float,
            price_hint: float = 0.0) -> dict | None:
        """
        Place a market buy on CoinSpot for ~aud_amount AUD.

        v3.1.0: pre-buy AUD balance check against the exchange. If the
        live AUD balance is below the requested trade size, the buy is
        skipped (returns None). This avoids wasting Claude API spend on
        execute paths that will be rejected with HTTP 400 'Insufficient
        funds' anyway, and prevents misleading 'execution_failed' rows
        in the log.

        v3.0.8: amount and rate sent as strings (CoinSpot requirement).
        v3.0.6: price_hint as fallback when CoinSpot's own price endpoints
        return 0 for mid-cap coins missing from their degraded listings API.
        """
        coin = symbol.lower()
        price = self.get_latest_price(coin)

        if price == 0 and price_hint > 0:
            log.warning(
                f"{symbol}: CoinSpot public price endpoints returned 0, "
                f"using validated price_hint ${price_hint:.6f} from prices.py"
            )
            price = price_hint

        if PAPER_MODE:
            coin_amount = round(aud_amount / price, 8) if price > 0 else 0.0
            if price > 0:
                log.info(f"[PAPER] BUY {coin_amount} {symbol} (~${aud_amount:.2f} AUD) @ ${price:.4f}")
            else:
                log.info(f"[PAPER] BUY {symbol} — ${aud_amount:.2f} AUD (price TBD; snapshot loop will backfill)")
            return {
                "status": "ok", "paper_mode": True, "symbol": symbol,
                "aud_amount": aud_amount, "coin_amount": coin_amount, "price": price,
            }

        if price == 0:
            log.error(f"Cannot buy {symbol} live — CoinSpot price lookup failed "
                      f"and no price_hint provided")
            return None

        # v3.1.0: pre-buy AUD balance check. The bot's internal cash model
        # (starting_capital - deployed + closed_pnl) can drift from actual
        # CoinSpot AUD over time. Verify with the exchange before submitting.
        aud_balance = self._available_balance("AUD")
        if aud_balance is None:
            log.error(
                f"Cannot buy {symbol}: CoinSpot AUD balance check failed "
                f"(balances API call returned no data). Aborting rather than "
                f"submitting a blind order."
            )
            return None
        if aud_balance < aud_amount:
            log.info(
                f"Skipping {symbol} buy: CoinSpot AUD balance "
                f"${aud_balance:.2f} < requested ${aud_amount:.2f}. "
                f"Bot's internal cash model is ahead of actual exchange "
                f"balance — this skip is the intended safety behaviour, "
                f"not an execution failure."
            )
            return None

        coin_amount = round(aud_amount / price, 8)
        log.info(f"[LIVE] BUY {coin_amount} {symbol} (~${aud_amount:.2f} AUD) @ ${price:.4f}")
        # v3.0.9: CoinSpot V2 requires amounttype field. 'coin' means the
        # amount below is in the coin unit (BTC), not AUD. We omit `rate`
        # entirely — per CoinSpot docs, market orders fill at current rate
        # regardless of submitted rate, and including rate forces us to
        # also include threshold. Simpler to leave it out.
        result = self._post("/api/v2/my/buy/now", {
            "cointype": symbol.upper(),
            "amounttype": "coin",
            "amount": str(coin_amount),
            "markettype": "AUD",
        })
        # Echo coin_amount and price into the response so callers can store qty
        # at insert time without re-deriving it.
        if result is not None:
            result.setdefault("coin_amount", coin_amount)
            result.setdefault("price", price)
        return result

    def sell(self, symbol: str, coin_amount: float = None,
             aud_amount: float = None) -> dict | None:
        """
        Sell `coin_amount` of `symbol` on CoinSpot.

        v3.1.0: clamps the sell qty to min(requested, actual CoinSpot
        balance). Tiny rounding and fee deductions at buy time can leave
        the Supabase-tracked qty slightly larger than what CoinSpot
        actually holds, which causes 'Insufficient funds' (HTTP 400) on
        every trail-stop trigger. Clamping silently resolves the typical
        case (sub-1% shortfall) while still refusing the trade if
        CoinSpot reports zero balance (sign of a real accounting
        mismatch that needs human eyes).

        v3.0.1 SAFETY: in LIVE mode, coin_amount is required (either passed
        directly or derived from aud_amount). If neither is provided, the
        sell is REFUSED. Previously this method fell back to "sell whatever
        balance CoinSpot reports for this symbol" which would dump non-bot
        holdings sitting in the same account. The new v3.1.0 balance check
        clamps DOWN only — it never sells more than the caller asked for,
        so non-bot holdings remain protected.
        """
        coin = symbol.lower()

        if PAPER_MODE:
            price = self.get_latest_price(coin)
            log.info(f"[PAPER] SELL {symbol}{f' @ ${price:.4f}' if price > 0 else ' (price TBD)'}")
            return {
                "status": "ok", "paper_mode": True, "symbol": symbol,
                "coin_amount": coin_amount or 1.0, "price": price,
            }

        # ─── LIVE MODE BELOW ───────────────────────────────────────────────

        # Derive from aud_amount only if explicitly given.
        if coin_amount is None and aud_amount is not None and aud_amount > 0:
            price = self.get_latest_price(coin)
            if price > 0:
                coin_amount = round(aud_amount / price, 8)
            else:
                log.error(f"REFUSING live sell {symbol}: aud_amount given but "
                          f"price lookup failed — cannot derive qty safely")
                return None

        # Hard refuse: never fall through to "sell entire balance."
        if coin_amount is None or coin_amount <= 0:
            log.error(f"REFUSING live sell {symbol}: coin_amount not specified. "
                      f"The bot must pass the exact qty it bought. Refusing to "
                      f"fall back to full-balance sell to protect non-bot holdings.")
            return None

        # v3.1.0: clamp to actual exchange balance.
        actual_balance = self._available_balance(symbol)
        if actual_balance is None:
            log.error(
                f"REFUSING live sell {symbol}: CoinSpot balance check failed. "
                f"Will retry on next trail-stop trigger. (If this persists, "
                f"check CoinSpot API status and key permissions.)"
            )
            return None
        if actual_balance <= 0:
            log.error(
                f"REFUSING live sell {symbol}: CoinSpot reports 0 balance, "
                f"but Supabase says we hold {coin_amount} {symbol}. This is a "
                f"real accounting mismatch — manual reconciliation required, "
                f"not a fee-dust shortfall."
            )
            return None

        sell_qty = min(coin_amount, actual_balance)
        if sell_qty < coin_amount:
            shortfall_pct = (1 - sell_qty / coin_amount) * 100
            log.warning(
                f"{symbol}: clamping sell qty {coin_amount} -> {sell_qty} "
                f"(shortfall {shortfall_pct:.4f}%). Supabase qty exceeds "
                f"actual CoinSpot balance by a small amount — typical when "
                f"buy-side fees were deducted in coin rather than AUD."
            )

        log.info(f"[LIVE] SELL {sell_qty} {symbol}")
        result = self._post("/api/v2/my/sell/now", {
            "cointype": symbol.upper(),
            "amounttype": "coin",
            "amount": str(sell_qty),
            "markettype": "AUD",
        })
        # v3.1.0: echo the actual sell_qty (post-clamp) into the response so
        # the caller can write the true sold amount to Supabase rather than
        # the originally-requested coin_amount.
        if result is not None:
            result.setdefault("coin_amount", sell_qty)
            if sell_qty < coin_amount:
                result.setdefault("clamped_from", coin_amount)
        return result

    def _get_balances(self) -> dict:
        """Live-mode only. Normalises CoinSpot's list-or-dict response."""
        if PAPER_MODE:
            return {}
        result = self._post("/api/v2/ro/my/balances", {})
        if not result:
            return {}
        balances = result.get("balances", {})
        if isinstance(balances, list):
            flat = {}
            for entry in balances:
                if isinstance(entry, dict):
                    flat.update(entry)
            return flat
        return balances if isinstance(balances, dict) else {}

    def get_holdings(self) -> dict:
        if PAPER_MODE:
            return {}
        balances = self._get_balances()
        holdings = {}
        for coin, entry in balances.items():
            if not isinstance(entry, dict):
                continue
            bal = float(entry.get("balance", 0) or 0)
            if bal > 0:
                price = self.get_latest_price(coin)
                holdings[coin] = {
                    "amount": bal, "price_aud": price,
                    "value_aud": round(bal * price, 2),
                }
        return holdings
