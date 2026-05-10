# RIVX_VERSION: v3.0.8-string-amounts-2026-05-10
"""
coinspot_trader.py — Executes crypto trades via CoinSpot API.
Paper mode never calls authenticated endpoints.

v3.0.1 changes from v2.x (2026-05-09):
  - LIVE sell now REFUSES if coin_amount is None. Previously fell through
    to "fetch full balance and sell that," which would dump non-bot
    holdings of the same coin in the same CoinSpot account. The bot must
    pass the exact qty it bought.
  - Removed the silent fallback to _get_balances() in the live sell path.
    A missing qty is now a hard error, not a "be helpful" guess.
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
            resp.raise_for_status()
            result = resp.json()
            if result.get("status") != "ok":
                log.error(f"CoinSpot error: {result}")
                return None
            return result
        except Exception as e:
            log.error(f"CoinSpot request failed: {e}")
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

    def buy(self, symbol: str, aud_amount: float,
            price_hint: float = 0.0) -> dict | None:
        """
        Place a market buy on CoinSpot for ~aud_amount AUD.

        v3.0.6: price_hint is an optional pre-validated AUD price coming
        from prices.py (which has Binance + CoinPaprika + multiple CoinSpot
        endpoints as fallback chain). If CoinSpot's own price endpoints
        return nothing (their public listings API has been degraded for
        weeks, missing many tradeable mid-cap coins), we accept the hint
        rather than refusing the trade. CoinSpot still fills at THEIR rate
        — `rate` is just a max-acceptable price. So the validated hint
        is safe to use as a ceiling.
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

        coin_amount = round(aud_amount / price, 8)
        log.info(f"[LIVE] BUY {coin_amount} {symbol} (~${aud_amount:.2f} AUD) @ ${price:.4f}")
        result = self._post("/api/v2/my/buy/now", {
            "cointype": symbol.upper(),
            "amount": str(coin_amount),
            "rate": str(price),
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

        v3.0.1 SAFETY: in LIVE mode, coin_amount is required (either passed
        directly or derived from aud_amount). If neither is provided, the
        sell is REFUSED. Previously this method fell back to "sell whatever
        balance CoinSpot reports for this symbol" which would dump non-bot
        holdings sitting in the same account.
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

        log.info(f"[LIVE] SELL {coin_amount} {symbol}")
        return self._post("/api/v2/my/sell/now", {
            "cointype": symbol.upper(),
            "amount": str(coin_amount),
            "markettype": "AUD",
        })

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
