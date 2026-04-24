"""
coinspot_trader.py — Executes crypto trades via CoinSpot API.
Paper mode never calls authenticated endpoints.
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
        try:
            resp = requests.get(f"{COINSPOT_BASE}/pubapi/v2/latest/{coin.upper()}", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            return float(data["prices"]["last"])
        except Exception as e:
            log.error(f"Price fetch failed for {coin}: {e}")
            return 0.0

    def buy(self, symbol: str, aud_amount: float) -> dict | None:
        coin = symbol.lower()
        price = self.get_latest_price(coin)
        if price == 0:
            log.error(f"Cannot buy {symbol} — price unavailable")
            return None

        coin_amount = round(aud_amount / price, 8)
        log.info(f"[{self.mode}] BUY {coin_amount} {symbol} (~${aud_amount:.2f} AUD) @ ${price:.4f}")

        if PAPER_MODE:
            return {
                "status": "ok", "paper_mode": True, "symbol": symbol,
                "aud_amount": aud_amount, "coin_amount": coin_amount, "price": price,
            }

        return self._post("/api/v2/my/buy/now", {
            "cointype": symbol.upper(),
            "amount": coin_amount,
            "rate": price,
            "markettype": "AUD",
        })

    def sell(self, symbol: str, coin_amount: float = None, aud_amount: float = None) -> dict | None:
        coin = symbol.lower()

        # Paper mode: simulate without touching any live API
        if PAPER_MODE:
            price = self.get_latest_price(coin)
            log.info(f"[PAPER] SELL {symbol} @ ${price:.4f}")
            return {
                "status": "ok", "paper_mode": True, "symbol": symbol,
                "coin_amount": coin_amount or 1.0, "price": price,
            }

        # Live mode
        if coin_amount is None and aud_amount is not None:
            price = self.get_latest_price(coin)
            coin_amount = round(aud_amount / price, 8) if price > 0 else None

        if coin_amount is None:
            balances = self._get_balances()
            entry = balances.get(symbol.upper(), {})
            if isinstance(entry, dict):
                coin_amount = float(entry.get("balance", 0) or 0)

        if not coin_amount or coin_amount == 0:
            log.warning(f"No {symbol} balance to sell")
            return None

        log.info(f"[LIVE] SELL {coin_amount} {symbol}")
        return self._post("/api/v2/my/sell/now", {
            "cointype": symbol.upper(),
            "amount": coin_amount,
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
