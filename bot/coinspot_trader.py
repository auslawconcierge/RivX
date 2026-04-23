"""
coinspot_trader.py — Executes BTC and ETH trades via CoinSpot API.
CoinSpot is Australian so trades are natively in AUD — no FX conversion needed.
Uses HMAC-SHA512 signature authentication as required by CoinSpot.
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
        """Generate HMAC-SHA512 signature for CoinSpot authentication."""
        payload_str = json.dumps(payload, separators=(",", ":"))
        signature   = hmac.new(
            COINSPOT_SECRET_KEY.encode("utf-8"),
            payload_str.encode("utf-8"),
            hashlib.sha512
        ).hexdigest()
        return payload_str, signature

    def _post(self, endpoint: str, data: dict) -> dict | None:
        """Make an authenticated POST request to CoinSpot API."""
        data["nonce"] = int(time.time() * 1000)
        payload_str, signature = self._sign(data)
        headers = {
            "Content-Type": "application/json",
            "key":           COINSPOT_API_KEY,
            "sign":          signature,
        }
        url = f"{COINSPOT_BASE}{endpoint}"
        try:
            resp = requests.post(url, data=payload_str, headers=headers, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            if result.get("status") != "ok":
                log.error(f"CoinSpot error: {result}")
                return None
            return result
        except requests.HTTPError as e:
            log.error(f"CoinSpot HTTP error {e}: {resp.text}")
            return None
        except Exception as e:
            log.error(f"CoinSpot request failed: {e}")
            return None

    def get_balance(self) -> dict:
        """Returns your CoinSpot AUD and crypto balances."""
        result = self._post("/api/v2/ro/my/balances", {})
        return result.get("balances", {}) if result else {}

    def get_latest_price(self, coin: str) -> float:
        """
        Get latest buy price for a coin in AUD.
        coin: 'btc' | 'eth'
        """
        try:
            resp = requests.get(
                f"{COINSPOT_BASE}/pubapi/v2/latest/{coin.upper()}",
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data["prices"]["last"])
        except Exception as e:
            log.error(f"Failed to get {coin} price: {e}")
            return 0.0

    def buy(self, symbol: str, aud_amount: float) -> dict | None:
        """
        Buy `aud_amount` AUD worth of `symbol` (BTC or ETH).
        CoinSpot requires the coin amount, so we calculate it from the AUD amount.
        """
        coin  = symbol.lower()
        price = self.get_latest_price(coin)
        if price == 0:
            log.error(f"Cannot buy {symbol} — price fetch failed")
            return None

        coin_amount = round(aud_amount / price, 8)
        log.info(f"[{self.mode}] BUY {coin_amount} {symbol} (~${aud_amount:.2f} AUD) at ${price:.2f}")

        if PAPER_MODE:
            # Paper mode: simulate order, don't call API
            return {
                "status":      "ok",
                "paper_mode":  True,
                "symbol":      symbol,
                "aud_amount":  aud_amount,
                "coin_amount": coin_amount,
                "price":       price,
            }

        return self._post("/api/v2/my/buy/now", {
            "cointype": symbol.upper(),
            "amount":   coin_amount,
            "rate":     price,
            "markettype": "AUD",
        })

    def sell(self, symbol: str, coin_amount: float = None, aud_amount: float = None) -> dict | None:
        """
        Sell crypto. Provide either coin_amount or aud_amount — not both.
        If neither provided, attempts to sell full balance.
        """
        coin = symbol.lower()

        if coin_amount is None and aud_amount is not None:
            price       = self.get_latest_price(coin)
            coin_amount = round(aud_amount / price, 8) if price > 0 else None

        if coin_amount is None:
            # Get full balance
            balances    = self.get_balance()
            coin_amount = float(balances.get(symbol.upper(), {}).get("balance", 0))

        if coin_amount == 0:
            log.warning(f"No {symbol} balance to sell")
            return None

        log.info(f"[{self.mode}] SELL {coin_amount} {symbol}")

        if PAPER_MODE:
            price = self.get_latest_price(coin)
            return {
                "status":      "ok",
                "paper_mode":  True,
                "symbol":      symbol,
                "coin_amount": coin_amount,
                "price":       price,
                "aud_value":   round(coin_amount * price, 2),
            }

        return self._post("/api/v2/my/sell/now", {
            "cointype": symbol.upper(),
            "amount":   coin_amount,
            "markettype": "AUD",
        })

    def get_holdings(self) -> dict:
        """
        Returns current BTC/ETH holdings and their AUD value.
        Used to calculate unrealised P&L.
        """
        balances = self.get_balance()
        holdings = {}
        for coin in ["BTC", "ETH"]:
            bal = float(balances.get(coin, {}).get("balance", 0))
            if bal > 0:
                price = self.get_latest_price(coin)
                holdings[coin] = {
                    "amount":    bal,
                    "price_aud": price,
                    "value_aud": round(bal * price, 2),
                }
        return holdings
