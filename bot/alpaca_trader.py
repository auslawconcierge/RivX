"""
alpaca_trader.py — Executes buy and sell orders on Alpaca.
Supports both paper trading and live trading (switched via PAPER_MODE in config).
Uses fractional shares so you can deploy exact AUD amounts regardless of share price.
"""

import logging
import requests
from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, PAPER_MODE

log = logging.getLogger(__name__)

HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    "Content-Type":        "application/json",
}


def get_aud_usd_rate() -> float:
    """Fetch live AUD/USD rate from a free FX API."""
    try:
        resp = requests.get(
            "https://api.frankfurter.app/latest?from=AUD&to=USD",
            timeout=5
        )
        resp.raise_for_status()
        return resp.json()["rates"]["USD"]
    except Exception as e:
        log.warning(f"FX fetch failed ({e}), using fallback 0.635")
        return 0.635


class AlpacaTrader:

    def __init__(self):
        self.base = ALPACA_BASE_URL
        self.mode = "PAPER" if PAPER_MODE else "LIVE"
        log.info(f"AlpacaTrader initialised — {self.mode} mode")

    def _post(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{self.base}{endpoint}"
        try:
            resp = requests.post(url, json=payload, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            log.error(f"Alpaca HTTP error {e}: {resp.text}")
            return None
        except Exception as e:
            log.error(f"Alpaca request failed: {e}")
            return None

    def _get(self, endpoint: str) -> dict | None:
        url = f"{self.base}{endpoint}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.error(f"Alpaca GET failed: {e}")
            return None

    def get_account(self) -> dict:
        """Returns account info including buying power."""
        return self._get("/v2/account") or {}

    def get_position(self, symbol: str) -> dict | None:
        """Returns current position for a symbol, or None if not held."""
        return self._get(f"/v2/positions/{symbol}")

    def get_all_positions(self) -> list:
        """Returns all open positions."""
        return self._get("/v2/positions") or []

    def buy(self, symbol: str, aud_amount: float) -> dict | None:
        """
        Place a notional buy order for `aud_amount` AUD worth of `symbol`.
        Converts to USD using live FX rate.
        Uses fractional shares — no need to worry about share price.
        """
        rate      = get_aud_usd_rate()
        usd_notional = round(aud_amount * rate, 2)
        log.info(f"[{self.mode}] BUY {symbol} — ${aud_amount} AUD (~${usd_notional} USD) at rate {rate:.4f}")

        payload = {
            "symbol":        symbol,
            "notional":      str(usd_notional),   # dollar amount, not share count
            "side":          "buy",
            "type":          "market",
            "time_in_force": "day",
        }
        order = self._post("/v2/orders", payload)
        if order:
            log.info(f"Order placed: {order.get('id')} — {order.get('status')}")
        return order

    def sell(self, symbol: str, qty: float = None) -> dict | None:
        """
        Sell entire position in `symbol`, or a specific `qty` of shares.
        If qty is None, closes the full position.
        """
        if qty is None:
            # Close full position
            log.info(f"[{self.mode}] SELL ALL {symbol}")
            try:
                resp = requests.delete(
                    f"{self.base}/v2/positions/{symbol}",
                    headers=HEADERS,
                    timeout=10
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                log.error(f"Failed to close position {symbol}: {e}")
                return None
        else:
            log.info(f"[{self.mode}] SELL {qty} shares of {symbol}")
            payload = {
                "symbol":        symbol,
                "qty":           str(qty),
                "side":          "sell",
                "type":          "market",
                "time_in_force": "day",
            }
            return self._post("/v2/orders", payload)

    def get_portfolio_value_usd(self) -> float:
        """Returns total portfolio equity in USD."""
        acct = self.get_account()
        return float(acct.get("equity", 0))
