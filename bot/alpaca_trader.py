# RIVX_VERSION: v3.0.1-explicit-qty-2026-05-09
"""
alpaca_trader.py — Executes buy and sell orders on Alpaca.
Supports both paper trading and live trading (switched via PAPER_MODE in config).
Uses fractional shares so you can deploy exact AUD amounts regardless of share price.

v3.0.1 changes from initial build (2026-05-09):
  - sell() now requires either an explicit qty, OR explicit consent via
    close_full_position=True. Previously qty=None silently closed the entire
    Alpaca position via DELETE /v2/positions/{symbol}. That's fine when the
    Alpaca account is bot-only, but if a user ever buys a stock personally
    on the same account, it would be liquidated by the bot. Defense in depth.
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
        return self._get("/v2/account") or {}

    def get_position(self, symbol: str) -> dict | None:
        return self._get(f"/v2/positions/{symbol}")

    def get_all_positions(self) -> list:
        return self._get("/v2/positions") or []

    def buy(self, symbol: str, aud_amount: float) -> dict | None:
        rate         = get_aud_usd_rate()
        usd_notional = round(aud_amount * rate, 2)
        log.info(f"[{self.mode}] BUY {symbol} — ${aud_amount} AUD (~${usd_notional} USD) at rate {rate:.4f}")

        payload = {
            "symbol":        symbol,
            "notional":      str(usd_notional),
            "side":          "buy",
            "type":          "market",
            "time_in_force": "day",
        }
        order = self._post("/v2/orders", payload)
        if order:
            log.info(f"Order placed: {order.get('id')} — {order.get('status')}")
        return order

    def sell(self, symbol: str, qty: float = None,
             close_full_position: bool = False) -> dict | None:
        """
        Sell `qty` shares of `symbol`, OR close the full position if
        close_full_position=True.

        v3.0.1: qty=None alone is no longer enough to liquidate a position.
        The caller must EXPLICITLY pass close_full_position=True. This
        prevents accidental "close everything for this symbol" calls — which
        matters if any non-bot holdings ever sit in the same Alpaca account.

        Typical bot path: pass qty (the exact share count we bought).
        Manual force-close path (e.g. from a kill-switch handler): pass
        close_full_position=True.
        """
        if qty is not None and qty > 0:
            log.info(f"[{self.mode}] SELL {qty} shares of {symbol}")
            payload = {
                "symbol":        symbol,
                "qty":           str(qty),
                "side":          "sell",
                "type":          "market",
                "time_in_force": "day",
            }
            return self._post("/v2/orders", payload)

        if close_full_position:
            log.info(f"[{self.mode}] CLOSE FULL POSITION {symbol} (explicit consent)")
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

        log.error(f"REFUSING sell {symbol}: no qty given and "
                  f"close_full_position not set. Pass exact qty or set "
                  f"close_full_position=True for an explicit full liquidation.")
        return None

    def get_portfolio_value_usd(self) -> float:
        acct = self.get_account()
        return float(acct.get("equity", 0))
