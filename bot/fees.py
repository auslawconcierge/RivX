# RIVX_VERSION: v1.0-fees-2026-05-09
"""
Fee model for paper-mode P&L.

Calibrated to match the dashboard's "Closed Positions" table to-the-cent.
Verified 2026-05-09 against JUP, ICP, AMD closes.

Used by:
  - supabase_logger.get_portfolio_value()  (headline totals + cash)
  - rich_summary.run_rich_daily_summary()  (Telegram daily summary)

In LIVE mode, real fees leave cash via the exchange API automatically;
these constants are for paper-mode estimation so the bot's view of cash
stays close to what it'll actually be once live.
"""

# ── Fee constants ────────────────────────────────────────────────────────

# CoinSpot Instant Buy/Sell — 1% per side
CRYPTO_BUY_FEE_PCT  = 0.0100
CRYPTO_SELL_FEE_PCT = 0.0100

# Alpaca paper — 0.5% buy / 0.55% sell (asymmetric matches dashboard).
# Real Alpaca commission is $0; the small model captures slippage and
# SEC/TAF fees that bite slightly harder on the sell side.
STOCK_BUY_FEE_PCT   = 0.0050
STOCK_SELL_FEE_PCT  = 0.0055


# ── Helpers ──────────────────────────────────────────────────────────────

def _is_stock(market: str | None) -> bool:
    return (market or "").lower() == "alpaca"


def buy_fee_pct(market: str | None) -> float:
    return STOCK_BUY_FEE_PCT if _is_stock(market) else CRYPTO_BUY_FEE_PCT


def sell_fee_pct(market: str | None) -> float:
    return STOCK_SELL_FEE_PCT if _is_stock(market) else CRYPTO_SELL_FEE_PCT


# ── Net P&L primitives ───────────────────────────────────────────────────

def realised_dollar_net(*, aud_amount: float, pnl_pct: float,
                        market: str | None) -> float:
    """
    Net realised dollars including BOTH buy and sell fees.

    For a CLOSED position: actual realised net P&L (matches dashboard).
    For an OPEN position:  estimated net P&L if liquidated right now.

    `pnl_pct` is the GROSS price-change ratio stored on the row:
        (exit_price - entry_price) / entry_price

    Math:
        total_in  = aud_amount * (1 + buy_fee_pct)
        gross_out = aud_amount * (1 + pnl_pct)
        net_out   = gross_out * (1 - sell_fee_pct)
        return    = net_out - total_in
    """
    bf = buy_fee_pct(market)
    sf = sell_fee_pct(market)
    total_in  = aud_amount * (1.0 + bf)
    gross_out = aud_amount * (1.0 + pnl_pct)
    net_out   = gross_out * (1.0 - sf)
    return net_out - total_in


def realised_pct_net(*, pnl_pct: float, market: str | None) -> float:
    """
    Net realised return as a ratio. Matches dashboard's REALISED % column.

        net = (1 + gross_pnl) * (1 - sell_fee) / (1 + buy_fee) - 1
    """
    bf = buy_fee_pct(market)
    sf = sell_fee_pct(market)
    return (1.0 + pnl_pct) * (1.0 - sf) / (1.0 + bf) - 1.0


def net_dollar_pct_for_position(position: dict) -> tuple[float, float]:
    """Returns (net_dollar, net_pct_as_percent) for a position row."""
    aud = float(position.get("aud_amount") or 0)
    pnl = float(position.get("pnl_pct") or 0)
    mkt = position.get("market")
    return (
        realised_dollar_net(aud_amount=aud, pnl_pct=pnl, market=mkt),
        realised_pct_net(pnl_pct=pnl, market=mkt) * 100.0,
    )


def buy_fee_paid(*, aud_amount: float, market: str | None) -> float:
    """Fee already paid on entry (out of cash)."""
    return aud_amount * buy_fee_pct(market)


def market_value_net_if_sold(*, aud_amount: float, pnl_pct: float,
                             market: str | None) -> float:
    """
    Liquidation value of an open position, net of estimated sell fee.
    Used by get_portfolio_value() to compute the "what's it worth right now"
    column.
    """
    sf = sell_fee_pct(market)
    return aud_amount * (1.0 + pnl_pct) * (1.0 - sf)
