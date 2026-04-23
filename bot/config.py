"""
RivX AutoTrader — config.py
All settings, portfolio allocation, and environment variable loading.
"""

import os

# ─── Mode ─────────────────────────────────────────────────────────────────────
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

# ─── API Keys ─────────────────────────────────────────────────────────────────
ALPACA_API_KEY      = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY   = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL     = (
    "https://paper-api.alpaca.markets" if PAPER_MODE
    else "https://api.alpaca.markets"
)
ALPACA_DATA_URL     = "https://data.alpaca.markets"

COINSPOT_API_KEY    = os.getenv("COINSPOT_API_KEY")
COINSPOT_SECRET_KEY = os.getenv("COINSPOT_SECRET_KEY")

SUPABASE_URL        = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY    = os.getenv("SUPABASE_API_KEY")

TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID")

ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY")

# ─── Timing (AEST = UTC+10) ───────────────────────────────────────────────────
EVENING_BRIEFING_HOUR_AEST  = 20   # 8pm AEST
MORNING_SUMMARY_HOUR_AEST   = 6    # 6:30am AEST
US_MARKET_OPEN_HOUR_AEST    = 23   # 11:30pm AEST
US_MARKET_CLOSE_HOUR_AEST   = 6    # 6am AEST

# ─── Portfolio ────────────────────────────────────────────────────────────────
# Scanner picks the actual assets each night — these are just allocation rules
# Claude is not limited to these symbols — it scans 50+ stocks and 25+ crypto
TOTAL_PORTFOLIO_AUD     = 5000
MAX_STOCK_PORTFOLIO     = 2000   # 40% stocks
MAX_CRYPTO_PORTFOLIO    = 2000   # 40% crypto
CASH_BUFFER             = 1000   # 20% cash buffer for opportunities
MAX_POSITION_SIZE       = 500    # max $500 per single position
MAX_CRYPTO_POSITIONS    = 5      # max 5 crypto positions open at once
MAX_STOCK_POSITIONS     = 5      # max 5 stock positions open at once

# Default stop/take-profit by asset type
STOP_LOSS = {
    "etf":    0.07,
    "stock":  0.07,
    "crypto": 0.10,
}
TAKE_PROFIT = {
    "etf":    0.05,
    "stock":  0.08,
    "crypto": 0.12,
}

# Legacy PORTFOLIO dict — kept for stop-loss checks on existing positions
# New trades are sized dynamically by Claude using the rules above
PORTFOLIO = {
    "BTC": {"tier": "high_risk", "allocated_aud": 500, "market": "coinspot",
            "type": "crypto", "stop_loss_pct": 0.10, "take_profit_pct": 0.12},
    "ETH": {"tier": "high_risk", "allocated_aud": 500, "market": "coinspot",
            "type": "crypto", "stop_loss_pct": 0.10, "take_profit_pct": 0.12},
    "SOL": {"tier": "high_risk", "allocated_aud": 400, "market": "coinspot",
            "type": "crypto", "stop_loss_pct": 0.10, "take_profit_pct": 0.12},
    "XRP": {"tier": "high_risk", "allocated_aud": 400, "market": "coinspot",
            "type": "crypto", "stop_loss_pct": 0.10, "take_profit_pct": 0.12},
    "SPY": {"tier": "conservative", "allocated_aud": 500, "market": "alpaca",
            "type": "etf", "stop_loss_pct": 0.07, "take_profit_pct": 0.05},
    "QQQ": {"tier": "conservative", "allocated_aud": 500, "market": "alpaca",
            "type": "etf", "stop_loss_pct": 0.07, "take_profit_pct": 0.05},
}

# ─── Trading rules ────────────────────────────────────────────────────────────
MIN_CONFIDENCE_TO_TRADE  = 0.60   # 60% confidence minimum
INTRADAY_CHECK_INTERVAL  = 120    # 2 mins between intraday checks
CRYPTO_CHECK_INTERVAL    = 300    # 5 mins between crypto checks
APPROVAL_TIMEOUT_SECONDS = 3600   # 1 hour to approve evening briefing
AUD_USD_FALLBACK         = 0.635
