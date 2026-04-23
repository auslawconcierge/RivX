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
# US market hours in AEST: 11:30pm - 6:00am
# Evening briefing: 9:00pm AEST
# Morning summary:  6:30am AEST
EVENING_BRIEFING_HOUR_AEST  = 20   # 8pm
MORNING_SUMMARY_HOUR_AEST   = 6    # 6am
US_MARKET_OPEN_HOUR_AEST    = 23   # 11pm (approx)
US_MARKET_CLOSE_HOUR_AEST   = 6    # 6am

# ─── Portfolio ────────────────────────────────────────────────────────────────
TOTAL_PORTFOLIO_AUD = 5000

PORTFOLIO = {
    # Conservative 50% = $2,500
    "SPY": {
        "tier":           "conservative",
        "allocated_aud":  1250,
        "market":         "alpaca",
        "type":           "etf",
        "stop_loss_pct":  0.07,
        "take_profit_pct": 0.05,
    },
    "QQQ": {
        "tier":           "conservative",
        "allocated_aud":  1250,
        "market":         "alpaca",
        "type":           "etf",
        "stop_loss_pct":  0.07,
        "take_profit_pct": 0.05,
    },
    # Moderate 30% = $1,500
    "NVDA": {
        "tier":           "moderate",
        "allocated_aud":  500,
        "market":         "alpaca",
        "type":           "stock",
        "stop_loss_pct":  0.07,
        "take_profit_pct": 0.08,
    },
    "TSLA": {
        "tier":           "moderate",
        "allocated_aud":  500,
        "market":         "alpaca",
        "type":           "stock",
        "stop_loss_pct":  0.07,
        "take_profit_pct": 0.08,
    },
    "META": {
        "tier":           "moderate",
        "allocated_aud":  500,
        "market":         "alpaca",
        "type":           "stock",
        "stop_loss_pct":  0.07,
        "take_profit_pct": 0.08,
    },
    # High risk 20% = $1,000
    "BTC": {
        "tier":           "high_risk",
        "allocated_aud":  700,
        "market":         "coinspot",
        "type":           "crypto",
        "stop_loss_pct":  0.10,
        "take_profit_pct": 0.12,
    },
    "ETH": {
        "tier":           "high_risk",
        "allocated_aud":  300,
        "market":         "coinspot",
        "type":           "crypto",
        "stop_loss_pct":  0.10,
        "take_profit_pct": 0.12,
    },
}

# ─── Trading rules ────────────────────────────────────────────────────────────
MIN_CONFIDENCE_TO_TRADE  = 0.55   # Claude must be >55% confident to act
INTRADAY_CHECK_INTERVAL  = 120    # seconds between intraday checks (2 mins)
CRYPTO_CHECK_INTERVAL    = 300    # seconds between crypto checks (5 mins)
APPROVAL_TIMEOUT_SECONDS = 3600   # 1 hour to approve evening briefing
AUD_USD_FALLBACK         = 0.635
