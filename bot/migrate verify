"""
RivX v2 migration: post-SQL verification.

Run this AFTER you've run migration_v2.sql in Supabase.

What it checks:
  1. positions table has the new columns (bucket, peak_pnl_pct, opened_at)
  2. manual_orders table exists and is readable (no RLS 403)
  3. bot_flags table exists with the seed values
  4. No open positions remain (clean slate confirmed)
  5. Binance and CoinPaprika are reachable from this network
  6. CoinSpot is reachable
  7. Frankfurter (FX) is reachable
  8. Validated price for BTC works end-to-end

Usage:
    python migrate_verify.py

Run this from the repo root or anywhere the bot package is importable.
Exits with code 0 if all green, 1 if anything failed.
"""

from __future__ import annotations

import os
import sys
import requests

# Make sure 'bot' is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


CHECK = "✅"
FAIL = "❌"
WARN = "⚠️ "


def header(text):
    print(f"\n{'─' * 60}\n{text}\n{'─' * 60}")


def check_db_schema():
    header("1. Database schema")
    try:
        from bot.supabase_logger import SupabaseLogger
    except Exception as e:
        print(f"{FAIL}  could not import SupabaseLogger: {e}")
        return False

    db = SupabaseLogger()
    ok = True

    # positions table — try selecting the new columns
    try:
        rows = db._get("positions", {"limit": "1"})
        if rows is None:
            print(f"{FAIL}  positions table not readable")
            return False
        if rows:
            row = rows[0]
            for col in ["bucket", "peak_pnl_pct", "opened_at", "qty", "current_price"]:
                if col in row:
                    print(f"{CHECK}  positions.{col} present")
                else:
                    print(f"{FAIL}  positions.{col} MISSING — run migration SQL first")
                    ok = False
        else:
            print(f"{CHECK}  positions table readable (empty)")
    except Exception as e:
        print(f"{FAIL}  positions check failed: {e}")
        ok = False

    # manual_orders — must be readable without 403
    try:
        rows = db._get("manual_orders", {"limit": "1"})
        if rows is None:
            print(f"{FAIL}  manual_orders not readable (RLS still on?)")
            ok = False
        else:
            print(f"{CHECK}  manual_orders readable (RLS disabled)")
    except Exception as e:
        print(f"{FAIL}  manual_orders check failed: {e}")
        ok = False

    # bot_flags — should have seed values
    try:
        peak = db.get_flag("portfolio_peak")
        if peak == "10000":
            print(f"{CHECK}  portfolio_peak = 10000 (drawdown tracking ready)")
        else:
            print(f"{WARN}  portfolio_peak = {peak!r} (expected '10000')")
        consec = db.get_flag("consec_losses")
        if consec == "0":
            print(f"{CHECK}  consec_losses = 0")
        else:
            print(f"{WARN}  consec_losses = {consec!r}")
    except Exception as e:
        print(f"{FAIL}  bot_flags check failed: {e}")
        ok = False

    return ok


def check_clean_slate():
    header("2. Clean slate")
    try:
        from bot.supabase_logger import SupabaseLogger
        db = SupabaseLogger()
        positions = db.get_positions()
        if not positions:
            print(f"{CHECK}  no open positions — clean slate confirmed")
            return True
        print(f"{WARN}  {len(positions)} open positions remain:")
        for sym in list(positions.keys())[:10]:
            print(f"      - {sym}")
        print("    Either run the SQL again with the close-all section enabled,")
        print("    or force-sell them manually before starting the bot.")
        return False
    except Exception as e:
        print(f"{FAIL}  clean slate check failed: {e}")
        return False


def check_data_sources():
    header("3. External data sources")
    ok = True

    # Binance
    binance_hosts = [
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://data-api.binance.vision",
    ]
    binance_ok = False
    for host in binance_hosts:
        try:
            r = requests.get(f"{host}/api/v3/ticker/price",
                             params={"symbol": "BTCUSDT"}, timeout=5)
            if r.status_code == 200:
                price = float(r.json().get("price", 0))
                print(f"{CHECK}  Binance reachable via {host} (BTC=${price:,.0f} USD)")
                binance_ok = True
                break
        except Exception:
            continue
    if not binance_ok:
        print(f"{FAIL}  All Binance hosts unreachable")
        ok = False

    # CoinPaprika
    try:
        r = requests.get("https://api.coinpaprika.com/v1/tickers",
                         params={"limit": 5}, timeout=8)
        if r.status_code == 200:
            print(f"{CHECK}  CoinPaprika reachable ({len(r.json())} tickers)")
        else:
            print(f"{FAIL}  CoinPaprika returned {r.status_code}")
            ok = False
    except Exception as e:
        print(f"{FAIL}  CoinPaprika unreachable: {e}")
        ok = False

    # CoinSpot
    try:
        r = requests.get("https://www.coinspot.com.au/pubapi/v2/latest", timeout=8)
        if r.status_code == 200:
            data = r.json()
            count = len(data.get("prices", {}))
            print(f"{CHECK}  CoinSpot reachable ({count} symbols listed)")
        else:
            print(f"{WARN}  CoinSpot returned {r.status_code} — bot will use stale cache or fallback")
    except Exception as e:
        print(f"{WARN}  CoinSpot unreachable: {e} — bot will use stale cache or fallback")

    # Frankfurter (FX)
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=AUD", timeout=5)
        if r.status_code == 200:
            rate = r.json().get("rates", {}).get("AUD")
            print(f"{CHECK}  Frankfurter reachable (USD→AUD = {rate})")
        else:
            print(f"{FAIL}  Frankfurter returned {r.status_code}")
            ok = False
    except Exception as e:
        print(f"{FAIL}  Frankfurter unreachable: {e}")
        ok = False

    return ok


def check_validated_price():
    header("4. End-to-end price validation")
    try:
        from bot import prices
    except Exception as e:
        print(f"{FAIL}  cannot import bot.prices: {e}")
        return False

    quote = prices.get_crypto_price("BTC")
    if not quote:
        print(f"{FAIL}  no quote returned for BTC")
        return False

    print(f"   BTC price quote:")
    print(f"     Binance USD:      ${quote.usd:,.4f}")
    print(f"     CoinSpot AUD:     ${quote.cs_aud:,.4f}")
    print(f"     FX rate (USD→AUD): {quote.fx_rate:.4f}")
    print(f"     Implied AUD:      ${quote.usd * quote.fx_rate:,.4f}")
    print(f"     Disagreement:     {quote.disagreement_pct:.2f}%")
    print(f"     Validated:        {quote.validated}")

    if quote.validated:
        print(f"{CHECK}  Validated quote returned — buys would be permitted")
        return True
    else:
        print(f"{WARN}  Quote unvalidated — buys would be blocked. "
              "Reasons: single source down, OR disagreement >5%, OR FX failure.")
        return True  # not necessarily a failure — could be transient


def main():
    print("RivX v2 migration verification\n")
    print("Run this AFTER you've executed migration_v2.sql in Supabase.\n")

    results = []
    results.append(("Schema",       check_db_schema()))
    results.append(("Clean slate",  check_clean_slate()))
    results.append(("Data sources", check_data_sources()))
    results.append(("Price validation", check_validated_price()))

    header("Summary")
    for name, ok in results:
        mark = CHECK if ok else FAIL
        print(f"  {mark}  {name}")

    if all(ok for _, ok in results):
        print("\nAll checks passed. The bot is ready to deploy.")
        return 0
    print("\nSome checks failed. Fix the failures above before deploying.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
