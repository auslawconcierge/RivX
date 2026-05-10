"""
test_coinspot_diag_v2.py — Verify v3.0.8 string-amounts fix works.

NO MONEY MOVES. NO REAL ORDER. NO BUYING.

Same approach as test_coinspot_diag.py — invalid cointype so CoinSpot
must reject — but THIS time we send amount/rate as STRINGS like v3.0.8 does.

Expected outcomes:
  GOOD: body contains 'invalid coin' / 'cointype' / similar — meaning
        CoinSpot accepted the amount format but rejected the fake coin.
        This proves v3.0.8's string fix works.

  BAD:  body contains 'Valid amount type required' — meaning strings
        ALSO don't work. We'd need to investigate further.

USAGE in Render shell:
    python test_coinspot_diag_v2.py
"""
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from bot.config import PAPER_MODE
from bot.coinspot_trader import CoinSpotTrader


def main():
    if PAPER_MODE:
        print("PAPER_MODE is True — diagnostic only meaningful in LIVE.")
        sys.exit(0)

    print("=" * 70)
    print("v3.0.8 verification — sending invalid coin with STRING amount/rate")
    print("If string format works: CoinSpot will reject with 'invalid coin'")
    print("If string format doesn't work: 'Valid amount type required' (same as before)")
    print("NO real coins purchased.")
    print("=" * 70)
    print()

    cs = CoinSpotTrader()

    # Send strings just like v3.0.8 does in production.
    # Fake cointype ensures no real order can possibly land.
    print("Sending: cointype=XXXFAKE, amount='0.001', rate='1.0' (strings)")
    print()

    result = cs._post("/api/v2/my/buy/now", {
        "cointype": "XXXFAKE",
        "amount": "0.001",   # STRING — v3.0.8 fix
        "rate": "1.0",       # STRING — v3.0.8 fix
        "markettype": "AUD",
    })

    print()
    print("=" * 70)
    print("INTERPRETATION:")
    print("=" * 70)
    if result is None:
        print("_post returned None (expected for invalid cointype).")
        print()
        print("Now check the 'CoinSpot HTTP 400 ... body=' line above:")
        print()
        print("  -> If body says 'Valid amount type required' AGAIN:")
        print("     v3.0.8 string fix did NOT work. Need different approach.")
        print()
        print("  -> If body says anything ELSE (invalid coin, unknown coin,")
        print("     cointype not found, etc):")
        print("     v3.0.8 string fix WORKS. CoinSpot got past amount validation")
        print("     and is now rejecting because XXXFAKE isn't a real coin.")
        print("     Real buys with real coin names will succeed.")
    else:
        print("UNEXPECTED: got a non-None result for fake coin:")
        print(result)


if __name__ == "__main__":
    main()
