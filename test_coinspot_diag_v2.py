"""
test_coinspot_diag_v2.py — Verify v3.0.9 amounttype fix works.

NO MONEY MOVES. NO REAL ORDER. NO BUYING.

v3.0.9 added the 'amounttype' field that CoinSpot's V2 API actually
requires. Without it the API rejects with "Valid amount type required."
This test sends an invalid coin (XXXFAKE) WITH the amounttype field.

Expected outcomes:
  GOOD: body contains 'invalid coin' / 'cointype not found' / similar
        — meaning CoinSpot accepted the payload format and got far
        enough to validate the cointype. v3.0.9 fix works.

  BAD:  body contains 'Valid amount type required' — strings/amounttype
        STILL not what CoinSpot wants. Need different approach.

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
    print("v3.0.9 verification — sending invalid coin with amounttype='coin'")
    print("=" * 70)
    print()

    cs = CoinSpotTrader()

    print("Sending: cointype=XXXFAKE, amounttype='coin', amount='0.001'")
    print()

    result = cs._post("/api/v2/my/buy/now", {
        "cointype": "XXXFAKE",
        "amounttype": "coin",
        "amount": "0.001",
        "markettype": "AUD",
    })

    print()
    print("=" * 70)
    print("INTERPRETATION:")
    print("=" * 70)
    if result is None:
        print("_post returned None (expected for invalid cointype).")
        print()
        print("Check the 'CoinSpot HTTP 400 ... body=' line above:")
        print()
        print("  -> If body STILL says 'Valid amount type required':")
        print("     v3.0.9 didn't help. amounttype isn't enough.")
        print()
        print("  -> If body says 'invalid coin' / 'cointype' / 'unknown' etc:")
        print("     v3.0.9 WORKS. Real coin names will succeed.")
        print()
        print("  -> Anything else: new info, share it.")
    else:
        print("UNEXPECTED: got non-None result for fake coin:")
        print(result)


if __name__ == "__main__":
    main()
