"""
test_buy_path.py — Verify v3.0.6 price_hint fallback works.

Run from Render shell:
    cd /opt/render/project/src
    python test_buy_path.py JASMY

What it does:
  1. Calls prices.get_crypto_price(SYMBOL) — same as a real buy cycle
  2. Confirms quote.validated == True
  3. Calls coinspot.get_latest_price(SYMBOL) — the broken-on-CoinSpot path
  4. Calls coinspot.buy(SYMBOL, $1, price_hint=quote.aud) WITH PAPER_MODE FORCED ON
     so it doesn't place a real order. We don't care about the order — we
     care that the function doesn't return None due to price=0.
  5. Reports each step PASS/FAIL.

Exit codes: 0 = all good, 1 = something broke.
"""
import os
import sys

# ── CRITICAL: force paper mode for this test, even if env says LIVE ──────
# This MUST happen before importing anything from bot/.
os.environ["PAPER_MODE"] = "true"

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from bot import prices
from bot.coinspot_trader import CoinSpotTrader
from bot.config import PAPER_MODE

if not PAPER_MODE:
    print("FATAL: PAPER_MODE override failed. Aborting to avoid live order.")
    sys.exit(1)


def test_symbol(sym: str) -> bool:
    print(f"\n{'='*60}\nTesting buy path for {sym}\n{'='*60}")

    # ── Step 1: prices.py — same as buy cycle does
    print(f"\n[1/4] prices.get_crypto_price({sym!r}) ...")
    quote = prices.get_crypto_price(sym)
    if not quote:
        print(f"  FAIL: no quote returned. Bot would log 'no price quote available'.")
        return False
    print(f"  quote.aud      = ${quote.aud:.6f}")
    print(f"  quote.usd      = ${quote.usd:.6f}")
    print(f"  quote.cs_aud   = ${quote.cs_aud:.6f}")
    print(f"  quote.source   = {quote.source}")
    print(f"  quote.validated= {quote.validated}")
    print(f"  quote.disagreement_pct = {quote.disagreement_pct}%")
    if not quote.validated:
        print(f"  FAIL: quote not validated. Bot would refuse to buy.")
        return False
    print(f"  PASS")

    # ── Step 2: confirm CoinSpot's own price lookup is broken for this sym
    print(f"\n[2/4] coinspot.get_latest_price({sym.lower()!r}) ...")
    cs = CoinSpotTrader()
    cs_price = cs.get_latest_price(sym.lower())
    print(f"  CoinSpot direct price = ${cs_price:.6f}")
    if cs_price > 0:
        print(f"  NOTE: CoinSpot returned a price for {sym}. v3.0.6 fallback "
              f"won't trigger today — buy will use this price directly.")
    else:
        print(f"  CoinSpot returned 0. v3.0.6 fallback path WILL trigger.")

    # ── Step 3: simulated buy with price_hint
    print(f"\n[3/4] coinspot.buy({sym!r}, aud_amount=1.0, price_hint=${quote.aud:.6f}) "
          f"[PAPER_MODE forced ON, no real order] ...")
    res = cs.buy(sym, 1.0, price_hint=quote.aud)
    if res is None:
        print(f"  FAIL: buy returned None even with price_hint. v3.0.6 fix not working.")
        return False
    print(f"  result.status      = {res.get('status')}")
    print(f"  result.price       = ${float(res.get('price') or 0):.6f}")
    print(f"  result.coin_amount = {res.get('coin_amount')}")
    print(f"  result.paper_mode  = {res.get('paper_mode')}")

    if not res.get("paper_mode"):
        print(f"  FAIL: result not paper-mode. PAPER_MODE override leaked. ABORT.")
        return False

    used_price = float(res.get("price") or 0)
    if used_price <= 0:
        print(f"  FAIL: buy succeeded but price=0. Position would be saved with bad data.")
        return False

    if cs_price <= 0 and used_price > 0:
        print(f"  PASS: CoinSpot had no price, but buy used price_hint. v3.0.6 working.")
    elif cs_price > 0:
        print(f"  PASS: buy used CoinSpot's own price (hint not needed today).")

    # ── Step 4: belt-and-braces — also verify buy() without hint returns None
    # for symbols CoinSpot can't price, proving the hint is what saves it.
    if cs_price <= 0:
        print(f"\n[4/4] Verifying buy without price_hint correctly fails for {sym} ...")
        res_nohint = cs.buy(sym, 1.0)  # no price_hint
        if res_nohint and float(res_nohint.get("price") or 0) > 0:
            print(f"  WEIRD: buy without hint also worked. CoinSpot may have "
                  f"resolved the price between calls. Not a failure.")
        else:
            # Paper mode returns a dict even with price=0 (it logs "price TBD")
            # so we check for price > 0 in the result.
            no_price = (res_nohint is None) or (float(res_nohint.get("price") or 0) <= 0)
            if no_price:
                print(f"  PASS: without hint, buy gets no usable price (live mode "
                      f"would refuse). Hint is doing real work.")
            else:
                print(f"  PASS: see above.")
    else:
        print(f"\n[4/4] Skipped — CoinSpot priced {sym} natively, no hint needed today.")

    print(f"\n{sym}: ALL CHECKS PASSED\n")
    return True


if __name__ == "__main__":
    targets = sys.argv[1:] or ["JASMY", "BTC", "JUP"]
    print(f"Testing v3.0.6 buy path for: {targets}")
    print(f"PAPER_MODE = {PAPER_MODE}  (must be True)")

    ok_all = True
    for sym in targets:
        try:
            ok = test_symbol(sym)
            if not ok:
                ok_all = False
        except Exception as e:
            import traceback
            print(f"\n{sym}: CRASHED")
            traceback.print_exc()
            ok_all = False

    print(f"\n{'='*60}")
    print("OVERALL: " + ("ALL PASS — v3.0.6 ready" if ok_all else "FAILURES — do not trust live buys"))
    print(f"{'='*60}")
    sys.exit(0 if ok_all else 1)
