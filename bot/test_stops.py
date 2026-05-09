"""
test_stops.py — verify v3.0 stop-loss branches fire correctly.

Imports strategy.py and calls each decide_exit_* function with synthetic
P&L values. Confirms:
  1. Stop loss fires when pnl <= threshold
  2. Stop loss does NOT fire one tick above threshold
  3. Trail does not interfere with stop loss
  4. The reason string is the expected stop-loss text

No DB writes. No network. Pure logic test. Safe to run anywhere.

Usage:  python test_stops.py
Expected: all 12 checks pass, exit code 0.
"""

import sys
from strategy import (
    decide_exit_swing_crypto,
    decide_exit_momentum,
    decide_exit_swing_stock,
    SWING_CRYPTO_STOP_PCT,
    MOMENTUM_STOP_PCT,
    SWING_STOCKS_STOP_PCT,
)


def check(label, condition, detail=""):
    """Print PASS/FAIL for one assertion."""
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}" + (f"  ({detail})" if detail else ""))
    return condition


def test_swing_crypto():
    print("\nSWING CRYPTO  (stop = -8%)")
    results = []

    # 1. Well below stop -> should exit, reason mentions stop loss
    d = decide_exit_swing_crypto(pnl_pct=-0.15, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop fires at -15%",
        d.should_exit and d.fraction == 1.0 and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    # 2. Exactly at stop -> should exit
    d = decide_exit_swing_crypto(pnl_pct=SWING_CRYPTO_STOP_PCT, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop fires exactly at -8%",
        d.should_exit and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    # 3. One tick above stop -> should NOT exit
    d = decide_exit_swing_crypto(pnl_pct=-0.0799, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop does NOT fire at -7.99%",
        not d.should_exit,
        d.reason,
    ))

    # 4. Below stop AND trail armed -> stop wins (it's checked first)
    d = decide_exit_swing_crypto(pnl_pct=-0.15, peak_pnl_pct=0.20, age_days=5.0)
    results.append(check(
        "stop fires even with trail armed",
        d.should_exit and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    return all(results)


def test_momentum():
    print("\nMOMENTUM CRYPTO  (stop = -10%)")
    results = []

    d = decide_exit_momentum(pnl_pct=-0.15, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop fires at -15%",
        d.should_exit and d.fraction == 1.0 and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    d = decide_exit_momentum(pnl_pct=MOMENTUM_STOP_PCT, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop fires exactly at -10%",
        d.should_exit and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    d = decide_exit_momentum(pnl_pct=-0.0999, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop does NOT fire at -9.99%",
        not d.should_exit,
        d.reason,
    ))

    d = decide_exit_momentum(pnl_pct=-0.20, peak_pnl_pct=0.25, age_days=2.0)
    results.append(check(
        "stop fires even with trail armed",
        d.should_exit and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    return all(results)


def test_swing_stock():
    print("\nSWING STOCKS  (stop = -5%)")
    results = []

    d = decide_exit_swing_stock(pnl_pct=-0.10, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop fires at -10%",
        d.should_exit and d.fraction == 1.0 and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    d = decide_exit_swing_stock(pnl_pct=SWING_STOCKS_STOP_PCT, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop fires exactly at -5%",
        d.should_exit and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    d = decide_exit_swing_stock(pnl_pct=-0.0499, peak_pnl_pct=0.0, age_days=1.0)
    results.append(check(
        "stop does NOT fire at -4.99%",
        not d.should_exit,
        d.reason,
    ))

    d = decide_exit_swing_stock(pnl_pct=-0.10, peak_pnl_pct=0.15, age_days=3.0)
    results.append(check(
        "stop fires even with trail armed",
        d.should_exit and "stop loss" in d.reason.lower(),
        d.reason,
    ))

    return all(results)


if __name__ == "__main__":
    print("=" * 60)
    print("RivX v3.0 stop-loss verification")
    print("=" * 60)

    all_passed = all([
        test_swing_crypto(),
        test_momentum(),
        test_swing_stock(),
    ])

    print("\n" + "=" * 60)
    if all_passed:
        print("ALL CHECKS PASSED — stop-loss logic verified")
        sys.exit(0)
    else:
        print("SOME CHECKS FAILED — do not go live until resolved")
        sys.exit(1)
