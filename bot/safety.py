"""
Tests for bot/strategy.py — the trading rules.

Critical behaviours these tests pin down:
  1. Don't buy already-pumped coins (yesterday's biggest mistake)
  2. Don't sell on sideways action (no 4hr time-exit anymore)
  3. Use wider stops than yesterday (-8% / -10% not -2.5%)
  4. Respect ops floor (don't deploy below $500 cash)
  5. Slot accounting matches the agreed split ($4K/$2K/$3.5K, 5/4/3 slots)
"""

import sys
import unittest

sys.path.insert(0, '/home/claude/build')
import strategy
from strategy import Bucket


# ─── Allocation invariants ─────────────────────────────────────────────────

class TestAllocation(unittest.TestCase):
    """The numbers we agreed on must match the constants."""

    def test_total_budget_is_10k_minus_floor(self):
        deployable = (strategy.SWING_CRYPTO_BUDGET
                      + strategy.MOMENTUM_CRYPTO_BUDGET
                      + strategy.SWING_STOCKS_BUDGET)
        self.assertEqual(deployable, 9_500.0)
        self.assertEqual(strategy.OPS_FLOOR_AUD, 500.0)
        self.assertEqual(strategy.STARTING_CAPITAL_AUD, 10_000.0)

    def test_slot_counts(self):
        self.assertEqual(strategy.SWING_CRYPTO_SLOTS, 5)
        self.assertEqual(strategy.MOMENTUM_CRYPTO_SLOTS, 4)
        self.assertEqual(strategy.SWING_STOCKS_SLOTS, 3)

    def test_position_sizes(self):
        self.assertEqual(strategy.SWING_CRYPTO_SIZE, 800.0)
        self.assertEqual(strategy.MOMENTUM_CRYPTO_SIZE, 500.0)
        self.assertAlmostEqual(strategy.SWING_STOCKS_SIZE, 1166.67, places=1)

    def test_position_size_lookup(self):
        self.assertEqual(strategy.position_size_for(Bucket.SWING_CRYPTO), 800.0)
        self.assertEqual(strategy.position_size_for(Bucket.MOMENTUM_CRYPTO), 500.0)
        self.assertAlmostEqual(strategy.position_size_for(Bucket.SWING_STOCK), 1166.67, places=1)
        self.assertEqual(strategy.position_size_for("nonsense"), 0.0)

    def test_slots_available(self):
        self.assertEqual(strategy.slots_available(Bucket.SWING_CRYPTO, 0), 5)
        self.assertEqual(strategy.slots_available(Bucket.SWING_CRYPTO, 3), 2)
        self.assertEqual(strategy.slots_available(Bucket.SWING_CRYPTO, 5), 0)
        self.assertEqual(strategy.slots_available(Bucket.SWING_CRYPTO, 7), 0)  # negative clamped


# ─── Swing crypto entry rules ──────────────────────────────────────────────

class TestSwingCryptoEntry(unittest.TestCase):

    def test_quality_pullback_in_uptrend_qualifies(self):
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=-0.08,
            above_50d_ma=True,
        )
        self.assertTrue(ok, reason)
        self.assertIn("top-10", reason)

    def test_already_pumped_does_not_qualify(self):
        """The yesterday-mistake test: don't buy something only -2% off its high."""
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=-0.02,
            above_50d_ma=True,
        )
        self.assertFalse(ok)
        self.assertIn("only", reason)

    def test_at_high_does_not_qualify(self):
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=0.0,
            above_50d_ma=True,
        )
        self.assertFalse(ok)

    def test_too_deep_pullback_rejected(self):
        """20% off recent high = possible breakdown, not a pullback."""
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=-0.20,
            above_50d_ma=True,
        )
        self.assertFalse(ok)
        self.assertIn("too deep", reason)

    def test_below_50d_ma_rejected(self):
        """Right pullback size, but uptrend is broken — skip."""
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=-0.08,
            above_50d_ma=False,
        )
        self.assertFalse(ok)
        self.assertIn("uptrend", reason)

    def test_micro_cap_rejected(self):
        """No $50M shitcoins in swing bucket — that's momentum's job."""
        ok, reason = strategy.qualifies_swing_crypto(
            market_cap_rank=150,
            pullback_from_7d_high_pct=-0.08,
            above_50d_ma=True,
        )
        self.assertFalse(ok)

    def test_unknown_rank_rejected(self):
        ok, _ = strategy.qualifies_swing_crypto(
            market_cap_rank=None,
            pullback_from_7d_high_pct=-0.08,
            above_50d_ma=True,
        )
        self.assertFalse(ok)

    def test_boundary_5pct_pullback_rejected(self):
        """Exactly -5% is borderline — we want a real pullback, so reject."""
        ok, _ = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=-0.05,
            above_50d_ma=True,
        )
        self.assertFalse(ok)

    def test_boundary_15pct_pullback_qualifies(self):
        """At -15% we're still in the window."""
        ok, _ = strategy.qualifies_swing_crypto(
            market_cap_rank=10,
            pullback_from_7d_high_pct=-0.15,
            above_50d_ma=True,
        )
        self.assertTrue(ok)


# ─── Momentum crypto entry rules ───────────────────────────────────────────

class TestMomentumCryptoEntry(unittest.TestCase):

    def test_breakout_with_volume_qualifies(self):
        ok, reason = strategy.qualifies_momentum_crypto(
            market_cap_rank=80,
            broke_7d_high_today=True,
            volume_vs_7d_avg_ratio=2.5,
        )
        self.assertTrue(ok, reason)

    def test_no_breakout_rejected(self):
        """Already up 15% but no NEW breakout — that's chasing, skip."""
        ok, _ = strategy.qualifies_momentum_crypto(
            market_cap_rank=80,
            broke_7d_high_today=False,
            volume_vs_7d_avg_ratio=3.0,
        )
        self.assertFalse(ok)

    def test_low_volume_breakout_rejected(self):
        """Breakout on low volume = not real interest, skip."""
        ok, _ = strategy.qualifies_momentum_crypto(
            market_cap_rank=80,
            broke_7d_high_today=True,
            volume_vs_7d_avg_ratio=1.2,
        )
        self.assertFalse(ok)

    def test_top_30_rejected(self):
        """BTC breakout doesn't go in momentum bucket — that's swing."""
        ok, reason = strategy.qualifies_momentum_crypto(
            market_cap_rank=5,
            broke_7d_high_today=True,
            volume_vs_7d_avg_ratio=3.0,
        )
        self.assertFalse(ok)
        self.assertIn("too big", reason)

    def test_obscure_rejected(self):
        """Rank 500 = barely traded, too risky."""
        ok, reason = strategy.qualifies_momentum_crypto(
            market_cap_rank=500,
            broke_7d_high_today=True,
            volume_vs_7d_avg_ratio=3.0,
        )
        self.assertFalse(ok)
        self.assertIn("obscure", reason)


# ─── Stock entry rules ─────────────────────────────────────────────────────

class TestSwingStockEntry(unittest.TestCase):

    def test_quality_pullback_qualifies(self):
        ok, _ = strategy.qualifies_swing_stock(
            is_quality=True,
            pullback_from_7d_high_pct=-0.05,
            above_50d_ma=True,
        )
        self.assertTrue(ok)

    def test_non_quality_rejected(self):
        ok, _ = strategy.qualifies_swing_stock(
            is_quality=False,
            pullback_from_7d_high_pct=-0.05,
            above_50d_ma=True,
        )
        self.assertFalse(ok)

    def test_shallow_pullback_rejected(self):
        """Stocks have tighter window — only -1% off high doesn't count."""
        ok, _ = strategy.qualifies_swing_stock(
            is_quality=True,
            pullback_from_7d_high_pct=-0.01,
            above_50d_ma=True,
        )
        self.assertFalse(ok)


# ─── Ops floor / cash management ──────────────────────────────────────────

class TestOpsFloor(unittest.TestCase):

    def test_buy_within_floor_allowed(self):
        ok, _ = strategy.buy_respects_ops_floor(
            current_cash_aud=2000,
            intended_buy_aud=800,
        )
        self.assertTrue(ok)

    def test_buy_at_floor_allowed(self):
        """$1300 cash, buy $800 = $500 left = exactly the floor."""
        ok, reason = strategy.buy_respects_ops_floor(
            current_cash_aud=1300,
            intended_buy_aud=800,
        )
        self.assertTrue(ok, reason)

    def test_buy_below_floor_rejected(self):
        """$1000 cash, buy $800 = $200 left, below the $500 floor."""
        ok, reason = strategy.buy_respects_ops_floor(
            current_cash_aud=1000,
            intended_buy_aud=800,
        )
        self.assertFalse(ok)
        self.assertIn("below ops floor", reason)

    def test_overdraft_rejected(self):
        """Trying to buy more than we have."""
        ok, _ = strategy.buy_respects_ops_floor(
            current_cash_aud=400,
            intended_buy_aud=800,
        )
        self.assertFalse(ok)


# ─── Swing crypto exit rules ──────────────────────────────────────────────

class TestSwingCryptoExit(unittest.TestCase):

    def test_at_break_even_holds(self):
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.001, peak_pnl_pct=0.001, age_days=1.0,
        )
        self.assertFalse(d.should_exit)

    def test_sideways_for_4hr_NOT_exited(self):
        """The yesterday-bug test. Sideways must hold, no time-exit."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.0, peak_pnl_pct=0.0, age_days=0.17,  # 4 hours
        )
        self.assertFalse(d.should_exit, "4hr time-exit must be removed")

    def test_sideways_for_5_days_still_holds(self):
        """No movement for 5 days is fine — review at 30."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.005, peak_pnl_pct=0.01, age_days=5.0,
        )
        self.assertFalse(d.should_exit)

    def test_minor_loss_holds(self):
        """-3% is normal noise, not a stop."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=-0.03, peak_pnl_pct=0.01, age_days=1.0,
        )
        self.assertFalse(d.should_exit)

    def test_stop_at_minus_8_exits(self):
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=-0.08, peak_pnl_pct=0.0, age_days=1.0,
        )
        self.assertTrue(d.should_exit)
        self.assertEqual(d.fraction, 1.0)
        self.assertIn("stop loss", d.reason)

    def test_stop_at_minus_10_exits(self):
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=-0.10, peak_pnl_pct=0.0, age_days=1.0,
        )
        self.assertTrue(d.should_exit)

    def test_at_target_no_trailing_yet(self):
        """Just hit +15%, the trailing stop arms but doesn't fire."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.15, peak_pnl_pct=0.15, age_days=3.0,
        )
        self.assertFalse(d.should_exit)
        self.assertEqual(d.new_peak_pnl_pct, 0.15)

    def test_trailing_stop_fires_after_giveback(self):
        """Peaked at +20%, now at +14% — gave back 6% from peak (>5% trail)."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.14, peak_pnl_pct=0.20, age_days=5.0,
        )
        self.assertTrue(d.should_exit)
        self.assertIn("trailing", d.reason)

    def test_trailing_stop_does_not_fire_for_small_giveback(self):
        """Peaked at +20%, now at +18% — only 2% giveback, hold."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.18, peak_pnl_pct=0.20, age_days=5.0,
        )
        self.assertFalse(d.should_exit)

    def test_trailing_stop_not_armed_below_target(self):
        """Peak +10% (under +15% trigger). Drop to +4% should NOT fire trailing."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.04, peak_pnl_pct=0.10, age_days=2.0,
        )
        self.assertFalse(d.should_exit)

    def test_30_day_review_exits(self):
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.05, peak_pnl_pct=0.10, age_days=31.0,
        )
        self.assertTrue(d.should_exit)
        self.assertIn("review", d.reason)

    def test_29_days_still_holds(self):
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.05, peak_pnl_pct=0.10, age_days=29.0,
        )
        self.assertFalse(d.should_exit)

    def test_peak_updated_when_pnl_higher(self):
        """If pnl_pct exceeds stored peak, new peak is pnl."""
        d = strategy.decide_exit_swing_crypto(
            pnl_pct=0.08, peak_pnl_pct=0.05, age_days=1.0,
        )
        self.assertEqual(d.new_peak_pnl_pct, 0.08)


# ─── Momentum exit rules ──────────────────────────────────────────────────

class TestMomentumExit(unittest.TestCase):

    def test_holds_at_zero(self):
        d = strategy.decide_exit_momentum(pnl_pct=0.0, age_days=1.0)
        self.assertFalse(d.should_exit)

    def test_stop_at_minus_10(self):
        d = strategy.decide_exit_momentum(pnl_pct=-0.10, age_days=1.0)
        self.assertTrue(d.should_exit)
        self.assertIn("stop", d.reason)

    def test_minus_8_holds(self):
        """-8% is closer to stop than swing's -8%, but momentum gives more room."""
        d = strategy.decide_exit_momentum(pnl_pct=-0.08, age_days=1.0)
        self.assertFalse(d.should_exit)

    def test_target_at_plus_30(self):
        d = strategy.decide_exit_momentum(pnl_pct=0.30, age_days=2.0)
        self.assertTrue(d.should_exit)
        self.assertIn("target", d.reason)

    def test_just_under_target_holds(self):
        d = strategy.decide_exit_momentum(pnl_pct=0.25, age_days=2.0)
        self.assertFalse(d.should_exit)

    def test_7_day_window_expires(self):
        d = strategy.decide_exit_momentum(pnl_pct=0.05, age_days=7.0)
        self.assertTrue(d.should_exit)
        self.assertIn("7-day", d.reason)

    def test_6_day_holds(self):
        d = strategy.decide_exit_momentum(pnl_pct=0.05, age_days=6.0)
        self.assertFalse(d.should_exit)


# ─── Stock exit rules ─────────────────────────────────────────────────────

class TestStockExit(unittest.TestCase):

    def test_stop_at_minus_5(self):
        d = strategy.decide_exit_swing_stock(
            pnl_pct=-0.05, peak_pnl_pct=0.0, age_days=1.0,
        )
        self.assertTrue(d.should_exit)

    def test_minus_3_holds(self):
        """Stocks: tighter than crypto. -3% is fine."""
        d = strategy.decide_exit_swing_stock(
            pnl_pct=-0.03, peak_pnl_pct=0.0, age_days=1.0,
        )
        self.assertFalse(d.should_exit)

    def test_trailing_at_plus_12(self):
        """Stocks trail tighter — armed at +12%, give back 4%."""
        # Peaked +15%, now +10% = 5% giveback (>4%)
        d = strategy.decide_exit_swing_stock(
            pnl_pct=0.10, peak_pnl_pct=0.15, age_days=5.0,
        )
        self.assertTrue(d.should_exit)


# ─── Pre-scoring ──────────────────────────────────────────────────────────

class TestPrescore(unittest.TestCase):
    """Deterministic ranking — used to send top-N to Claude."""

    def test_swing_crypto_zero_when_below_ma(self):
        s = strategy.prescore_swing_crypto(
            market_cap_rank=1, pullback_pct=-0.08, above_50d_ma=False,
        )
        self.assertEqual(s, 0.0)

    def test_swing_crypto_top5_in_sweet_spot(self):
        """BTC-tier coin in the perfect pullback window: max score."""
        s = strategy.prescore_swing_crypto(
            market_cap_rank=1, pullback_pct=-0.08, above_50d_ma=True,
        )
        self.assertEqual(s, 4.0)  # 2.0 (top5) + 2.0 (sweet spot)

    def test_swing_crypto_top_5_beats_top_30(self):
        a = strategy.prescore_swing_crypto(
            market_cap_rank=2, pullback_pct=-0.08, above_50d_ma=True,
        )
        b = strategy.prescore_swing_crypto(
            market_cap_rank=25, pullback_pct=-0.08, above_50d_ma=True,
        )
        self.assertGreater(a, b)

    def test_momentum_zero_without_breakout(self):
        s = strategy.prescore_momentum_crypto(
            market_cap_rank=80, broke_7d_high_today=False, volume_ratio=3.0,
        )
        self.assertEqual(s, 0.0)

    def test_momentum_strong_breakout(self):
        s = strategy.prescore_momentum_crypto(
            market_cap_rank=50, broke_7d_high_today=True, volume_ratio=4.5,
        )
        # 1 (base) + 2 (4x+ vol) + 1.5 (mid-cap sweet spot)
        self.assertEqual(s, 4.5)

    def test_swing_stock_zero_when_below_ma(self):
        s = strategy.prescore_swing_stock(pullback_pct=-0.05, above_50d_ma=False)
        self.assertEqual(s, 0.0)

    def test_swing_stock_perfect_pullback(self):
        s = strategy.prescore_swing_stock(pullback_pct=-0.05, above_50d_ma=True)
        self.assertEqual(s, 3.0)  # 1 (quality base) + 2 (sweet spot)


if __name__ == "__main__":
    unittest.main(verbosity=2)
