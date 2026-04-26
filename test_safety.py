"""
Tests for bot/safety.py — circuit breakers and bot-level safeguards.
"""

import sys
import unittest
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/home/claude/build')
import safety


class TestDrawdown(unittest.TestCase):

    def test_no_peak_yet_allows(self):
        v = safety.check_drawdown(current_total_aud=10000, peak_total_aud=0)
        self.assertTrue(v.allowed)

    def test_at_peak_allows(self):
        v = safety.check_drawdown(current_total_aud=10000, peak_total_aud=10000)
        self.assertTrue(v.allowed)

    def test_above_peak_allows(self):
        # Should be impossible if update_peak runs, but defensive
        v = safety.check_drawdown(current_total_aud=10500, peak_total_aud=10000)
        self.assertTrue(v.allowed)

    def test_3_percent_down_allowed(self):
        # Below 5% halt threshold
        v = safety.check_drawdown(current_total_aud=9700, peak_total_aud=10000)
        self.assertTrue(v.allowed)

    def test_5_percent_exactly_halts(self):
        v = safety.check_drawdown(current_total_aud=9500, peak_total_aud=10000)
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "drawdown")
        self.assertIn("5.0%", v.reason)

    def test_10_percent_down_halts_with_clear_reason(self):
        v = safety.check_drawdown(current_total_aud=9000, peak_total_aud=10000)
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "drawdown")
        self.assertIn("10.0%", v.reason)
        self.assertIn("$10000", v.reason)

    def test_update_peak_takes_max(self):
        self.assertEqual(safety.update_peak(10500, 10000), 10500)
        self.assertEqual(safety.update_peak(9500, 10000), 10000)
        self.assertEqual(safety.update_peak(10000, 0), 10000)


class TestSellLoss(unittest.TestCase):
    """The ARB-99% scenario protection lives here."""

    def test_small_loss_allowed(self):
        # 5% loss — well under 15% threshold
        v = safety.check_sell_loss(entry_aud=100, exit_aud=95, symbol="BTC")
        self.assertTrue(v.allowed)

    def test_break_even_allowed(self):
        v = safety.check_sell_loss(entry_aud=100, exit_aud=100, symbol="BTC")
        self.assertTrue(v.allowed)

    def test_profit_allowed(self):
        v = safety.check_sell_loss(entry_aud=100, exit_aud=110, symbol="BTC")
        self.assertTrue(v.allowed)

    def test_15_percent_exactly_halts(self):
        v = safety.check_sell_loss(entry_aud=100, exit_aud=85, symbol="ETH")
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "max_loss")

    def test_arb_disaster_halted(self):
        """The yesterday-ARB scenario: $300 buy, sell would realize -99%."""
        # Bot would try to sell at ~$1.35 against entry of $300 (10 ARB at $30 entry)
        # This represents per-unit: entry $30, exit $0.13
        v = safety.check_sell_loss(entry_aud=29.51, exit_aud=0.13, symbol="ARB")
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "max_loss")
        self.assertIn("ARB", v.reason)
        self.assertIn("99", v.reason)  # the loss percentage shows up

    def test_zero_entry_halts(self):
        """If entry_price is 0, refuse — we have a data bug, not a real loss."""
        v = safety.check_sell_loss(entry_aud=0, exit_aud=0.50, symbol="ALGO")
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "max_loss")
        self.assertIn("entry_price is 0", v.reason)


class TestDailyCap(unittest.TestCase):

    def test_under_cap_allowed(self):
        v = safety.check_daily_cap(buys_today=3)
        self.assertTrue(v.allowed)

    def test_at_cap_blocks(self):
        v = safety.check_daily_cap(buys_today=safety.DAILY_BUY_CAP)
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "daily_cap")

    def test_over_cap_blocks(self):
        v = safety.check_daily_cap(buys_today=safety.DAILY_BUY_CAP + 1)
        self.assertFalse(v.allowed)


class TestConsecutiveLosses(unittest.TestCase):

    def test_few_losses_allowed(self):
        v = safety.check_consecutive_losses(consecutive_losses=2)
        self.assertTrue(v.allowed)

    def test_threshold_halts(self):
        v = safety.check_consecutive_losses(consecutive_losses=safety.CONSECUTIVE_LOSS_HALT)
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "consec_loss")

    def test_update_increments_on_loss(self):
        self.assertEqual(safety.update_consecutive_losses(2, last_trade_was_loss=True), 3)

    def test_update_resets_on_win(self):
        self.assertEqual(safety.update_consecutive_losses(5, last_trade_was_loss=False), 0)


class TestHeartbeat(unittest.TestCase):

    def test_no_heartbeat_not_stale(self):
        # Fresh deploy, no prior heartbeat — not "stale", just absent
        stale, mins = safety.is_heartbeat_stale("")
        self.assertFalse(stale)

    def test_recent_heartbeat_fresh(self):
        recent = datetime.now(timezone.utc).isoformat()
        stale, mins = safety.is_heartbeat_stale(recent)
        self.assertFalse(stale)
        self.assertLess(mins, 1)

    def test_old_heartbeat_stale(self):
        old = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        stale, mins = safety.is_heartbeat_stale(old)
        self.assertTrue(stale)
        self.assertGreaterEqual(mins, 15)

    def test_threshold_boundary(self):
        # Exactly at threshold — should be stale
        boundary = (datetime.now(timezone.utc)
                    - timedelta(minutes=safety.HEARTBEAT_STALE_MINUTES)).isoformat()
        stale, _ = safety.is_heartbeat_stale(boundary)
        self.assertTrue(stale)

    def test_garbage_heartbeat_handled(self):
        stale, _ = safety.is_heartbeat_stale("not-a-timestamp")
        self.assertFalse(stale)  # don't false-alarm on bad data


class TestCompositeBuyCheck(unittest.TestCase):
    """check_can_buy combines all guards and returns first failure."""

    def test_all_pass_allows(self):
        v = safety.check_can_buy(
            current_total_aud=10000,
            peak_total_aud=10000,
            buys_today=0,
            consecutive_losses=0,
        )
        self.assertTrue(v.allowed)

    def test_kill_switch_blocks(self):
        v = safety.check_can_buy(
            current_total_aud=10000,
            peak_total_aud=10000,
            buys_today=0,
            consecutive_losses=0,
            manual_kill=True,
        )
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "manual")

    def test_drawdown_reported_first(self):
        """Drawdown takes precedence in reason ordering when multiple fail."""
        v = safety.check_can_buy(
            current_total_aud=8000,         # 20% drawdown
            peak_total_aud=10000,
            buys_today=999,                 # also over cap
            consecutive_losses=999,         # also too many losses
        )
        self.assertFalse(v.allowed)
        # Drawdown is checked first, should be reported
        self.assertEqual(v.halt_kind, "drawdown")

    def test_consecutive_losses_reported_when_only_failure(self):
        v = safety.check_can_buy(
            current_total_aud=10000,
            peak_total_aud=10000,
            buys_today=0,
            consecutive_losses=safety.CONSECUTIVE_LOSS_HALT,
        )
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "consec_loss")

    def test_daily_cap_reported_when_only_failure(self):
        v = safety.check_can_buy(
            current_total_aud=10000,
            peak_total_aud=10000,
            buys_today=safety.DAILY_BUY_CAP,
            consecutive_losses=0,
        )
        self.assertFalse(v.allowed)
        self.assertEqual(v.halt_kind, "daily_cap")


class TestCompositeSellCheck(unittest.TestCase):

    def test_normal_sell_allowed(self):
        v = safety.check_can_sell(
            symbol="BTC",
            entry_aud=100,
            exit_aud=95,
        )
        self.assertTrue(v.allowed)

    def test_huge_loss_blocked(self):
        v = safety.check_can_sell(
            symbol="ARB",
            entry_aud=29.51,
            exit_aud=0.13,
        )
        self.assertFalse(v.allowed)

    def test_forced_sell_bypasses_loss_check(self):
        """User clicked force-sell. Even at -99%, allow it."""
        v = safety.check_can_sell(
            symbol="ARB",
            entry_aud=29.51,
            exit_aud=0.13,
            is_forced=True,
        )
        self.assertTrue(v.allowed)
        self.assertIn("forced", v.reason)


if __name__ == "__main__":
    unittest.main(verbosity=2)
