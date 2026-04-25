"""
Tests for bot.py — focused on the orchestrator's pure-logic helpers.

We don't test the main loop directly (it's an infinite loop with side effects).
Instead we test the logic that the main loop wires together:
  - compute_slot_state (positions → bucket counts)
  - compute_cash_aud (positions → cash remaining)
  - at_or_past_time_today (scheduling helper)

Integration of these into the main loop is verified by inspection + by
the underlying-module tests, which already cover safety, brain, etc.

This test file uses module-level mocking to avoid importing real
config/Alpaca/Telegram/Anthropic clients at test time.
"""

import sys
import os
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta


# Stub heavy modules BEFORE importing bot
def _install_stubs():
    """Insert minimal stubs for bot.py's imports so it can load without secrets."""
    import types

    # bot.config
    cfg = types.ModuleType("bot.config")
    cfg.PAPER_MODE = True
    cfg.ALPACA_API_KEY = ""
    cfg.ALPACA_SECRET_KEY = ""
    cfg.ALPACA_DATA_URL = "https://data.alpaca.markets"
    cfg.TELEGRAM_TOKEN = ""
    cfg.TELEGRAM_CHAT_ID = ""
    cfg.ANTHROPIC_API_KEY = ""
    sys.modules["bot.config"] = cfg

    # Make build/ importable AS the `bot` package
    import importlib, importlib.util
    bot_pkg = types.ModuleType("bot")
    bot_pkg.__path__ = ["/home/claude/build"]
    sys.modules["bot"] = bot_pkg

    # bot.prices, bot.safety, bot.strategy, bot.scanner, bot.brain — load from build/
    for mod in ("prices", "safety", "strategy", "scanner", "brain"):
        spec = importlib.util.spec_from_file_location(
            f"bot.{mod}", f"/home/claude/build/{mod}.py"
        )
        m = importlib.util.module_from_spec(spec)
        sys.modules[f"bot.{mod}"] = m
        spec.loader.exec_module(m)

    # Stub the trader/logger/notifier modules
    sl = types.ModuleType("bot.supabase_logger")
    sl.SupabaseLogger = MagicMock
    sys.modules["bot.supabase_logger"] = sl

    tn = types.ModuleType("bot.telegram_notify")
    tn.TelegramNotifier = MagicMock
    sys.modules["bot.telegram_notify"] = tn

    at = types.ModuleType("bot.alpaca_trader")
    at.AlpacaTrader = MagicMock
    sys.modules["bot.alpaca_trader"] = at

    ct = types.ModuleType("bot.coinspot_trader")
    ct.CoinSpotTrader = MagicMock
    sys.modules["bot.coinspot_trader"] = ct


_install_stubs()

# Now we can import bot.py from the build folder
import importlib.util
_spec = importlib.util.spec_from_file_location("rivx_bot", "/home/claude/build/bot.py")
rivx_bot = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rivx_bot)

# Also import strategy directly for asserting bucket constants
sys.path.insert(0, "/home/claude/build")
import strategy


# ── Slot accounting ───────────────────────────────────────────────────────

class TestSlotState(unittest.TestCase):

    def test_empty_positions(self):
        state = rivx_bot.compute_slot_state({})
        self.assertEqual(state[strategy.Bucket.SWING_CRYPTO], 0)
        self.assertEqual(state[strategy.Bucket.MOMENTUM_CRYPTO], 0)
        self.assertEqual(state[strategy.Bucket.SWING_STOCK], 0)

    def test_all_buckets_with_explicit_field(self):
        positions = {
            "BTC":  {"bucket": strategy.Bucket.SWING_CRYPTO, "market": "coinspot"},
            "ETH":  {"bucket": strategy.Bucket.SWING_CRYPTO, "market": "coinspot"},
            "DOGE": {"bucket": strategy.Bucket.MOMENTUM_CRYPTO, "market": "coinspot"},
            "NVDA": {"bucket": strategy.Bucket.SWING_STOCK, "market": "alpaca"},
        }
        state = rivx_bot.compute_slot_state(positions)
        self.assertEqual(state[strategy.Bucket.SWING_CRYPTO], 2)
        self.assertEqual(state[strategy.Bucket.MOMENTUM_CRYPTO], 1)
        self.assertEqual(state[strategy.Bucket.SWING_STOCK], 1)

    def test_legacy_position_no_bucket_alpaca_routes_to_stock(self):
        """Positions from before the v2 strategy don't have a bucket field."""
        positions = {
            "AAPL": {"market": "alpaca"},  # legacy stock position
        }
        state = rivx_bot.compute_slot_state(positions)
        self.assertEqual(state[strategy.Bucket.SWING_STOCK], 1)
        self.assertEqual(state[strategy.Bucket.SWING_CRYPTO], 0)

    def test_legacy_crypto_routes_to_swing_default(self):
        positions = {
            "DOGE": {"market": "coinspot"},  # no bucket
        }
        state = rivx_bot.compute_slot_state(positions)
        self.assertEqual(state[strategy.Bucket.SWING_CRYPTO], 1)


# ── Cash math ──────────────────────────────────────────────────────────────

class TestCashAud(unittest.TestCase):

    def test_no_positions_full_capital(self):
        cash = rivx_bot.compute_cash_aud({})
        self.assertEqual(cash, 10_000.0)

    def test_simple_deduction(self):
        positions = {
            "BTC": {"aud_amount": 800},
            "ETH": {"aud_amount": 800},
        }
        cash = rivx_bot.compute_cash_aud(positions)
        self.assertEqual(cash, 8_400.0)

    def test_handles_string_amounts(self):
        """Supabase sometimes returns numbers as strings."""
        positions = {
            "BTC": {"aud_amount": "800"},
        }
        cash = rivx_bot.compute_cash_aud(positions)
        self.assertEqual(cash, 9_200.0)

    def test_clamped_to_zero(self):
        """Over-deployed shouldn't return negative."""
        positions = {f"X{i}": {"aud_amount": 1000} for i in range(15)}
        cash = rivx_bot.compute_cash_aud(positions)
        self.assertEqual(cash, 0.0)


# ── Scheduling ────────────────────────────────────────────────────────────

class TestScheduling(unittest.TestCase):

    def test_no_prior_run_at_target_time(self):
        """Never run before — if it's past 8 AM, run."""
        # Patch aest_now to be 9 AM
        future = datetime(2026, 4, 26, 9, 0, tzinfo=rivx_bot.AEST)
        with patch.object(rivx_bot, "aest_now", return_value=future):
            self.assertTrue(rivx_bot.at_or_past_time_today("08:00", None))

    def test_no_prior_run_before_target_time(self):
        """Never run before — if it's not yet 8 AM, don't run."""
        morning = datetime(2026, 4, 26, 7, 30, tzinfo=rivx_bot.AEST)
        with patch.object(rivx_bot, "aest_now", return_value=morning):
            self.assertFalse(rivx_bot.at_or_past_time_today("08:00", None))

    def test_already_ran_today(self):
        """Ran at 8:05 AM, currently 9 AM, target 8:00 — should NOT run again."""
        now = datetime(2026, 4, 26, 9, 0, tzinfo=rivx_bot.AEST)
        last_run = datetime(2026, 4, 26, 8, 5, tzinfo=rivx_bot.AEST).isoformat()
        with patch.object(rivx_bot, "aest_now", return_value=now):
            self.assertFalse(rivx_bot.at_or_past_time_today("08:00", last_run))

    def test_ran_yesterday_runs_again_today(self):
        """Last run was yesterday morning — today 8 AM, run again."""
        now = datetime(2026, 4, 26, 9, 0, tzinfo=rivx_bot.AEST)
        last_run = datetime(2026, 4, 25, 8, 5, tzinfo=rivx_bot.AEST).isoformat()
        with patch.object(rivx_bot, "aest_now", return_value=now):
            self.assertTrue(rivx_bot.at_or_past_time_today("08:00", last_run))

    def test_garbage_last_run_runs(self):
        now = datetime(2026, 4, 26, 9, 0, tzinfo=rivx_bot.AEST)
        with patch.object(rivx_bot, "aest_now", return_value=now):
            self.assertTrue(rivx_bot.at_or_past_time_today("08:00", "garbage"))

    def test_two_targets_per_day_independent(self):
        """8 AM and 4 PM schedules tracked independently."""
        # Currently 5 PM. 8 AM ran at 8:05. 4 PM has never run.
        now = datetime(2026, 4, 26, 17, 0, tzinfo=rivx_bot.AEST)
        last_8am = datetime(2026, 4, 26, 8, 5, tzinfo=rivx_bot.AEST).isoformat()
        with patch.object(rivx_bot, "aest_now", return_value=now):
            # 8 AM already ran today → no
            self.assertFalse(rivx_bot.at_or_past_time_today("08:00", last_8am))
            # 4 PM hasn't run today → yes
            self.assertTrue(rivx_bot.at_or_past_time_today("16:00", None))


# ── Heartbeat (pure functions, easy) ──────────────────────────────────────

class TestHeartbeatHelpers(unittest.TestCase):

    def test_check_prior_heartbeat_does_not_alert_if_fresh(self):
        """Fresh heartbeat (<10 min old) → no alert."""
        db = MagicMock()
        tg = MagicMock()
        recent = datetime.now(timezone.utc).isoformat()
        db.get_flag.return_value = recent
        rivx_bot.check_prior_heartbeat(db, tg)
        tg.send.assert_not_called()

    def test_check_prior_heartbeat_alerts_on_stale(self):
        """Old heartbeat (15 min) → Telegram alert."""
        db = MagicMock()
        tg = MagicMock()
        old = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        db.get_flag.return_value = old
        rivx_bot.check_prior_heartbeat(db, tg)
        tg.send.assert_called_once()
        msg = tg.send.call_args.args[0]
        self.assertIn("restart", msg.lower())

    def test_check_prior_heartbeat_no_flag_no_alert(self):
        """No prior heartbeat (fresh deploy) → no alert."""
        db = MagicMock()
        tg = MagicMock()
        db.get_flag.return_value = ""
        rivx_bot.check_prior_heartbeat(db, tg)
        tg.send.assert_not_called()

    def test_check_prior_heartbeat_skip_old_24h_plus(self):
        """If heartbeat is >24h old, that's a fresh deploy from a long pause — don't alert."""
        db = MagicMock()
        tg = MagicMock()
        very_old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        db.get_flag.return_value = very_old
        rivx_bot.check_prior_heartbeat(db, tg)
        tg.send.assert_not_called()


# ── Position age ──────────────────────────────────────────────────────────

class TestPositionAge(unittest.TestCase):

    def test_no_opened_at_returns_zero(self):
        self.assertEqual(rivx_bot._position_age_days({}), 0.0)

    def test_recent_position(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        age = rivx_bot._position_age_days({"opened_at": recent})
        # 2 hours ≈ 0.083 days
        self.assertGreater(age, 0.05)
        self.assertLess(age, 0.15)

    def test_old_position(self):
        old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        age = rivx_bot._position_age_days({"opened_at": old})
        self.assertGreater(age, 9.5)
        self.assertLess(age, 10.5)

    def test_falls_back_to_created_at(self):
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        age = rivx_bot._position_age_days({"created_at": recent})
        self.assertGreater(age, 0.5)
        self.assertLess(age, 1.5)


# ── Attribution log (change 6: log Claude decisions for later analysis) ───

class TestAttributionLog(unittest.TestCase):
    """
    Every Claude decision (buy/skip/rejected) must be written to claude_decisions.
    This is what lets us answer "did Claude actually add value?" months later.
    """

    def _make_db(self):
        """Mock DB that records calls to _post and _patch."""
        db = MagicMock()
        db.get_positions.return_value = {}
        db.get_portfolio_value.return_value = {"total_aud": 10000}
        db.get_flag.return_value = ""  # no kill, no peak set, no spend, no buys today
        db._post.return_value = {"id": 1}
        db._get.return_value = []
        return db

    def _make_brain_result(self, decisions):
        """Build a BrainResult with the supplied decisions."""
        from brain import BrainResult
        return BrainResult(
            decisions=decisions,
            summary="test",
            estimated_cost_usd=0.01,
        )

    def test_buy_decision_logged_as_buy(self):
        """Claude said buy, safety passed, execution succeeded → log row with action=buy, executed=true."""
        from brain import TradeDecision
        db = self._make_db()
        tg = MagicMock()
        alpaca = MagicMock()
        coinspot = MagicMock()

        decisions = [TradeDecision(
            symbol="BTC", bucket="swing_crypto",
            action="buy", confidence=0.7, reason="clean pullback",
        )]

        with patch.object(rivx_bot.scanner, "scan_crypto", return_value=[
                {"symbol": "BTC", "bucket": "swing_crypto",
                 "signal": {"rank": 1}, "reasoning": "x"}
             ]), \
             patch.object(rivx_bot.brain, "decide_buys",
                          return_value=self._make_brain_result(decisions)), \
             patch.object(rivx_bot, "execute_buy",
                          return_value=(True, "ok")), \
             patch.object(rivx_bot, "get_anthropic_client",
                          return_value=MagicMock()):

            rivx_bot.run_buy_cycle(
                mode="swing_crypto",
                db=db, alpaca=alpaca, coinspot=coinspot, tg=tg,
            )

        # Find the call to _post on claude_decisions
        attribution_calls = [
            c for c in db._post.call_args_list
            if len(c.args) >= 1 and c.args[0] == "claude_decisions"
        ]
        self.assertEqual(len(attribution_calls), 1, "must log exactly one decision")
        row = attribution_calls[0].args[1]
        self.assertEqual(row["symbol"], "BTC")
        self.assertEqual(row["action"], "buy")
        self.assertTrue(row["executed"])
        self.assertEqual(row["confidence"], 0.7)

    def test_skip_decision_logged_as_skip(self):
        """Claude said skip → log row with action=skip, executed=false."""
        from brain import TradeDecision
        db = self._make_db()
        tg = MagicMock()

        decisions = [TradeDecision(
            symbol="DOGE", bucket="momentum_crypto",
            action="skip", confidence=0.8, reason="late entry",
        )]

        with patch.object(rivx_bot.scanner, "scan_crypto", return_value=[
                {"symbol": "DOGE", "bucket": "momentum_crypto",
                 "signal": {"rank": 80}, "reasoning": "x"}
             ]), \
             patch.object(rivx_bot.brain, "decide_buys",
                          return_value=self._make_brain_result(decisions)), \
             patch.object(rivx_bot, "get_anthropic_client",
                          return_value=MagicMock()):

            rivx_bot.run_buy_cycle(
                mode="momentum_crypto",
                db=db, alpaca=MagicMock(), coinspot=MagicMock(), tg=tg,
            )

        attribution_calls = [
            c for c in db._post.call_args_list
            if len(c.args) >= 1 and c.args[0] == "claude_decisions"
        ]
        self.assertEqual(len(attribution_calls), 1)
        row = attribution_calls[0].args[1]
        self.assertEqual(row["action"], "skip")
        self.assertFalse(row["executed"])

    def test_buy_rejected_by_safety_logged_correctly(self):
        """Claude said buy but safety filter rejected (e.g. low confidence)
        → log row with action=rejected_by_safety, executed=false."""
        from brain import TradeDecision
        db = self._make_db()
        tg = MagicMock()

        # Confidence 0.4 → below MIN_CONFIDENCE → safety filter rejects
        decisions = [TradeDecision(
            symbol="ARB", bucket="swing_crypto",
            action="buy", confidence=0.4, reason="hedging",
        )]

        with patch.object(rivx_bot.scanner, "scan_crypto", return_value=[
                {"symbol": "ARB", "bucket": "swing_crypto",
                 "signal": {"rank": 50}, "reasoning": "x"}
             ]), \
             patch.object(rivx_bot.brain, "decide_buys",
                          return_value=self._make_brain_result(decisions)), \
             patch.object(rivx_bot, "execute_buy",
                          return_value=(True, "ok")), \
             patch.object(rivx_bot, "get_anthropic_client",
                          return_value=MagicMock()):

            rivx_bot.run_buy_cycle(
                mode="swing_crypto",
                db=db, alpaca=MagicMock(), coinspot=MagicMock(), tg=tg,
            )

        attribution_calls = [
            c for c in db._post.call_args_list
            if len(c.args) >= 1 and c.args[0] == "claude_decisions"
        ]
        self.assertEqual(len(attribution_calls), 1)
        row = attribution_calls[0].args[1]
        self.assertEqual(row["action"], "rejected_by_safety")
        self.assertFalse(row["executed"])

    def test_outcome_patched_on_sell(self):
        """When a position closes, the most recent claude_decisions row for
        that symbol gets patched with realized_pnl_pct + closed_at."""
        db = self._make_db()
        # Mock _get to return a recent decision row
        db._get.return_value = [{
            "id": 42, "symbol": "BTC", "executed": True, "closed_at": None,
        }]

        # Mock alpaca.sell return — wait, BTC is crypto, mock coinspot
        coinspot = MagicMock()
        coinspot.sell.return_value = {"price": 95000.0}

        position = {
            "market": "coinspot",
            "entry_price": 100000.0,
        }

        with patch.object(rivx_bot.prices, "get_crypto_price",
                          return_value=MagicMock(aud=95000.0, validated=True,
                                                 cs_aud=95000.0, fx_rate=1.55)):
            ok, msg = rivx_bot.execute_sell(
                symbol="BTC", position=position,
                db=db, alpaca=MagicMock(), coinspot=coinspot,
                is_forced=False, reason="stop loss",
            )

        # The outcome patch should have been called on claude_decisions row id 42
        outcome_patches = [
            c for c in db._patch.call_args_list
            if len(c.args) >= 1 and c.args[0] == "claude_decisions"
        ]
        self.assertEqual(len(outcome_patches), 1, "outcome must be patched onto attribution row")
        patch_args = outcome_patches[0]
        # Args: (table, fields_dict, col, val)
        fields = patch_args.args[1]
        self.assertIn("realized_pnl_pct", fields)
        self.assertIn("closed_at", fields)
        self.assertIn("exit_reason", fields)
        # -5% loss
        self.assertAlmostEqual(fields["realized_pnl_pct"], -0.05, places=3)
        self.assertEqual(patch_args.args[2], "id")
        self.assertEqual(patch_args.args[3], "42")


if __name__ == "__main__":
    unittest.main(verbosity=2)
