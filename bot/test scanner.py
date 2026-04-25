"""
Tests for bot/scanner.py — candidate finder.

The scanner has two layers:
  1. Pure logic (signal computation from klines)
  2. Network calls (mocked)

Pure logic is deterministic — that's the bulk of the tests.
"""

import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, '/home/claude/build')
import scanner


# ── Test data builders ─────────────────────────────────────────────────────

def make_kline(ts, o, h, l, c, v):
    """Mimics Binance kline format: array, not dict."""
    return [ts, str(o), str(h), str(l), str(c), str(v), 0, 0, 0, 0, 0, 0]


def make_uptrend_klines(days=60, base=100.0):
    """50d MA below current price, gradual uptrend, latest close near recent high."""
    klines = []
    for i in range(days):
        # Slow uptrend: each day +0.5%
        c = base * (1.005 ** i)
        h = c * 1.01
        l = c * 0.99
        o = c * 0.995
        v = 1_000_000
        klines.append(make_kline(i * 86400 * 1000, o, h, l, c, v))
    return klines


def make_pullback_klines(days=60, base=100.0, pullback_pct=-0.08):
    """Uptrend, then a pullback spread over the last 3 bars (realistic, not a single-day crash).
    The final close ends `pullback_pct` below the recent 7d high."""
    klines = make_uptrend_klines(days, base)
    # Use the high before the pullback period as the reference
    pre_pullback_high = max(float(k[2]) for k in klines[-10:-3])
    target_close = pre_pullback_high * (1 + pullback_pct)
    # Spread the move across days -3, -2, -1
    pre_pullback_close = float(klines[-4][4])
    step = (target_close - pre_pullback_close) / 3
    for i, idx in enumerate([-3, -2, -1]):
        new_close = pre_pullback_close + step * (i + 1)
        klines[idx][4] = str(new_close)
        klines[idx][3] = str(new_close * 0.995)
        klines[idx][2] = str(new_close * 1.005)
        klines[idx][1] = str(new_close * 1.001)
    return klines


# ── Signal computation ─────────────────────────────────────────────────────

class TestSignalComputation(unittest.TestCase):

    def test_returns_none_for_short_data(self):
        klines = make_uptrend_klines(days=20)  # not enough for 50d MA
        sig = scanner._compute_pullback_signal(klines)
        self.assertIsNone(sig)

    def test_uptrend_no_pullback_signal(self):
        """Uptrend with latest close at the high → pullback close to 0."""
        klines = make_uptrend_klines(days=60)
        sig = scanner._compute_pullback_signal(klines)
        self.assertIsNotNone(sig)
        self.assertGreater(sig["close"], 0)
        # Latest close is below latest BAR HIGH by ~1% (helper sets h = c*1.01),
        # which counts as a tiny pullback. We just want to confirm it's small,
        # not -8% or anything that would qualify as a real pullback.
        self.assertGreater(sig["pullback_pct"], -0.02)
        self.assertTrue(sig["above_50d_ma"])

    def test_8_percent_pullback_detected(self):
        """Force a clean -8% pullback, scanner should report it."""
        klines = make_pullback_klines(days=60, pullback_pct=-0.08)
        sig = scanner._compute_pullback_signal(klines)
        self.assertIsNotNone(sig)
        # -8% with rounding tolerance
        self.assertAlmostEqual(sig["pullback_pct"], -0.08, delta=0.005)

    def test_15_percent_pullback_detected(self):
        klines = make_pullback_klines(days=60, pullback_pct=-0.15)
        sig = scanner._compute_pullback_signal(klines)
        self.assertAlmostEqual(sig["pullback_pct"], -0.15, delta=0.005)

    def test_above_50d_ma_in_uptrend(self):
        klines = make_uptrend_klines(days=60)
        sig = scanner._compute_pullback_signal(klines)
        self.assertTrue(sig["above_50d_ma"])

    def test_below_50d_ma_in_downtrend(self):
        # Downtrend: each day -0.5%
        klines = []
        for i in range(60):
            c = 100.0 * (0.995 ** i)
            klines.append(make_kline(i * 86400 * 1000, c*0.999, c*1.005, c*0.995, c, 1_000_000))
        sig = scanner._compute_pullback_signal(klines)
        self.assertIsNotNone(sig)
        self.assertFalse(sig["above_50d_ma"])

    def test_volume_ratio_normal(self):
        """Constant volume → ratio ~1.0."""
        klines = make_uptrend_klines(days=60)
        sig = scanner._compute_pullback_signal(klines)
        self.assertAlmostEqual(sig["volume_ratio"], 1.0, delta=0.05)

    def test_volume_spike_detected(self):
        """If today's volume is 3x the 7d avg, ratio should reflect that."""
        klines = make_uptrend_klines(days=60)
        # Bump last day's volume to 3x
        klines[-1][5] = str(3_000_000)
        sig = scanner._compute_pullback_signal(klines)
        self.assertAlmostEqual(sig["volume_ratio"], 3.0, delta=0.05)

    def test_breakout_detection(self):
        """Today's high > prior 7-day high = breakout."""
        klines = make_uptrend_klines(days=60)
        # Force last day's HIGH well above prior highs
        prior_highs = [float(k[2]) for k in klines[-8:-1]]
        prior_max = max(prior_highs)
        klines[-1][2] = str(prior_max * 1.05)  # 5% above prior max
        klines[-1][4] = str(prior_max * 1.04)  # close just below new high
        sig = scanner._compute_pullback_signal(klines)
        self.assertTrue(sig["broke_7d_high_today"])

    def test_no_breakout_when_inside_range(self):
        klines = make_uptrend_klines(days=60)
        # Force last day's high BELOW prior 7d max
        prior_max = max(float(k[2]) for k in klines[-8:-1])
        klines[-1][2] = str(prior_max * 0.99)
        klines[-1][4] = str(prior_max * 0.985)
        sig = scanner._compute_pullback_signal(klines)
        self.assertFalse(sig["broke_7d_high_today"])

    def test_handles_garbage_input(self):
        """Malformed klines shouldn't crash, just return None."""
        sig = scanner._compute_pullback_signal([["x"] * 12] * 60)
        self.assertIsNone(sig)


# ── Source intersection / universe building ────────────────────────────────

class TestUniverseIntersection(unittest.TestCase):
    """Universe = CoinSpot listings ∩ Binance USDT pairs ∩ Paprika ranks."""

    def setUp(self):
        for f in scanner.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_only_intersection_considered(self):
        """A coin on CoinSpot but not Binance must NOT be in the universe."""
        # Coin "AUSSIE" — only on CoinSpot
        # Coin "BTC" — on all three
        # Coin "SOLANA" — on Binance + ranks but NOT CoinSpot
        with patch("scanner._coinspot_listings") as mock_cs, \
             patch("scanner._market_cap_ranks") as mock_ranks, \
             patch("scanner._binance_24h_all") as mock_24h, \
             patch("scanner._binance_klines") as mock_kl:
            mock_cs.return_value = {"BTC", "AUSSIE"}
            mock_ranks.return_value = {"BTC": 1, "SOLANA": 5, "AUSSIE": 50}
            mock_24h.return_value = [
                {"symbol": "BTCUSDT"},
                {"symbol": "SOLANAUSDT"},
            ]
            mock_kl.return_value = []  # forces _compute_pullback_signal=None,
                                        # so no candidates produced — but logs
                                        # show universe size

            # We're testing universe construction, not candidate output —
            # patch logger to capture the size message
            with self.assertLogs("scanner", level="INFO") as captured:
                scanner.scan_crypto()
            log_text = "\n".join(captured.output)
            # Universe = CS({BTC, AUSSIE}) ∩ Binance({BTC, SOLANA}) ∩ ranks({BTC, SOLANA, AUSSIE})
            #         = {BTC}
            self.assertIn("universe 1", log_text)


# ── Failure handling ───────────────────────────────────────────────────────

class TestFailureHandling(unittest.TestCase):

    def setUp(self):
        for f in scanner.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_no_coinspot_returns_empty(self):
        with patch("scanner._coinspot_listings", return_value=set()), \
             patch("scanner._market_cap_ranks", return_value={"BTC": 1}), \
             patch("scanner._binance_24h_all", return_value=[]):
            result = scanner.scan_crypto()
            self.assertEqual(result, [])

    def test_no_ranks_returns_empty(self):
        with patch("scanner._coinspot_listings", return_value={"BTC"}), \
             patch("scanner._market_cap_ranks", return_value={}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "BTCUSDT"}]):
            result = scanner.scan_crypto()
            self.assertEqual(result, [])

    def test_no_binance_24h_returns_empty(self):
        with patch("scanner._coinspot_listings", return_value={"BTC"}), \
             patch("scanner._market_cap_ranks", return_value={"BTC": 1}), \
             patch("scanner._binance_24h_all", return_value=[]):
            result = scanner.scan_crypto()
            self.assertEqual(result, [])


# ── Bucket routing (the key functional integration) ───────────────────────

class TestBucketRouting(unittest.TestCase):
    """End-to-end: given mocked data, the right candidates land in the right buckets."""

    def setUp(self):
        for f in scanner.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_top_cap_pullback_routes_to_swing(self):
        """BTC at rank 1, -8% pullback, above 50d MA → swing_crypto."""
        klines = make_pullback_klines(days=60, pullback_pct=-0.08)
        with patch("scanner._coinspot_listings", return_value={"BTC"}), \
             patch("scanner._market_cap_ranks", return_value={"BTC": 1}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "BTCUSDT"}]), \
             patch("scanner._binance_klines", return_value=klines):
            result = scanner.scan_crypto()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["symbol"], "BTC")
            self.assertEqual(result[0]["bucket"], "swing_crypto")

    def test_mid_cap_breakout_routes_to_momentum(self):
        """Rank 80 + breakout today + high volume → momentum_crypto."""
        klines = make_uptrend_klines(days=60)
        # Widen the recent bars so ATR is higher (baseline volatile coin behavior)
        for idx in range(-15, 0):
            c = float(klines[idx][4])
            klines[idx][2] = str(c * 1.03)
            klines[idx][3] = str(c * 0.97)
        # Force a breakout TODAY: today's high exceeds prior 7d max
        prior_highs = [float(k[2]) for k in klines[-8:-1]]
        prior_max = max(prior_highs)
        klines[-1][2] = str(prior_max * 1.025)  # 2.5% above prior max — clear breakout, not crazy
        klines[-1][4] = str(prior_max * 1.015)  # close just below new high
        klines[-1][3] = str(prior_max * 0.99)
        # Bump volume to 3x
        klines[-1][5] = str(3_000_000)

        with patch("scanner._coinspot_listings", return_value={"DOGE"}), \
             patch("scanner._market_cap_ranks", return_value={"DOGE": 80}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "DOGEUSDT"}]), \
             patch("scanner._binance_klines", return_value=klines):
            result = scanner.scan_crypto()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["bucket"], "momentum_crypto")

    def test_pumped_coin_does_not_qualify(self):
        """The yesterday-failure: a coin at its 7d high should NOT qualify swing."""
        klines = make_uptrend_klines(days=60)  # latest close ~ at high
        with patch("scanner._coinspot_listings", return_value={"BTC"}), \
             patch("scanner._market_cap_ranks", return_value={"BTC": 1}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "BTCUSDT"}]), \
             patch("scanner._binance_klines", return_value=klines):
            result = scanner.scan_crypto()
            # No pullback → no swing qualification.
            # Also no breakout (uptrend, latest high not above prior 7d max).
            # → empty result
            self.assertEqual(len(result), 0,
                "must not buy a coin at its 7-day high — that's chasing")

    def test_too_deep_pullback_skipped(self):
        """-25% from high = breakdown, should NOT qualify swing."""
        klines = make_pullback_klines(days=60, pullback_pct=-0.25)
        with patch("scanner._coinspot_listings", return_value={"BTC"}), \
             patch("scanner._market_cap_ranks", return_value={"BTC": 1}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "BTCUSDT"}]), \
             patch("scanner._binance_klines", return_value=klines):
            result = scanner.scan_crypto()
            self.assertEqual(len(result), 0)


# ── scan_all aggregation ──────────────────────────────────────────────────

class TestScanAll(unittest.TestCase):

    def test_scan_all_groups_by_bucket(self):
        with patch("scanner.scan_crypto") as mock_c, patch("scanner.scan_stocks") as mock_s:
            mock_c.return_value = [
                {"symbol": "BTC", "bucket": "swing_crypto", "signal": {}, "reasoning": ""},
                {"symbol": "DOGE", "bucket": "momentum_crypto", "signal": {}, "reasoning": ""},
            ]
            mock_s.return_value = [
                {"symbol": "NVDA", "bucket": "swing_stock", "signal": {}, "reasoning": ""},
            ]
            result = scanner.scan_all()
            self.assertEqual(len(result["swing_crypto"]), 1)
            self.assertEqual(len(result["momentum_crypto"]), 1)
            self.assertEqual(len(result["swing_stock"]), 1)
            self.assertIn("scanned_at", result)


class TestVolatilitySpike(unittest.TestCase):
    """Today's true range > 3× 14-day ATR = news chaos, exclude."""

    def test_normal_day_no_spike(self):
        klines = make_uptrend_klines(days=60)
        sig = scanner._compute_pullback_signal(klines)
        self.assertFalse(sig["volatility_spike"])

    def test_5x_range_today_is_spike(self):
        """Today's H-L is 5x the typical daily range."""
        klines = make_uptrend_klines(days=60)
        # Find typical range
        prior_ranges = [float(k[2]) - float(k[3]) for k in klines[-15:-1]]
        typical = sum(prior_ranges) / len(prior_ranges)
        # Force today's range to 5x typical
        last_close = float(klines[-1][4])
        klines[-1][2] = str(last_close + typical * 3)  # high way up
        klines[-1][3] = str(last_close - typical * 2)  # low way down
        sig = scanner._compute_pullback_signal(klines)
        self.assertTrue(sig["volatility_spike"])

    def test_2x_range_today_not_spike(self):
        """2x normal range = elevated but not chaos. Trade-able."""
        klines = make_uptrend_klines(days=60)
        prior_ranges = [float(k[2]) - float(k[3]) for k in klines[-15:-1]]
        typical = sum(prior_ranges) / len(prior_ranges)
        last_close = float(klines[-1][4])
        klines[-1][2] = str(last_close + typical * 1.0)
        klines[-1][3] = str(last_close - typical * 1.0)
        sig = scanner._compute_pullback_signal(klines)
        self.assertFalse(sig["volatility_spike"])

    def test_spike_excluded_from_scan(self):
        """A candidate with otherwise-perfect setup is excluded if volatility spikes."""
        # Build an -8% pullback setup that would normally qualify
        klines = make_pullback_klines(days=60, pullback_pct=-0.08)
        # Now force a volatility spike on the last day
        prior_ranges = [float(k[2]) - float(k[3]) for k in klines[-15:-1]]
        typical = sum(prior_ranges) / len(prior_ranges)
        last_close = float(klines[-1][4])
        klines[-1][2] = str(last_close + typical * 4)
        klines[-1][3] = str(last_close - typical * 3)

        with patch("scanner._coinspot_listings", return_value={"BTC"}), \
             patch("scanner._market_cap_ranks", return_value={"BTC": 1}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "BTCUSDT"}]), \
             patch("scanner._binance_klines", return_value=klines):
            result = scanner.scan_crypto()
            self.assertEqual(len(result), 0,
                "volatility spike should exclude even a perfect pullback")


class TestFallingKnife(unittest.TestCase):
    """Mechanical exclusions: don't even consider these patterns."""

    def test_normal_uptrend_not_a_knife(self):
        klines = make_uptrend_klines(days=60)
        sig = scanner._compute_pullback_signal(klines)
        self.assertFalse(sig["falling_knife"])

    def test_oversold_and_dropping_is_knife(self):
        """Build a clear downtrend: RSI <30 and last close < previous."""
        # Strong downtrend so RSI ends below 30
        klines = []
        for i in range(60):
            c = 100.0 * (0.97 ** i)  # -3% per day, hard down
            klines.append(make_kline(i*86400000, c*1.005, c*1.01, c*0.98, c, 1_000_000))
        sig = scanner._compute_pullback_signal(klines)
        self.assertIsNotNone(sig)
        self.assertLess(sig["rsi"], 30)
        self.assertTrue(sig["falling_knife"])

    def test_three_red_days_rising_volume_is_knife(self):
        """Last 3 days each red, each higher volume = capitulation."""
        klines = make_uptrend_klines(days=60)
        # Make last 4 days a clear capitulation pattern
        # We need: closes[-4] > closes[-3] > closes[-2] > closes[-1]
        #         volumes[-4] < volumes[-3] < volumes[-2] < volumes[-1]
        base = float(klines[-4][4])
        klines[-4][4] = str(base)
        klines[-3][4] = str(base * 0.98)
        klines[-2][4] = str(base * 0.96)
        klines[-1][4] = str(base * 0.94)
        klines[-4][5] = str(1_000_000)
        klines[-3][5] = str(1_500_000)
        klines[-2][5] = str(2_000_000)
        klines[-1][5] = str(3_000_000)
        sig = scanner._compute_pullback_signal(klines)
        self.assertTrue(sig["falling_knife"])

    def test_three_red_days_flat_volume_not_knife(self):
        """Three reds but no volume escalation = normal pullback, not capitulation."""
        klines = make_uptrend_klines(days=60)
        base = float(klines[-4][4])
        klines[-4][4] = str(base)
        klines[-3][4] = str(base * 0.99)
        klines[-2][4] = str(base * 0.98)
        klines[-1][4] = str(base * 0.97)
        # Flat volume
        for i in [-4, -3, -2, -1]:
            klines[i][5] = str(1_000_000)
        sig = scanner._compute_pullback_signal(klines)
        # RSI shouldn't be that low from just 3% drop on uptrend, and no rising volume
        self.assertFalse(sig["falling_knife"])

    def test_knife_excluded_from_scan(self):
        """A coin that would normally qualify but is a falling knife → not in scan output."""
        # Strong downtrend, ends in a knife — but make pullback look like a -8% setup
        # by setting recent 7d high vs current
        klines = []
        for i in range(60):
            c = 100.0 * (0.97 ** i)
            klines.append(make_kline(i*86400000, c*1.005, c*1.01, c*0.98, c, 1_000_000))
        # Force last day to be -8% from 7d high (would normally qualify swing)
        recent_high = max(float(k[2]) for k in klines[-7:])
        new_close = recent_high * 0.92
        klines[-1][4] = str(new_close)
        klines[-1][2] = str(new_close * 1.005)

        with patch("scanner._coinspot_listings", return_value={"FALLINGCOIN"}), \
             patch("scanner._market_cap_ranks", return_value={"FALLINGCOIN": 5}), \
             patch("scanner._binance_24h_all", return_value=[{"symbol": "FALLINGCOINUSDT"}]), \
             patch("scanner._binance_klines", return_value=klines):
            result = scanner.scan_crypto()
            # Even though pullback looks right, falling-knife flag excludes it
            self.assertEqual(len(result), 0,
                "falling knife must be excluded mechanically, before reaching Claude")


if __name__ == "__main__":
    unittest.main(verbosity=2)
