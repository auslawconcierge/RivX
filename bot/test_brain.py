"""
Tests for bot/brain.py — Claude wrapper.

We can't and shouldn't hit the real Anthropic API in unit tests. Instead:
  - Mock the client and verify what we send to it
  - Hand-craft response shapes and verify what we extract
  - Verify the safety filter catches edge cases Claude might produce
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, '/home/claude/build')
import brain
import strategy


# ── Helpers ────────────────────────────────────────────────────────────────

def fake_anthropic_response(text: str, in_tok: int = 100, out_tok: int = 50):
    """Build a mock object that mimics Anthropic SDK's response."""
    resp = MagicMock()
    resp.content = [MagicMock(text=text)]
    resp.usage = MagicMock(input_tokens=in_tok, output_tokens=out_tok)
    return resp


def make_candidate(symbol="BTC", bucket="swing_crypto", **signal_overrides):
    sig = {"rank": 1, "pullback_pct": -0.08, "above_50d_ma": True,
           "broke_7d_high_today": False, "volume_ratio": 1.2, "close": 90000.0}
    sig.update(signal_overrides)
    return {
        "symbol": symbol,
        "bucket": bucket,
        "signal": sig,
        "reasoning": f"qualifies for {bucket}",
    }


# ── Prompt construction ────────────────────────────────────────────────────

class TestPromptBuilding(unittest.TestCase):

    def test_candidate_formatting_includes_all_signals(self):
        c = make_candidate("BTC", "swing_crypto", rank=1, pullback_pct=-0.08,
                           above_50d_ma=True, volume_ratio=1.5)
        line = brain._format_candidate(c)
        self.assertIn("BTC", line)
        self.assertIn("swing_crypto", line)
        self.assertIn("rank 1", line)
        self.assertIn("-8.0%", line)  # pullback
        self.assertIn("above 50dMA", line)
        self.assertIn("1.5x", line)

    def test_candidate_formatting_below_ma_visible(self):
        c = make_candidate("FOO", "swing_crypto", above_50d_ma=False)
        line = brain._format_candidate(c)
        self.assertIn("BELOW 50dMA", line)

    def test_breakout_flag_in_format(self):
        c = make_candidate("DOGE", "momentum_crypto",
                          broke_7d_high_today=True, volume_ratio=3.0)
        line = brain._format_candidate(c)
        self.assertIn("broke 7d high today", line)

    def test_portfolio_block_renders_capacity(self):
        block = brain._format_portfolio(
            positions={},
            slot_state={"swing_crypto": 2, "momentum_crypto": 0, "swing_stock": 1},
            cash_aud=4500.0,
        )
        self.assertIn("$4500.00", block)
        self.assertIn("2/5", block)  # swing_crypto used
        self.assertIn("0/4", block)  # momentum
        self.assertIn("1/3", block)  # stocks
        self.assertIn("No current positions", block)

    def test_portfolio_with_open_positions(self):
        block = brain._format_portfolio(
            positions={
                "BTC": {"market": "coinspot", "pnl_pct": 0.05},
                "NVDA": {"market": "alpaca", "pnl_pct": -0.02},
            },
            slot_state={"swing_crypto": 1, "swing_stock": 1},
            cash_aud=8000.0,
        )
        self.assertIn("BTC (coinspot) +5.00%", block)
        self.assertIn("NVDA (alpaca) -2.00%", block)


# ── Response parsing ───────────────────────────────────────────────────────

class TestResponseParsing(unittest.TestCase):

    def test_clean_json_response(self):
        text = '''{
          "decisions": [
            {"symbol": "BTC", "bucket": "swing_crypto", "action": "buy",
             "confidence": 0.7, "reason": "clean pullback"}
          ],
          "summary": "1 buy"
        }'''
        decisions, summary, err = brain._parse_response(text)
        self.assertEqual(err, "")
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].symbol, "BTC")
        self.assertEqual(decisions[0].action, "buy")
        self.assertEqual(decisions[0].confidence, 0.7)
        self.assertEqual(summary, "1 buy")

    def test_markdown_wrapped_json(self):
        """Claude sometimes wraps JSON in ```json ... ``` despite system prompt."""
        text = '''```json
{
  "decisions": [{"symbol":"BTC","bucket":"swing_crypto","action":"skip","confidence":0.6,"reason":"unsure"}],
  "summary": "no buys"
}
```'''
        decisions, summary, err = brain._parse_response(text)
        self.assertEqual(err, "")
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].action, "skip")

    def test_invalid_json_returns_error(self):
        text = "I don't have enough information to decide."
        decisions, summary, err = brain._parse_response(text)
        self.assertNotEqual(err, "")
        self.assertEqual(decisions, [])

    def test_invalid_action_filtered(self):
        """Claude might return action="hold" — drop it, only buy/skip allowed."""
        text = '''{"decisions": [
          {"symbol":"BTC","bucket":"swing_crypto","action":"hold","confidence":0.5,"reason":"x"},
          {"symbol":"ETH","bucket":"swing_crypto","action":"buy","confidence":0.7,"reason":"y"}
        ], "summary":""}'''
        decisions, _, err = brain._parse_response(text)
        self.assertEqual(err, "")
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].symbol, "ETH")

    def test_missing_required_field_filtered(self):
        text = '''{"decisions": [
          {"action":"buy"},
          {"symbol":"ETH","bucket":"swing_crypto","action":"buy","confidence":0.7,"reason":"ok"}
        ], "summary":""}'''
        decisions, _, _ = brain._parse_response(text)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].symbol, "ETH")

    def test_confidence_clamped(self):
        """If Claude returns confidence=2.0 or -0.5, clamp to [0,1]."""
        text = '''{"decisions": [
          {"symbol":"BTC","bucket":"swing_crypto","action":"buy","confidence":2.5,"reason":"x"},
          {"symbol":"ETH","bucket":"swing_crypto","action":"buy","confidence":-0.3,"reason":"y"}
        ], "summary":""}'''
        decisions, _, _ = brain._parse_response(text)
        self.assertEqual(decisions[0].confidence, 1.0)
        self.assertEqual(decisions[1].confidence, 0.0)

    def test_reason_truncated(self):
        long_reason = "x" * 1000
        text = json.dumps({
            "decisions": [{
                "symbol": "BTC", "bucket": "swing_crypto", "action": "buy",
                "confidence": 0.7, "reason": long_reason,
            }],
            "summary": "y" * 1000,
        })
        decisions, summary, _ = brain._parse_response(text)
        self.assertEqual(len(decisions[0].reason), 300)
        self.assertEqual(len(summary), 500)


# ── Daily cap behavior ────────────────────────────────────────────────────

class TestDailyCap(unittest.TestCase):

    def test_under_cap_proceeds(self):
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '{"decisions":[],"summary":"no buys"}'
        )
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={},
            slot_state={"swing_crypto": 0},
            cash_aud=10000.0,
            anthropic_client=client,
            daily_spent_usd=0.5,
        )
        self.assertEqual(result.error, "")
        client.messages.create.assert_called_once()

    def test_at_cap_aborts_without_calling_api(self):
        client = MagicMock()
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={},
            slot_state={"swing_crypto": 0},
            cash_aud=10000.0,
            anthropic_client=client,
            daily_spent_usd=2.0,
        )
        self.assertNotEqual(result.error, "")
        self.assertIn("daily", result.error.lower())
        client.messages.create.assert_not_called()


# ── Held-position filter ─────────────────────────────────────────────────

class TestHeldFilter(unittest.TestCase):

    def test_held_symbols_dropped_before_api(self):
        """If we already hold BTC, BTC must not be sent to Claude."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '{"decisions":[],"summary":"no fresh"}'
        )
        result = brain.decide_buys(
            candidates=[make_candidate("BTC"), make_candidate("ETH", "swing_crypto")],
            positions={"BTC": {"market": "coinspot", "pnl_pct": 0.0}},
            slot_state={"swing_crypto": 1},
            cash_aud=10000.0,
            anthropic_client=client,
        )
        # Verify what was actually sent: should only be ETH in the message
        call = client.messages.create.call_args
        user_msg = call.kwargs["messages"][0]["content"]
        self.assertNotIn("- BTC", user_msg)
        self.assertIn("ETH", user_msg)

    def test_all_held_skips_api_call(self):
        client = MagicMock()
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={"BTC": {"market": "coinspot", "pnl_pct": 0.0}},
            slot_state={"swing_crypto": 1},
            cash_aud=10000.0,
            anthropic_client=client,
        )
        client.messages.create.assert_not_called()
        self.assertEqual(result.decisions, [])


# ── Bucket-cap pre-filter ────────────────────────────────────────────────

class TestBucketPrefilter(unittest.TestCase):

    def test_full_bucket_drops_candidates(self):
        """If swing_crypto is full, swing_crypto candidates aren't shown to Claude."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '{"decisions":[],"summary":""}'
        )
        result = brain.decide_buys(
            candidates=[
                make_candidate("BTC", "swing_crypto"),
                make_candidate("DOGE", "momentum_crypto"),
            ],
            positions={},
            slot_state={"swing_crypto": 5, "momentum_crypto": 0},  # swing full
            cash_aud=10000.0,
            anthropic_client=client,
        )
        call = client.messages.create.call_args
        user_msg = call.kwargs["messages"][0]["content"]
        self.assertNotIn("- BTC", user_msg)
        self.assertIn("DOGE", user_msg)


# ── Safety filter (final gate after Claude says buy) ─────────────────────

class TestSafetyFilter(unittest.TestCase):

    def _decision(self, sym, bucket, action="buy"):
        return brain.TradeDecision(
            symbol=sym, bucket=bucket, action=action, confidence=0.7, reason="x",
        )

    def test_low_confidence_blocked(self):
        """Claude said buy at 0.5 — below MIN_CONFIDENCE 0.6 — should be skipped."""
        decisions = [brain.TradeDecision(
            symbol="BTC", bucket="swing_crypto", action="buy",
            confidence=0.5, reason="hedging",
        )]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("confidence", rejected[0][1].lower())

    def test_at_confidence_floor_passes(self):
        """Exactly 0.6 — should pass."""
        decisions = [brain.TradeDecision(
            symbol="BTC", bucket="swing_crypto", action="buy",
            confidence=0.6, reason="ok",
        )]
        allowed, _ = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 1)

    def test_high_confidence_passes(self):
        decisions = [brain.TradeDecision(
            symbol="BTC", bucket="swing_crypto", action="buy",
            confidence=0.9, reason="strong",
        )]
        allowed, _ = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 1)

    def test_normal_buys_pass(self):
        decisions = [
            self._decision("BTC", "swing_crypto"),
            self._decision("DOGE", "momentum_crypto"),
        ]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0, "momentum_crypto": 0},
        )
        self.assertEqual(len(allowed), 2)
        self.assertEqual(len(rejected), 0)

    def test_skip_decisions_ignored(self):
        decisions = [self._decision("BTC", "swing_crypto", action="skip")]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 0)
        self.assertEqual(len(rejected), 0)  # not rejected, just not a buy

    def test_duplicate_symbol_blocked(self):
        decisions = [
            self._decision("BTC", "swing_crypto"),
            self._decision("BTC", "momentum_crypto"),  # same symbol, different bucket
        ]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0, "momentum_crypto": 0},
        )
        self.assertEqual(len(allowed), 1)
        self.assertEqual(len(rejected), 1)
        self.assertIn("duplicate", rejected[0][1])

    def test_bucket_cap_enforced_during_decisions(self):
        """Claude said buy 6 swing_cryptos. Only 5 slots, only 5 should pass."""
        decisions = [self._decision(f"COIN{i}", "swing_crypto") for i in range(6)]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 5)
        self.assertEqual(len(rejected), 1)

    def test_starts_full_blocks_all(self):
        decisions = [self._decision("BTC", "swing_crypto")]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 5},
        )
        self.assertEqual(len(allowed), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("full", rejected[0][1])

    def test_ops_floor_enforced(self):
        """Cash $1300, three swing_crypto buys at $800 each → only one fits."""
        decisions = [
            self._decision("BTC", "swing_crypto"),
            self._decision("ETH", "swing_crypto"),
            self._decision("SOL", "swing_crypto"),
        ]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=1300, slot_state={"swing_crypto": 0},
        )
        # First buy: cash 1300 → 500 (at floor, OK)
        # Second buy: would need cash 1300, only 500 left → reject
        self.assertEqual(len(allowed), 1)
        self.assertGreaterEqual(len(rejected), 2)
        self.assertIn("floor", rejected[0][1])


# ── End-to-end mini integration ─────────────────────────────────────────

class TestEndToEnd(unittest.TestCase):

    def test_full_decision_path(self):
        """Candidates → Claude → parsed decisions → safety filter."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '''{
              "decisions": [
                {"symbol":"BTC","bucket":"swing_crypto","action":"buy","confidence":0.7,"reason":"clean pullback"},
                {"symbol":"ETH","bucket":"swing_crypto","action":"skip","confidence":0.6,"reason":"too late in move"}
              ],
              "summary": "1 buy, 1 skip"
            }''',
            in_tok=300, out_tok=80,
        )
        result = brain.decide_buys(
            candidates=[
                make_candidate("BTC", "swing_crypto"),
                make_candidate("ETH", "swing_crypto"),
            ],
            positions={},
            slot_state={"swing_crypto": 0},
            cash_aud=10000.0,
            anthropic_client=client,
        )
        self.assertEqual(len(result.decisions), 2)
        self.assertEqual(result.summary, "1 buy, 1 skip")
        self.assertGreater(result.estimated_cost_usd, 0)
        self.assertLess(result.estimated_cost_usd, 0.05)  # one call, very cheap

        # Now filter by safety
        allowed, rejected = brain.filter_decisions_by_safety(
            result.decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 1)  # only the buy
        self.assertEqual(allowed[0].symbol, "BTC")


class TestClaudeFailureSafety(unittest.TestCase):
    """If Claude fails in any way, the result must be NO TRADE.
    The only fallback is empty decisions — there is never a 'mechanical buy
    because Claude hesitated' path. This is non-negotiable."""

    def test_api_exception_returns_empty(self):
        """Claude raises an exception → empty decisions, error logged."""
        client = MagicMock()
        client.messages.create.side_effect = Exception("API timeout")
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=client,
        )
        self.assertEqual(result.decisions, [])
        self.assertNotEqual(result.error, "")

    def test_invalid_json_returns_empty(self):
        """Claude returns prose instead of JSON → empty decisions."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            "I don't have enough information to make a recommendation today."
        )
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=client,
        )
        self.assertEqual(result.decisions, [])
        self.assertIn("JSON", result.error)

    def test_empty_decisions_returns_empty(self):
        """Claude says 'nothing qualifies' → empty decisions, no error."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '{"decisions": [], "summary": "all candidates look like falling knives today"}'
        )
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=client,
        )
        self.assertEqual(result.decisions, [])
        self.assertEqual(result.error, "")
        # Most importantly: no fallback happened. There is no path that
        # creates a buy when Claude returns empty.

    def test_no_client_returns_empty(self):
        """If no anthropic_client passed (e.g. SDK init failed) → empty decisions."""
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=None,
        )
        self.assertEqual(result.decisions, [])

    def test_unexpected_response_shape_returns_empty(self):
        """SDK changes break .content[0].text? → still safe, no trade."""
        client = MagicMock()
        bad_response = MagicMock()
        bad_response.content = []  # empty content array
        client.messages.create.return_value = bad_response
        result = brain.decide_buys(
            candidates=[make_candidate("BTC")],
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=client,
        )
        self.assertEqual(result.decisions, [])
        self.assertNotEqual(result.error, "")


class TestCandidateCap(unittest.TestCase):
    """When >MAX candidates qualify, only top-N by pre-score reach Claude."""

    def test_caps_at_max_candidates(self):
        """Send 12 candidates, expect only 8 to reach Claude."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '{"decisions":[],"summary":""}'
        )
        # Build 12 swing_crypto candidates with varying ranks (lower = better)
        candidates = []
        for i in range(12):
            candidates.append({
                "symbol": f"COIN{i:02d}",
                "bucket": "swing_crypto",
                "signal": {
                    "rank": (i + 1),  # rank 1, 2, ..., 12
                    "pullback_pct": -0.08,
                    "above_50d_ma": True,
                    "broke_7d_high_today": False,
                    "volume_ratio": 1.0,
                    "close": 100.0,
                },
                "reasoning": "test",
            })
        result = brain.decide_buys(
            candidates=candidates,
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=client,
        )
        # Verify what was actually sent in the prompt
        sent_msg = client.messages.create.call_args.kwargs["messages"][0]["content"]
        # Count how many "- COIN" lines appear in the prompt — should be 8
        coin_lines = [line for line in sent_msg.split("\n") if line.strip().startswith("- COIN")]
        self.assertEqual(len(coin_lines), brain.MAX_CANDIDATES_TO_CLAUDE)
        # Top-ranked (COIN00 = rank 1) should be in the prompt
        self.assertTrue(any("COIN00" in line for line in coin_lines))
        # Worst-ranked (COIN11 = rank 12) should NOT be
        self.assertFalse(any("COIN11" in line for line in coin_lines))

    def test_under_cap_no_filtering(self):
        """5 candidates, cap is 8 → all 5 sent."""
        client = MagicMock()
        client.messages.create.return_value = fake_anthropic_response(
            '{"decisions":[],"summary":""}'
        )
        candidates = [
            {
                "symbol": f"COIN{i}", "bucket": "swing_crypto",
                "signal": {"rank": i+1, "pullback_pct": -0.08, "above_50d_ma": True,
                           "broke_7d_high_today": False, "volume_ratio": 1.0, "close": 100},
                "reasoning": "x",
            } for i in range(5)
        ]
        brain.decide_buys(
            candidates=candidates,
            positions={}, slot_state={"swing_crypto": 0},
            cash_aud=10000.0, anthropic_client=client,
        )
        sent = client.messages.create.call_args.kwargs["messages"][0]["content"]
        coin_lines = [l for l in sent.split("\n") if l.strip().startswith("- COIN")]
        self.assertEqual(len(coin_lines), 5)


class TestCorrelationCluster(unittest.TestCase):
    """Don't buy 3 things in the same cluster — that's concentration not diversification."""

    def _decision(self, sym, bucket="momentum_crypto"):
        return brain.TradeDecision(
            symbol=sym, bucket=bucket, action="buy", confidence=0.7, reason="x",
        )

    def test_three_l1s_only_two_pass(self):
        """SOL + AVAX + NEAR = all in 'l1' cluster. Only first 2 pass."""
        decisions = [
            self._decision("SOL"),
            self._decision("AVAX"),
            self._decision("NEAR"),
        ]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"momentum_crypto": 0},
        )
        self.assertEqual(len(allowed), 2)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0][0].symbol, "NEAR")
        self.assertIn("cluster", rejected[0][1].lower())

    def test_one_l1_one_meme_one_ai_all_pass(self):
        """SOL + DOGE + FET = three different clusters, all should pass."""
        decisions = [
            self._decision("SOL"),
            self._decision("DOGE"),
            self._decision("FET"),
        ]
        allowed, rejected = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"momentum_crypto": 0},
        )
        self.assertEqual(len(allowed), 3)
        self.assertEqual(len(rejected), 0)

    def test_btc_is_solo_cluster(self):
        """BTC + 2 L1s = 3 clusters, all pass."""
        decisions = [
            self._decision("BTC", bucket="swing_crypto"),
            self._decision("SOL", bucket="swing_crypto"),
            self._decision("AVAX", bucket="swing_crypto"),
        ]
        allowed, _ = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"swing_crypto": 0},
        )
        self.assertEqual(len(allowed), 3)

    def test_unknown_symbol_is_solo(self):
        """A symbol not in the cluster map gets its own cluster."""
        decisions = [
            self._decision("OBSCURECOIN"),
            self._decision("OTHERTOKEN"),
        ]
        allowed, _ = brain.filter_decisions_by_safety(
            decisions, cash_aud=10000, slot_state={"momentum_crypto": 0},
        )
        self.assertEqual(len(allowed), 2)


# json import for test_reason_truncated
import json


if __name__ == "__main__":
    unittest.main(verbosity=2)
