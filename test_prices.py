"""
Tests for bot/prices.py — the data integrity layer.

These tests use mocks instead of hitting real APIs because:
  1. This sandbox can't reach Binance (the production bot can)
  2. Tests must be deterministic — real prices change every second
  3. We need to deliberately trigger failure modes (ARB-99% scenario)
"""

import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, '/home/claude/build')
import prices


def fake_binance_response(price_usd):
    """Returns a mock requests.Response that looks like Binance's reply."""
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {"symbol": "BTCUSDT", "price": str(price_usd)}
    return m

def fake_coinspot_response(prices_dict):
    """Returns a mock CoinSpot /pubapi/v2/latest response."""
    m = MagicMock()
    m.status_code = 200
    cs_payload = {
        "status": "ok",
        "prices": {sym: {"bid": p*0.995, "ask": p*1.005, "last": p}
                   for sym, p in prices_dict.items()}
    }
    m.json.return_value = cs_payload
    return m

def fake_frankfurter_response(rate):
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {"rates": {"AUD": rate}}
    return m


class TestHappyPath(unittest.TestCase):
    """Both sources agree → validated quote returned."""

    def setUp(self):
        # Clear the file cache between tests
        import shutil
        if prices.CACHE_DIR.exists():
            for f in prices.CACHE_DIR.glob("*.json"):
                f.unlink()

    def test_btc_validated_when_sources_agree(self):
        """BTC at $90k USD on Binance, ~$140k AUD on CoinSpot, FX 1.55 → agrees → validated."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    return fake_binance_response(90_000)
                if "coinspot" in url:
                    return fake_coinspot_response({"BTC": 139_500})  # close to 90000*1.55
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
                raise AssertionError(f"unexpected URL: {url}")
            mock_get.side_effect = router

            quote = prices.get_crypto_price("BTC")
            self.assertIsNotNone(quote)
            self.assertTrue(quote.validated, f"Expected validated, disagreement={quote.disagreement_pct}%")
            self.assertEqual(quote.usd, 90_000)
            self.assertEqual(quote.cs_aud, 139_500)
            self.assertEqual(quote.aud, 139_500)  # uses CoinSpot for trade price
            self.assertLess(quote.disagreement_pct, 5.0)


class TestARBDisaster(unittest.TestCase):
    """The yesterday-disaster scenario: prevent the ARB-99% bug from ever recurring."""

    def setUp(self):
        for f in prices.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_arb_garbage_price_rejected(self):
        """
        Recreate yesterday's exact scenario:
        - Binance: ARB = $0.40 USD (real price)
        - CoinSpot: ARB = $29.51 AUD (corrupt data — 70x too high)
        - FX: 1.55
        Implied AUD: $0.40 × 1.55 = $0.62
        CoinSpot says: $29.51
        Disagreement: ~98% — must refuse the trade.
        """
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    return fake_binance_response(0.40)
                if "coinspot" in url:
                    return fake_coinspot_response({"ARB": 29.51})
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
                raise AssertionError(f"unexpected URL: {url}")
            mock_get.side_effect = router

            quote = prices.get_crypto_price("ARB")
            self.assertIsNotNone(quote)
            self.assertFalse(quote.validated, "MUST refuse this trade")
            self.assertEqual(quote.aud, 0.0, "aud=0 signals 'do not trade' to caller")
            self.assertGreater(quote.disagreement_pct, 90)

    def test_borderline_disagreement_under_5_passes(self):
        """4% disagreement should still pass — within tolerance."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    return fake_binance_response(100)  # implies 155 AUD
                if "coinspot" in url:
                    return fake_coinspot_response({"ETH": 161})  # 3.9% off
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
            mock_get.side_effect = router

            quote = prices.get_crypto_price("ETH")
            self.assertTrue(quote.validated)
            self.assertLess(quote.disagreement_pct, 5.0)

    def test_borderline_disagreement_over_5_fails(self):
        """6% disagreement should fail — outside tolerance."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    return fake_binance_response(100)
                if "coinspot" in url:
                    return fake_coinspot_response({"ETH": 165})  # 6% off
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
            mock_get.side_effect = router

            quote = prices.get_crypto_price("ETH")
            self.assertFalse(quote.validated)
            self.assertEqual(quote.aud, 0.0)


class TestSourceFailures(unittest.TestCase):
    """When a source is down, behave correctly."""

    def setUp(self):
        for f in prices.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_coinspot_down_binance_only(self):
        """CoinSpot down: return Binance USD × FX with validated=False (single source)."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    return fake_binance_response(90_000)
                if "coinspot" in url:
                    raise ConnectionError("CoinSpot down")
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
            mock_get.side_effect = router

            quote = prices.get_crypto_price("BTC")
            self.assertIsNotNone(quote)
            self.assertFalse(quote.validated, "single source = not validated for buys")
            self.assertEqual(quote.usd, 90_000)
            self.assertEqual(quote.cs_aud, 0.0)
            self.assertAlmostEqual(quote.aud, 90_000 * 1.55, places=2)

    def test_binance_down_coinspot_only_treated_unvalidated(self):
        """If only CoinSpot has a price, it's suspicious — treat as unvalidated."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    raise ConnectionError("All Binance hosts down")
                if "coinspot" in url:
                    return fake_coinspot_response({"OBSCURECOIN": 0.05})
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
            mock_get.side_effect = router

            quote = prices.get_crypto_price("OBSCURECOIN")
            self.assertIsNotNone(quote)
            self.assertFalse(quote.validated)

    def test_both_down_returns_none(self):
        """Both data sources unreachable: return None, caller must handle."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
                raise ConnectionError("everything down")
            mock_get.side_effect = router

            quote = prices.get_crypto_price("BTC")
            self.assertIsNone(quote, "Both sources down = no quote, full stop")

    def test_binance_400_means_unlisted(self):
        """Binance returning 400 = symbol doesn't exist there. Don't retry other hosts."""
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    m = MagicMock()
                    m.status_code = 400
                    return m
                if "coinspot" in url:
                    return fake_coinspot_response({"AUSSIECOIN": 0.50})
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
            mock_get.side_effect = router

            # Should treat as "Binance doesn't list it", end up CoinSpot-only path
            quote = prices.get_crypto_price("AUSSIECOIN")
            self.assertIsNotNone(quote)
            self.assertFalse(quote.validated)


class TestFXRate(unittest.TestCase):
    """FX rate fetching, caching, fallbacks."""

    def setUp(self):
        for f in prices.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_fx_fetched_when_no_cache(self):
        with patch("prices.requests.get") as mock_get:
            mock_get.return_value = fake_frankfurter_response(1.5723)
            r = prices.get_usd_aud_rate()
            self.assertEqual(r, 1.5723)

    def test_fx_uses_cache_within_ttl(self):
        # Pre-populate cache
        prices._cache_set("fx_usd_aud", {"rate": 1.62, "fetched": __import__("time").time()})
        with patch("prices.requests.get") as mock_get:
            r = prices.get_usd_aud_rate()
            self.assertEqual(r, 1.62)
            mock_get.assert_not_called()  # cache hit, no network

    def test_fx_fallback_to_hardcoded_when_all_fails(self):
        with patch("prices.requests.get") as mock_get:
            mock_get.side_effect = ConnectionError("Frankfurter down")
            r = prices.get_usd_aud_rate()
            self.assertEqual(r, prices.USD_AUD_FALLBACK)

    def test_fx_rejects_garbage_rate(self):
        """If Frankfurter returns 9999 (garbage), reject and fall back."""
        with patch("prices.requests.get") as mock_get:
            mock_get.return_value = fake_frankfurter_response(9999)
            r = prices.get_usd_aud_rate()
            self.assertEqual(r, prices.USD_AUD_FALLBACK)


class TestBinanceFallback(unittest.TestCase):
    """When primary Binance host is down, fallbacks must work."""

    def setUp(self):
        for f in prices.CACHE_DIR.glob("*.json"):
            f.unlink()

    def test_falls_through_to_secondary_host(self):
        """First host returns 5xx, second host returns 200 — should succeed."""
        call_count = [0]
        with patch("prices.requests.get") as mock_get:
            def router(url, **kwargs):
                if "binance" in url:
                    call_count[0] += 1
                    if call_count[0] == 1:
                        m = MagicMock(); m.status_code = 503
                        return m
                    return fake_binance_response(90_000)
                if "coinspot" in url:
                    return fake_coinspot_response({"BTC": 139_500})
                if "frankfurter" in url:
                    return fake_frankfurter_response(1.55)
            mock_get.side_effect = router

            quote = prices.get_crypto_price("BTC")
            self.assertIsNotNone(quote)
            self.assertEqual(quote.usd, 90_000)
            self.assertGreater(call_count[0], 1, "should have tried fallback host")


if __name__ == "__main__":
    unittest.main(verbosity=2)
