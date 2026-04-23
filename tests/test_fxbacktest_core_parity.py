from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from native_acceleration import (
    FXBACKTEST_CORE_AVAILABLE,
    hawkes_intensity_accelerated,
    hawkes_intensity_reference,
    rolling_gmm_nodes_accelerated,
    rolling_gmm_nodes_reference,
    simulate_trade_path_accelerated,
    simulate_trade_path_reference,
)


class FXBacktestCoreParityTests(unittest.TestCase):
    @unittest.skipUnless(FXBACKTEST_CORE_AVAILABLE, "Rust extension is not built in this environment.")
    def test_hawkes_intensity_parity_10000_cases(self) -> None:
        rng = np.random.default_rng(42)
        for _ in range(10_000):
            length = int(rng.integers(4, 64))
            shocks = rng.normal(0.0, 2.0, size=length)
            alpha = float(rng.uniform(0.01, 0.75))
            np.testing.assert_allclose(
                hawkes_intensity_accelerated(shocks, alpha),
                hawkes_intensity_reference(shocks, alpha),
                rtol=1e-9,
                atol=1e-9,
            )

    @unittest.skipUnless(FXBACKTEST_CORE_AVAILABLE, "Rust extension is not built in this environment.")
    def test_simulate_trade_path_parity_10000_cases(self) -> None:
        rng = np.random.default_rng(7)
        for _ in range(10_000):
            length = int(rng.integers(4, 24))
            base_price = float(rng.uniform(1.0, 2.0))
            increments = rng.normal(0.0, 0.0003, size=length)
            close = base_price + np.cumsum(increments)
            high = close + np.abs(rng.normal(0.0002, 0.0001, size=length))
            low = close - np.abs(rng.normal(0.0002, 0.0001, size=length))
            open_ = np.concatenate(([base_price], close[:-1]))
            side = "long" if rng.random() > 0.5 else "short"
            trail_delta = float(rng.uniform(0.0003, 0.0015))
            ladder = sorted(float(value) for value in rng.uniform(0.0002, 0.0020, size=3))
            fractions = [0.3, 0.3, 0.3]
            stop = base_price - 0.0015 if side == "long" else base_price + 0.0015

            expected = simulate_trade_path_reference(open_, high, low, close, base_price, stop, ladder, fractions, trail_delta, length - 1, side)
            actual = simulate_trade_path_accelerated(open_, high, low, close, base_price, stop, ladder, fractions, trail_delta, length - 1, side)
            self.assertEqual(actual["exit_idx"], expected["exit_idx"])
            self.assertAlmostEqual(actual["total_pnl_delta"], expected["total_pnl_delta"], places=9)
            self.assertEqual(len(actual["partials"]), len(expected["partials"]))

    @unittest.skipUnless(FXBACKTEST_CORE_AVAILABLE, "Rust extension is not built in this environment.")
    def test_rolling_gmm_nodes_parity_smoke(self) -> None:
        rng = np.random.default_rng(99)
        minutes = 60 * 24 * 40
        timestamp = pd.date_range("2024-01-01", periods=minutes, freq="min", tz="UTC")
        prices = 1.10 + np.cumsum(rng.normal(0.0, 0.0001, size=minutes))
        actual = rolling_gmm_nodes_accelerated(timestamp, prices, 4, 720, 4)
        expected = rolling_gmm_nodes_reference(timestamp, prices, 4, 720, 4)
        self.assertEqual(len(actual), len(expected))
        self.assertTrue(np.isfinite(actual).all())
        np.testing.assert_allclose(actual[-500:], expected[-500:], rtol=5e-3, atol=5e-4)


if __name__ == "__main__":
    unittest.main()
