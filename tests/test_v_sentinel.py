from __future__ import annotations

import unittest

import numpy as np

from fxbacktest.strategies.v_sentinel import DEFAULT_INSTRUMENTS, build_v_sentinel_specs, session_mask


class VSentinelTests(unittest.TestCase):
    def test_build_specs_covers_both_sides_for_all_pairs(self) -> None:
        specs = build_v_sentinel_specs()
        instruments = {spec.instrument for spec in specs}
        self.assertEqual(instruments, set(DEFAULT_INSTRUMENTS))
        self.assertTrue(any(spec.side == "long" for spec in specs))
        self.assertTrue(any(spec.side == "short" for spec in specs))

    def test_session_mask_handles_wraparound_window(self) -> None:
        hours = np.arange(24, dtype=np.int16)
        mask = session_mask(hours, 22, 2)
        selected = set(hours[mask].tolist())
        self.assertEqual(selected, {22, 23, 0, 1, 2})


if __name__ == "__main__":
    unittest.main()
