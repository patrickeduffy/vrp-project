from __future__ import annotations

import math
import unittest
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import numpy as np
import pandas as pd

from vrp.reference_history.ids import stable_id
from vrp.reference_history.models import CurrentRow
from vrp.reference_history.planning import assert_current_matches, plan_changes
from vrp.reference_history.sources import (
    RSI_FORMULA_VERSION,
    _validate_xnys_sessions,
    normalize_sofr_frame,
    normalize_spy_daily_frames,
)


def synthetic_spy_frames(count: int = 25):
    dates = pd.bdate_range("2026-02-02", periods=count)
    closes = pd.Series(
        [round(100.0 + index * 0.3 + (index % 3) * 0.1, 2) for index in range(count)],
        dtype=float,
    )
    changes = closes.diff()
    changes.iloc[0] = 0.2
    gains: list[float] = []
    losses: list[float] = []
    rsis: list[float] = []
    average_gain = 0.5
    average_loss = 0.4
    for index, change in enumerate(changes):
        if index:
            average_gain = (average_gain * 13.0 + max(float(change), 0.0)) / 14.0
            average_loss = (average_loss * 13.0 + max(-float(change), 0.0)) / 14.0
        gains.append(average_gain)
        losses.append(average_loss)
        rsis.append(
            100.0
            if average_loss == 0
            else 100.0 - 100.0 / (1.0 + average_gain / average_loss)
        )
    returns = np.log(closes).diff()
    rv21d = returns.rolling(21).std(ddof=1) * math.sqrt(252.0) * 100.0
    prices = pd.DataFrame({"trade_date": dates, "spy_close": closes})
    rsi = pd.DataFrame(
        {
            "trade_date": dates,
            "spy_close": closes,
            "spy_change": changes,
            "wilder_avg_gain_14": gains,
            "wilder_avg_loss_14": losses,
            "spy_wilder_rsi14": rsis,
            "rsi_formula_version": RSI_FORMULA_VERSION,
        }
    )
    realized = pd.DataFrame(
        {
            "trade_date": dates,
            "spy_close": closes,
            "spy_log_return": returns,
            "rv21d_vol_pct": rv21d,
        }
    )
    return prices, rsi, realized


class ReferenceHistoryNormalizationTests(unittest.TestCase):
    def test_sofr_preserves_decimal_units_and_is_order_invariant(self):
        source = pd.DataFrame(
            {
                "observation_date": [20260403, 20260402],
                "SOFR": ["3.570", "3.56"],
            }
        )
        history = normalize_sofr_frame(source, enforce_production_coverage=False)
        reversed_history = normalize_sofr_frame(
            source.iloc[::-1], enforce_production_coverage=False
        )
        self.assertEqual(history.content_sha256, reversed_history.content_sha256)
        self.assertEqual(history.rows[1].rate_percent, Decimal("3.570000000000"))
        self.assertEqual(history.rows[1].rate_decimal, Decimal("0.035700000000"))

    def test_sofr_rejects_duplicates_and_truncated_production_source(self):
        duplicate = pd.DataFrame(
            {"observation_date": ["2018-04-03", "2018-04-03"], "SOFR": [1.8, 1.8]}
        )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            normalize_sofr_frame(duplicate)
        truncated = pd.DataFrame(
            {"observation_date": ["2018-04-04"], "SOFR": [1.8]}
        )
        with self.assertRaisesRegex(ValueError, "must begin"):
            normalize_sofr_frame(truncated)
        incomplete = pd.DataFrame(
            {"observation_date": ["2018-04-03", "2018-04-04"], "SOFR": [1.8, 1.8]}
        )
        with self.assertRaisesRegex(ValueError, "complete accepted baseline"):
            normalize_sofr_frame(incomplete)
        weekend = pd.DataFrame(
            {"observation_date": ["2026-04-04"], "SOFR": [3.57]}
        )
        with self.assertRaisesRegex(ValueError, "weekend"):
            normalize_sofr_frame(weekend, enforce_production_coverage=False)

    def test_spy_reconciles_locked_rsi_and_rv_formulas(self):
        prices, rsi, realized = synthetic_spy_frames()
        history = normalize_spy_daily_frames(
            prices,
            rsi,
            realized,
            enforce_production_coverage=False,
        )
        self.assertEqual(len(history.rows), 25)
        self.assertTrue(all(row.calculation_status == "WARMUP" for row in history.rows[:21]))
        self.assertTrue(
            all(row.calculation_status == "AVAILABLE" for row in history.rows[21:])
        )
        latest = history.rows[-1]
        self.assertAlmostEqual(
            latest.rv21d_variance,
            (latest.rv21d_volatility_pct / 100.0) ** 2,
            places=16,
        )
        shuffled = normalize_spy_daily_frames(
            prices.sample(frac=1, random_state=1),
            rsi.sample(frac=1, random_state=2),
            realized.sample(frac=1, random_state=3),
            enforce_production_coverage=False,
        )
        self.assertEqual(history.content_sha256, shuffled.content_sha256)

    def test_nullable_numeric_corruption_and_missing_formula_version_fail(self):
        prices, rsi, realized = synthetic_spy_frames()
        corrupt_rv = realized.copy()
        corrupt_rv["rv21d_vol_pct"] = corrupt_rv["rv21d_vol_pct"].astype(object)
        corrupt_rv.loc[0, "rv21d_vol_pct"] = "bad"
        with self.assertRaisesRegex(ValueError, "non-numeric text"):
            normalize_spy_daily_frames(
                prices,
                rsi,
                corrupt_rv,
                enforce_production_coverage=False,
            )
        missing_version = rsi.copy()
        missing_version.loc[5, "rsi_formula_version"] = None
        with self.assertRaisesRegex(ValueError, "cannot be missing"):
            normalize_spy_daily_frames(
                prices,
                missing_version,
                realized,
                enforce_production_coverage=False,
            )

    def test_formula_or_date_mismatch_fails_closed(self):
        prices, rsi, realized = synthetic_spy_frames()
        bad_rsi = rsi.copy()
        bad_rsi.loc[10, "wilder_avg_gain_14"] += 0.01
        with self.assertRaisesRegex(ValueError, "recursive state"):
            normalize_spy_daily_frames(
                prices, bad_rsi, realized, enforce_production_coverage=False
            )
        bad_dates = realized.drop(index=4)
        with self.assertRaisesRegex(ValueError, "dates do not match"):
            normalize_spy_daily_frames(
                prices, rsi, bad_dates, enforce_production_coverage=False
            )

    def test_production_spy_contract_requires_provenance_and_anchor(self):
        prices, rsi, realized = synthetic_spy_frames()
        with self.assertRaisesRegex(ValueError, "provenance"):
            normalize_spy_daily_frames(prices, rsi, realized)
        prices["source_symbol"] = "SPY"
        rsi["source_name"] = "accepted-rsi"
        realized["source_symbol"] = "SPY"
        realized["rv21d_source"] = "accepted-rv21d"
        with self.assertRaisesRegex(ValueError, "must begin"):
            normalize_spy_daily_frames(prices, rsi, realized)

    def test_xnys_gate_rejects_missing_session_and_weekday_holiday(self):
        try:
            import pandas_market_calendars as market_calendars
        except ImportError:
            self.skipTest("pandas-market-calendars is not installed")
        schedule = market_calendars.get_calendar("XNYS").schedule(
            start_date="2026-07-01", end_date="2026-07-10"
        )
        valid = pd.Series(schedule.index)
        _validate_xnys_sessions(valid)
        with self.assertRaisesRegex(ValueError, "XNYS"):
            _validate_xnys_sessions(valid.drop(index=valid.index[2]).reset_index(drop=True))
        extra = pd.concat([valid, pd.Series([pd.Timestamp("2026-07-03")])]).sort_values()
        with self.assertRaisesRegex(ValueError, "XNYS"):
            _validate_xnys_sessions(extra.reset_index(drop=True))

    def test_normalized_model_and_planning_fail_closed(self):
        history = normalize_sofr_frame(
            pd.DataFrame(
                {"observation_date": ["2018-04-03", "2018-04-04"], "SOFR": [1.8, 1.9]}
            ),
            enforce_production_coverage=False,
        )
        with self.assertRaisesRegex(ValueError, "hash canonical_bytes"):
            replace(history, content_sha256="0" * 64)
        current = (
            CurrentRow(
                natural_key=history.rows[0].observation_date,
                record_id=uuid4(),
                row_sha256=history.rows[0].row_sha256,
            ),
        )
        plan = plan_changes(history, current)
        self.assertEqual(plan.new_count, 1)
        self.assertEqual(plan.unchanged_count, 1)
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
            assert_current_matches(history, (current[0], current[0]))

    def test_stable_ids_are_typed_and_reject_ambiguous_separator(self):
        self.assertNotEqual(stable_id("example", 1), stable_id("example", "1"))
        with self.assertRaisesRegex(ValueError, "unit separator"):
            stable_id("example", "a\x1fb")


if __name__ == "__main__":
    unittest.main()
