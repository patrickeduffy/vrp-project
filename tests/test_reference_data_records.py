from __future__ import annotations

import math
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.storage.reference_data import (
    DailyMarketFeature,
    DailyMarketFeatureDefinition,
    ReferenceDataRelease,
    ReferenceRateObservation,
    daily_market_feature_definition_sha256,
)


class ReferenceDataRecordTests(unittest.TestCase):
    def test_sofr_preserves_source_percent_and_derives_decimal(self):
        row = ReferenceRateObservation(date(2026, 7, 20), Decimal("3.57"))
        self.assertEqual(row.rate_percent, Decimal("3.57"))
        self.assertEqual(row.rate_decimal, Decimal("0.0357"))
        self.assertEqual(len(row.row_sha256), 64)

    def test_equivalent_decimal_spellings_have_same_row_hash(self):
        first = ReferenceRateObservation(date(2026, 7, 20), Decimal("3.5700"))
        second = ReferenceRateObservation(date(2026, 7, 20), Decimal("3.57"))
        self.assertEqual(first.row_sha256, second.row_sha256)

    def test_sofr_rejects_out_of_range_and_nonfinite_input(self):
        with self.assertRaisesRegex(ValueError, "between -5 and 25"):
            ReferenceRateObservation(date(2026, 7, 20), Decimal("35.7"))
        with self.assertRaisesRegex(ValueError, "finite"):
            ReferenceRateObservation(date(2026, 7, 20), Decimal("NaN"))

    def test_available_daily_feature_requires_complete_values(self):
        with self.assertRaisesRegex(ValueError, "require complete"):
            DailyMarketFeature(
                trade_date=date(2026, 7, 21),
                spy_close=Decimal("748.32"),
                prior_trade_date=date(2026, 7, 20),
                spy_change=None,
                spy_log_return=None,
                wilder_avg_gain_14=2.1,
                wilder_avg_loss_14=1.9,
                rsi14=52.48,
                rv21d_variance=0.0138,
                rv21d_volatility_pct=11.746,
                calculation_status="AVAILABLE",
                quality_status="PASS",
            )

    def test_warmup_nulls_are_allowed_but_nan_is_not(self):
        row = DailyMarketFeature(
            trade_date=date(2018, 1, 2),
            spy_close=Decimal("268.77"),
            prior_trade_date=None,
            spy_change=None,
            spy_log_return=None,
            wilder_avg_gain_14=None,
            wilder_avg_loss_14=None,
            rsi14=None,
            rv21d_variance=None,
            rv21d_volatility_pct=None,
            calculation_status="WARMUP",
            quality_status="PASS",
        )
        self.assertEqual(row.calculation_status, "WARMUP")
        with self.assertRaisesRegex(ValueError, "finite"):
            DailyMarketFeature(
                trade_date=date(2018, 1, 3),
                spy_close=Decimal("270"),
                prior_trade_date=date(2018, 1, 2),
                spy_change=math.nan,
                spy_log_return=0.004,
                wilder_avg_gain_14=1.0,
                wilder_avg_loss_14=1.0,
                rsi14=50.0,
                rv21d_variance=0.01,
                rv21d_volatility_pct=10.0,
                calculation_status="AVAILABLE",
                quality_status="PASS",
            )

    def test_release_rejects_naive_timestamp_and_bad_counts(self):
        kwargs = dict(
            release_id=uuid4(),
            dataset_key="FRED_SOFR",
            dataset_kind="REFERENCE_RATE",
            dataset_schema_version="v1",
            normalized_content_sha256="a" * 64,
            source_system="FRED",
            loader_version="test-v1",
            normalized_data_asset_id=uuid4(),
            vintage_kind="LATEST_REVISED",
            retrieved_at=datetime(2026, 7, 21),
            source_row_count=10,
            persisted_row_count=1,
        )
        with self.assertRaisesRegex(ValueError, "timezone"):
            ReferenceDataRelease(**kwargs)
        kwargs["retrieved_at"] = datetime(2026, 7, 21, tzinfo=timezone.utc)
        kwargs["persisted_row_count"] = 11
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            ReferenceDataRelease(**kwargs)

    def test_definition_keeps_unknown_price_adjustment_explicit(self):
        definition_payload = {
            "price_source_field": "close",
            "adjustment_attested": False,
        }
        definition_sha256 = daily_market_feature_definition_sha256(
            definition_key="SPY_SIGNAL_FEATURES",
            price_adjustment="UNKNOWN",
            return_formula_version="canonical_spy_close_log_return_v1",
            rsi_formula_version="wilder_rsi14_spy_close_v3_clean_session_rebuild",
            rv_formula_version="sample_std_log_return_21d_ddof1_annualized_252_v1",
            definition=definition_payload,
        )
        definition = DailyMarketFeatureDefinition(
            definition_id=uuid4(),
            definition_key="SPY_SIGNAL_FEATURES",
            version_label="v1",
            content_sha256=definition_sha256,
            price_adjustment="UNKNOWN",
            return_formula_version="canonical_spy_close_log_return_v1",
            rsi_formula_version="wilder_rsi14_spy_close_v3_clean_session_rebuild",
            rv_formula_version="sample_std_log_return_21d_ddof1_annualized_252_v1",
            definition=definition_payload,
        )
        self.assertEqual(definition.price_adjustment, "UNKNOWN")

        with self.assertRaisesRegex(ValueError, "does not match"):
            DailyMarketFeatureDefinition(
                definition_id=uuid4(),
                definition_key="SPY_SIGNAL_FEATURES",
                version_label="v1",
                content_sha256="b" * 64,
                price_adjustment="UNKNOWN",
                return_formula_version="canonical_spy_close_log_return_v1",
                rsi_formula_version="wilder_rsi14_spy_close_v3_clean_session_rebuild",
                rv_formula_version="sample_std_log_return_21d_ddof1_annualized_252_v1",
                definition=definition_payload,
            )

    def test_database_date_and_decimal_contracts_are_exact(self):
        with self.assertRaisesRegex(ValueError, "without a time"):
            ReferenceRateObservation(
                datetime(2026, 7, 20, 12, 30, tzinfo=timezone.utc),
                Decimal("3.57"),
            )
        with self.assertRaisesRegex(ValueError, "12 fractional"):
            ReferenceRateObservation(
                date(2026, 7, 20),
                Decimal("3.5700000000001"),
            )
        with self.assertRaisesRegex(ValueError, "8 fractional"):
            DailyMarketFeature(
                trade_date=date(2026, 7, 21),
                spy_close=Decimal("748.320000001"),
                prior_trade_date=None,
                spy_change=None,
                spy_log_return=None,
                wilder_avg_gain_14=None,
                wilder_avg_loss_14=None,
                rsi14=None,
                rv21d_variance=None,
                rv21d_volatility_pct=None,
                calculation_status="WARMUP",
                quality_status="PASS",
            )

    def test_json_payload_is_deeply_immutable_after_validation(self):
        source = {"nested": {"values": [1, 2]}}
        row = DailyMarketFeature(
            trade_date=date(2018, 1, 2),
            spy_close=Decimal("268.77"),
            prior_trade_date=None,
            spy_change=None,
            spy_log_return=None,
            wilder_avg_gain_14=None,
            wilder_avg_loss_14=None,
            rsi14=None,
            rv21d_variance=None,
            rv21d_volatility_pct=None,
            calculation_status="WARMUP",
            quality_status="PASS",
            details=source,
        )
        digest = row.row_sha256
        source["nested"]["values"].append(3)
        self.assertEqual(row.row_sha256, digest)
        with self.assertRaises(TypeError):
            row.details["new"] = "not allowed"


if __name__ == "__main__":
    unittest.main(verbosity=2)
