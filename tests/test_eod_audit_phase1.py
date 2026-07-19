from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

def load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

iv = load_module("vrp_iv_phase1_test", "notebooks/vrp_implied_variance_eod_update_v1.py")
feature = load_module("vrp_feature_phase1_test", "notebooks/vrp_locked_cell4_feature_panel_update_v1.py")
corsi = load_module("vrp_corsi_phase1_test", "notebooks/vrp_corsi_source_update_v1.py")

class ExpirationClockTests(unittest.TestCase):
    def test_spx_monthly_remains_am_settled(self):
        self.assertEqual(iv.production_settlement_minutes_after_midnight_et("SPX", 20251128), 9 * 60 + 30)

    def test_spxw_normal_session_uses_1600(self):
        self.assertEqual(iv.production_settlement_minutes_after_midnight_et("SPXW", 20251121), 16 * 60)

    def test_spxw_early_close_uses_1300(self):
        self.assertEqual(iv.production_settlement_minutes_after_midnight_et("SPXW", 20251128), 13 * 60)

    def test_same_day_early_close_has_zero_minutes_at_close(self):
        self.assertEqual(iv.production_minutes_to_expiry_vix_method(20251128, 20251128, "SPXW", 13 * 60 * 60 * 1000), 0)

class LockedFeatureSourceTests(unittest.TestCase):
    def setUp(self):
        dates = pd.bdate_range("2025-01-02", periods=80)
        spy_close = 500 * np.exp(np.linspace(0.0, 0.06, len(dates)))
        spy_ret = pd.Series(np.log(spy_close / np.roll(spy_close, 1)))
        spy_ret.iloc[0] = np.nan
        rows = []
        for date, close, ret in zip(dates, spy_close, spy_ret):
            for tenor in feature.EXPECTED_TENORS:
                rows.append({
                    "date": date, "tenor": tenor,
                    "spy_close": close, "spy_log_return": ret,
                    "spx_close": close * 10.0,
                    "eod_close_sanitized": close * 10.0,
                    "spx_log_return": 0.123,
                })
        self.source = pd.DataFrame(rows)

    def test_uses_spy_close_only(self):
        out = feature.build_daily_locked_features(self.source)
        expected = np.log(out["spy_close_for_features"] / out["spy_close_for_features"].shift(1))
        np.testing.assert_allclose(out["spy_log_return"].iloc[1:].to_numpy(), expected.iloc[1:].to_numpy(), rtol=0, atol=1e-15)
        np.testing.assert_allclose(out["spx_close_for_features"], out["spy_close_for_features"], rtol=0, atol=0)
        np.testing.assert_allclose(out["spx_log_return"].iloc[1:], out["spy_log_return"].iloc[1:], rtol=0, atol=0)
        self.assertEqual(out["feature_return_source"].unique().tolist(), ["SPY"])
        self.assertLess(float(out["spy_close_for_features"].iloc[-1]), 1000)

    def test_missing_spy_close_hard_fails(self):
        with self.assertRaisesRegex(ValueError, "canonical SPY fields"):
            feature.build_daily_locked_features(self.source.drop(columns=["spy_close"]))

    def test_spx_or_generic_close_cannot_substitute(self):
        broken = self.source.drop(columns=["spy_close", "spy_log_return"])
        with self.assertRaisesRegex(ValueError, "fallback is prohibited"):
            feature.build_daily_locked_features(broken)

    def test_scale_break_hard_fails_without_fallback(self):
        broken = self.source.copy()
        break_date = broken["date"].drop_duplicates().iloc[40]
        broken.loc[broken["date"].eq(break_date), "spy_close"] /= 10.0
        # Keep canonical return internally consistent so the scale-break check is reached.
        daily = broken[["date", "spy_close"]].drop_duplicates("date").sort_values("date")
        daily["spy_log_return"] = np.log(daily["spy_close"] / daily["spy_close"].shift(1))
        broken = broken.drop(columns=["spy_log_return"]).merge(daily[["date", "spy_log_return"]], on="date", how="left")
        with self.assertRaisesRegex(RuntimeError, "scale break"):
            feature.build_daily_locked_features(broken)

class TargetContractTests(unittest.TestCase):
    def test_maps_authoritative_corsi_target_to_locked_contract(self):
        source = pd.DataFrame({
            "date": pd.to_datetime(["2019-12-20", "2019-12-23"]),
            "tenor": [9, 9],
            "log_forward_realized_variance_corsi": [-4.2, -4.1],
            "last_forward_rv_date": ["2019-12-30", "2019-12-31"],
            "forward_window_complete_corsi": [True, True],
        })
        out = feature.materialize_target_contract_columns(source)
        np.testing.assert_allclose(
            out["target_log_variance"].to_numpy(),
            source["log_forward_realized_variance_corsi"].to_numpy(),
            rtol=0, atol=0,
        )
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(out["last_forward_rv_date"]))

    def test_conflicting_target_contract_hard_fails(self):
        source = pd.DataFrame({
            "date": [pd.Timestamp("2019-12-20")],
            "tenor": [9],
            "target_log_variance": [-3.0],
            "log_forward_realized_variance_corsi": [-4.2],
            "last_forward_rv_date": [pd.Timestamp("2019-12-30")],
            "forward_window_complete_corsi": [True],
        })
        with self.assertRaisesRegex(RuntimeError, "conflicts with the authoritative Corsi target"):
            feature.materialize_target_contract_columns(source)


class CanonicalSpyMergeTests(unittest.TestCase):
    def test_corsi_loader_returns_spy_and_spy_return(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "data/processed/market_data/spy_eod_prices_v1.parquet"
            path.parent.mkdir(parents=True)
            dates = pd.bdate_range("2026-01-02", periods=10)
            closes = np.linspace(680.0, 690.0, len(dates))
            source = pd.DataFrame({"trade_date": dates, "spy_close": closes})
            path.touch()
            expected = pd.DatetimeIndex(dates[1:])
            with patch.object(corsi.pd, "read_parquet", return_value=source), \
                 patch.object(corsi, "xnys_sessions", return_value=expected):
                out = corsi.load_canonical_spy_eod(root, dates[1], dates[-1])
            self.assertEqual(out["return_source_underlying"].unique().tolist(), ["SPY"])
            self.assertEqual(out["spx_source"].unique().tolist(), ["SPY_COMPATIBILITY_ALIAS"])
            self.assertTrue(out["spy_close"].between(600, 800).all())
            self.assertTrue(np.isfinite(out["spy_log_return"]).all())

    def test_non_session_start_date_is_calendar_aware(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "data/processed/market_data/spy_eod_prices_v1.parquet"
            path.parent.mkdir(parents=True)
            dates = pd.to_datetime(["2018-01-02", "2018-01-03", "2018-01-04"])
            source = pd.DataFrame({"trade_date": dates, "spy_close": [268.77, 270.47, 271.61]})
            path.touch()
            with patch.object(corsi.pd, "read_parquet", return_value=source), \
                 patch.object(corsi, "xnys_sessions", return_value=pd.DatetimeIndex(dates)):
                out = corsi.load_canonical_spy_eod(
                    root, pd.Timestamp("2018-01-01"), pd.Timestamp("2018-01-04")
                )
            self.assertEqual(out["date"].min(), pd.Timestamp("2018-01-02"))
            self.assertEqual(out["date"].max(), pd.Timestamp("2018-01-04"))

    def test_missing_expected_xnys_session_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "data/processed/market_data/spy_eod_prices_v1.parquet"
            path.parent.mkdir(parents=True)
            source_dates = pd.to_datetime(["2018-01-02", "2018-01-04"])
            expected_dates = pd.to_datetime(["2018-01-02", "2018-01-03", "2018-01-04"])
            source = pd.DataFrame({"trade_date": source_dates, "spy_close": [268.77, 271.61]})
            path.touch()
            with patch.object(corsi.pd, "read_parquet", return_value=source), \
                 patch.object(corsi, "xnys_sessions", return_value=pd.DatetimeIndex(expected_dates)):
                with self.assertRaisesRegex(RuntimeError, "missing required XNYS sessions"):
                    corsi.load_canonical_spy_eod(
                        root, pd.Timestamp("2018-01-01"), pd.Timestamp("2018-01-04")
                    )


class ModelPanelSpyContractTests(unittest.TestCase):
    def test_persists_canonical_spy_fields_and_overwrites_aliases(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            model_path = root / "model.parquet"
            control_path = root / "control.parquet"
            dates = pd.to_datetime(["2026-07-15", "2026-07-16"])
            model = pd.DataFrame({
                "date": np.repeat(dates, 2),
                "tenor": [9, 12, 9, 12],
                "spx_close": [7500.0, 7500.0, 7510.0, 7510.0],
                "spx_log_return": [0.10, 0.10, 0.20, 0.20],
            })
            control = pd.DataFrame({
                "date": np.repeat(dates, 2),
                "tenor": [9, 12, 9, 12],
                "spy_close": [754.81, 754.81, 750.72, 750.72],
                "spy_log_return": [0.0032, 0.0032, -0.0054, -0.0054],
                "return_source_underlying": ["SPY"] * 4,
                "return_source_version": ["canonical_spy_eod_prices_v1"] * 4,
            })
            model.to_parquet(model_path, index=False)
            control.to_parquet(control_path, index=False)
            audit = corsi.enforce_model_panel_spy_contract(model_path, control_path)
            out = pd.read_parquet(model_path)
            np.testing.assert_allclose(out["spy_close"], [754.81, 754.81, 750.72, 750.72], rtol=0, atol=0)
            np.testing.assert_allclose(out["spx_close"], out["spy_close"], rtol=0, atol=0)
            np.testing.assert_allclose(out["spx_log_return"], out["spy_log_return"], rtol=0, atol=0)
            self.assertEqual(out["return_source_underlying"].unique().tolist(), ["SPY"])
            self.assertEqual(out["spx_source"].unique().tolist(), ["SPY_COMPATIBILITY_ALIAS"])
            self.assertEqual(audit["rows"], 4)

    def test_nonconstant_control_contract_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            model_path = root / "model.parquet"
            control_path = root / "control.parquet"
            model = pd.DataFrame({"date": [pd.Timestamp("2026-07-16")], "tenor": [9]})
            control = pd.DataFrame({
                "date": [pd.Timestamp("2026-07-16"), pd.Timestamp("2026-07-16")],
                "tenor": [9, 12],
                "spy_close": [750.72, 751.00],
                "spy_log_return": [-0.0054, -0.0054],
                "return_source_underlying": ["SPY", "SPY"],
                "return_source_version": ["canonical_spy_eod_prices_v1", "canonical_spy_eod_prices_v1"],
            })
            model.to_parquet(model_path, index=False)
            control.to_parquet(control_path, index=False)
            with self.assertRaisesRegex(RuntimeError, "not daily-constant"):
                corsi.enforce_model_panel_spy_contract(model_path, control_path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
