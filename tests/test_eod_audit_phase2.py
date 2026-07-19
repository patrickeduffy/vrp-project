from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "notebooks"))


def load_module(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


iv = load_module("phase2_iv", "notebooks/vrp_implied_variance_eod_update_v1.py")
rsi = load_module("phase2_rsi", "notebooks/vrp_hybrid_v2_wilder_rsi_update.py")
publisher = load_module("phase2_publisher", "notebooks/vrp_hybrid_v2_signal_publish.py")


class SofrAndUpsertTests(unittest.TestCase):
    def test_explicit_upsert_new_key_wins_with_null_old_timestamp(self):
        old = pd.DataFrame({
            "trade_date": [pd.Timestamp("2026-07-02")],
            "tenor": [9],
            "target_days": [9],
            "rate_pct": [3.66],
            "run_timestamp": [None],
        })
        new = pd.DataFrame({
            "trade_date": [pd.Timestamp("2026-07-02")],
            "tenor": [9],
            "target_days": [9],
            "rate_pct": [3.64],
            "run_timestamp": ["20260713_195309"],
        })
        out = iv.upsert_rows(old, new, None)
        self.assertEqual(len(out), 1)
        self.assertAlmostEqual(float(out.iloc[0]["rate_pct"]), 3.64)

    def test_duplicate_new_keys_fail(self):
        old = pd.DataFrame(columns=["trade_date", "tenor", "target_days"])
        new = pd.DataFrame({"trade_date": ["2026-07-02", "2026-07-02"], "tenor": [9, 9]})
        with self.assertRaises(RuntimeError):
            iv.upsert_rows(old, new, None)


    def test_t_minus_one_rate_selection_excludes_same_date(self):
        rates = pd.DataFrame({
            "trade_date": pd.to_datetime(["2026-07-01", "2026-07-02", "2026-07-06"]),
            "rate_decimal": [0.0366, 0.0364, 0.0363],
        })
        july2 = iv.select_prior_rate_record(rates, "2026-07-02")
        self.assertEqual(july2["rate_observation_date"], pd.Timestamp("2026-07-01"))
        self.assertAlmostEqual(july2["rate_decimal"], 0.0366)
        july6 = iv.select_prior_rate_record(rates, "2026-07-06")
        self.assertEqual(july6["rate_observation_date"], pd.Timestamp("2026-07-02"))
        self.assertAlmostEqual(july6["rate_decimal"], 0.0364)


class RsiRepairTests(unittest.TestCase):
    def synthetic_inputs(self):
        dates = pd.bdate_range("2018-01-02", periods=40)
        closes = 270.0 + np.cumsum(np.sin(np.arange(40) / 3.0) + 0.2)
        canonical = pd.DataFrame({"trade_date": dates, "spy_close": closes})
        existing = pd.DataFrame({
            "trade_date": dates,
            "spy_close": closes,
            "spy_change": np.r_[0.0, np.diff(closes)],
            "wilder_avg_gain_14": 0.5,
            "wilder_avg_loss_14": 0.4,
            "spy_wilder_rsi14": rsi.compute_rsi(0.5, 0.4),
            "rsi_formula_version": "wilder_rsi14_spy_close_v2_long_warmup",
            "source_name": "legacy",
        })
        # Inject a corrupted change and recursive state after the seed.
        existing.loc[10, "spy_change"] = 999.0
        existing.loc[10:, "wilder_avg_gain_14"] = 50.0
        existing.loc[10:, "spy_wilder_rsi14"] = 99.0
        return canonical, existing

    def test_formula_migration_rebuilds_every_row_after_initial_seed(self):
        canonical, existing = self.synthetic_inputs()
        start, reasons = rsi.find_recalc_start(canonical, existing, force_full_refresh=False)
        self.assertEqual(start, canonical.loc[1, "trade_date"])
        self.assertTrue(any("formula_migration" in value for value in reasons))
        output, meta = rsi.rebuild_from_seed(canonical, existing, start)
        self.assertEqual(meta["preserved_rows"], 1)
        self.assertTrue(output["rsi_formula_version"].eq(rsi.FORMULA_VERSION).all())
        diag = rsi.recurrence_diagnostics(output)
        self.assertLessEqual(max(v for k, v in diag.items() if k.startswith("max_")), 1e-10)

    def test_validation_detects_corrupted_change(self):
        canonical, existing = self.synthetic_inputs()
        start, _ = rsi.find_recalc_start(canonical, existing, force_full_refresh=False)
        output, _ = rsi.rebuild_from_seed(canonical, existing, start)
        output.loc[12, "spy_change"] += 1.0
        validation = rsi.validate_output(output, canonical, canonical["trade_date"].max())
        status = validation.set_index("check").loc["spy_change_recurrence_exact", "status"]
        self.assertEqual(status, "FAIL")


class FitContractTests(unittest.TestCase):
    def make_fit_log(self, path: Path, corrupt: str | None = None):
        rows = []
        for tenor in publisher.EXPECTED_TENORS:
            rows.append({
                "model_spec": publisher.MODEL_SPEC,
                "tenor": tenor,
                "test_year": 2019,
                "fit_status": "skipped_insufficient_train_rows",
                "selected_alpha": np.nan,
                "train_rows_used": 60,
                "test_rows_scored": 0,
            })
            rows.append({
                "model_spec": publisher.MODEL_SPEC,
                "tenor": tenor,
                "test_year": 2020,
                "fit_status": "candidate_fit",
                "selected_alpha": 100.0,
                "train_rows_used": 250,
                "test_rows_scored": 252,
            })
        frame = pd.DataFrame(rows)
        if corrupt == "blank_active_alpha":
            frame.loc[frame["fit_status"].eq("candidate_fit"), "selected_alpha"] = np.nan
        if corrupt == "scored_skipped":
            frame.loc[frame["fit_status"].eq("skipped_insufficient_train_rows"), "test_rows_scored"] = 1
        frame.to_csv(path, index=False)

    def test_skipped_2019_rows_are_not_active_contracts(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "fit.csv"
            self.make_fit_log(path)
            active = publisher.load_fit_log(path)
            self.assertEqual(set(active["test_year"]), {2020})
            self.assertEqual(active.attrs["ignored_skipped_rows"], len(publisher.EXPECTED_TENORS))

    def test_blank_alpha_on_active_contract_fails(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "fit.csv"
            self.make_fit_log(path, "blank_active_alpha")
            with self.assertRaises(RuntimeError):
                publisher.load_fit_log(path)

    def test_skipped_contract_cannot_score_test_rows(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "fit.csv"
            self.make_fit_log(path, "scored_skipped")
            with self.assertRaises(RuntimeError):
                publisher.load_fit_log(path)


class ReferenceGuardTests(unittest.TestCase):
    def test_publish_and_reference_override_are_mutually_exclusive(self):
        # This is enforced before any file IO.
        cfg = publisher.PublishConfig(
            project_root=ROOT,
            runtime_config_path=ROOT / "missing",
            lock_config_path=ROOT / "missing",
            target_date=pd.Timestamp("2026-07-17"),
            approved_nav=1_000_000,
            feature_panel=ROOT / "missing",
            component_source=ROOT / "missing",
            forecast_benchmark=ROOT / "missing",
            locked_forecast_reference=ROOT / "missing",
            implied_variance=ROOT / "missing",
            spy_eod=ROOT / "missing",
            rv21d=ROOT / "missing",
            rsi_history=ROOT / "missing",
            fit_log=ROOT / "missing",
            staging_dir=ROOT / "missing",
            publish=True,
            allow_reference_difference=True,
        )
        with self.assertRaises(RuntimeError):
            publisher.run_publish(cfg)


if __name__ == "__main__":
    unittest.main()
