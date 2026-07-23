from __future__ import annotations

import hashlib
import json
import math
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.eod_shadow import (  # noqa: E402
    EodOutputContractError,
    TARGET_TENORS,
    load_staged_eod_snapshot,
)
from vrp.eod_shadow.sofr_evidence import SofrUpdaterEvidence  # noqa: E402


LOCK_ID = "vrp_corsi_intraday_hybrid_v2"
TARGET = date(2026, 7, 21)


class StagedEodOutputTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fixture = self.root / "fixture.json"
        self.fixture.write_text("{}", encoding="utf-8")
        self.runtime = self.root / "runtime.json"
        self.production = self.root / "production.json"
        self.model = self.root / "model-lock.json"
        self.sofr_manifest = self.root / "sofr_update_manifest.json"
        self.sofr_snapshot = self.root / "fred_sofr_history_refreshed_snapshot.csv"
        self.model.write_text(json.dumps({"lock_id": LOCK_ID}), encoding="utf-8")
        self.sofr_manifest.write_text("{}", encoding="utf-8")
        self.sofr_snapshot.write_text("observation_date,SOFR\n2026-07-20,3.57\n", encoding="utf-8")
        digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        self.sofr_evidence = SofrUpdaterEvidence(
            updater_manifest_path=self.sofr_manifest.resolve(),
            updater_manifest_sha256=digest(self.sofr_manifest),
            refreshed_snapshot_path=self.sofr_snapshot.resolve(),
            refreshed_snapshot_sha256=digest(self.sofr_snapshot),
            normalized_content_sha256="b" * 64,
            start_date=date(2018, 4, 3),
            end_date=date(2026, 7, 20),
            row_count=2071,
            observation_date=date(2026, 7, 20),
            rate_decimal=Decimal("0.0357"),
            row_sha256="c" * 64,
        )
        self.production.write_text(
            json.dumps(
                {
                    "release_id": LOCK_ID,
                    "forecast_model": "intraday_ridge_locked",
                    "inactive": {"Core_Front": False, "tenor_9D": True},
                    "core_thresholds": [
                        {
                            "bucket": "Front",
                            "layer": "Core",
                            "rsi_cap": 99.0,
                            "rv21d_floor": 0.0,
                            "tenor": tenor,
                            "vrp_log": 99.0,
                            "z_1y": -99.0,
                            "z_3m": -99.0,
                        }
                        for tenor in (12, 15)
                    ],
                    "secondary_thresholds": [],
                    "sizes": [
                        {
                            "bucket": "Front",
                            "layer": "Core",
                            "size_pct_nav": size,
                            "tenor": tenor,
                        }
                        for tenor, size in ((12, 0.01), (15, 0.02))
                    ],
                    "selection": {
                        "one_trade_per_day": True,
                        "order": [
                            "size_desc",
                            "Core_before_Secondary",
                            "continuous_quality_desc",
                            "research_win_rate_desc",
                            "research_tail_desc",
                            "tenor_desc",
                        ],
                        "rule_id": "revised_size_benchmark",
                    },
                    "strict_operators": {
                        "rsi14": "<",
                        "rv21d_vol_pct": ">",
                        "vrp_log": ">",
                        "z_1y": ">",
                        "z_3m": ">",
                    },
                }
            ),
            encoding="utf-8",
        )
        self.runtime.write_text(
            json.dumps(
                {
                    "canonical": {"lock_manifest": str(self.model)},
                    "accepted_rsi_versions": ["accepted"],
                }
            ),
            encoding="utf-8",
        )
        self.tiebreaks = pd.DataFrame(
            [
                {
                    "layer": "Core",
                    "bucket": "Front",
                    "tenor": 12,
                    "continuous_quality_score": 0.875,
                    "research_sleeve_win_rate_pct": 92.5,
                    "research_sleeve_worst_1pct_mean_return": -0.20,
                },
                {
                    "layer": "Core",
                    "bucket": "Front",
                    "tenor": 15,
                    "continuous_quality_score": 0.900,
                    "research_sleeve_win_rate_pct": 93.0,
                    "research_sleeve_worst_1pct_mean_return": -0.19,
                },
            ]
        )
        self.signals, self.latest, self.decisions, self.forecast = self._frames()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _frames(self):
        prior_dates = [TARGET - timedelta(days=offset) for offset in range(253, 0, -1)]
        rows = []
        for tenor in TARGET_TENORS:
            prior_values = [0.02 + tenor / 1000 + index / 100_000 for index in range(253)]
            for prior_date, vrp in zip(prior_dates, prior_values, strict=True):
                prior_forecast = 0.04 + tenor / 10_000
                rows.append(
                    {
                        "date": prior_date,
                        "tenor": tenor,
                        "model_vrp_log": vrp,
                        "predicted_log_variance_candidate": math.log(prior_forecast),
                        "forecast_variance_candidate": prior_forecast,
                        "forecast_vol_pct": math.sqrt(prior_forecast) * 100,
                        "forecast_model": "intraday_ridge_locked",
                    }
                )
            current_vrp = prior_values[-1] + 0.001
            prior63 = pd.Series(prior_values[-63:])
            prior252 = pd.Series(prior_values[-252:])
            forecast_variance = 0.04 + tenor / 10_000
            implied_variance = forecast_variance * math.exp(current_vrp)
            rows.append(
                {
                    "date": TARGET,
                    "tenor": tenor,
                    "test_year": 2026,
                    "selected_alpha": 100.0,
                    "train_rows_used": 1000,
                    "predicted_log_variance_candidate": math.log(forecast_variance),
                    "forecast_variance_candidate": forecast_variance,
                    "forecast_vol_pct": math.sqrt(forecast_variance) * 100,
                    "forecast_model": "intraday_ridge_locked",
                    "implied_variance": implied_variance,
                    "implied_vol_pct": math.sqrt(implied_variance) * 100,
                    "model_vrp_log": current_vrp,
                    "z_3m": (current_vrp - prior63.mean()) / prior63.std(ddof=1),
                    "z_1y": (current_vrp - prior252.mean()) / prior252.std(ddof=1),
                    "prior_63_rows": 63,
                    "prior_252_rows": 252,
                    "spy_close": 620.0,
                    "rsi14": 52.0,
                    "rsi_formula_version": "accepted",
                    "rv21d_vol_pct": 12.0,
                    "core_bucket": "Front" if tenor in {12, 15} else None,
                    "core_threshold_vrp": 99.0 if tenor in {12, 15} else None,
                    "core_threshold_z3": -99.0 if tenor in {12, 15} else None,
                    "core_threshold_z1": -99.0 if tenor in {12, 15} else None,
                    "core_threshold_rsi": 99.0 if tenor in {12, 15} else None,
                    "core_threshold_rv": 0.0 if tenor in {12, 15} else None,
                    "core_rule_exists": tenor in {12, 15},
                    "core_size_pct_nav": (
                        0.01 if tenor == 12 else 0.02 if tenor == 15 else None
                    ),
                    "core_pass": False,
                    "core_failure_reason": (
                        "vrp<=99" if tenor in {12, 15} else "inactive"
                    ),
                    "secondary_bucket": None,
                    "secondary_threshold_vrp": None,
                    "secondary_threshold_z3": None,
                    "secondary_threshold_z1": None,
                    "secondary_threshold_rsi": None,
                    "secondary_threshold_rv": None,
                    "secondary_rule_exists": False,
                    "secondary_size_pct_nav": None,
                    "secondary_pass": False,
                    "secondary_failure_reason": "inactive",
                    "selected_layer": None,
                    "selected_tenor": None,
                    "selected_trade": False,
                    "lock_id": LOCK_ID,
                }
            )
        signals = pd.DataFrame(rows)
        latest = signals.loc[signals["date"].eq(TARGET)].copy()
        decisions = pd.DataFrame(
            [
                {
                    "date": decision_date,
                    "layer": None,
                    "tenor": None,
                    "size_pct_nav": None,
                    "target_max_risk_dollars": None,
                    "selection_reason": None,
                    "decision_status": "NO_TRADE",
                    "lock_id": LOCK_ID,
                    "selection_rule": "revised_size_benchmark",
                    "approved_nav_dollars": 1_000_000.0,
                    "bucket": None,
                    "continuous_quality_score": None,
                    "research_sleeve_win_rate_pct": None,
                    "research_sleeve_worst_1pct_mean_return": None,
                    "implied_variance": None,
                    "implied_vol_pct": None,
                    "forecast_variance_candidate": None,
                    "forecast_vol_pct": None,
                    "model_vrp_log": None,
                    "z_3m": None,
                    "z_1y": None,
                    "rsi14": None,
                    "rv21d_vol_pct": None,
                    "threshold_vrp": None,
                    "threshold_z3": None,
                    "threshold_z1": None,
                    "threshold_rsi": None,
                    "threshold_rv": None,
                }
                for decision_date in [*prior_dates, TARGET]
            ]
        )
        for column in ("layer", "bucket", "selection_reason"):
            decisions[column] = decisions[column].astype("object")
        signals["selected_layer"] = signals["selected_layer"].astype("object")
        signals["selected_tenor"] = signals["selected_tenor"].astype("object")
        forecast = signals[
            [
                "date",
                "tenor",
                "predicted_log_variance_candidate",
                "forecast_variance_candidate",
                "forecast_vol_pct",
                "forecast_model",
            ]
        ].copy()
        return signals, latest, decisions, forecast

    @staticmethod
    def _manifest_value(value):
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (date, datetime, pd.Timestamp)):
            return pd.Timestamp(value).isoformat()
        return value.item() if hasattr(value, "item") else value

    def _run_dir(self, name: str = "run", *, status: str = "PASS") -> Path:
        run = self.root / name
        staging = run / "staging"
        staging.mkdir(parents=True)
        files = {
            "signal_history": "vrp_hybrid_v2_signal_history.parquet",
            "latest_snapshot": "vrp_hybrid_v2_latest_snapshot.parquet",
            "selected_decisions": "vrp_hybrid_v2_selected_decisions.parquet",
            "forecast_history": "vrp_hybrid_v2_forecast_history.parquet",
        }
        target_decision = self.decisions.loc[self.decisions["date"].eq(TARGET)].iloc[0]
        for file_name in files.values():
            (staging / file_name).write_bytes(b"stable-test-artifact")
        tiebreak_path = staging / "vrp_hybrid_v2_static_tiebreaks.csv"
        self.tiebreaks.to_csv(tiebreak_path, index=False)
        publish_path = staging / "vrp_hybrid_v2_publish_manifest.json"
        manifest_decision = {
            key: self._manifest_value(value) for key, value in target_decision.items()
        }
        publish = {
            "release_id": LOCK_ID,
            "generated_at": "2026-07-21T22:00:00+00:00",
            "target_date": TARGET.isoformat(),
            "approved_nav": 1_000_000.0,
            "runtime_config": str(self.runtime),
            "lock_config": str(self.production),
            "latest_decision": [manifest_decision],
            "row_counts": {
                "signal_history": len(self.signals),
                "latest_snapshot": len(self.latest),
                "decision_history": len(self.decisions),
                "forecast_history": len(self.forecast),
            },
            "staged_outputs": {
                **{key: str(staging / value) for key, value in files.items()},
                "manifest": str(publish_path),
                "tiebreaks": str(tiebreak_path),
            },
        }
        publish_path.write_text(json.dumps(publish), encoding="utf-8")
        manifest = {
            "status": status,
            "final_health": "PASS",
            "release_id": LOCK_ID,
            "target_date": TARGET.isoformat(),
            "approved_nav": 1_000_000.0,
            "finished_at": "2026-07-21T22:01:00+00:00",
            "run_timestamp": name,
            "publish_manifest": str(publish_path),
            "sofr_manifest": str(self.sofr_manifest),
        }
        (run / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return run

    def _set_valid_trade(self, *, tenor: int = 12, layer: str = "Core") -> None:
        target_mask = self.signals["date"].eq(TARGET)
        selected_mask = target_mask & self.signals["tenor"].eq(tenor)
        self.assertEqual(int(selected_mask.sum()), 1)
        selected = self.signals.loc[selected_mask].iloc[0]
        prefix = layer.lower()

        production = json.loads(self.production.read_text(encoding="utf-8"))
        if layer.upper() == "CORE":
            threshold_entry = next(
                entry
                for entry in production["core_thresholds"]
                if int(entry["tenor"]) == tenor
            )
        else:
            threshold_entry = next(
                entry
                for entry in production["secondary_thresholds"]
                if tenor in [int(value) for value in entry["tenors"]]
            )
        size_entry = next(
            entry
            for entry in production["sizes"]
            if str(entry["layer"]).upper() == layer.upper()
            and int(entry["tenor"]) == tenor
        )
        bucket = str(threshold_entry["bucket"])
        size = float(size_entry["size_pct_nav"])

        thresholds = {
            "vrp": float(selected["model_vrp_log"]) - 0.1,
            "z3": float(selected["z_3m"]) - 0.1,
            "z1": float(selected["z_1y"]) - 0.1,
            "rsi": float(selected["rsi14"]) + 1.0,
            "rv": float(selected["rv21d_vol_pct"]) - 1.0,
        }
        threshold_config_names = {
            "vrp": "vrp_log",
            "z3": "z_3m",
            "z1": "z_1y",
            "rsi": "rsi_cap",
            "rv": "rv21d_floor",
        }
        for short_name, value in thresholds.items():
            threshold_entry[threshold_config_names[short_name]] = value
        self.production.write_text(json.dumps(production), encoding="utf-8")

        self.signals.loc[selected_mask, f"{prefix}_bucket"] = bucket
        self.signals.loc[selected_mask, f"{prefix}_rule_exists"] = True
        self.signals.loc[selected_mask, f"{prefix}_size_pct_nav"] = size
        self.signals.loc[selected_mask, f"{prefix}_pass"] = True
        self.signals.loc[selected_mask, f"{prefix}_failure_reason"] = "PASS"
        for short_name, value in thresholds.items():
            self.signals.loc[selected_mask, f"{prefix}_threshold_{short_name}"] = value

        self.signals.loc[target_mask, "selected_trade"] = False
        self.signals.loc[selected_mask, "selected_trade"] = True
        self.signals.loc[target_mask, "selected_layer"] = layer
        self.signals.loc[target_mask, "selected_tenor"] = tenor
        self.latest = self.signals.loc[target_mask].copy()

        selected = self.signals.loc[selected_mask].iloc[0]
        decision_mask = self.decisions["date"].eq(TARGET)
        metrics = self.tiebreaks.loc[
            self.tiebreaks["layer"].str.upper().eq(layer.upper())
            & self.tiebreaks["tenor"].eq(tenor)
        ].iloc[0]
        decision_values = {
            "layer": layer,
            "tenor": tenor,
            "bucket": bucket,
            "size_pct_nav": size,
            "target_max_risk_dollars": 1_000_000.0 * size,
            "selection_reason": "highest locked quality score",
            "decision_status": "TRADE",
            "continuous_quality_score": float(metrics["continuous_quality_score"]),
            "research_sleeve_win_rate_pct": float(metrics["research_sleeve_win_rate_pct"]),
            "research_sleeve_worst_1pct_mean_return": float(
                metrics["research_sleeve_worst_1pct_mean_return"]
            ),
            "implied_variance": float(selected["implied_variance"]),
            "implied_vol_pct": float(selected["implied_vol_pct"]),
            "forecast_variance_candidate": float(selected["forecast_variance_candidate"]),
            "forecast_vol_pct": float(selected["forecast_vol_pct"]),
            "model_vrp_log": float(selected["model_vrp_log"]),
            "z_3m": float(selected["z_3m"]),
            "z_1y": float(selected["z_1y"]),
            "rsi14": float(selected["rsi14"]),
            "rv21d_vol_pct": float(selected["rv21d_vol_pct"]),
            **{f"threshold_{name}": value for name, value in thresholds.items()},
        }
        for column, value in decision_values.items():
            self.decisions.loc[decision_mask, column] = value

    def _read_parquet(self, path):
        name = Path(path).name
        return {
            "vrp_hybrid_v2_signal_history.parquet": self.signals,
            "vrp_hybrid_v2_latest_snapshot.parquet": self.latest,
            "vrp_hybrid_v2_selected_decisions.parquet": self.decisions,
            "vrp_hybrid_v2_forecast_history.parquet": self.forecast,
        }[name].copy()

    @staticmethod
    def _golden(*args, **kwargs):
        fixture_path = Path(args[1])
        signal_path = Path(kwargs["signal_history_path"])
        decision_path = Path(kwargs["selected_decisions_path"])
        digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        return [], {
            "status": "PASS",
            "mode": "STAGED",
            "verification_id": "a" * 64,
            "fixture": {"sha256": digest(fixture_path)},
            "artifacts": {
                "signal_history": {"sha256": digest(signal_path)},
                "selected_decisions": {"sha256": digest(decision_path)},
            },
        }

    def _load(self, run: Path):
        with (
            patch("vrp.eod_shadow.outputs.pd.read_parquet", side_effect=self._read_parquet),
            patch(
                "vrp.eod_shadow.outputs.load_sofr_updater_evidence",
                return_value=self.sofr_evidence,
            ),
            patch("vrp.eod_shadow.outputs.verify_golden_contract_with_manifest", side_effect=self._golden) as golden,
            patch(
                "vrp.eod_shadow.outputs._official_xnys_close",
                return_value=datetime(2026, 7, 21, 20, 0, tzinfo=timezone.utc),
            ),
        ):
            result = load_staged_eod_snapshot(run, self.root, fixture_path=self.fixture)
        self.assertEqual(golden.call_count, 1)
        return result

    def _assert_load_fails(self, run: Path, pattern: str) -> None:
        with (
            patch("vrp.eod_shadow.outputs.pd.read_parquet", side_effect=self._read_parquet),
            patch(
                "vrp.eod_shadow.outputs.load_sofr_updater_evidence",
                return_value=self.sofr_evidence,
            ),
            patch(
                "vrp.eod_shadow.outputs.verify_golden_contract_with_manifest",
                side_effect=self._golden,
            ),
            patch(
                "vrp.eod_shadow.outputs._official_xnys_close",
                return_value=datetime(2026, 7, 21, 20, 0, tzinfo=timezone.utc),
            ),
            self.assertRaisesRegex(EodOutputContractError, pattern),
        ):
            load_staged_eod_snapshot(run, self.root, fixture_path=self.fixture)

    def test_normalizes_complete_no_trade_snapshot_and_prior_windows(self):
        snapshot = self._load(self._run_dir())
        self.assertEqual([row.tenor_days for row in snapshot.signal_features], list(TARGET_TENORS))
        self.assertEqual(len(snapshot.signal_evaluations), 18)
        core_12 = next(row for row in snapshot.signal_evaluations if row.evaluation_key == "CORE:12")
        self.assertEqual(core_12.failed_checks, ("VRP",))
        inactive = next(row for row in snapshot.signal_evaluations if row.evaluation_key == "CORE:9")
        self.assertEqual(inactive.evaluation_status, "INACTIVE")
        self.assertEqual(inactive.failed_checks, ("RULE_INACTIVE",))
        self.assertEqual(snapshot.selected_signal.no_trade_reason, "NO_CORE_OR_SECONDARY_SIGNAL_QUALIFIED")
        self.assertAlmostEqual(snapshot.signal_features[0].rv21d_variance, 0.0144)
        self.assertEqual(snapshot.implied_variance[0].target_expiration, TARGET + timedelta(days=9))
        self.assertEqual(snapshot.implied_variance[0].effective_dte, 9.0)
        self.assertEqual(snapshot.snapshot_at, datetime(2026, 7, 21, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(len(snapshot.content_sha256), 64)
        self.assertTrue(snapshot.configuration_identity.version_label.startswith(f"{LOCK_ID}:"))
        self.assertNotEqual(snapshot.configuration_identity.version_label, LOCK_ID)

    def test_rejects_run_that_is_not_successful_before_reading_parquet(self):
        run = self._run_dir(status="FAILED_NOT_PUBLISHED")
        with patch("vrp.eod_shadow.outputs.pd.read_parquet") as read:
            with self.assertRaisesRegex(EodOutputContractError, "status=PASS"):
                load_staged_eod_snapshot(run, self.root, fixture_path=self.fixture)
        read.assert_not_called()

    def test_rejects_a_run_manifest_that_disagrees_with_the_caller_pin(self):
        run = self._run_dir()
        with patch("vrp.eod_shadow.outputs.pd.read_parquet") as read:
            with self.assertRaisesRegex(EodOutputContractError, "caller-pinned"):
                load_staged_eod_snapshot(
                    run,
                    self.root,
                    fixture_path=self.fixture,
                    expected_run_manifest_sha256="0" * 64,
                )
        read.assert_not_called()

    def test_content_identity_excludes_run_paths_and_timestamps(self):
        first = self._load(self._run_dir("first"))
        second_run = self._run_dir("second")
        publish_path = second_run / "staging/vrp_hybrid_v2_publish_manifest.json"
        publish = json.loads(publish_path.read_text(encoding="utf-8"))
        publish["generated_at"] = "2026-07-21T23:00:00+00:00"
        publish_path.write_text(json.dumps(publish), encoding="utf-8")
        run_manifest_path = second_run / "run_manifest.json"
        run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
        run_manifest["finished_at"] = "2026-07-21T23:01:00+00:00"
        run_manifest_path.write_text(json.dumps(run_manifest), encoding="utf-8")
        second = self._load(second_run)
        self.assertEqual(first.projection(), second.projection())
        self.assertEqual(first.content_identity, second.content_identity)
        self.assertEqual(first.content_sha256, second.content_sha256)
        manifest_assets = [artifact for artifact in first.artifacts if not artifact.identity_input]
        self.assertEqual({artifact.logical_name for artifact in manifest_assets}, {"run_manifest", "publish_manifest"})

    def test_normalizes_trade_selection_rank_trace_and_reconciled_payload(self):
        self._set_valid_trade()
        snapshot = self._load(self._run_dir("trade"))

        self.assertEqual(snapshot.selected_signal.decision, "TRADE")
        self.assertEqual(snapshot.selected_signal.selected_evaluation_key, "CORE:12")
        self.assertIsNone(snapshot.selected_signal.no_trade_reason)
        self.assertEqual(snapshot.selected_signal.target_max_risk_dollars, 10_000.0)
        trace = snapshot.selected_signal.selection_trace
        self.assertEqual(trace["selected"], "CORE:12")
        self.assertEqual(trace["qualified_candidates"], ["CORE:12"])
        self.assertEqual(trace["legacy_selection_reason"], "highest locked quality score")
        self.assertEqual(trace["continuous_quality_score"], 0.875)
        self.assertFalse(trace["tie_break_applied"])

        selected = next(
            row
            for row in snapshot.signal_evaluations
            if row.evaluation_key == "CORE:12"
        )
        self.assertTrue(selected.qualifies)
        self.assertEqual(selected.evaluation_status, "QUALIFIED")
        self.assertEqual(selected.failed_checks, ())
        self.assertEqual(selected.rank_position, 1)
        self.assertEqual(selected.rank_score, 0.875)
        self.assertEqual(selected.target_size_pct_nav, 0.01)

    def test_rejects_trade_decision_value_that_disagrees_with_selected_signal(self):
        self._set_valid_trade()
        decision_mask = self.decisions["date"].eq(TARGET)
        self.decisions.loc[decision_mask, "implied_variance"] += 0.001

        self._assert_load_fails(
            self._run_dir("trade-decision-mismatch"),
            "TRADE decision implied_variance does not match the target signal row",
        )

    def test_rejects_variance_volatility_identity_mismatch(self):
        target_nine = self.signals["date"].eq(TARGET) & self.signals["tenor"].eq(9)
        self.signals.loc[target_nine, "implied_vol_pct"] += 0.5
        self.latest = self.signals.loc[self.signals["date"].eq(TARGET)].copy()

        self._assert_load_fails(
            self._run_dir("variance-volatility-mismatch"),
            r"9D implied volatility does not equal 100\*sqrt\(variance\)",
        )

    def test_rejects_truncated_latest_snapshot_schema(self):
        self.latest = self.latest[["date", "tenor"]].copy()
        self._assert_load_fails(
            self._run_dir("truncated-latest-schema"),
            "latest snapshot schema does not match",
        )

    def test_rejects_missing_manifest_count_and_decision_drift(self):
        missing_count = self._run_dir("missing-row-count")
        publish_path = missing_count / "staging/vrp_hybrid_v2_publish_manifest.json"
        payload = json.loads(publish_path.read_text(encoding="utf-8"))
        del payload["row_counts"]["forecast_history"]
        publish_path.write_text(json.dumps(payload), encoding="utf-8")
        self._assert_load_fails(missing_count, "row_counts is missing required keys")

        decision_drift = self._run_dir("manifest-decision-drift")
        publish_path = decision_drift / "staging/vrp_hybrid_v2_publish_manifest.json"
        payload = json.loads(publish_path.read_text(encoding="utf-8"))
        payload["latest_decision"][0]["approved_nav_dollars"] = 999_999.0
        publish_path.write_text(json.dumps(payload), encoding="utf-8")
        self._assert_load_fails(
            decision_drift,
            "latest decision approved_nav_dollars disagrees",
        )

    def test_rejects_trade_only_values_on_no_trade_decision(self):
        decision_target = self.decisions["date"].eq(TARGET)
        self.decisions.loc[decision_target, "selection_reason"] = "should be null"
        self._assert_load_fails(
            self._run_dir("no-trade-payload"),
            "NO_TRADE decision contains trade-only values",
        )

    def test_rejects_preclose_generation_and_reversed_completion_timestamps(self):
        preclose = self._run_dir("preclose")
        preclose_publish = preclose / "staging/vrp_hybrid_v2_publish_manifest.json"
        payload = json.loads(preclose_publish.read_text(encoding="utf-8"))
        payload["generated_at"] = "2026-07-21T19:59:59+00:00"
        preclose_publish.write_text(json.dumps(payload), encoding="utf-8")
        self._assert_load_fails(preclose, "generated before the official XNYS close")

        reversed_run = self._run_dir("reversed-timestamps")
        reversed_manifest = reversed_run / "run_manifest.json"
        payload = json.loads(reversed_manifest.read_text(encoding="utf-8"))
        payload["finished_at"] = "2026-07-21T21:59:59+00:00"
        reversed_manifest.write_text(json.dumps(payload), encoding="utf-8")
        self._assert_load_fails(
            reversed_run,
            "completed run finished before its staged publish artifact was generated",
        )

        late_start = self._run_dir("late-start")
        late_start_manifest = late_start / "run_manifest.json"
        payload = json.loads(late_start_manifest.read_text(encoding="utf-8"))
        payload["started_at"] = "2026-07-21T22:00:01+00:00"
        late_start_manifest.write_text(json.dumps(payload), encoding="utf-8")
        self._assert_load_fails(
            late_start,
            "completed run started after its staged publish artifact was generated",
        )

    def test_rejects_incomplete_prior_history_tenor_grid(self):
        prior_date = min(self.signals["date"])
        missing_row = self.signals["date"].eq(prior_date) & self.signals["tenor"].eq(33)
        self.assertEqual(int(missing_row.sum()), 1)
        self.signals = self.signals.loc[~missing_row].copy()

        self._assert_load_fails(
            self._run_dir("incomplete-history-grid"),
            "signal history has incomplete or unexpected tenor grids",
        )

    def test_rejects_values_attached_to_an_inactive_rule(self):
        inactive = self.signals["date"].eq(TARGET) & self.signals["tenor"].eq(9)
        self.signals.loc[inactive, "core_threshold_vrp"] = 0.0
        self.latest = self.signals.loc[self.signals["date"].eq(TARGET)].copy()

        self._assert_load_fails(
            self._run_dir("inactive-rule-contradiction"),
            "CORE:9 inactive rule contains bucket, threshold, or size values",
        )

    def test_rejects_target_rule_values_that_drift_from_locked_configuration(self):
        target_12 = self.signals["date"].eq(TARGET) & self.signals["tenor"].eq(12)
        self.signals.loc[target_12, "core_threshold_vrp"] += 0.01
        self.latest = self.signals.loc[self.signals["date"].eq(TARGET)].copy()

        self._assert_load_fails(
            self._run_dir("locked-threshold-drift"),
            "target CORE:12 threshold_vrp does not match the locked production configuration",
        )

    def test_rejects_active_map_and_inactive_flag_drift(self):
        target_15 = self.signals["date"].eq(TARGET) & self.signals["tenor"].eq(15)
        self.signals.loc[target_15, "core_rule_exists"] = False
        self.latest = self.signals.loc[self.signals["date"].eq(TARGET)].copy()
        self._assert_load_fails(
            self._run_dir("locked-active-map-drift"),
            "target CORE active tenor map does not match",
        )

        self.signals.loc[target_15, "core_rule_exists"] = True
        self.latest = self.signals.loc[self.signals["date"].eq(TARGET)].copy()
        production = json.loads(self.production.read_text(encoding="utf-8"))
        production["inactive"]["Core_Front"] = True
        self.production.write_text(json.dumps(production), encoding="utf-8")
        self._assert_load_fails(
            self._run_dir("locked-inactive-flag-drift"),
            "Core_Front inactive flag contradicts its rule map",
        )

    def test_rejects_forecast_operator_and_selection_rule_drift(self):
        target = self.signals["date"].eq(TARGET)
        self.signals.loc[target, "forecast_model"] = "wrong_model"
        self.latest = self.signals.loc[target].copy()
        forecast_target = self.forecast["date"].eq(TARGET)
        self.forecast.loc[forecast_target, "forecast_model"] = "wrong_model"
        self._assert_load_fails(
            self._run_dir("locked-forecast-drift"),
            "target forecast model does not match the locked production configuration",
        )

        self.signals.loc[target, "forecast_model"] = "intraday_ridge_locked"
        self.latest = self.signals.loc[target].copy()
        self.forecast.loc[forecast_target, "forecast_model"] = "intraday_ridge_locked"
        production = json.loads(self.production.read_text(encoding="utf-8"))
        production["strict_operators"]["rsi14"] = "<="
        self.production.write_text(json.dumps(production), encoding="utf-8")
        self._assert_load_fails(
            self._run_dir("locked-operator-drift"),
            "strict_operators do not match",
        )

        production["strict_operators"]["rsi14"] = "<"
        self.production.write_text(json.dumps(production), encoding="utf-8")
        decision_target = self.decisions["date"].eq(TARGET)
        self.decisions.loc[decision_target, "selection_rule"] = "wrong_rule"
        self._assert_load_fails(
            self._run_dir("locked-selection-rule-drift"),
            "staged decision selection_rule does not match",
        )

    def test_rejects_trade_that_is_not_locked_winner(self):
        decision_target = self.decisions["date"].eq(TARGET)
        signal_target = self.signals["date"].eq(TARGET)
        self._set_valid_trade(tenor=12)
        selected_12 = self.decisions.loc[decision_target].copy()
        self._set_valid_trade(tenor=15)
        for column in self.decisions.columns:
            self.decisions.loc[decision_target, column] = selected_12.iloc[0][column]
        self.signals.loc[signal_target, "selected_trade"] = False
        self.signals.loc[signal_target & self.signals["tenor"].eq(12), "selected_trade"] = True
        self.signals.loc[signal_target, "selected_layer"] = "Core"
        self.signals.loc[signal_target, "selected_tenor"] = 12
        self.latest = self.signals.loc[signal_target].copy()

        self._assert_load_fails(
            self._run_dir("wrong-locked-winner"),
            "staged TRADE decision is not the winner under locked selection.order",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
