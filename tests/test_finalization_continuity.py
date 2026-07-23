from __future__ import annotations

import copy
import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.orchestration.eod_finalization_gate import (  # noqa: E402
    CompletedEodFinalizationEvidence,
)
from vrp.eod_shadow.service import database_readback_fingerprint  # noqa: E402
from vrp.storage.finalization_continuity import (  # noqa: E402
    DatabaseFinalizationContinuityError,
    verify_database_finalization_continuity,
)


TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
EVALUATIONS = [[tenor, layer] for tenor in TENORS for layer in ("CORE", "SECONDARY")]
METRICS = {
    "forecast_variance_count": 9,
    "implied_variance_count": 9,
    "selected_signal_count": 1,
    "signal_evaluation_count": 18,
    "signal_feature_count": 9,
}
DATABASE_PROJECTION_SHA256 = "4" * 64
EFFECTIVE_INPUT_SHA256 = "f" * 64
GOLDEN_VERIFICATION_ID = "e" * 64
SIGNAL_HISTORY_SHA256 = "1" * 64
SELECTED_DECISIONS_SHA256 = "2" * 64
GOLDEN_FIXTURE_SHA256 = "3" * 64


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = connection.rows
        self.projections = connection.projections
        self.row = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        if query.startswith("SET LOCAL TIME ZONE"):
            if self.connection.autocommit and not self.connection.transaction_depth:
                raise AssertionError(
                    "SET LOCAL executed outside an explicit transaction"
                )
            self.row = None
            return
        self.params = params
        if "'market_snapshot', (" in query:
            projection = self.projections.get(str(params[0]))
            self.row = None if projection is None else (projection,)
            return
        if "vrp.pipeline_runs" not in query:
            raise AssertionError(query)
        payload = self.rows.get(str(params[0]))
        self.row = None if payload is None else (payload,)

    def fetchone(self):
        return self.row


class FakeConnection:
    class _Transaction:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            self.connection.transaction_entries += 1
            self.connection.transaction_depth += 1
            return self

        def __exit__(self, *_args):
            self.connection.transaction_depth -= 1
            return False

    def __init__(self, rows, projections=None, *, autocommit=False):
        self.rows = rows
        self.projections = projections or {}
        self.autocommit = autocommit
        self.transaction_calls = 0
        self.transaction_entries = 0
        self.transaction_depth = 0

    def cursor(self):
        return FakeCursor(self)

    def transaction(self):
        self.transaction_calls += 1
        return self._Transaction(self)


def _asset(
    logical_name: str,
    content_sha256: str,
    *,
    identity_input: bool,
) -> dict:
    relative_path = f"staging/{logical_name}.json"
    if logical_name == "golden_eod_fixture":
        usage_role = "QA_EVIDENCE"
        asset_class = "REPORT"
    elif logical_name in {"run_manifest", "publish_manifest"}:
        usage_role = "MANIFEST"
        asset_class = "MANIFEST"
    else:
        usage_role = "INPUT"
        asset_class = "DERIVED"
    return {
        "data_asset_id": f"asset-{logical_name}",
        "dataset_name": f"vrp_eod_shadow/{logical_name}",
        "asset_class": asset_class,
        "asset_format": "JSON",
        "storage_uri": f"file:///C:/vrp/{relative_path}",
        "content_sha256": content_sha256,
        "schema_version": "eod-shadow-v1",
        "source_system": "VRP_HYBRID_V2_EOD",
        "row_count": 1,
        "byte_size": 100,
        "is_immutable": True,
        "metadata": {
            "identity_input": identity_input,
            "logical_name": logical_name,
            "relative_path": relative_path,
        },
        "usage_role": usage_role,
        "logical_name": logical_name,
        "stage_name": "RECORD_EOD_SHADOW",
        "is_required": True,
        "lineage": {
            "content_sha256": content_sha256,
            "identity_input": identity_input,
            "relative_path": relative_path,
        },
    }


class FinalizationContinuityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.current_projection = {
            "run": {
                "pipeline_run_id": "00000000-0000-0000-0000-000000000001",
                "environment": "local",
            },
            "assets": [
                {
                    "logical_name": "signal_history",
                    "content_sha256": SIGNAL_HISTORY_SHA256,
                }
            ],
            "market_snapshot": {
                "market_snapshot_id": "00000000-0000-0000-0000-000000000002",
            },
            "implied_variances": [
                {"tenor_days": tenor, "annualized_variance": tenor / 1000.0}
                for tenor in TENORS
            ],
            "forecast_variances": [{"tenor_days": tenor} for tenor in TENORS],
            "signal_features": [{"tenor_days": tenor} for tenor in TENORS],
            "signal_evaluations": [
                {"tenor_days": tenor, "signal_layer": layer}
                for tenor, layer in (tuple(value) for value in EVALUATIONS)
            ],
            "selected_signal": {
                "selected_signal_id": "00000000-0000-0000-0000-000000000003",
            },
        }
        self.evidence = CompletedEodFinalizationEvidence(
            run_dir=Path("C:/vrp/data/audit/vrp_hybrid_v2_eod/20260721_170000"),
            pipeline_run_id="00000000-0000-0000-0000-000000000001",
            market_snapshot_id="00000000-0000-0000-0000-000000000002",
            selected_signal_id="00000000-0000-0000-0000-000000000003",
            environment="local",
            code_version="a" * 40,
            snapshot_content_sha256="b" * 64,
            run_manifest_sha256="c" * 64,
            source_bundle_sha256="d" * 64,
            database_projection_sha256=DATABASE_PROJECTION_SHA256,
            database_readback_sha256=database_readback_fingerprint(
                self.current_projection
            ),
        )
        self.exact_payload = self._exact_payload()

    def _exact_payload(self) -> dict:
        golden_evidence = {
            "fixture_sha256": GOLDEN_FIXTURE_SHA256,
            "selected_decisions_sha256": SELECTED_DECISIONS_SHA256,
            "signal_history_sha256": SIGNAL_HISTORY_SHA256,
            "verification_id": GOLDEN_VERIFICATION_ID,
        }
        readback_value = {
            "output_fingerprint": DATABASE_PROJECTION_SHA256,
            **METRICS,
        }
        return {
            "run": {
                "environment": self.evidence.environment,
                "code_version": self.evidence.code_version,
                "run_kind": "EOD",
                "orchestrator_version": "eod-postgres-shadow-v1",
                "status": "COMPLETED",
                "qa_status": "PASS",
                "snapshot_content_sha256": self.evidence.snapshot_content_sha256,
                "effective_input_sha256": EFFECTIVE_INPUT_SHA256,
                "golden_verification_id": GOLDEN_VERIFICATION_ID,
                "output_fingerprint": "9" * 64,
                "artifact_digests": [
                    {
                        "logical_name": "selected_decisions",
                        "sha256": SELECTED_DECISIONS_SHA256,
                    },
                    {
                        "logical_name": "signal_history",
                        "sha256": SIGNAL_HISTORY_SHA256,
                    },
                ],
                "authoritative": False,
                "shadow_recorder": True,
                "publishes_signal": False,
            },
            "market_snapshot_ids": [self.evidence.market_snapshot_id],
            "implied_tenors": list(TENORS),
            "forecast_tenors": list(TENORS),
            "feature_tenors": list(TENORS),
            "evaluation_identities": copy.deepcopy(EVALUATIONS),
            "selected_signal_ids": [self.evidence.selected_signal_id],
            "selected_market_snapshot_ids": [self.evidence.market_snapshot_id],
            "stages": [
                {
                    "stage_name": "RECORD_EOD_SHADOW",
                    "stage_order": 0,
                    "is_required": True,
                    "status": "COMPLETED",
                    "attempt_count": 1,
                    "input_fingerprint": EFFECTIVE_INPUT_SHA256,
                    "output_fingerprint": DATABASE_PROJECTION_SHA256,
                    "metrics": dict(METRICS),
                    "last_error": {},
                }
            ],
            "qa_results": [
                {
                    "stage_name": "RECORD_EOD_SHADOW",
                    "check_code": "golden_eod_contract",
                    "scope_key": "run",
                    "severity": "ERROR",
                    "outcome": "PASS",
                    "is_hard_gate": True,
                    "observed_value": {"status": "PASS"},
                    "expected_value": {"status": "PASS"},
                    "evidence": golden_evidence,
                },
                {
                    "stage_name": "RECORD_EOD_SHADOW",
                    "check_code": "postgres_projection_reconciliation",
                    "scope_key": "run",
                    "severity": "ERROR",
                    "outcome": "PASS",
                    "is_hard_gate": True,
                    "observed_value": dict(readback_value),
                    "expected_value": dict(readback_value),
                    "evidence": {
                        "absolute_tolerance": 1e-12,
                        "relative_tolerance": 1e-10,
                    },
                },
            ],
            "assets": [
                _asset(
                    "selected_decisions",
                    SELECTED_DECISIONS_SHA256,
                    identity_input=True,
                ),
                _asset(
                    "signal_history",
                    SIGNAL_HISTORY_SHA256,
                    identity_input=True,
                ),
                _asset(
                    "golden_eod_fixture",
                    GOLDEN_FIXTURE_SHA256,
                    identity_input=True,
                ),
                _asset(
                    "run_manifest",
                    self.evidence.run_manifest_sha256,
                    identity_input=False,
                ),
                _asset("publish_manifest", "6" * 64, identity_input=False),
            ],
            "signal_publication_count": 0,
        }

    def _assert_rejected(self, payload: dict) -> None:
        with self.assertRaisesRegex(
            DatabaseFinalizationContinuityError,
            "exact prior EOD shadow projection",
        ):
            verify_database_finalization_continuity(
                FakeConnection(
                    {self.evidence.pipeline_run_id: payload},
                    {self.evidence.pipeline_run_id: self.current_projection},
                ),
                [self.evidence],
            )

    def test_exact_prior_database_projection_passes(self):
        verify_database_finalization_continuity(
            FakeConnection(
                {self.evidence.pipeline_run_id: copy.deepcopy(self.exact_payload)},
                {
                    self.evidence.pipeline_run_id: copy.deepcopy(
                        self.current_projection
                    )
                },
            ),
            [self.evidence],
        )

    def test_autocommit_connection_uses_one_explicit_transaction(self):
        connection = FakeConnection(
            {self.evidence.pipeline_run_id: copy.deepcopy(self.exact_payload)},
            {
                self.evidence.pipeline_run_id: copy.deepcopy(
                    self.current_projection
                )
            },
            autocommit=True,
        )

        verify_database_finalization_continuity(connection, [self.evidence])

        self.assertEqual(connection.transaction_calls, 1)
        self.assertEqual(connection.transaction_entries, 1)
        self.assertEqual(connection.transaction_depth, 0)

    def test_empty_evidence_does_not_open_autocommit_transaction(self):
        connection = FakeConnection({}, autocommit=True)

        verify_database_finalization_continuity(connection, [])

        self.assertEqual(connection.transaction_calls, 0)

    def test_missing_prior_pipeline_run_fails_closed(self):
        with self.assertRaisesRegex(
            DatabaseFinalizationContinuityError,
            "missing prior EOD pipeline run",
        ):
            verify_database_finalization_continuity(FakeConnection({}), [self.evidence])

    def test_missing_projection_components_fail_closed(self):
        mutations = {
            "market snapshot": lambda value: value["market_snapshot_ids"].clear(),
            "implied tenor": lambda value: value["implied_tenors"].pop(),
            "forecast tenor": lambda value: value["forecast_tenors"].pop(),
            "signal feature": lambda value: value["feature_tenors"].pop(),
            "signal evaluation": lambda value: value["evaluation_identities"].pop(),
            "selected signal": lambda value: value["selected_signal_ids"].clear(),
            "stage": lambda value: value["stages"].clear(),
            "hard QA": lambda value: value["qa_results"].pop(),
            "run data asset": lambda value: value["assets"].pop(1),
            "run-manifest asset": lambda value: value["assets"].pop(3),
            "publish-manifest asset": lambda value: value["assets"].pop(4),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = copy.deepcopy(self.exact_payload)
                mutate(payload)
                self._assert_rejected(payload)

    def test_corrupt_projection_evidence_fails_closed(self):
        mutations = {
            "snapshot digest": lambda value: value["run"].__setitem__(
                "snapshot_content_sha256", "0" * 64
            ),
            "stage input fingerprint": lambda value: value["stages"][0].__setitem__(
                "input_fingerprint", "0" * 64
            ),
            "stage output fingerprint": lambda value: value["stages"][0].__setitem__(
                "output_fingerprint", "0" * 64
            ),
            "stage metrics": lambda value: value["stages"][0]["metrics"].__setitem__(
                "signal_feature_count", 8
            ),
            "QA hard gate": lambda value: value["qa_results"][0].__setitem__(
                "is_hard_gate", False
            ),
            "QA output fingerprint": lambda value: value["qa_results"][1][
                "observed_value"
            ].__setitem__("output_fingerprint", "0" * 64),
            "asset content digest": lambda value: value["assets"][0].__setitem__(
                "content_sha256", "0" * 64
            ),
            "asset lineage digest": lambda value: value["assets"][0][
                "lineage"
            ].__setitem__("content_sha256", "0" * 64),
            "run manifest digest": lambda value: value["assets"][3].__setitem__(
                "content_sha256", "0" * 64
            ),
            "publication": lambda value: value.__setitem__(
                "signal_publication_count", 1
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = copy.deepcopy(self.exact_payload)
                mutate(payload)
                self._assert_rejected(payload)

    def test_sidecar_projection_fingerprint_is_independent_hard_gate(self):
        self.evidence = replace(
            self.evidence,
            database_projection_sha256="0" * 64,
        )

        self._assert_rejected(copy.deepcopy(self.exact_payload))

    def test_same_count_database_value_corruption_fails_readback_digest(self):
        changed_projection = copy.deepcopy(self.current_projection)
        changed_projection["implied_variances"][0]["annualized_variance"] = 0.999

        with self.assertRaisesRegex(
            DatabaseFinalizationContinuityError,
            "has changed since the prior EOD shadow projection",
        ):
            verify_database_finalization_continuity(
                FakeConnection(
                    {
                        self.evidence.pipeline_run_id: copy.deepcopy(
                            self.exact_payload
                        )
                    },
                    {self.evidence.pipeline_run_id: changed_projection},
                ),
                [self.evidence],
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
