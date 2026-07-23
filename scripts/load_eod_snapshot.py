from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.eod_shadow import load_staged_eod_snapshot  # noqa: E402
from vrp.eod_shadow.service import (  # noqa: E402
    execute_eod_shadow_load,
    validate_code_version,
)
from vrp.storage.postgres import connect_from_environment  # noqa: E402
from vrp.orchestration.eod_bundle import load_eod_source_bundle  # noqa: E402
from vrp.orchestration.eod_finalization_gate import (  # noqa: E402
    assert_no_unresolved_eod_finalizations,
    completed_eod_finalization_evidence,
    resolve_eod_audit_root,
)
from vrp.orchestration.eod_lock import exclusive_eod_writer_lock  # noqa: E402
from vrp.storage.finalization_coordination import (  # noqa: E402
    delegated_finalization_child_lease,
    has_delegated_finalization_lease,
    standalone_finalization_lease,
)
from vrp.storage.finalization_continuity import (  # noqa: E402
    verify_database_finalization_continuity,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a completed Hybrid v2 EOD run and record its exact "
            "PostgreSQL shadow projection."
        )
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        required=True,
        help="Explicit data-bearing VRP project root.",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Explicit completed EOD audit run directory.",
    )
    parser.add_argument("--fixture-path", type=Path, default=None)
    parser.add_argument(
        "--expected-run-manifest-sha256",
        default=None,
        help="Optional caller-pinned SHA-256 for the exact run manifest.",
    )
    parser.add_argument(
        "--expected-content-sha256",
        default=None,
        help="Optional caller-pinned semantic snapshot SHA-256.",
    )
    parser.add_argument(
        "--expected-source-bundle-sha256",
        default=None,
        help="Optional producer-pinned SHA-256 for the exact EOD source bundle.",
    )
    parser.add_argument("--environment", default=None)
    parser.add_argument("--code-version", default=None)
    parser.add_argument("--requested-by", default=None)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate staged evidence without opening a database connection.",
    )
    return parser.parse_args(argv)


def _validation_summary(snapshot) -> dict[str, object]:
    return {
        "artifact_count": len(snapshot.artifacts),
        "configuration_content_sha256": snapshot.configuration_identity.sha256,
        "content_sha256": snapshot.content_sha256,
        "decision": snapshot.selected_signal.decision,
        "forecast_variance_count": len(snapshot.forecast_variance),
        "golden_verification_id": snapshot.golden_evidence.verification_id,
        "implied_variance_count": len(snapshot.implied_variance),
        "model_content_sha256": snapshot.model_identity.sha256,
        "signal_evaluation_count": len(snapshot.signal_evaluations),
        "signal_feature_count": len(snapshot.signal_features),
        "snapshot_at": snapshot.snapshot_at.isoformat(),
        "sofr_normalized_content_sha256": (
            snapshot.sofr_evidence.normalized_content_sha256
        ),
        "sofr_refreshed_snapshot_path": str(
            snapshot.sofr_evidence.refreshed_snapshot_path
        ),
        "sofr_refreshed_snapshot_sha256": (
            snapshot.sofr_evidence.refreshed_snapshot_sha256
        ),
        "sofr_observation_date": snapshot.sofr_evidence.observation_date.isoformat(),
        "sofr_observation_rate_decimal": str(snapshot.sofr_evidence.rate_decimal),
        "status": "VALID",
        "valuation_date": snapshot.valuation_date.isoformat(),
    }


def _required_load_value(value: str | None, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"--{name.replace('_', '-')} is required for a database load")
    return value.strip()


def run(args: argparse.Namespace) -> dict[str, object]:
    project_root = args.project_root.expanduser().resolve()
    run_dir = args.run_dir.expanduser().resolve()
    fixture = (
        None
        if args.fixture_path is None
        else (
            args.fixture_path.expanduser().resolve()
            if args.fixture_path.is_absolute()
            else (project_root / args.fixture_path).resolve()
        )
    )
    expected_source_bundle_sha256 = args.expected_source_bundle_sha256
    source_bundle_before = None
    if expected_source_bundle_sha256 is not None:
        if re.fullmatch(r"[0-9a-f]{64}", expected_source_bundle_sha256) is None:
            raise ValueError(
                "--expected-source-bundle-sha256 must be a lowercase SHA-256"
            )
        source_bundle_before = load_eod_source_bundle(run_dir)
        if source_bundle_before.content_sha256 != expected_source_bundle_sha256:
            raise ValueError(
                "EOD source bundle does not match the producer-pinned digest"
            )
    snapshot = load_staged_eod_snapshot(
        run_dir,
        project_root,
        fixture_path=fixture,
        expected_run_manifest_sha256=args.expected_run_manifest_sha256,
    )
    if source_bundle_before is not None:
        source_bundle_after = load_eod_source_bundle(run_dir)
        if (
            source_bundle_after.content_sha256 != expected_source_bundle_sha256
            or source_bundle_after.artifact_sha256
            != source_bundle_before.artifact_sha256
        ):
            raise ValueError("EOD source bundle changed during snapshot validation")
    if args.expected_content_sha256 is not None:
        if re.fullmatch(r"[0-9a-f]{64}", args.expected_content_sha256) is None:
            raise ValueError(
                "--expected-content-sha256 must be a lowercase SHA-256"
            )
        if snapshot.content_sha256 != args.expected_content_sha256:
            raise ValueError(
                "staged EOD snapshot does not match the caller-pinned content digest"
            )
    if args.validate_only:
        return _validation_summary(snapshot)

    environment = _required_load_value(args.environment, "environment")
    code_version = validate_code_version(
        _required_load_value(args.code_version, "code_version")
    )
    requested_by = _required_load_value(args.requested_by, "requested_by")
    if re.fullmatch(r"\d{8}_\d{6}", run_dir.name) is None:
        raise ValueError(
            "mutating shadow loads require a timestamped EOD run directory"
        )
    canonical_audit_root = resolve_eod_audit_root(project_root, None)
    if run_dir.parent != canonical_audit_root:
        raise ValueError(
            "mutating shadow loads require a run in the canonical EOD audit root: "
            f"{canonical_audit_root}"
        )

    def load(connection):
        result = execute_eod_shadow_load(
            connection,
            snapshot,
            environment=environment,
            code_version=code_version,
            requested_by=requested_by,
        )
        return result.to_json_dict()

    if has_delegated_finalization_lease():
        connection = connect_from_environment()
        try:
            with delegated_finalization_child_lease(connection):
                return load(connection)
        finally:
            connection.close()

    with exclusive_eod_writer_lock(project_root):
        assert_no_unresolved_eod_finalizations(
            canonical_audit_root,
            before_timestamp=run_dir.name,
        )
        connection = connect_from_environment()
        try:
            with standalone_finalization_lease(connection):
                assert_no_unresolved_eod_finalizations(
                    canonical_audit_root,
                    before_timestamp=run_dir.name,
                )
                prior_evidence = completed_eod_finalization_evidence(
                    canonical_audit_root,
                    before_timestamp=run_dir.name,
                    environment=environment,
                )
                verify_database_finalization_continuity(
                    connection,
                    prior_evidence,
                )
                # Continuity SELECTs open a transaction on the normal
                # non-autocommit connection.  The shadow service requires an
                # IDLE connection so it can own its complete atomic write.
                connection.rollback()
                return load(connection)
        finally:
            connection.close()


def main(argv: list[str] | None = None) -> int:
    try:
        output = run(parse_args(argv))
    except Exception as exc:
        print(f"EOD shadow load failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
