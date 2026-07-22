from __future__ import annotations

import argparse
import json
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
    snapshot = load_staged_eod_snapshot(
        run_dir,
        project_root,
        fixture_path=fixture,
    )
    if args.validate_only:
        return _validation_summary(snapshot)

    environment = _required_load_value(args.environment, "environment")
    code_version = validate_code_version(
        _required_load_value(args.code_version, "code_version")
    )
    requested_by = _required_load_value(args.requested_by, "requested_by")
    connection = connect_from_environment()
    try:
        result = execute_eod_shadow_load(
            connection,
            snapshot,
            environment=environment,
            code_version=code_version,
            requested_by=requested_by,
        )
    finally:
        connection.close()
    return result.to_json_dict()


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
