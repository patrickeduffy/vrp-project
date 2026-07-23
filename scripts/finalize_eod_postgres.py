from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.orchestration.eod_postgres import (  # noqa: E402
    EodPostgresFinalizerRequest,
    finalize_eod_postgres,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Finalize one exact completed and published EOD audit run in PostgreSQL. "
            "Database credentials must be supplied through the process environment."
        )
    )
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Exact EOD audit directory; latest-directory discovery is not supported.",
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument(
        "--code-version",
        required=True,
        help="Full 40-character lowercase Git commit SHA used for this finalization.",
    )
    parser.add_argument(
        "--run-manifest-sha256",
        required=True,
        help="Caller-pinned SHA-256 of the exact completed run manifest.",
    )
    parser.add_argument(
        "--source-bundle-sha256",
        required=True,
        help="Caller-pinned SHA-256 of the exact completed EOD source bundle.",
    )
    parser.add_argument("--requested-by", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = finalize_eod_postgres(
        EodPostgresFinalizerRequest(
            project_root=args.project_root,
            run_dir=args.run_dir,
            artifact_root=args.artifact_root,
            environment=args.environment,
            code_version=args.code_version,
            requested_by=args.requested_by,
            run_manifest_sha256=args.run_manifest_sha256,
            source_bundle_sha256=args.source_bundle_sha256,
        )
    )
    rendered = json.dumps(result.to_json_dict(), sort_keys=True)
    print(rendered, file=sys.stdout if result.exit_code == 0 else sys.stderr)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
