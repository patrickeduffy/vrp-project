from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.golden import (  # noqa: E402
    DEFAULT_CASES,
    DEFAULT_FIXTURE_REL,
    GoldenCase,
    capture_golden_contract,
    ensure_manifest_output_path,
    resolve_output_paths,
    verify_golden_contract_with_manifest,
    write_verification_manifest,
)


def _parse_case(value: str) -> GoldenCase:
    try:
        case_id, date = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Case must use CASE_ID=YYYY-MM-DD.") from exc
    if not case_id.strip() or not date.strip():
        raise argparse.ArgumentTypeError("Case ID and date must both be present.")
    return GoldenCase(case_id.strip(), date.strip(), "User-specified golden case.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture or verify locked Hybrid v2 EOD golden examples."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser("capture", help="Create a fixture from canonical outputs.")
    capture.add_argument("--source-root", type=Path, required=True)
    capture.add_argument(
        "--fixture",
        type=Path,
        default=REPOSITORY_ROOT / DEFAULT_FIXTURE_REL,
    )
    capture.add_argument("--baseline-commit", required=True)
    capture.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing fixture after an explicitly approved recapture.",
    )
    capture.add_argument(
        "--case",
        action="append",
        type=_parse_case,
        dest="cases",
        help="Optional CASE_ID=YYYY-MM-DD override; repeat for multiple cases.",
    )

    verify = subparsers.add_parser("verify", help="Compare canonical outputs with a fixture.")
    verify.add_argument("--source-root", type=Path, required=True)
    verify.add_argument(
        "--fixture",
        type=Path,
        default=REPOSITORY_ROOT / DEFAULT_FIXTURE_REL,
    )
    verify.add_argument(
        "--signal-history",
        type=Path,
        default=None,
        help="Optional staged signal-history Parquet instead of the canonical file.",
    )
    verify.add_argument(
        "--selected-decisions",
        type=Path,
        default=None,
        help="Optional staged selected-decisions Parquet instead of the canonical file.",
    )
    verify.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Write an atomic verification manifest that binds the result to artifact hashes.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "capture":
        try:
            payload = capture_golden_contract(
                args.source_root,
                args.fixture,
                baseline_commit=args.baseline_commit,
                cases=args.cases or DEFAULT_CASES,
                overwrite=args.overwrite,
            )
        except (FileExistsError, FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(f"Captured {len(payload['cases'])} golden cases in {args.fixture.resolve()}")
        return 0

    if (args.signal_history is None) != (args.selected_decisions is None):
        print(
            "ERROR: --signal-history and --selected-decisions must be supplied together.",
            file=sys.stderr,
        )
        return 2
    if args.signal_history is not None and args.manifest is None:
        print(
            "ERROR: staged verification requires --manifest so publication can bind to exact hashes.",
            file=sys.stderr,
        )
        return 2
    try:
        signal_path, decision_path = resolve_output_paths(
            args.source_root.resolve(),
            signal_history_path=args.signal_history,
            selected_decisions_path=args.selected_decisions,
        )
        if args.manifest is not None:
            ensure_manifest_output_path(
                args.manifest,
                [args.fixture.resolve(), signal_path, decision_path],
            )
        mismatches, manifest = verify_golden_contract_with_manifest(
            args.source_root,
            args.fixture,
            signal_history_path=signal_path if args.signal_history is not None else None,
            selected_decisions_path=(
                decision_path if args.selected_decisions is not None else None
            ),
        )
        if args.manifest is not None:
            write_verification_manifest(args.manifest, manifest)
            print(f"VERIFICATION_MANIFEST: {args.manifest.resolve()}")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"VERIFICATION_ID: {manifest['verification_id']}")
    print(f"SIGNAL_SHA256: {manifest['artifacts']['signal_history']['sha256']}")
    print(f"DECISION_SHA256: {manifest['artifacts']['selected_decisions']['sha256']}")
    print(f"FIXTURE_SHA256: {manifest['fixture']['sha256']}")
    if mismatches:
        print(f"GOLDEN_STATUS: FAIL ({len(mismatches)} mismatches)")
        for mismatch in mismatches:
            print(f"- {mismatch}")
        return 1
    print("GOLDEN_STATUS: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
