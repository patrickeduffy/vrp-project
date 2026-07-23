from __future__ import annotations

import argparse
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.orchestration.eod import (  # noqa: E402
    EodRunRequest,
    build_eod_command,
    load_completed_eod_handoff,
    render_command,
    resolve_clean_code_version,
    run_eod,
)
from vrp.orchestration.eod_postgres import (  # noqa: E402
    EodPostgresFinalizerRequest,
    finalize_eod_postgres,
)
from vrp.orchestration.eod_lock import (  # noqa: E402
    EodWriterAlreadyRunningError,
    delegated_eod_writer_environment,
    exclusive_eod_writer_lock,
)
from vrp.orchestration.eod_finalization_gate import (  # noqa: E402
    UnresolvedEodFinalizationError,
    assert_no_unresolved_eod_finalizations,
    require_canonical_eod_runtime_config,
    resolve_eod_audit_root,
)


EXIT_WRAPPER_CONFIGURATION_FAILED = 4
EXIT_HANDOFF_INVALID = 5
EXIT_EOD_ALREADY_RUNNING = 6
EXIT_OLDER_FINALIZATION_UNRESOLVED = 7


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the accepted Hybrid v2 EOD pipeline through its stable production interface."
    )
    parser.add_argument("--project-root", type=Path, default=REPOSITORY_ROOT)
    parser.add_argument("--runtime-config", type=Path, default=None)
    parser.add_argument("--target-date", default=None)
    parser.add_argument("--recalc-start", default=None)
    parser.add_argument("--approved-nav", type=float, default=1_000_000.0)
    parser.add_argument("--skip-upstream", action="store_true")
    parser.add_argument("--force-recalculate", action="store_true")
    parser.add_argument("--no-publish", action="store_true")
    parser.add_argument(
        "--no-postgres-shadow",
        action="store_true",
        help=(
            "Explicitly skip the non-authoritative PostgreSQL post-pass. "
            "Normal published EOD runs finalize PostgreSQL by default."
        ),
    )
    parser.add_argument("--postgres-environment", default="local")
    parser.add_argument("--postgres-artifact-root", type=Path, default=None)
    parser.add_argument(
        "--code-version",
        default=None,
        help=(
            "Optional full Git SHA assertion. The executing source and data "
            "checkouts must still be clean and match it."
        ),
    )
    parser.add_argument("--requested-by", default=None)
    parser.add_argument("--python-executable", type=Path, default=Path(sys.executable))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the delegated command without running the pipeline.",
    )
    return parser.parse_args()


def _resolved_artifact_root(value: Path | None, project_root: Path) -> Path:
    candidate = value or Path("data/reference_history")
    return (
        candidate.expanduser().resolve()
        if candidate.is_absolute()
        else (project_root / candidate).resolve()
    )


def _requested_by(value: str | None) -> str:
    candidate = value or os.environ.get("USERNAME") or os.environ.get("USER")
    if not isinstance(candidate, str) or not candidate.strip():
        raise ValueError(
            "--requested-by is required when no operating-system user is available"
        )
    return candidate.strip()


def _shadow_skip_reason(args: argparse.Namespace) -> str | None:
    if args.no_postgres_shadow:
        return "explicit-no-postgres-shadow"
    if args.no_publish:
        return "no-publish"
    if args.skip_upstream:
        return "skip-upstream-has-no-sofr-evidence"
    return None


def main() -> int:
    args = parse_args()
    if args.skip_upstream and not args.no_publish:
        print(
            "EOD wrapper configuration failed: --skip-upstream requires --no-publish",
            file=sys.stderr,
        )
        return EXIT_WRAPPER_CONFIGURATION_FAILED
    request = EodRunRequest(
        project_root=args.project_root,
        approved_nav=args.approved_nav,
        target_date=args.target_date,
        runtime_config=args.runtime_config,
        recalc_start=args.recalc_start,
        skip_upstream=args.skip_upstream,
        force_recalculate=args.force_recalculate,
        publish=not args.no_publish,
    )
    if args.no_postgres_shadow and request.publish:
        request = replace(
            request,
            postgres_postpass_bypass_reason="explicit-no-postgres-shadow",
        )
    write_capable = request.publish or not request.skip_upstream
    if write_capable:
        try:
            require_canonical_eod_runtime_config(
                request.project_root,
                request.runtime_config,
            )
        except (OSError, ValueError) as exc:
            print(f"EOD wrapper configuration failed: {exc}", file=sys.stderr)
            return EXIT_WRAPPER_CONFIGURATION_FAILED
    command = build_eod_command(request, python_executable=args.python_executable)
    if args.dry_run:
        print(render_command(command))
        reason = _shadow_skip_reason(args)
        if reason is None:
            print(
                "POSTPASS: exact completed run -> reference sync -> "
                "PostgreSQL EOD shadow"
            )
        else:
            print(f"POSTPASS: skipped ({reason})")
        return 0

    skip_reason = _shadow_skip_reason(args)
    if skip_reason == "no-publish" and request.skip_upstream:
        result = run_eod(request, python_executable=args.python_executable)
        if result == 0:
            print(f"FILE_EOD=PASS SHADOW=SKIPPED REASON={skip_reason}")
        return result
    if skip_reason is not None:
        try:
            with exclusive_eod_writer_lock(request.project_root) as writer_lease:
                try:
                    audit_root = resolve_eod_audit_root(request.project_root, None)
                    assert_no_unresolved_eod_finalizations(audit_root)
                except (OSError, ValueError, UnresolvedEodFinalizationError) as exc:
                    print(f"EOD finalization order gate failed: {exc}", file=sys.stderr)
                    return EXIT_OLDER_FINALIZATION_UNRESOLVED
                result = run_eod(
                    request,
                    python_executable=args.python_executable,
                    extra_environment=delegated_eod_writer_environment(writer_lease),
                )
                if result == 0:
                    print(f"FILE_EOD=PASS SHADOW=SKIPPED REASON={skip_reason}")
                return result
        except EodWriterAlreadyRunningError as exc:
            print(f"EOD writer lock unavailable: {exc}", file=sys.stderr)
            return EXIT_EOD_ALREADY_RUNNING

    try:
        code_version = resolve_clean_code_version(
            source_root=REPOSITORY_ROOT,
            project_root=request.project_root,
            explicit=args.code_version,
        )
        requested_by = _requested_by(args.requested_by)
        if not os.environ.get("VRP_DATABASE_URL"):
            raise ValueError("VRP_DATABASE_URL is required for the PostgreSQL post-pass")
    except ValueError as exc:
        print(f"EOD wrapper configuration failed: {exc}", file=sys.stderr)
        return EXIT_WRAPPER_CONFIGURATION_FAILED

    request = replace(
        request,
        code_version=code_version,
        postgres_postpass_required=True,
        postgres_environment=args.postgres_environment,
    )

    try:
        with exclusive_eod_writer_lock(request.project_root) as writer_lease:
            try:
                audit_root = resolve_eod_audit_root(request.project_root, None)
                assert_no_unresolved_eod_finalizations(audit_root)
            except (OSError, ValueError, UnresolvedEodFinalizationError) as exc:
                print(f"EOD finalization order gate failed: {exc}", file=sys.stderr)
                return EXIT_OLDER_FINALIZATION_UNRESOLVED
            with tempfile.TemporaryDirectory(prefix="vrp-eod-handoff-") as temporary:
                handoff_path = Path(temporary) / "completed-eod.json"
                file_result = run_eod(
                    request,
                    python_executable=args.python_executable,
                    extra_environment=delegated_eod_writer_environment(writer_lease),
                    result_handoff=handoff_path,
                )
                if file_result != 0:
                    return file_result
                try:
                    handoff = load_completed_eod_handoff(handoff_path, request)
                except (OSError, ValueError) as exc:
                    print(
                        "FILE_EOD=PASS SHADOW=FAILED "
                        f"REASON=invalid-handoff DETAIL={exc}",
                        file=sys.stderr,
                    )
                    return EXIT_HANDOFF_INVALID

                finalization = finalize_eod_postgres(
                    EodPostgresFinalizerRequest(
                        project_root=request.project_root,
                        run_dir=handoff.run_dir,
                        artifact_root=_resolved_artifact_root(
                            args.postgres_artifact_root,
                            request.project_root,
                        ),
                        environment=args.postgres_environment,
                        code_version=code_version,
                        requested_by=requested_by,
                        run_manifest_sha256=handoff.run_manifest_sha256,
                        source_bundle_sha256=handoff.source_bundle_sha256,
                        python_executable=args.python_executable,
                    ),
                    writer_lease=writer_lease,
                )
                if finalization.exit_code == 0:
                    print(
                        f"FILE_EOD=PASS SHADOW=PASS RUN_DIR={handoff.run_dir} "
                        f"STATUS={finalization.status_path}"
                    )
                else:
                    print(
                        f"FILE_EOD=PASS SHADOW=FAILED RUN_DIR={handoff.run_dir} "
                        f"STATUS={finalization.status_path}",
                        file=sys.stderr,
                    )
                return finalization.exit_code
    except EodWriterAlreadyRunningError as exc:
        print(f"EOD writer lock unavailable: {exc}", file=sys.stderr)
        return EXIT_EOD_ALREADY_RUNNING


if __name__ == "__main__":
    raise SystemExit(main())
