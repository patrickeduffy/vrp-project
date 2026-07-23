from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.orchestration.eod import (  # noqa: E402
    EodRunRequest,
    build_eod_command,
    render_command,
    run_eod,
    run_eod_observed,
)
from vrp.orchestration.post_publish_shadow import (  # noqa: E402
    EOD_DATABASE_ENV,
    LOADER_DATABASE_ENV,
    REFERENCE_DATABASE_ENV,
    SHADOW_FAILURE_EXIT_CODE,
    execute_post_publish_shadow,
    resolve_shadow_database_targets,
    resolve_shadow_identity,
    resolve_shadow_runtime_config,
    validate_shadow_checkout,
    validate_shadow_database_roles,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
    parser.add_argument("--python-executable", type=Path, default=Path(sys.executable))
    parser.add_argument(
        "--shadow-write",
        action="store_true",
        help=(
            "After a successful file publication, update PostgreSQL reference "
            "history and record the exact EOD shadow snapshot."
        ),
    )
    parser.add_argument("--shadow-environment", default="local")
    parser.add_argument("--shadow-code-version", default=None)
    parser.add_argument("--shadow-requested-by", default=None)
    parser.add_argument("--shadow-timeout-seconds", type=float, default=300.0)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the delegated command without running the pipeline.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
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
    command = build_eod_command(request, python_executable=args.python_executable)
    if args.shadow_write and not request.publish:
        print(
            "--shadow-write cannot be combined with --no-publish because there "
            "is no canonical publication to record.",
            file=sys.stderr,
        )
        return 2
    if args.dry_run:
        print(render_command(command))
        if args.shadow_write:
            print(
                "Post-publication PostgreSQL shadow recording: enabled "
                "(credentials are read from environment variables at run time)."
            )
        return 0
    if not args.shadow_write:
        return run_eod(request, python_executable=args.python_executable)

    project_root = args.project_root.expanduser().resolve()
    try:
        if not math.isfinite(args.shadow_timeout_seconds) or not (
            0 < args.shadow_timeout_seconds <= 3600
        ):
            raise ValueError(
                "shadow loader timeout must be between 0 and 3600 seconds"
            )
        identity = resolve_shadow_identity(
            project_root,
            explicit_code_version=args.shadow_code_version,
            explicit_requested_by=args.shadow_requested_by,
        )
        databases = resolve_shadow_database_targets()
        database_users = validate_shadow_database_roles(databases)
        shadow_runtime_config = resolve_shadow_runtime_config(
            project_root, args.runtime_config
        )
        validate_shadow_checkout(
            project_root,
            identity=identity,
            runtime_config=shadow_runtime_config,
        )
    except ValueError as exc:
        print(f"Post-publication shadow preflight failed: {exc}", file=sys.stderr)
        return 2

    legacy_environment = os.environ.copy()
    for key in (
        REFERENCE_DATABASE_ENV,
        EOD_DATABASE_ENV,
        LOADER_DATABASE_ENV,
        "PGPASSWORD",
        "PGPASSFILE",
    ):
        legacy_environment.pop(key, None)

    observed = run_eod_observed(
        request,
        python_executable=args.python_executable,
        process_environment=legacy_environment,
    )
    if observed.return_code != 0:
        return observed.return_code
    if len(observed.manifest_paths) != 1:
        print(
            "VRP_SHADOW|FAILED|Canonical publication succeeded, but the exact "
            "EOD run manifest could not be identified safely.",
            file=sys.stderr,
        )
        return SHADOW_FAILURE_EXIT_CODE

    try:
        shadow = execute_post_publish_shadow(
            project_root=project_root,
            manifest_path=observed.manifest_paths[0],
            runtime_config=shadow_runtime_config,
            environment=args.shadow_environment,
            identity=identity,
            databases=databases,
            python_executable=args.python_executable,
            base_environment=os.environ,
            loader_timeout_seconds=args.shadow_timeout_seconds,
            validated_database_users=database_users,
        )
    except Exception as exc:
        print(
            "VRP_SHADOW|FAILED|Canonical publication succeeded and remains "
            f"authoritative; PostgreSQL shadow recording failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return SHADOW_FAILURE_EXIT_CODE
    if not shadow.success:
        print(
            "VRP_SHADOW|FAILED|Canonical publication succeeded and remains "
            f"authoritative; {shadow.error}. Status: {shadow.status_path}",
            file=sys.stderr,
        )
        return SHADOW_FAILURE_EXIT_CODE
    print(f"VRP_SHADOW_STATUS_PATH={shadow.status_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
