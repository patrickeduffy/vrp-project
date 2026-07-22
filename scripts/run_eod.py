from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.orchestration.eod import (  # noqa: E402
    EodRunRequest,
    build_eod_command,
    render_command,
    run_eod,
)


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
    parser.add_argument("--python-executable", type=Path, default=Path(sys.executable))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the delegated command without running the pipeline.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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
    if args.dry_run:
        print(render_command(command))
        return 0
    return run_eod(request, python_executable=args.python_executable)


if __name__ == "__main__":
    raise SystemExit(main())
