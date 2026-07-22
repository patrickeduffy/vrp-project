"""Stable entry point for the validated Hybrid v2 EOD orchestrator.

This module deliberately delegates to the accepted legacy runner. It creates a
production-facing interface without duplicating or changing locked model logic.
"""

from __future__ import annotations

import math
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

LEGACY_EOD_REL = Path("notebooks/vrp_hybrid_v2_eod_pipeline.py")


@dataclass(frozen=True)
class EodRunRequest:
    project_root: Path
    approved_nav: float = 1_000_000.0
    target_date: str | None = None
    runtime_config: Path | None = None
    recalc_start: str | None = None
    skip_upstream: bool = False
    force_recalculate: bool = False
    publish: bool = True

    def __post_init__(self) -> None:
        if not math.isfinite(float(self.approved_nav)) or float(self.approved_nav) <= 0:
            raise ValueError("approved_nav must be a finite number greater than zero")


def build_eod_command(
    request: EodRunRequest,
    *,
    python_executable: str | Path | None = None,
) -> list[str]:
    """Build the exact command for the accepted EOD runner."""

    project_root = request.project_root.resolve()
    command = [
        str(python_executable or sys.executable),
        "-u",
        str(project_root / LEGACY_EOD_REL),
        "--project-root",
        str(project_root),
        "--approved-nav",
        format(float(request.approved_nav), ".15g"),
    ]
    if request.runtime_config is not None:
        runtime_config = request.runtime_config
        if not runtime_config.is_absolute():
            runtime_config = project_root / runtime_config
        command.extend(["--runtime-config", str(runtime_config.resolve())])
    if request.target_date:
        command.extend(["--target-date", request.target_date])
    if request.recalc_start:
        command.extend(["--recalc-start", request.recalc_start])
    if request.skip_upstream:
        command.append("--skip-upstream")
    if request.force_recalculate:
        command.append("--force-recalculate")
    if not request.publish:
        command.append("--no-publish")
    return command


def run_eod(
    request: EodRunRequest,
    *,
    python_executable: str | Path | None = None,
    extra_environment: dict[str, str] | None = None,
) -> int:
    """Run the accepted EOD pipeline and return its process exit code."""

    legacy_script = request.project_root.resolve() / LEGACY_EOD_REL
    if not legacy_script.is_file():
        raise FileNotFoundError(f"Validated EOD runner not found: {legacy_script}")
    environment = None
    if extra_environment:
        environment = os.environ.copy()
        environment.update(extra_environment)
    completed = subprocess.run(
        build_eod_command(request, python_executable=python_executable),
        cwd=request.project_root.resolve(),
        env=environment,
        check=False,
    )
    return int(completed.returncode)


def render_command(command: Sequence[str]) -> str:
    """Render a Windows-safe diagnostic command without executing it."""

    return subprocess.list2cmdline(list(command))
