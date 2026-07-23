"""Stable entry point for the validated Hybrid v2 EOD orchestrator.

This module deliberately delegates to the accepted legacy runner. It creates a
production-facing interface without duplicating or changing locked model logic.
"""

from __future__ import annotations

import math
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

LEGACY_EOD_REL = Path("notebooks/vrp_hybrid_v2_eod_pipeline.py")
EOD_AUDIT_REL = Path("data/audit/vrp_hybrid_v2_eod")
EOD_MANIFEST_NAME = "run_manifest.json"
EOD_MANIFEST_PREFIX = "VRP_EOD_MANIFEST_PATH="


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


@dataclass(frozen=True)
class EodExecutionResult:
    """Observed result from one delegated legacy EOD execution."""

    return_code: int
    manifest_paths: tuple[Path, ...]


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


def parse_eod_manifest_line(line: str, project_root: Path) -> Path | None:
    """Return a safe EOD manifest path from one legacy-runner output line.

    Only the dedicated machine marker emitted by the final EOD runner is
    accepted; human-readable upstream ``Manifest:`` lines are ignored.
    """

    if not line.startswith(EOD_MANIFEST_PREFIX):
        return None
    raw_path = line.removeprefix(EOD_MANIFEST_PREFIX).strip()
    if not raw_path:
        return None
    root = project_root.resolve()
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    audit_root = (root / EOD_AUDIT_REL).resolve()
    if candidate.name != EOD_MANIFEST_NAME:
        return None
    try:
        candidate.relative_to(audit_root)
    except ValueError:
        return None
    return candidate


def terminate_process_tree(
    process: subprocess.Popen[str],
    *,
    grace_seconds: float = 30.0,
) -> None:
    """Request graceful cancellation, then force cleanup after a deadline.

    The legacy EOD runner owns canonical rollback. Giving it a bounded chance
    to handle ``KeyboardInterrupt`` is important if cancellation arrives
    during its multi-file publication window.
    """

    if process.poll() is not None:
        return
    if os.name == "nt":
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT)
        except (OSError, ValueError):
            pass
    else:  # pragma: no cover - production is Windows; retained for portability
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            process.terminate()
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                process.kill()
        process.wait(timeout=grace_seconds)


def run_eod_observed(
    request: EodRunRequest,
    *,
    python_executable: str | Path | None = None,
    extra_environment: dict[str, str] | None = None,
    process_environment: dict[str, str] | None = None,
    output_handler: Callable[[str], None] | None = None,
) -> EodExecutionResult:
    """Run EOD with live output and capture its exact emitted manifest path."""

    if extra_environment is not None and process_environment is not None:
        raise ValueError(
            "extra_environment and process_environment are mutually exclusive"
        )
    project_root = request.project_root.resolve()
    legacy_script = project_root / LEGACY_EOD_REL
    if not legacy_script.is_file():
        raise FileNotFoundError(f"Validated EOD runner not found: {legacy_script}")
    environment = process_environment
    if extra_environment is not None:
        environment = os.environ.copy()
        environment.update(extra_environment)
    process = subprocess.Popen(
        build_eod_command(request, python_executable=python_executable),
        cwd=project_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        creationflags=(
            subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        ),
        start_new_session=os.name != "nt",
    )
    if process.stdout is None:
        raise RuntimeError("EOD subprocess stdout was not captured")

    manifests: list[Path] = []
    try:
        for raw_line in process.stdout:
            sys.stdout.write(raw_line)
            sys.stdout.flush()
            line = raw_line.rstrip("\r\n")
            if output_handler is not None:
                output_handler(line)
            manifest = parse_eod_manifest_line(line, project_root)
            if manifest is not None and manifest not in manifests:
                manifests.append(manifest)
        return_code = int(process.wait())
    except BaseException:
        terminate_process_tree(process)
        raise
    finally:
        close_stdout = getattr(process.stdout, "close", None)
        if close_stdout is not None:
            close_stdout()

    return EodExecutionResult(
        return_code=return_code,
        manifest_paths=tuple(manifests),
    )


def render_command(command: Sequence[str]) -> str:
    """Render a Windows-safe diagnostic command without executing it."""

    return subprocess.list2cmdline(list(command))
