"""Stable entry point for the validated Hybrid v2 EOD orchestrator.

This module deliberately delegates to the accepted legacy runner. It creates a
production-facing interface without duplicating or changing locked model logic.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from .eod_bundle import (
    EOD_SOURCE_BUNDLE_CONTRACT,
    EOD_SOURCE_BUNDLE_SCHEMA_VERSION,
    load_eod_source_bundle,
)

LEGACY_EOD_REL = Path("notebooks/vrp_hybrid_v2_eod_pipeline.py")
DATABASE_SECRET_ENVIRONMENT_KEYS = frozenset(
    {
        "DATABASE_URL",
        "PGPASSFILE",
        "PGPASSWORD",
        "PGSERVICE",
        "PGSERVICEFILE",
        "VRP_DATABASE_URL",
        "VRP_TEST_DATABASE_URL",
    }
)
FULL_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
EOD_RESULT_HANDOFF_CONTRACT = "vrp.hybrid_v2.eod_result"
EOD_RESULT_HANDOFF_SCHEMA_VERSION = 1
EOD_RUN_TIMESTAMP = re.compile(r"^\d{8}_\d{6}$")


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
    code_version: str | None = None
    postgres_postpass_required: bool = False
    postgres_environment: str | None = None
    postgres_postpass_bypass_reason: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(float(self.approved_nav)) or float(self.approved_nav) <= 0:
            raise ValueError("approved_nav must be a finite number greater than zero")
        if self.code_version is not None and FULL_GIT_SHA.fullmatch(
            self.code_version
        ) is None:
            raise ValueError("code_version must be a full lowercase Git SHA")
        if self.postgres_postpass_required and (
            not isinstance(self.postgres_environment, str)
            or not self.postgres_environment.strip()
        ):
            raise ValueError(
                "postgres_environment is required when the PostgreSQL post-pass is required"
            )
        if (
            isinstance(self.postgres_environment, str)
            and self.postgres_environment != self.postgres_environment.strip()
        ):
            raise ValueError("postgres_environment may not contain surrounding whitespace")
        if self.postgres_postpass_bypass_reason not in (
            None,
            "explicit-no-postgres-shadow",
        ):
            raise ValueError("postgres_postpass_bypass_reason is invalid")
        if self.postgres_postpass_required and self.postgres_postpass_bypass_reason:
            raise ValueError("a PostgreSQL post-pass cannot be required and bypassed")


@dataclass(frozen=True)
class CompletedEodHandoff:
    run_dir: Path
    run_manifest_path: Path
    run_manifest_sha256: str
    target_date: date
    manifest: Mapping[str, Any]
    source_bundle_sha256: str
    source_artifact_sha256: Mapping[str, str]


def build_eod_command(
    request: EodRunRequest,
    *,
    python_executable: str | Path | None = None,
    result_handoff: Path | None = None,
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
    if request.code_version is not None:
        command.extend(["--code-version", request.code_version])
    if request.postgres_postpass_required:
        command.append("--postgres-postpass-required")
        command.extend(["--postgres-environment", str(request.postgres_environment)])
    if request.postgres_postpass_bypass_reason is not None:
        command.extend(
            [
                "--postgres-postpass-bypass-reason",
                request.postgres_postpass_bypass_reason,
            ]
        )
    if result_handoff is not None:
        command.extend(["--result-handoff", str(result_handoff.resolve())])
    return command


def _legacy_environment(
    extra_environment: dict[str, str] | None,
) -> dict[str, str]:
    """Build the legacy child environment without database credentials."""

    environment = os.environ.copy()
    if extra_environment:
        environment.update(extra_environment)
    for key in tuple(environment):
        upper = key.upper()
        if upper in DATABASE_SECRET_ENVIRONMENT_KEYS or upper.endswith(
            "_DATABASE_URL"
        ):
            environment.pop(key, None)
    return environment


def _clean_git_head(root: Path, *, include_untracked: bool) -> str:
    """Return a clean checkout's exact commit or fail before production work."""

    root = root.resolve()
    try:
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
            env=_legacy_environment(None),
        ).stdout.strip()
        dirty = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "status",
                "--porcelain",
                f"--untracked-files={'normal' if include_untracked else 'no'}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
            env=_legacy_environment(None),
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"cannot resolve production code identity at {root}") from exc
    if FULL_GIT_SHA.fullmatch(head) is None:
        raise ValueError(f"Git returned an invalid production code identity at {root}")
    if dirty:
        raise ValueError(f"production checkout has working-tree changes: {root}")
    return head


def resolve_clean_code_version(
    *,
    source_root: Path,
    project_root: Path,
    explicit: str | None = None,
) -> str:
    """Bind the stable wrapper and data-bearing runner to one clean commit."""

    source_head = _clean_git_head(source_root, include_untracked=True)
    project_head = (
        source_head
        if source_root.resolve() == project_root.resolve()
        else _clean_git_head(project_root, include_untracked=True)
    )
    if source_head != project_head:
        raise ValueError(
            "stable wrapper and data-bearing EOD checkout are on different commits"
        )
    if explicit is not None:
        explicit = explicit.strip()
        if FULL_GIT_SHA.fullmatch(explicit) is None:
            raise ValueError("explicit code version must be a full lowercase Git SHA")
        if explicit != source_head:
            raise ValueError("explicit code version does not match the executing checkout")
    return source_head


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_json(path: Path, label: str) -> tuple[dict[str, Any], str]:
    try:
        before = _sha256_file(path)
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        after = _sha256_file(path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid {label}: {path}") from exc
    if before != hashlib.sha256(raw).hexdigest() or before != after:
        raise ValueError(f"{label} changed while it was being read")
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain a JSON object")
    return payload, before


def _canonical_date(value: Any, label: str) -> date:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a canonical ISO date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{label} must be a canonical ISO date")
    return parsed


def _requested_date(value: str | None) -> date | None:
    if value is None:
        return None
    compact = value.strip()
    if re.fullmatch(r"\d{8}", compact):
        compact = f"{compact[:4]}-{compact[4:6]}-{compact[6:]}"
    return _canonical_date(compact, "requested target date")


def load_completed_eod_handoff(
    handoff_path: Path,
    request: EodRunRequest,
) -> CompletedEodHandoff:
    """Validate the exact published file run before starting the DB post-pass."""

    handoff_path = handoff_path.expanduser().resolve(strict=True)
    payload, _ = _stable_json(handoff_path, "EOD result handoff")
    if payload.get("contract") != EOD_RESULT_HANDOFF_CONTRACT:
        raise ValueError("EOD result handoff contract is invalid")
    if payload.get("schema_version") != EOD_RESULT_HANDOFF_SCHEMA_VERSION:
        raise ValueError("EOD result handoff schema version is invalid")
    if payload.get("status") != "PASS" or payload.get("published") is not True:
        raise ValueError("EOD result handoff is not a published PASS run")

    raw_run_dir = payload.get("run_dir")
    manifest_identity = payload.get("run_manifest")
    if not isinstance(raw_run_dir, str) or not Path(raw_run_dir).is_absolute():
        raise ValueError("EOD result handoff run_dir must be absolute")
    if not isinstance(manifest_identity, dict):
        raise ValueError("EOD result handoff run_manifest must be an object")
    raw_manifest_path = manifest_identity.get("path")
    expected_manifest_sha = manifest_identity.get("sha256")
    if not isinstance(raw_manifest_path, str) or not Path(raw_manifest_path).is_absolute():
        raise ValueError("EOD result handoff manifest path must be absolute")
    if not isinstance(expected_manifest_sha, str) or SHA256_HEX.fullmatch(
        expected_manifest_sha
    ) is None:
        raise ValueError("EOD result handoff manifest digest is invalid")

    run_dir = Path(raw_run_dir).resolve(strict=True)
    manifest_path = Path(raw_manifest_path).resolve(strict=True)
    if not run_dir.is_dir():
        raise ValueError("EOD result handoff run_dir is not a directory")
    if EOD_RUN_TIMESTAMP.fullmatch(run_dir.name) is None:
        raise ValueError("EOD result handoff run_dir is not timestamped")
    if manifest_path != run_dir / "run_manifest.json" or not manifest_path.is_file():
        raise ValueError("EOD result handoff does not identify its exact run manifest")

    manifest, observed_manifest_sha = _stable_json(manifest_path, "EOD run manifest")
    if observed_manifest_sha != expected_manifest_sha:
        raise ValueError("EOD run manifest digest disagrees with the handoff")
    target_date = _canonical_date(payload.get("target_date"), "handoff target date")
    if _canonical_date(manifest.get("target_date"), "manifest target date") != target_date:
        raise ValueError("EOD handoff and manifest target dates disagree")
    expected_target = _requested_date(request.target_date)
    if expected_target is not None and target_date != expected_target:
        raise ValueError("EOD handoff target date disagrees with the request")

    if manifest.get("run_timestamp") != run_dir.name:
        raise ValueError("EOD run directory and manifest timestamp disagree")
    if manifest.get("status") != "PASS" or manifest.get("final_health") != "PASS":
        raise ValueError("EOD run manifest is not PASS/PASS")
    if request.code_version is None:
        raise ValueError("published EOD request is missing its producing code version")
    if manifest.get("code_version") != request.code_version:
        raise ValueError("EOD run manifest code version disagrees with the request")
    if payload.get("code_version") != request.code_version:
        raise ValueError("EOD result handoff code version disagrees with the request")
    if (
        request.postgres_postpass_required is not True
        or manifest.get("postgres_postpass_required") is not True
        or payload.get("postgres_postpass_required") is not True
    ):
        raise ValueError("EOD run is not bound to the required PostgreSQL post-pass")
    if (
        manifest.get("postgres_environment") != request.postgres_environment
        or payload.get("postgres_environment") != request.postgres_environment
    ):
        raise ValueError("EOD PostgreSQL environment disagrees with the request")
    if manifest.get("publish_requested") is not True:
        raise ValueError("EOD run manifest was not produced by a published run")
    if manifest.get("skip_upstream") is not False or request.skip_upstream:
        raise ValueError("automatic PostgreSQL finalization forbids skip-upstream runs")
    if not isinstance(manifest.get("sofr_manifest"), str) or not manifest[
        "sofr_manifest"
    ].strip():
        raise ValueError("EOD run manifest does not pin SOFR updater evidence")
    published_outputs = manifest.get("published_outputs")
    if not isinstance(published_outputs, dict) or not published_outputs:
        raise ValueError("EOD run manifest has no published output evidence")

    project_root = request.project_root.expanduser().resolve()
    recorded_project_root = manifest.get("project_root")
    if not isinstance(recorded_project_root, str) or Path(
        recorded_project_root
    ).expanduser().resolve() != project_root:
        raise ValueError("EOD run manifest project root disagrees with the request")
    try:
        recorded_nav = Decimal(str(manifest.get("approved_nav")))
        requested_nav = Decimal(str(request.approved_nav))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("EOD run manifest approved NAV is invalid") from exc
    if recorded_nav != requested_nav:
        raise ValueError("EOD run manifest approved NAV disagrees with the request")

    runtime_path = manifest.get("runtime_config")
    if not isinstance(runtime_path, str):
        raise ValueError("EOD run manifest runtime config is invalid")
    expected_runtime = request.runtime_config or Path(
        "config/vrp_hybrid_v2_eod_runtime_config.json"
    )
    if not expected_runtime.is_absolute():
        expected_runtime = project_root / expected_runtime
    if Path(runtime_path).expanduser().resolve() != expected_runtime.resolve():
        raise ValueError("EOD run manifest runtime config disagrees with the request")
    runtime, _ = _stable_json(expected_runtime.resolve(strict=True), "EOD runtime config")
    runtime_outputs = runtime.get("outputs", {})
    if not isinstance(runtime_outputs, dict):
        raise ValueError("EOD runtime outputs must be an object")
    audit_value = runtime_outputs.get(
        "audit_dir",
        "data/audit/vrp_hybrid_v2_eod",
    )
    if not isinstance(audit_value, str) or not audit_value.strip():
        raise ValueError("EOD runtime audit directory is invalid")
    audit_root = Path(audit_value)
    if not audit_root.is_absolute():
        audit_root = project_root / audit_root
    if run_dir.parent != audit_root.resolve():
        raise ValueError("EOD result handoff is outside the configured audit root")

    source_bundle_payload = payload.get("source_bundle")
    if not isinstance(source_bundle_payload, dict):
        raise ValueError("EOD result handoff source_bundle must be an object")
    if source_bundle_payload.get("contract") != EOD_SOURCE_BUNDLE_CONTRACT:
        raise ValueError("EOD result handoff source-bundle contract is invalid")
    if (
        source_bundle_payload.get("schema_version")
        != EOD_SOURCE_BUNDLE_SCHEMA_VERSION
    ):
        raise ValueError("EOD result handoff source-bundle schema is invalid")
    source_bundle = load_eod_source_bundle(run_dir)
    if source_bundle.to_json_dict() != source_bundle_payload:
        raise ValueError("EOD source bundle disagrees with the completed-run handoff")

    return CompletedEodHandoff(
        run_dir=run_dir,
        run_manifest_path=manifest_path,
        run_manifest_sha256=observed_manifest_sha,
        target_date=target_date,
        manifest=manifest,
        source_bundle_sha256=source_bundle.content_sha256,
        source_artifact_sha256=source_bundle.artifact_sha256,
    )


def run_eod(
    request: EodRunRequest,
    *,
    python_executable: str | Path | None = None,
    extra_environment: dict[str, str] | None = None,
    result_handoff: Path | None = None,
) -> int:
    """Run the accepted file EOD pipeline and return its process exit code.

    PostgreSQL credentials are deliberately withheld from the legacy child.
    Database recording belongs to the separate, non-authoritative post-pass.
    """

    legacy_script = request.project_root.resolve() / LEGACY_EOD_REL
    if not legacy_script.is_file():
        raise FileNotFoundError(f"Validated EOD runner not found: {legacy_script}")
    completed = subprocess.run(
        build_eod_command(
            request,
            python_executable=python_executable,
            result_handoff=result_handoff,
        ),
        cwd=request.project_root.resolve(),
        env=_legacy_environment(extra_environment),
        check=False,
    )
    return int(completed.returncode)


def render_command(command: Sequence[str]) -> str:
    """Render a Windows-safe diagnostic command without executing it."""

    return subprocess.list2cmdline(list(command))
