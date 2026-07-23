"""Oldest-first gate for non-authoritative PostgreSQL EOD finalization."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EOD_RUN_TIMESTAMP = re.compile(r"^\d{8}_\d{6}$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
STATUS_FILE_NAME = "postgres_finalization_status.json"
ATTEMPT_FILE_NAME = "postgres_finalization_last_attempt.json"
PUBLISH_MANIFEST_RELATIVE_PATH = Path(
    "staging/vrp_hybrid_v2_publish_manifest.json"
)
CANONICAL_RUNTIME_CONFIG_RELATIVE_PATH = Path(
    "config/vrp_hybrid_v2_eod_runtime_config.json"
)
CANONICAL_EOD_AUDIT_RELATIVE_PATH = Path("data/audit/vrp_hybrid_v2_eod")


class UnresolvedEodFinalizationError(RuntimeError):
    """An older published file result must be finalized before advancing."""


@dataclass(frozen=True)
class UnresolvedEodFinalization:
    run_dir: Path
    reason: str


@dataclass(frozen=True)
class CompletedEodFinalizationEvidence:
    """Exact file evidence that must still exist in the connected database."""

    run_dir: Path
    pipeline_run_id: str
    market_snapshot_id: str
    selected_signal_id: str
    environment: str
    code_version: str
    snapshot_content_sha256: str
    database_projection_sha256: str
    database_readback_sha256: str
    run_manifest_sha256: str
    source_bundle_sha256: str


def _stable_json(path: Path, label: str) -> tuple[dict[str, Any], str]:
    if not os.path.lexists(path):
        raise ValueError(f"{label} does not exist: {path}")
    is_junction = getattr(path, "is_junction", lambda: False)
    if path.is_symlink() or is_junction() or not path.is_file():
        raise ValueError(f"{label} must be a regular file: {path}")
    try:
        before = path.read_bytes()
        middle = path.read_bytes()
        if before != middle:
            raise ValueError(f"{label} changed while it was read")
        payload = json.loads(before.decode("utf-8"))
        after = path.read_bytes()
        if before != after:
            raise ValueError(f"{label} changed while it was read")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"cannot read stable {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain a JSON object")
    return payload, hashlib.sha256(before).hexdigest()


def resolve_eod_runtime_config_path(
    project_root: Path,
    runtime_config: Path | None,
) -> Path:
    """Resolve one explicit runtime contract inside its data-bearing project."""

    project_root = project_root.expanduser().resolve()
    config_path = runtime_config or CANONICAL_RUNTIME_CONFIG_RELATIVE_PATH
    if not config_path.is_absolute():
        config_path = project_root / config_path
    return config_path.expanduser().resolve(strict=True)


def require_canonical_eod_runtime_config(
    project_root: Path,
    runtime_config: Path | None,
) -> Path:
    """Reject a redirected runtime contract on any write-capable operation."""

    project_root = project_root.expanduser().resolve()
    canonical = (project_root / CANONICAL_RUNTIME_CONFIG_RELATIVE_PATH).resolve(
        strict=True
    )
    selected = resolve_eod_runtime_config_path(project_root, runtime_config)
    if selected != canonical:
        raise ValueError(
            "write-capable EOD operations require the canonical production "
            f"runtime configuration: {canonical}"
        )
    configured_audit_root = resolve_eod_audit_root(project_root, canonical)
    fixed_audit_root = (project_root / CANONICAL_EOD_AUDIT_RELATIVE_PATH).resolve()
    if configured_audit_root != fixed_audit_root:
        raise ValueError(
            "the production EOD audit queue cannot be redirected; expected "
            f"{fixed_audit_root}, found {configured_audit_root}"
        )
    return canonical


def resolve_eod_audit_root(project_root: Path, runtime_config: Path | None) -> Path:
    """Resolve the configured timestamped EOD audit-root directory."""

    project_root = project_root.expanduser().resolve()
    canonical_runtime_selected = runtime_config is None
    config_path = resolve_eod_runtime_config_path(project_root, runtime_config)
    payload, _ = _stable_json(config_path, "EOD runtime configuration")
    outputs = payload.get("outputs", {})
    if not isinstance(outputs, dict):
        raise ValueError("EOD runtime configuration outputs must be an object")
    audit_value = outputs.get("audit_dir", "data/audit/vrp_hybrid_v2_eod")
    if not isinstance(audit_value, str) or not audit_value.strip():
        raise ValueError("EOD runtime configuration audit_dir is invalid")
    audit_root = Path(audit_value).expanduser()
    if not audit_root.is_absolute():
        audit_root = project_root / audit_root
    audit_root = audit_root.resolve()
    if canonical_runtime_selected:
        fixed_audit_root = (project_root / CANONICAL_EOD_AUDIT_RELATIVE_PATH).resolve()
        if audit_root != fixed_audit_root:
            raise ValueError(
                "the production EOD audit queue cannot be redirected; expected "
                f"{fixed_audit_root}, found {audit_root}"
            )
    return audit_root


def _published_pass(manifest: dict[str, Any]) -> bool:
    return bool(
        manifest.get("status") == "PASS"
        and manifest.get("final_health") == "PASS"
        and manifest.get("publish_requested") is True
        and manifest.get("skip_upstream") is False
        and manifest.get("postgres_postpass_required") is True
        and isinstance(manifest.get("published_outputs"), dict)
        and manifest["published_outputs"]
    )


def _missing_manifest_has_completion_evidence(run_dir: Path) -> bool:
    """Distinguish an abandoned empty claim from a damaged completed audit."""

    return any(
        os.path.lexists(run_dir / relative_path)
        for relative_path in (
            Path("run_status.json"),
            Path(STATUS_FILE_NAME),
            Path(ATTEMPT_FILE_NAME),
            PUBLISH_MANIFEST_RELATIVE_PATH,
        )
    )


def _completed_status_matches(
    status: dict[str, Any],
    *,
    run_dir: Path,
    manifest_sha256: str,
    environment: str,
    code_version: str,
) -> bool:
    try:
        recorded_run_dir = Path(str(status.get("run_dir"))).expanduser().resolve()
    except (OSError, TypeError, ValueError):
        return False
    try:
        _continuity_identity(status)
    except ValueError:
        return False
    return bool(
        status.get("status") == "COMPLETED"
        and status.get("exit_code") == 0
        and recorded_run_dir == run_dir
        and status.get("run_manifest_sha256") == manifest_sha256
        and status.get("environment") == environment
        and status.get("code_version") == code_version
        and isinstance(status.get("source_bundle_sha256"), str)
        and SHA256_HEX.fullmatch(status["source_bundle_sha256"]) is not None
    )


def _canonical_uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} is missing from finalization evidence")
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not a UUID") from exc
    canonical = str(parsed)
    if value != canonical:
        raise ValueError(f"{label} is not a canonical lowercase UUID")
    return canonical


def _continuity_identity(status: dict[str, Any]) -> dict[str, str]:
    preflight = status.get("preflight")
    postgres_shadow = status.get("postgres_shadow")
    if not isinstance(preflight, dict) or not isinstance(postgres_shadow, dict):
        raise ValueError("finalization evidence lacks preflight or shadow results")
    snapshot = preflight.get("snapshot_validation")
    result = postgres_shadow.get("result")
    if not isinstance(snapshot, dict) or not isinstance(result, dict):
        raise ValueError("finalization evidence lacks exact database identity")
    snapshot_content_sha256 = snapshot.get("content_sha256")
    if (
        not isinstance(snapshot_content_sha256, str)
        or SHA256_HEX.fullmatch(snapshot_content_sha256) is None
    ):
        raise ValueError("finalization evidence has no snapshot content digest")
    database_projection_sha256 = result.get("database_projection_sha256")
    if (
        not isinstance(database_projection_sha256, str)
        or SHA256_HEX.fullmatch(database_projection_sha256) is None
    ):
        raise ValueError("finalization evidence has no database projection digest")
    database_readback_sha256 = result.get("database_readback_sha256")
    if (
        not isinstance(database_readback_sha256, str)
        or SHA256_HEX.fullmatch(database_readback_sha256) is None
    ):
        raise ValueError("finalization evidence has no database read-back digest")
    return {
        "pipeline_run_id": _canonical_uuid(
            result.get("pipeline_run_id"), "pipeline_run_id"
        ),
        "market_snapshot_id": _canonical_uuid(
            result.get("market_snapshot_id"), "market_snapshot_id"
        ),
        "selected_signal_id": _canonical_uuid(
            result.get("selected_signal_id"), "selected_signal_id"
        ),
        "snapshot_content_sha256": snapshot_content_sha256,
        "database_projection_sha256": database_projection_sha256,
        "database_readback_sha256": database_readback_sha256,
    }


def find_unresolved_eod_finalizations(
    audit_root: Path,
    *,
    before_timestamp: str | None = None,
) -> list[UnresolvedEodFinalization]:
    """Return published PASS runs that lack matching terminal DB evidence."""

    audit_root = audit_root.expanduser().resolve()
    if not audit_root.exists():
        return []
    if not audit_root.is_dir():
        raise ValueError(f"EOD audit root is not a directory: {audit_root}")
    if before_timestamp is not None and EOD_RUN_TIMESTAMP.fullmatch(
        before_timestamp
    ) is None:
        raise ValueError("before_timestamp must be YYYYMMDD_HHMMSS")

    unresolved: list[UnresolvedEodFinalization] = []
    for run_dir in sorted(audit_root.iterdir(), key=lambda path: path.name):
        if not run_dir.is_dir() or EOD_RUN_TIMESTAMP.fullmatch(run_dir.name) is None:
            continue
        if before_timestamp is not None and run_dir.name >= before_timestamp:
            continue
        manifest_path = run_dir / "run_manifest.json"
        if not os.path.lexists(manifest_path):
            if _missing_manifest_has_completion_evidence(run_dir):
                unresolved.append(
                    UnresolvedEodFinalization(
                        run_dir,
                        "EOD run manifest is missing while completion or "
                        "finalization evidence remains",
                    )
                )
            continue
        try:
            manifest, manifest_sha256 = _stable_json(
                manifest_path,
                "EOD run manifest",
            )
        except ValueError as exc:
            unresolved.append(UnresolvedEodFinalization(run_dir, str(exc)))
            continue
        if not _published_pass(manifest):
            continue
        if manifest.get("run_timestamp") != run_dir.name:
            unresolved.append(
                UnresolvedEodFinalization(
                    run_dir,
                    "published PASS manifest timestamp does not match its directory",
                )
            )
            continue
        environment = manifest.get("postgres_environment")
        if not isinstance(environment, str) or not environment.strip():
            unresolved.append(
                UnresolvedEodFinalization(
                    run_dir,
                    "post-pass-required manifest has no PostgreSQL environment",
                )
            )
            continue
        code_version = manifest.get("code_version")
        if (
            not isinstance(code_version, str)
            or re.fullmatch(r"[0-9a-f]{40}", code_version) is None
        ):
            unresolved.append(
                UnresolvedEodFinalization(
                    run_dir,
                    "post-pass-required manifest has no exact producing Git commit",
                )
            )
            continue
        status_path = run_dir / STATUS_FILE_NAME
        if not os.path.lexists(status_path):
            unresolved.append(
                UnresolvedEodFinalization(run_dir, "finalization status is missing")
            )
            continue
        try:
            status, _ = _stable_json(status_path, "PostgreSQL finalization status")
        except ValueError as exc:
            unresolved.append(UnresolvedEodFinalization(run_dir, str(exc)))
            continue
        if not _completed_status_matches(
            status,
            run_dir=run_dir.resolve(),
            manifest_sha256=manifest_sha256,
            environment=environment,
            code_version=code_version,
        ):
            unresolved.append(
                UnresolvedEodFinalization(
                    run_dir,
                    "finalization status is not a matching terminal COMPLETED record",
                )
            )
            continue
        attempt_path = run_dir / ATTEMPT_FILE_NAME
        if os.path.lexists(attempt_path):
            try:
                attempt, _ = _stable_json(
                    attempt_path,
                    "PostgreSQL finalization retry status",
                )
            except ValueError as exc:
                unresolved.append(UnresolvedEodFinalization(run_dir, str(exc)))
                continue
            if not _completed_status_matches(
                attempt,
                run_dir=run_dir.resolve(),
                manifest_sha256=manifest_sha256,
                environment=environment,
                code_version=code_version,
            ):
                unresolved.append(
                    UnresolvedEodFinalization(
                        run_dir,
                        "latest finalization retry is not a matching COMPLETED record",
                    )
                )
    return unresolved


def completed_eod_finalization_evidence(
    audit_root: Path,
    *,
    before_timestamp: str | None = None,
    environment: str | None = None,
) -> list[CompletedEodFinalizationEvidence]:
    """Return DB identities for every earlier, file-complete obligated run."""

    assert_no_unresolved_eod_finalizations(
        audit_root,
        before_timestamp=before_timestamp,
    )
    if environment is not None and (
        not isinstance(environment, str)
        or not environment.strip()
        or environment != environment.strip()
    ):
        raise ValueError("environment must be canonical non-empty text")
    audit_root = audit_root.expanduser().resolve()
    evidence: list[CompletedEodFinalizationEvidence] = []
    if not audit_root.exists():
        return evidence
    for run_dir in sorted(audit_root.iterdir(), key=lambda path: path.name):
        if not run_dir.is_dir() or EOD_RUN_TIMESTAMP.fullmatch(run_dir.name) is None:
            continue
        if before_timestamp is not None and run_dir.name >= before_timestamp:
            continue
        manifest_path = run_dir / "run_manifest.json"
        if not os.path.lexists(manifest_path):
            continue
        manifest, manifest_sha256 = _stable_json(manifest_path, "EOD run manifest")
        if not _published_pass(manifest):
            continue
        if environment is not None and manifest.get("postgres_environment") != environment:
            continue
        status_path = run_dir / STATUS_FILE_NAME
        attempt_path = run_dir / ATTEMPT_FILE_NAME
        selected_path = attempt_path if os.path.lexists(attempt_path) else status_path
        status, _ = _stable_json(
            selected_path,
            "PostgreSQL finalization database-continuity evidence",
        )
        identity = _continuity_identity(status)
        evidence.append(
            CompletedEodFinalizationEvidence(
                run_dir=run_dir.resolve(),
                pipeline_run_id=identity["pipeline_run_id"],
                market_snapshot_id=identity["market_snapshot_id"],
                selected_signal_id=identity["selected_signal_id"],
                environment=str(manifest["postgres_environment"]),
                code_version=str(manifest["code_version"]),
                snapshot_content_sha256=identity["snapshot_content_sha256"],
                database_projection_sha256=identity[
                    "database_projection_sha256"
                ],
                database_readback_sha256=identity[
                    "database_readback_sha256"
                ],
                run_manifest_sha256=manifest_sha256,
                source_bundle_sha256=str(status["source_bundle_sha256"]),
            )
        )
    return evidence


def assert_no_unresolved_eod_finalizations(
    audit_root: Path,
    *,
    before_timestamp: str | None = None,
) -> None:
    """Fail closed on the earliest unresolved published PASS file run."""

    unresolved = find_unresolved_eod_finalizations(
        audit_root,
        before_timestamp=before_timestamp,
    )
    if not unresolved:
        return
    earliest = unresolved[0]
    raise UnresolvedEodFinalizationError(
        "oldest published EOD PostgreSQL finalization is unresolved: "
        f"{earliest.run_dir} ({earliest.reason})"
    )


__all__ = [
    "CANONICAL_EOD_AUDIT_RELATIVE_PATH",
    "CANONICAL_RUNTIME_CONFIG_RELATIVE_PATH",
    "CompletedEodFinalizationEvidence",
    "UnresolvedEodFinalization",
    "UnresolvedEodFinalizationError",
    "assert_no_unresolved_eod_finalizations",
    "completed_eod_finalization_evidence",
    "find_unresolved_eod_finalizations",
    "require_canonical_eod_runtime_config",
    "resolve_eod_audit_root",
    "resolve_eod_runtime_config_path",
]
