"""Standalone post-EOD PostgreSQL finalization.

The accepted file pipeline remains authoritative.  This module operates only
on one explicitly supplied, already-published EOD audit directory.  It first
validates that directory, then advances the compact PostgreSQL reference data,
and finally records the reconciled EOD shadow projection.

Database credentials are inherited by the two database subprocesses.  They
are never accepted as arguments, rendered in commands, or persisted in the
status sidecar.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, ContextManager, Mapping, Sequence

from .eod import resolve_clean_code_version
from .eod_bundle import load_eod_source_bundle
from .eod_finalization_gate import (
    EOD_RUN_TIMESTAMP,
    assert_no_unresolved_eod_finalizations,
    completed_eod_finalization_evidence,
    require_canonical_eod_runtime_config,
    resolve_eod_audit_root,
)
from .eod_lock import (
    EOD_WRITER_LOCK_PATH_ENV,
    EOD_WRITER_LOCK_TOKEN_ENV,
    EodWriterAlreadyRunningError,
    EodWriterLease,
    delegated_eod_writer_environment,
    eod_writer_execution_lock,
    exclusive_eod_canonical_writer_lock,
)
from vrp.storage.finalization_coordination import (
    DELEGATED_CHILD_ACTIVE_ADVISORY_KEY,
    FINALIZATION_LEASE_TOKEN_ENV,
    GLOBAL_FINALIZATION_ADVISORY_KEY,
    token_advisory_key,
)
from vrp.storage.finalization_continuity import (
    verify_database_finalization_continuity,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
REFERENCE_LOADER = REPOSITORY_ROOT / "scripts" / "load_reference_history.py"
EOD_SNAPSHOT_LOADER = REPOSITORY_ROOT / "scripts" / "load_eod_snapshot.py"

STATUS_FILE_NAME = "postgres_finalization_status.json"
ATTEMPT_FILE_NAME = "postgres_finalization_last_attempt.json"
LOCK_FILE_NAME = ".postgres_finalization.lock"
STATUS_SCHEMA_VERSION = 1

EXIT_SUCCESS = 0
EXIT_PREFLIGHT_FAILED = 10
EXIT_REFERENCE_SYNC_FAILED = 20
EXIT_SHADOW_LOAD_FAILED = 30
EXIT_STATUS_WRITE_FAILED = 40
SUBPROCESS_TIMEOUT_SECONDS = 900
DATABASE_ADVISORY_LOCK_KEY = GLOBAL_FINALIZATION_ADVISORY_KEY

_DATABASE_ENVIRONMENT_KEYS = frozenset(
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

_REQUIRED_PUBLISHED_OUTPUTS = frozenset(
    {
        "execution_handoff",
        "forecast_history",
        "latest_snapshot",
        "selected_decisions",
        "signal_history",
        "static_tiebreaks",
    }
)
_DATABASE_URL_PATTERN = re.compile(
    r"(?i)\b(?:postgres|postgresql)(?:\+[a-z0-9_-]+)?://[^\s]+"
)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(?:database_url|password|pgpassword|vrp_database_url)\s*=\s*[^\s;]+"
)


class FinalizerPreflightError(RuntimeError):
    """Raised when an exact audit directory is not a published PASS run."""


class FinalizerStepError(RuntimeError):
    """Raised when one isolated child step fails or returns malformed output."""

    def __init__(self, step: str, message: str, *, return_code: int | None = None):
        super().__init__(message)
        self.step = step
        self.return_code = return_code


class FinalizerAlreadyRunningError(RuntimeError):
    """Raised when another process owns the exact run's finalization lock."""


class DatabaseFinalizationAlreadyRunningError(RuntimeError):
    """Raised when another database session owns the global finalization lock."""


_EXPECTED_LOCK_CONTENTION_ERRNOS = frozenset({errno.EACCES, errno.EAGAIN})
_EXPECTED_WINDOWS_LOCK_ERRORS = frozenset({32, 33})


def _is_expected_lock_contention(exc: OSError) -> bool:
    return (
        exc.errno in _EXPECTED_LOCK_CONTENTION_ERRNOS
        or getattr(exc, "winerror", None) in _EXPECTED_WINDOWS_LOCK_ERRORS
    )


@dataclass(frozen=True)
class EodPostgresFinalizerRequest:
    project_root: Path
    run_dir: Path
    artifact_root: Path
    environment: str
    code_version: str
    requested_by: str
    run_manifest_sha256: str
    source_bundle_sha256: str
    python_executable: Path = Path(sys.executable)


@dataclass(frozen=True)
class EodPostgresFinalizerResult:
    exit_code: int
    status: str
    payload: Mapping[str, Any]
    status_path: Path | None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "exit_code": self.exit_code,
            "run_dir": self.payload.get("run_dir"),
            "status": self.status,
            "status_path": None if self.status_path is None else str(self.status_path),
        }


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
StatusWriter = Callable[[Path, Mapping[str, Any]], None]
DatabaseLockFactory = Callable[
    [EodPostgresFinalizerRequest],
    ContextManager[Any],
]


@contextmanager
def exclusive_run_lock(run_dir: Path):
    """Hold a non-blocking OS lock for one exact run until finalization exits."""

    lock_path = run_dir.resolve() / LOCK_FILE_NAME
    if os.path.lexists(lock_path):
        is_junction = getattr(lock_path, "is_junction", lambda: False)
        if lock_path.is_symlink() or is_junction() or not lock_path.is_file():
            raise OSError(f"finalization lock must be a regular file: {lock_path}")
    handle = lock_path.open("a+b")
    acquired = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if not _is_expected_lock_contention(exc):
                raise
            raise FinalizerAlreadyRunningError(
                f"PostgreSQL finalization is already running for {run_dir.resolve()}"
            ) from exc
        acquired = True
        yield lock_path
    finally:
        if acquired:
            try:
                handle.seek(0)
                try:
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    # Closing the descriptor also releases an OS-owned lock.
                    pass
            finally:
                handle.close()
        else:
            handle.close()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FinalizerPreflightError(f"{name} must be a non-empty string")
    return value.strip()


def _safe_text(value: object, *, limit: int = 2000) -> str:
    rendered = str(value)
    rendered = _DATABASE_URL_PATTERN.sub("[REDACTED_DATABASE_URL]", rendered)
    rendered = _SECRET_ASSIGNMENT_PATTERN.sub("[REDACTED_SECRET]", rendered)
    if len(rendered) > limit:
        rendered = rendered[:limit] + "..."
    return rendered


def _safe_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        safe: dict[str, Any] = {}
        for key, item in value.items():
            rendered_key = str(key)
            if rendered_key.lower() in {
                "database_url",
                "dsn",
                "password",
                "pgpassword",
                "vrp_database_url",
            }:
                safe[rendered_key] = "[REDACTED_SECRET]"
            else:
                safe[rendered_key] = _safe_value(item)
        return safe
    if isinstance(value, (tuple, list)):
        return [_safe_value(item) for item in value]
    if isinstance(value, str):
        return _safe_text(value, limit=max(2000, len(value)))
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _safe_text(value)


def write_status_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Replace the status sidecar atomically without leaving a partial JSON file."""

    parent = path.expanduser().parent.resolve(strict=True)
    path = parent / path.name
    if not parent.is_dir():
        raise FileNotFoundError(f"status directory does not exist: {parent}")
    if os.path.lexists(path):
        is_junction = getattr(path, "is_junction", lambda: False)
        if path.is_symlink() or is_junction() or not path.is_file():
            raise FinalizerPreflightError(
                f"status sidecar must be a regular file: {path}"
            )
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        handle = os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n")
        file_descriptor = -1
        with handle:
            json.dump(
                _safe_value(dict(payload)),
                handle,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        try:
            if file_descriptor >= 0:
                os.close(file_descriptor)
        finally:
            temporary_path.unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_object(
    path: Path,
    label: str,
    *,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    if not path.is_file():
        raise FinalizerPreflightError(f"{label} does not exist: {path}")
    try:
        before = _sha256_file(path)
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        after = _sha256_file(path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FinalizerPreflightError(f"{label} is not valid JSON: {path}") from exc
    observed = hashlib.sha256(raw).hexdigest()
    if before != observed or after != observed:
        raise FinalizerPreflightError(f"{label} changed while it was being read")
    if expected_sha256 is not None and observed != expected_sha256:
        raise FinalizerPreflightError(
            f"{label} does not match the caller-pinned digest"
        )
    if not isinstance(payload, dict):
        raise FinalizerPreflightError(f"{label} must contain a JSON object: {path}")
    return payload


def _resolve_recorded_path(value: object, *, project_root: Path, label: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise FinalizerPreflightError(f"{label} is missing from the run manifest")
    path = Path(value).expanduser()
    return (path if path.is_absolute() else project_root / path).resolve()


def _validate_request_identity(
    request: EodPostgresFinalizerRequest,
) -> EodPostgresFinalizerRequest:
    """Normalize and validate the immutable retry identity without reading Git."""

    project_root = request.project_root.expanduser().resolve()
    run_dir = request.run_dir.expanduser().resolve()
    artifact_root = request.artifact_root.expanduser().resolve()
    python_executable = request.python_executable.expanduser()
    if python_executable.is_absolute():
        python_executable = python_executable.resolve()
    if not project_root.is_dir():
        raise FinalizerPreflightError(f"project root does not exist: {project_root}")
    if not run_dir.is_dir():
        raise FinalizerPreflightError(f"exact EOD run directory does not exist: {run_dir}")
    try:
        runtime_config = require_canonical_eod_runtime_config(project_root, None)
        canonical_audit_root = resolve_eod_audit_root(
            project_root,
            runtime_config,
        )
    except (OSError, ValueError) as exc:
        raise FinalizerPreflightError(
            f"canonical EOD runtime validation failed: {exc}"
        ) from exc
    if run_dir.parent != canonical_audit_root:
        raise FinalizerPreflightError(
            "exact run directory is outside the canonical EOD audit root"
        )
    if EOD_RUN_TIMESTAMP.fullmatch(run_dir.name) is None:
        raise FinalizerPreflightError(
            "exact run directory must be a timestamped direct child of the "
            "canonical EOD audit root"
        )
    environment = _required_text(request.environment, "environment")
    requested_by = _required_text(request.requested_by, "requested_by")
    code_version = _required_text(request.code_version, "code_version")
    run_manifest_sha256 = _required_text(
        request.run_manifest_sha256,
        "run_manifest_sha256",
    )
    source_bundle_sha256 = _required_text(
        request.source_bundle_sha256,
        "source_bundle_sha256",
    )
    if re.fullmatch(r"[0-9a-f]{40}", code_version) is None:
        raise FinalizerPreflightError(
            "code_version must be a full 40-character lowercase Git SHA"
        )
    if re.fullmatch(r"[0-9a-f]{64}", run_manifest_sha256) is None:
        raise FinalizerPreflightError(
            "run_manifest_sha256 must be a lowercase SHA-256"
        )
    if re.fullmatch(r"[0-9a-f]{64}", source_bundle_sha256) is None:
        raise FinalizerPreflightError(
            "source_bundle_sha256 must be a lowercase SHA-256"
        )
    return EodPostgresFinalizerRequest(
        project_root=project_root,
        run_dir=run_dir,
        artifact_root=artifact_root,
        environment=environment,
        code_version=code_version,
        requested_by=requested_by,
        run_manifest_sha256=run_manifest_sha256,
        source_bundle_sha256=source_bundle_sha256,
        python_executable=python_executable,
    )


def _validate_checkout_identity(
    request: EodPostgresFinalizerRequest,
) -> EodPostgresFinalizerRequest:
    """Require the live checkout to match the already-validated immutable SHA."""

    try:
        code_version = resolve_clean_code_version(
            source_root=REPOSITORY_ROOT,
            project_root=request.project_root,
            explicit=request.code_version,
        )
    except ValueError as exc:
        raise FinalizerPreflightError(
            f"production code identity validation failed: {exc}"
        ) from exc
    return EodPostgresFinalizerRequest(
        project_root=request.project_root,
        run_dir=request.run_dir,
        artifact_root=request.artifact_root,
        environment=request.environment,
        code_version=code_version,
        requested_by=request.requested_by,
        run_manifest_sha256=request.run_manifest_sha256,
        source_bundle_sha256=request.source_bundle_sha256,
        python_executable=request.python_executable,
    )


def _validate_published_run(request: EodPostgresFinalizerRequest) -> dict[str, Any]:
    source_bundle = load_eod_source_bundle(request.run_dir)
    if source_bundle.content_sha256 != request.source_bundle_sha256:
        raise FinalizerPreflightError(
            "EOD source bundle does not match the caller-pinned digest"
        )
    run_manifest = _read_json_object(
        request.run_dir / "run_manifest.json",
        "run manifest",
        expected_sha256=request.run_manifest_sha256,
    )
    run_status = _read_json_object(
        request.run_dir / "run_status.json",
        "run status",
        expected_sha256=source_bundle.artifact_sha256["run_status"],
    )

    if run_manifest.get("status") != "PASS":
        raise FinalizerPreflightError("run manifest status must be PASS")
    if run_manifest.get("final_health") != "PASS":
        raise FinalizerPreflightError("run manifest final_health must be PASS")
    if run_manifest.get("publish_requested") is not True:
        raise FinalizerPreflightError("the finalizer rejects no-publish EOD runs")
    if run_manifest.get("skip_upstream") is not False:
        raise FinalizerPreflightError(
            "the finalizer requires a fresh EOD run with skip_upstream=false"
        )
    if run_manifest.get("code_version") != request.code_version:
        raise FinalizerPreflightError(
            "run manifest code_version does not match the exact producing commit"
        )
    if run_manifest.get("postgres_postpass_required") is not True:
        raise FinalizerPreflightError(
            "run manifest is not bound to the automatic PostgreSQL post-pass"
        )
    if run_manifest.get("postgres_environment") != request.environment:
        raise FinalizerPreflightError(
            "run manifest PostgreSQL environment does not match the finalizer request"
        )
    if not isinstance(run_manifest.get("finished_at"), str):
        raise FinalizerPreflightError("run manifest does not contain finished_at")

    recorded_project_root = _resolve_recorded_path(
        run_manifest.get("project_root"),
        project_root=request.project_root,
        label="project_root",
    )
    if recorded_project_root != request.project_root:
        raise FinalizerPreflightError(
            "run manifest project_root does not match the explicit project root"
        )

    published_outputs = run_manifest.get("published_outputs")
    if not isinstance(published_outputs, dict):
        raise FinalizerPreflightError("run manifest published_outputs must be an object")
    missing_outputs = sorted(_REQUIRED_PUBLISHED_OUTPUTS - set(published_outputs))
    if missing_outputs:
        raise FinalizerPreflightError(
            f"run manifest is missing published outputs: {missing_outputs}"
        )
    if any(
        not isinstance(published_outputs[name], str) or not published_outputs[name].strip()
        for name in _REQUIRED_PUBLISHED_OUTPUTS
    ):
        raise FinalizerPreflightError("run manifest contains an invalid published output path")

    if run_status.get("status") != "PASS" or run_status.get("published") is not True:
        raise FinalizerPreflightError("run status must be PASS with published=true")
    recorded_audit_dir = _resolve_recorded_path(
        run_status.get("audit_dir"),
        project_root=request.project_root,
        label="audit_dir",
    )
    if recorded_audit_dir != request.run_dir:
        raise FinalizerPreflightError(
            "run status audit_dir does not match the explicit run directory"
        )
    for field in ("target_date", "run_timestamp"):
        if run_status.get(field) != run_manifest.get(field):
            raise FinalizerPreflightError(f"run status {field} does not match the manifest")

    data_health = run_status.get("data_health")
    if not isinstance(data_health, dict) or data_health.get("overall_status") != "PASS":
        raise FinalizerPreflightError("run status data health must be PASS")

    runtime_config = _resolve_recorded_path(
        run_manifest.get("runtime_config"),
        project_root=request.project_root,
        label="runtime_config",
    )
    if not runtime_config.is_file():
        raise FinalizerPreflightError(
            f"recorded runtime configuration does not exist: {runtime_config}"
        )
    try:
        require_canonical_eod_runtime_config(
            request.project_root,
            runtime_config,
        )
    except (OSError, ValueError) as exc:
        raise FinalizerPreflightError(str(exc)) from exc
    runtime_payload = _read_json_object(runtime_config, "runtime configuration")
    runtime_outputs = runtime_payload.get("outputs", {})
    if not isinstance(runtime_outputs, dict):
        raise FinalizerPreflightError("runtime configuration outputs must be an object")
    audit_value = runtime_outputs.get(
        "audit_dir",
        "data/audit/vrp_hybrid_v2_eod",
    )
    if not isinstance(audit_value, str) or not audit_value.strip():
        raise FinalizerPreflightError(
            "runtime configuration audit directory is invalid"
        )
    configured_audit_root = Path(audit_value).expanduser()
    if not configured_audit_root.is_absolute():
        configured_audit_root = request.project_root / configured_audit_root
    configured_audit_root = configured_audit_root.resolve()
    canonical_audit_root = resolve_eod_audit_root(request.project_root, None)
    if configured_audit_root != canonical_audit_root:
        raise FinalizerPreflightError(
            "recorded runtime configuration redirects the canonical EOD audit root"
        )
    if request.run_dir.parent != configured_audit_root:
        raise FinalizerPreflightError(
            "exact run directory is outside the configured EOD audit root"
        )
    sofr_manifest = _resolve_recorded_path(
        run_manifest.get("sofr_manifest"),
        project_root=request.project_root,
        label="sofr_manifest",
    )
    if not sofr_manifest.is_file():
        raise FinalizerPreflightError(
            f"recorded SOFR updater manifest does not exist: {sofr_manifest}"
        )

    return {
        "final_health": "PASS",
        "published": True,
        "published_output_count": len(_REQUIRED_PUBLISHED_OUTPUTS),
        "run_timestamp": run_manifest.get("run_timestamp"),
        "runtime_config": str(runtime_config),
        "sofr_manifest": str(sofr_manifest),
        "source_bundle": source_bundle.to_json_dict(),
        "target_date": run_manifest.get("target_date"),
    }


def build_validation_command(request: EodPostgresFinalizerRequest) -> list[str]:
    return [
        str(request.python_executable),
        "-u",
        str(EOD_SNAPSHOT_LOADER),
        "--project-root",
        str(request.project_root),
        "--run-dir",
        str(request.run_dir),
        "--expected-run-manifest-sha256",
        request.run_manifest_sha256,
        "--expected-source-bundle-sha256",
        request.source_bundle_sha256,
        "--validate-only",
    ]


def build_reference_sync_command(
    request: EodPostgresFinalizerRequest,
    *,
    runtime_config: Path,
    sofr_source: Path,
    expected_sofr_source_sha256: str,
) -> list[str]:
    return [
        str(request.python_executable),
        "-u",
        str(REFERENCE_LOADER),
        "all",
        "--project-root",
        str(request.project_root),
        "--runtime-config",
        str(runtime_config),
        "--sofr-source",
        str(sofr_source),
        "--expected-sofr-source-sha256",
        expected_sofr_source_sha256,
        "--artifact-root",
        str(request.artifact_root),
        "--environment",
        request.environment,
        "--code-version",
        request.code_version,
        "--requested-by",
        request.requested_by,
    ]


def build_shadow_load_command(
    request: EodPostgresFinalizerRequest,
    *,
    expected_content_sha256: str,
) -> list[str]:
    return [
        str(request.python_executable),
        "-u",
        str(EOD_SNAPSHOT_LOADER),
        "--project-root",
        str(request.project_root),
        "--run-dir",
        str(request.run_dir),
        "--expected-run-manifest-sha256",
        request.run_manifest_sha256,
        "--expected-source-bundle-sha256",
        request.source_bundle_sha256,
        "--expected-content-sha256",
        expected_content_sha256,
        "--environment",
        request.environment,
        "--code-version",
        request.code_version,
        "--requested-by",
        request.requested_by,
    ]


def _run_json_command(
    step: str,
    command: Sequence[str],
    *,
    environment: Mapping[str, str],
    runner: CommandRunner,
    timeout_seconds: int = SUBPROCESS_TIMEOUT_SECONDS,
) -> Any:
    try:
        completed = runner(
            list(command),
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            env=dict(environment),
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise FinalizerStepError(
            step,
            f"child process timed out after {timeout_seconds} seconds",
        ) from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise FinalizerStepError(step, _safe_text(exc)) from exc
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if completed.returncode != 0:
        diagnostic = stderr.strip() or stdout.strip()
        if not diagnostic:
            diagnostic = f"child process exited with code {completed.returncode}"
        raise FinalizerStepError(
            step,
            _safe_text(diagnostic),
            return_code=int(completed.returncode),
        )
    try:
        return json.loads(stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise FinalizerStepError(
            step,
            f"child process returned invalid JSON: {_safe_text(stdout)}",
            return_code=int(completed.returncode),
        ) from exc


def _error_payload(exc: Exception) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": type(exc).__name__,
        "message": _safe_text(exc),
    }
    return_code = getattr(exc, "return_code", None)
    if return_code is not None:
        payload["child_return_code"] = int(return_code)
    return payload


def _validation_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for key in tuple(environment):
        upper = key.upper()
        if upper in _DATABASE_ENVIRONMENT_KEYS or upper.endswith("_DATABASE_URL"):
            environment.pop(key, None)
    return environment


def _database_environment(lease_token: str | None = None) -> dict[str, str]:
    environment = os.environ.copy()
    for key in (
        EOD_WRITER_LOCK_PATH_ENV,
        EOD_WRITER_LOCK_TOKEN_ENV,
    ):
        environment.pop(key, None)
    environment.pop(FINALIZATION_LEASE_TOKEN_ENV, None)
    if lease_token is not None:
        environment[FINALIZATION_LEASE_TOKEN_ENV] = lease_token
    return environment


@contextmanager
def exclusive_database_finalization_lock(
    request: EodPostgresFinalizerRequest,
):
    """Hold one database-global session lock across reference sync and shadow."""

    database_url = os.environ.get("VRP_DATABASE_URL")
    if not database_url:
        raise FinalizerPreflightError(
            "VRP_DATABASE_URL is required for PostgreSQL finalization"
        )
    try:
        import psycopg
    except ImportError as exc:
        raise FinalizerPreflightError(
            "Psycopg is required for the database finalization lock"
        ) from exc

    connection = None
    acquired_keys: list[int] = []
    try:
        connection = psycopg.connect(
            database_url,
            autocommit=True,
            connect_timeout=10,
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (DATABASE_ADVISORY_LOCK_KEY,))
            row = cursor.fetchone()
        if not bool(row and row[0] is True):
            raise DatabaseFinalizationAlreadyRunningError(
                "another PostgreSQL reference/shadow finalization is already running"
            )
        acquired_keys.append(DATABASE_ADVISORY_LOCK_KEY)

        # A delegated child holds this fixed key for its complete mutation.
        # Check it while the global lock is held so an orphan from a dead
        # coordinator cannot overlap this finalizer.
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (DELEGATED_CHILD_ACTIVE_ADVISORY_KEY,),
            )
            row = cursor.fetchone()
        if not bool(row and row[0] is True):
            raise DatabaseFinalizationAlreadyRunningError(
                "a delegated PostgreSQL mutating child is still active"
            )
        acquired_keys.append(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY)

        lease_token = secrets.token_hex(32)
        lease_key = token_advisory_key(lease_token)
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (lease_key,))
            row = cursor.fetchone()
        if not bool(row and row[0] is True):
            raise DatabaseFinalizationAlreadyRunningError(
                "could not establish the delegated PostgreSQL finalization lease"
            )
        acquired_keys.append(lease_key)

        prior_evidence = completed_eod_finalization_evidence(
            request.run_dir.parent,
            before_timestamp=request.run_dir.name,
            environment=request.environment,
        )
        verify_database_finalization_continuity(connection, prior_evidence)

        # Release the startup barrier only after the new random parent token
        # exists.  Any late child from a dead parent then fails its exact-token
        # proof before it can mutate.
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_unlock(%s)",
                (DELEGATED_CHILD_ACTIVE_ADVISORY_KEY,),
            )
            row = cursor.fetchone()
        if not bool(row and row[0] is True):
            raise DatabaseFinalizationAlreadyRunningError(
                "could not release the delegated-child startup barrier"
            )
        acquired_keys.remove(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY)
        yield lease_token
    finally:
        if connection is not None:
            for key in reversed(acquired_keys):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_advisory_unlock(%s)", (key,))
                except Exception:
                    pass
            connection.close()


def _base_payload(request: EodPostgresFinalizerRequest) -> dict[str, Any]:
    return {
        "artifact_root": str(request.artifact_root.expanduser().resolve()),
        "authoritative_file_result": "UNCHANGED",
        "code_version": request.code_version,
        "database_projection_authoritative": False,
        "environment": request.environment,
        "exit_code": None,
        "project_root": str(request.project_root.expanduser().resolve()),
        "publishes_signal": False,
        "requested_by": request.requested_by,
        "run_manifest_sha256": request.run_manifest_sha256,
        "source_bundle_sha256": request.source_bundle_sha256,
        "run_dir": str(request.run_dir.expanduser().resolve()),
        "schema_version": STATUS_SCHEMA_VERSION,
        "started_at": _utc_now(),
        "status": "RUNNING",
    }


_STATUS_IDENTITY_FIELDS = (
    "artifact_root",
    "code_version",
    "environment",
    "project_root",
    "run_dir",
    "run_manifest_sha256",
    "source_bundle_sha256",
)


def _in_memory_preflight_failure(
    request: EodPostgresFinalizerRequest,
    exc: Exception,
    *,
    status_path: Path | None,
) -> EodPostgresFinalizerResult:
    payload = _base_payload(request)
    payload.update(
        {
            "exit_code": EXIT_PREFLIGHT_FAILED,
            "finished_at": _utc_now(),
            "preflight": {"error": _error_payload(exc), "status": "FAILED"},
            "status": "PREFLIGHT_FAILED",
        }
    )
    return EodPostgresFinalizerResult(
        exit_code=EXIT_PREFLIGHT_FAILED,
        status="PREFLIGHT_FAILED",
        payload=_safe_value(payload),
        status_path=status_path,
    )


def _existing_attempt_result(
    request: EodPostgresFinalizerRequest,
    status_path: Path,
) -> tuple[EodPostgresFinalizerResult | None, dict[str, Any] | None]:
    """Preserve immutable retry identity and any terminal COMPLETED evidence."""

    if not os.path.lexists(status_path):
        return None, None
    is_junction = getattr(status_path, "is_junction", lambda: False)
    if status_path.is_symlink() or is_junction() or not status_path.is_file():
        return (
            _in_memory_preflight_failure(
                request,
                FinalizerPreflightError(
                    f"existing finalization status must be a regular file: {status_path}"
                ),
                status_path=status_path,
            ),
            None,
        )
    try:
        existing = _read_json_object(
            status_path,
            "existing PostgreSQL finalization status",
        )
    except Exception as exc:
        return (
            _in_memory_preflight_failure(
                request,
                exc,
                status_path=status_path,
            ),
            None,
        )

    expected = _base_payload(request)
    mismatches = [
        field
        for field in _STATUS_IDENTITY_FIELDS
        if existing.get(field) != expected.get(field)
    ]
    if mismatches:
        return (
            _in_memory_preflight_failure(
                request,
                FinalizerPreflightError(
                    "existing finalization attempt has different immutable identity: "
                    + ", ".join(mismatches)
                ),
                status_path=status_path,
            ),
            None,
        )
    if existing.get("status") == "COMPLETED":
        if existing.get("exit_code") != EXIT_SUCCESS:
            return (
                _in_memory_preflight_failure(
                    request,
                    FinalizerPreflightError(
                        "existing terminal COMPLETED status has a nonzero exit code"
                    ),
                    status_path=status_path,
                ),
                None,
            )
        return None, existing
    return None, None


def _status_write_failure(
    payload: Mapping[str, Any],
    status_path: Path,
    exc: Exception,
) -> EodPostgresFinalizerResult:
    failed = dict(payload)
    failed.update(
        {
            "exit_code": EXIT_STATUS_WRITE_FAILED,
            "finished_at": _utc_now(),
            "status": "STATUS_WRITE_FAILED",
            "status_write_error": _error_payload(exc),
        }
    )
    return EodPostgresFinalizerResult(
        exit_code=EXIT_STATUS_WRITE_FAILED,
        status="STATUS_WRITE_FAILED",
        payload=_safe_value(failed),
        status_path=status_path,
    )


def _write_checkpoint(
    status_path: Path,
    payload: Mapping[str, Any],
    *,
    status_writer: StatusWriter,
) -> EodPostgresFinalizerResult | None:
    try:
        status_writer(status_path, payload)
    except Exception as exc:  # status failures have their own operational code
        return _status_write_failure(payload, status_path, exc)
    return None


def _finish(
    status_path: Path,
    payload: dict[str, Any],
    *,
    status: str,
    exit_code: int,
    status_writer: StatusWriter,
) -> EodPostgresFinalizerResult:
    payload.update(
        {
            "exit_code": exit_code,
            "finished_at": _utc_now(),
            "status": status,
        }
    )
    write_failure = _write_checkpoint(status_path, payload, status_writer=status_writer)
    if write_failure is not None:
        return write_failure
    return EodPostgresFinalizerResult(
        exit_code=exit_code,
        status=status,
        payload=_safe_value(payload),
        status_path=status_path,
    )


def _run_database_projection(
    request: EodPostgresFinalizerRequest,
    *,
    published: Mapping[str, Any],
    validation: Mapping[str, Any],
    sofr_source: Path,
    sofr_source_sha256: str,
    snapshot_content_sha256: str,
    database_lease_token: str | None,
    status_path: Path,
    payload: dict[str, Any],
    runner: CommandRunner,
    status_writer: StatusWriter,
) -> EodPostgresFinalizerResult:
    """Run the two credentialed children while the caller holds the DB lease."""

    runtime_config = Path(str(published["runtime_config"]))
    try:
        reference_result = _run_json_command(
            "reference_history_sync",
            build_reference_sync_command(
                request,
                runtime_config=runtime_config,
                sofr_source=sofr_source,
                expected_sofr_source_sha256=sofr_source_sha256,
            ),
            environment=_database_environment(database_lease_token),
            runner=runner,
        )
        if not isinstance(reference_result, list):
            raise FinalizerStepError(
                "reference_history_sync",
                "reference-history child did not return a JSON list",
            )
        if len(reference_result) != 2 or not all(
            isinstance(item, dict) for item in reference_result
        ):
            raise FinalizerStepError(
                "reference_history_sync",
                "reference-history child must return exactly two result objects",
            )
        dataset_keys = [item.get("dataset_key") for item in reference_result]
        if len(set(dataset_keys)) != 2 or set(dataset_keys) != {
            "FRED_SOFR",
            "SPY_SIGNAL_DAILY_FEATURES",
        }:
            raise FinalizerStepError(
                "reference_history_sync",
                f"unexpected reference datasets: {sorted(str(item) for item in dataset_keys)}",
            )
        fred_result = next(
            item for item in reference_result if item["dataset_key"] == "FRED_SOFR"
        )
        if fred_result.get("content_sha256") != validation.get(
            "sofr_normalized_content_sha256"
        ):
            raise FinalizerStepError(
                "reference_history_sync",
                "FRED SOFR digest does not match the validated EOD snapshot",
            )
        payload["reference_history"] = {
            "datasets": _safe_value(reference_result),
            "status": "COMPLETED",
        }
    except Exception as exc:
        payload["reference_history"] = {
            "error": _error_payload(exc),
            "status": "FAILED",
        }
        return _finish(
            status_path,
            payload,
            status="REFERENCE_SYNC_FAILED",
            exit_code=EXIT_REFERENCE_SYNC_FAILED,
            status_writer=status_writer,
        )

    write_failure = _write_checkpoint(status_path, payload, status_writer=status_writer)
    if write_failure is not None:
        return write_failure

    try:
        shadow_result = _run_json_command(
            "eod_shadow_load",
            build_shadow_load_command(
                request,
                expected_content_sha256=snapshot_content_sha256,
            ),
            environment=_database_environment(database_lease_token),
            runner=runner,
        )
        if not isinstance(shadow_result, dict) or shadow_result.get(
            "status"
        ) != "COMPLETED":
            raise FinalizerStepError(
                "eod_shadow_load",
                "shadow child did not return status=COMPLETED",
            )
        if shadow_result.get("valuation_date") != validation.get("valuation_date"):
            raise FinalizerStepError(
                "eod_shadow_load",
                "shadow valuation_date does not match the validated EOD snapshot",
            )
        for identity_field in (
            "pipeline_run_id",
            "market_snapshot_id",
            "selected_signal_id",
        ):
            identity_value = shadow_result.get(identity_field)
            try:
                canonical_identity = str(uuid.UUID(str(identity_value)))
            except (AttributeError, TypeError, ValueError) as exc:
                raise FinalizerStepError(
                    "eod_shadow_load",
                    f"shadow child did not return a valid {identity_field}",
                ) from exc
            if identity_value != canonical_identity:
                raise FinalizerStepError(
                    "eod_shadow_load",
                    f"shadow child did not return a canonical {identity_field}",
                )
        for digest_field, digest_label in (
            ("database_projection_sha256", "database projection"),
            ("database_readback_sha256", "database read-back"),
        ):
            digest_value = shadow_result.get(digest_field)
            if (
                not isinstance(digest_value, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest_value) is None
            ):
                raise FinalizerStepError(
                    "eod_shadow_load",
                    f"shadow child did not return a valid {digest_label} digest",
                )
        payload["postgres_shadow"] = {
            "result": _safe_value(shadow_result),
            "status": "COMPLETED",
        }
    except Exception as exc:
        payload["postgres_shadow"] = {
            "error": _error_payload(exc),
            "status": "FAILED",
        }
        return _finish(
            status_path,
            payload,
            status="SHADOW_LOAD_FAILED",
            exit_code=EXIT_SHADOW_LOAD_FAILED,
            status_writer=status_writer,
        )

    return _finish(
        status_path,
        payload,
        status="COMPLETED",
        exit_code=EXIT_SUCCESS,
        status_writer=status_writer,
    )


def _finalize_eod_postgres_locked(
    request: EodPostgresFinalizerRequest,
    *,
    runner: CommandRunner = subprocess.run,
    status_writer: StatusWriter = write_status_atomic,
    database_lock_factory: DatabaseLockFactory | None = None,
) -> EodPostgresFinalizerResult:
    """Finalize one exact published EOD run without changing its file result."""

    raw_status_path = request.run_dir.expanduser().resolve() / STATUS_FILE_NAME
    terminal_payload: dict[str, Any] | None = None
    try:
        request = _validate_request_identity(request)
    except Exception as exc:
        return _in_memory_preflight_failure(
            request,
            exc,
            status_path=(
                raw_status_path
                if request.run_dir.expanduser().resolve().is_dir()
                else None
            ),
        )
    existing_result, terminal_payload = _existing_attempt_result(
        request,
        raw_status_path,
    )
    if existing_result is not None:
        return existing_result

    status_path = request.run_dir / (
        ATTEMPT_FILE_NAME if terminal_payload is not None else STATUS_FILE_NAME
    )
    try:
        request = _validate_checkout_identity(request)
    except Exception as exc:
        if terminal_payload is not None:
            payload = _base_payload(request)
            payload["preserved_terminal_status"] = str(raw_status_path)
            payload["preflight"] = {
                "error": _error_payload(exc),
                "status": "FAILED",
            }
            return _finish(
                status_path,
                payload,
                status="PREFLIGHT_FAILED",
                exit_code=EXIT_PREFLIGHT_FAILED,
                status_writer=status_writer,
            )
        return _in_memory_preflight_failure(
            request,
            exc,
            status_path=(raw_status_path if raw_status_path.exists() else None),
        )
    try:
        published = _validate_published_run(request)
        assert_no_unresolved_eod_finalizations(
            request.run_dir.parent,
            before_timestamp=str(published["run_timestamp"]),
        )
    except Exception as exc:
        if terminal_payload is not None:
            payload = _base_payload(request)
            payload["preserved_terminal_status"] = str(raw_status_path)
            payload["preflight"] = {
                "error": _error_payload(exc),
                "status": "FAILED",
            }
            return _finish(
                status_path,
                payload,
                status="PREFLIGHT_FAILED",
                exit_code=EXIT_PREFLIGHT_FAILED,
                status_writer=status_writer,
            )
        return _in_memory_preflight_failure(
            request,
            exc,
            status_path=(status_path if status_path.exists() else None),
        )

    payload = _base_payload(request)
    if terminal_payload is not None:
        payload["preserved_terminal_status"] = str(raw_status_path)
    write_failure = _write_checkpoint(status_path, payload, status_writer=status_writer)
    if write_failure is not None:
        return write_failure

    try:
        validation = _run_json_command(
            "preflight_validation",
            build_validation_command(request),
            environment=_validation_environment(),
            runner=runner,
        )
        if not isinstance(validation, dict) or validation.get("status") != "VALID":
            raise FinalizerStepError(
                "preflight_validation",
                "validation child did not return status=VALID",
            )
        snapshot_content_sha256 = validation.get("content_sha256")
        if not isinstance(snapshot_content_sha256, str) or re.fullmatch(
            r"[0-9a-f]{64}", snapshot_content_sha256
        ) is None:
            raise FinalizerStepError(
                "preflight_validation",
                "validation child did not return a valid content_sha256",
            )
        raw_sofr_source = validation.get("sofr_refreshed_snapshot_path")
        sofr_source_sha256 = validation.get("sofr_refreshed_snapshot_sha256")
        if (
            not isinstance(raw_sofr_source, str)
            or not Path(raw_sofr_source).is_absolute()
            or not isinstance(sofr_source_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sofr_source_sha256) is None
        ):
            raise FinalizerStepError(
                "preflight_validation",
                "validation child did not return exact SOFR source evidence",
            )
        sofr_source = Path(raw_sofr_source).resolve(strict=True)
        sofr_manifest_path = Path(str(published["sofr_manifest"]))
        if (
            not sofr_source.is_file()
            or sofr_source.parent != sofr_manifest_path.parent
            or sofr_source.name != "fred_sofr_history_refreshed_snapshot.csv"
        ):
            raise FinalizerStepError(
                "preflight_validation",
                "validated SOFR source is outside its updater audit directory",
            )
        source_bundle_evidence = published["source_bundle"]
        if (
            source_bundle_evidence["artifact_sha256"]["sofr_refreshed_snapshot"]
            != sofr_source_sha256
        ):
            raise FinalizerStepError(
                "preflight_validation",
                "validated SOFR source disagrees with the pinned source bundle",
            )
        payload["preflight"] = {
            "published_run": published,
            "snapshot_validation": _safe_value(validation),
            "status": "PASS",
        }
    except Exception as exc:
        payload["preflight"] = {"error": _error_payload(exc), "status": "FAILED"}
        return _finish(
            status_path,
            payload,
            status="PREFLIGHT_FAILED",
            exit_code=EXIT_PREFLIGHT_FAILED,
            status_writer=status_writer,
        )

    write_failure = _write_checkpoint(status_path, payload, status_writer=status_writer)
    if write_failure is not None:
        return write_failure

    try:
        resolve_clean_code_version(
            source_root=REPOSITORY_ROOT,
            project_root=request.project_root,
            explicit=request.code_version,
        )
    except ValueError as exc:
        payload["preflight"] = {
            "error": _error_payload(
                FinalizerPreflightError(
                    "production code identity changed after secretless validation: "
                    f"{exc}"
                )
            ),
            "status": "FAILED",
        }
        return _finish(
            status_path,
            payload,
            status="PREFLIGHT_FAILED",
            exit_code=EXIT_PREFLIGHT_FAILED,
            status_writer=status_writer,
        )

    try:
        lock_factory = database_lock_factory or exclusive_database_finalization_lock
        with lock_factory(request) as database_lease_token:
            assert_no_unresolved_eod_finalizations(
                request.run_dir.parent,
                before_timestamp=str(published["run_timestamp"]),
            )
            return _run_database_projection(
                request,
                published=published,
                validation=validation,
                sofr_source=sofr_source,
                sofr_source_sha256=sofr_source_sha256,
                snapshot_content_sha256=snapshot_content_sha256,
                database_lease_token=database_lease_token,
                status_path=status_path,
                payload=payload,
                runner=runner,
                status_writer=status_writer,
            )
    except Exception as exc:
        payload["preflight"] = {
            "error": _error_payload(exc),
            "published_run": payload.get("preflight", {}).get("published_run"),
            "snapshot_validation": payload.get("preflight", {}).get(
                "snapshot_validation"
            ),
            "status": "FAILED",
        }
        return _finish(
            status_path,
            payload,
            status="PREFLIGHT_FAILED",
            exit_code=EXIT_PREFLIGHT_FAILED,
            status_writer=status_writer,
        )


def finalize_eod_postgres(
    request: EodPostgresFinalizerRequest,
    *,
    runner: CommandRunner = subprocess.run,
    status_writer: StatusWriter = write_status_atomic,
    writer_lease: EodWriterLease | None = None,
    database_lock_factory: DatabaseLockFactory | None = None,
) -> EodPostgresFinalizerResult:
    """Serialize one project/run/DB namespace and finalize an exact file run."""

    run_dir = request.run_dir.expanduser().resolve()
    delegated = (
        delegated_eod_writer_environment(writer_lease)
        if writer_lease is not None
        else {}
    )
    try:
        with eod_writer_execution_lock(
            request.project_root,
            environment=delegated,
        ):
            with exclusive_eod_canonical_writer_lock(request.project_root):
                if not run_dir.is_dir():
                    return _finalize_eod_postgres_locked(
                        request,
                        runner=runner,
                        status_writer=status_writer,
                        database_lock_factory=database_lock_factory,
                    )
                with exclusive_run_lock(run_dir):
                    return _finalize_eod_postgres_locked(
                        request,
                        runner=runner,
                        status_writer=status_writer,
                        database_lock_factory=database_lock_factory,
                    )
    except (FinalizerAlreadyRunningError, EodWriterAlreadyRunningError) as exc:
        payload = _base_payload(request)
        payload.update(
            {
                "exit_code": EXIT_PREFLIGHT_FAILED,
                "finished_at": _utc_now(),
                "preflight": {
                    "error": _error_payload(exc),
                    "status": "FAILED",
                },
                "status": "ALREADY_RUNNING",
            }
        )
        return EodPostgresFinalizerResult(
            exit_code=EXIT_PREFLIGHT_FAILED,
            status="ALREADY_RUNNING",
            payload=_safe_value(payload),
            status_path=run_dir / STATUS_FILE_NAME,
        )
    except OSError as exc:
        payload = _base_payload(request)
        payload.update(
            {
                "exit_code": EXIT_PREFLIGHT_FAILED,
                "finished_at": _utc_now(),
                "preflight": {
                    "error": _error_payload(exc),
                    "status": "FAILED",
                },
                "status": "LOCK_FAILED",
            }
        )
        return EodPostgresFinalizerResult(
            exit_code=EXIT_PREFLIGHT_FAILED,
            status="LOCK_FAILED",
            payload=_safe_value(payload),
            status_path=None,
        )
