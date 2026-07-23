"""Opt-in PostgreSQL shadow recording after a successful file publication.

The file pipeline remains authoritative. This module never invokes canonical
rollback and never writes to ``vrp.signal_publications``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from vrp.orchestration.eod import EOD_AUDIT_REL

REFERENCE_DATABASE_ENV = "VRP_REFERENCE_DATABASE_URL"
EOD_DATABASE_ENV = "VRP_EOD_DATABASE_URL"
LOADER_DATABASE_ENV = "VRP_DATABASE_URL"
SHADOW_FAILURE_EXIT_CODE = 3
SHADOW_STATUS_NAME = "post_publish_shadow_status.json"
DEFAULT_RUNTIME_CONFIG_REL = Path("config/vrp_hybrid_v2_eod_runtime_config.json")
REFERENCE_LOADER_REL = Path("scripts/load_reference_history.py")
EOD_LOADER_REL = Path("scripts/load_eod_snapshot.py")
DEFAULT_LOADER_TIMEOUT_SECONDS = 300.0
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_LOCK_TIMEOUT_SECONDS = 30
_FULL_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")

_REFERENCE_SELECT_TABLES = frozenset(
    {
        "model_versions",
        "configuration_versions",
        "pipeline_runs",
        "pipeline_run_stages",
        "data_assets",
        "pipeline_run_data_assets",
        "qa_results",
        "reference_data_releases",
        "daily_market_feature_definitions",
        "reference_rate_observations",
        "daily_market_features",
        "current_reference_rate_observations",
        "current_daily_market_features",
    }
)
_REFERENCE_INSERT_TABLES = frozenset(
    {
        "model_versions",
        "configuration_versions",
        "pipeline_runs",
        "pipeline_run_stages",
        "data_assets",
        "pipeline_run_data_assets",
        "qa_results",
        "reference_data_releases",
        "daily_market_feature_definitions",
        "reference_rate_observations",
        "daily_market_features",
    }
)
_REFERENCE_UPDATE_COLUMNS = frozenset(
    {
        ("pipeline_runs", "status"),
        ("pipeline_runs", "qa_status"),
        ("pipeline_runs", "started_at"),
        ("pipeline_runs", "completed_at"),
        ("pipeline_runs", "error_summary"),
        ("pipeline_runs", "updated_at"),
        ("pipeline_run_stages", "status"),
        ("pipeline_run_stages", "attempt_count"),
        ("pipeline_run_stages", "input_fingerprint"),
        ("pipeline_run_stages", "output_fingerprint"),
        ("pipeline_run_stages", "started_at"),
        ("pipeline_run_stages", "finished_at"),
        ("pipeline_run_stages", "last_error"),
        ("pipeline_run_stages", "metrics"),
        ("pipeline_run_stages", "updated_at"),
        ("qa_results", "outcome"),
        ("qa_results", "severity"),
        ("qa_results", "is_hard_gate"),
        ("qa_results", "message"),
        ("qa_results", "observed_value"),
        ("qa_results", "expected_value"),
        ("qa_results", "evidence"),
        ("qa_results", "checked_at"),
    }
)
_REFERENCE_EXECUTE_FUNCTIONS = frozenset(
    {
        "force_current_load_transaction",
        "assert_compatible_reference_data_releases",
        "validate_reference_rate_successor",
        "validate_daily_market_feature_successor",
    }
)

_EOD_SELECT_TABLES = frozenset(
    {
        "model_versions",
        "configuration_versions",
        "pipeline_runs",
        "pipeline_run_stages",
        "data_assets",
        "pipeline_run_data_assets",
        "market_snapshots",
        "implied_variance_term_structure",
        "forecast_variance_term_structure",
        "signal_features",
        "signal_evaluations",
        "selected_signals",
        "qa_results",
        "signal_publications",
        "reference_data_releases",
        "daily_market_feature_definitions",
        "current_reference_rate_observations",
        "current_daily_market_features",
    }
)
_EOD_INSERT_TABLES = frozenset(
    {
        "model_versions",
        "configuration_versions",
        "pipeline_runs",
        "pipeline_run_stages",
        "data_assets",
        "pipeline_run_data_assets",
        "market_snapshots",
        "implied_variance_term_structure",
        "forecast_variance_term_structure",
        "signal_features",
        "signal_evaluations",
        "selected_signals",
        "qa_results",
    }
)
_EOD_UPDATE_COLUMNS = frozenset(
    {
        ("pipeline_runs", "status"),
        ("pipeline_runs", "qa_status"),
        ("pipeline_runs", "completed_at"),
        ("pipeline_runs", "updated_at"),
        ("pipeline_run_stages", "status"),
        ("pipeline_run_stages", "output_fingerprint"),
        ("pipeline_run_stages", "finished_at"),
        ("pipeline_run_stages", "metrics"),
        ("pipeline_run_stages", "updated_at"),
    }
)


@dataclass(frozen=True)
class ShadowIdentity:
    code_version: str
    requested_by: str


@dataclass(frozen=True)
class ShadowDatabaseTargets:
    """Connection strings kept in memory and out of commands/status files."""

    reference_database_url: str
    eod_database_url: str


@dataclass(frozen=True)
class ShadowWriteResult:
    success: bool
    status_path: Path
    error: str | None = None


def _required_text(value: str | None, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} is required")
    return value.strip()


def _validate_code_version(value: str) -> str:
    normalized = value.strip().lower()
    if not _FULL_GIT_OBJECT.fullmatch(normalized):
        raise ValueError("shadow code version must be a full 40- or 64-character Git object ID")
    return normalized


def _run_git(project_root: Path, arguments: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(project_root), *arguments],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"could not resolve shadow Git identity: {exc}") from exc
    return completed.stdout.strip()


def resolve_shadow_identity(
    project_root: Path,
    *,
    explicit_code_version: str | None = None,
    explicit_requested_by: str | None = None,
    environment: Mapping[str, str] | None = None,
) -> ShadowIdentity:
    """Pin one code/user identity before canonical execution starts."""

    env = os.environ if environment is None else environment
    dirty = _run_git(project_root, ["status", "--porcelain", "--untracked-files=all"])
    if dirty:
        raise ValueError("shadow writing requires a clean Git checkout")
    head = _validate_code_version(_run_git(project_root, ["rev-parse", "HEAD"]))
    candidate = explicit_code_version or env.get("VRP_CODE_VERSION") or head
    code_version = _validate_code_version(candidate)
    _run_git(project_root, ["cat-file", "-e", f"{code_version}^{{commit}}"])
    if code_version != head:
        raise ValueError("shadow code version must equal the clean checkout HEAD")

    requested_by = _required_text(
        explicit_requested_by or env.get("USERNAME") or env.get("USER"),
        "shadow requested-by identity",
    )
    return ShadowIdentity(code_version=code_version, requested_by=requested_by)


def validate_shadow_checkout(
    project_root: Path,
    *,
    identity: ShadowIdentity,
    runtime_config: Path,
) -> None:
    """Re-prove committed source identity immediately before credential use."""

    root = project_root.resolve()
    observed = resolve_shadow_identity(
        root,
        explicit_code_version=identity.code_version,
        explicit_requested_by=identity.requested_by,
        environment={},
    )
    if observed != identity:
        raise ValueError("shadow source identity changed after preflight")
    required = (
        runtime_config.resolve(),
        (root / REFERENCE_LOADER_REL).resolve(),
        (root / EOD_LOADER_REL).resolve(),
    )
    for path in required:
        try:
            relative = path.relative_to(root)
        except ValueError as exc:
            raise ValueError("shadow runtime files must remain inside the checkout") from exc
        _run_git(
            root,
            ["ls-files", "--error-unmatch", "--", relative.as_posix()],
        )


def resolve_shadow_database_targets(
    environment: Mapping[str, str] | None = None,
) -> ShadowDatabaseTargets:
    """Require separate least-privilege database identities."""

    env = os.environ if environment is None else environment
    return ShadowDatabaseTargets(
        reference_database_url=_required_text(
            env.get(REFERENCE_DATABASE_ENV), REFERENCE_DATABASE_ENV
        ),
        eod_database_url=_required_text(
            env.get(EOD_DATABASE_ENV), EOD_DATABASE_ENV
        ),
    )


def _inspect_database_role(
    database_url: str,
    *,
    expected_role: str,
    forbidden_role: str,
) -> dict[str, Any]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - declared production dependency
        raise ValueError("Psycopg is required for shadow database preflight") from exc
    try:
        with psycopg.connect(
            database_url,
            connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        current_user,
                        role.rolsuper,
                        role.rolcreatedb,
                        role.rolcreaterole,
                        role.rolreplication,
                        role.rolbypassrls,
                        pg_has_role(current_user, %s, 'MEMBER'),
                        pg_has_role(current_user, %s, 'MEMBER'),
                        has_schema_privilege(current_user, 'vrp', 'CREATE'),
                        has_schema_privilege(current_user, 'vrp', 'USAGE'),
                        has_database_privilege(
                            current_user, current_database(), 'CREATE'
                        ),
                        has_table_privilege(
                            current_user, 'vrp.signal_publications', 'INSERT'
                        ),
                        has_table_privilege(
                            current_user, 'vrp.reference_rate_observations', 'INSERT'
                        ),
                        has_table_privilege(
                            current_user, 'vrp.daily_market_features', 'INSERT'
                        ),
                        has_table_privilege(
                            current_user, 'vrp.market_snapshots', 'INSERT'
                        ),
                        has_table_privilege(
                            current_user, 'vrp.selected_signals', 'INSERT'
                        )
                    FROM pg_roles AS role
                    WHERE role.rolname = current_user
                    """,
                    (expected_role, forbidden_role),
                )
                row = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT candidate.rolname
                    FROM pg_roles AS candidate
                    WHERE candidate.rolname <> current_user
                      AND pg_has_role(current_user, candidate.oid, 'MEMBER')
                    ORDER BY candidate.rolname
                    """
                )
                memberships = tuple(item[0] for item in cursor.fetchall())
                cursor.execute(
                    """
                    SELECT
                        EXISTS (
                            SELECT 1
                            FROM pg_database
                            WHERE datname = current_database()
                              AND datdba = (
                                  SELECT oid FROM pg_roles
                                  WHERE rolname = current_user
                              )
                        ),
                        EXISTS (
                            SELECT 1
                            FROM pg_namespace
                            WHERE nspname = 'vrp'
                              AND nspowner = (
                                  SELECT oid FROM pg_roles
                                  WHERE rolname = current_user
                              )
                        ),
                        EXISTS (
                            SELECT 1
                            FROM pg_class AS relation
                            JOIN pg_namespace AS namespace
                              ON namespace.oid = relation.relnamespace
                            WHERE namespace.nspname = 'vrp'
                              AND relation.relowner = (
                                  SELECT oid FROM pg_roles
                                  WHERE rolname = current_user
                              )
                        ),
                        EXISTS (
                            SELECT 1
                            FROM pg_proc AS routine
                            JOIN pg_namespace AS namespace
                              ON namespace.oid = routine.pronamespace
                            WHERE namespace.nspname = 'vrp'
                              AND routine.proowner = (
                                  SELECT oid FROM pg_roles
                                  WHERE rolname = current_user
                              )
                        )
                    """
                )
                ownership = cursor.fetchone()
                cursor.execute(
                    """
                    WITH privileges(privilege) AS (
                        VALUES
                            ('SELECT'),
                            ('INSERT'),
                            ('UPDATE'),
                            ('DELETE'),
                            ('TRUNCATE'),
                            ('REFERENCES'),
                            ('TRIGGER'),
                            ('MAINTAIN')
                    )
                    SELECT relation.relname, privileges.privilege
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    CROSS JOIN privileges
                    WHERE namespace.nspname = 'vrp'
                      AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                      AND has_table_privilege(
                          current_user,
                          relation.oid,
                          privileges.privilege
                      )
                    ORDER BY relation.relname, privileges.privilege
                    """
                )
                table_privileges = tuple(
                    (item[0], item[1]) for item in cursor.fetchall()
                )
                cursor.execute(
                    """
                    SELECT relation.relname, attribute.attname
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    JOIN pg_attribute AS attribute
                      ON attribute.attrelid = relation.oid
                    WHERE namespace.nspname = 'vrp'
                      AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                      AND attribute.attnum > 0
                      AND NOT attribute.attisdropped
                      AND has_column_privilege(
                          current_user,
                          relation.oid,
                          attribute.attnum,
                          'UPDATE'
                      )
                    ORDER BY relation.relname, attribute.attname
                    """
                )
                update_columns = tuple(
                    (item[0], item[1]) for item in cursor.fetchall()
                )
                cursor.execute(
                    """
                    WITH privileges(privilege) AS (
                        VALUES ('SELECT'), ('USAGE'), ('UPDATE')
                    )
                    SELECT relation.relname, privileges.privilege
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    CROSS JOIN privileges
                    WHERE namespace.nspname = 'vrp'
                      AND relation.relkind = 'S'
                      AND has_sequence_privilege(
                          current_user,
                          relation.oid,
                          privileges.privilege
                      )
                    ORDER BY relation.relname, privileges.privilege
                    """
                )
                sequence_privileges = tuple(
                    (item[0], item[1]) for item in cursor.fetchall()
                )
                cursor.execute(
                    """
                    SELECT routine.proname
                    FROM pg_proc AS routine
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = routine.pronamespace
                    WHERE namespace.nspname = 'vrp'
                      AND has_function_privilege(
                          current_user,
                          routine.oid,
                          'EXECUTE'
                      )
                    ORDER BY routine.proname
                    """
                )
                executable_functions = tuple(item[0] for item in cursor.fetchall())
    except Exception as exc:
        raise ValueError(
            f"could not validate the {expected_role} database identity: "
            f"{type(exc).__name__}"
        ) from exc
    if row is None:
        raise ValueError(f"database current_user is missing for {expected_role}")
    return {
        "current_user": row[0],
        "is_superuser": bool(row[1]),
        "can_create_database": bool(row[2]),
        "can_create_role": bool(row[3]),
        "can_replicate": bool(row[4]),
        "can_bypass_rls": bool(row[5]),
        "has_expected_role": bool(row[6]),
        "has_forbidden_role": bool(row[7]),
        "can_create_in_schema": bool(row[8]),
        "can_use_schema": bool(row[9]),
        "can_create_in_database": bool(row[10]),
        "can_publish": bool(row[11]),
        "can_insert_reference_rates": bool(row[12]),
        "can_insert_daily_features": bool(row[13]),
        "can_insert_market_snapshots": bool(row[14]),
        "can_insert_selected_signals": bool(row[15]),
        "memberships": memberships,
        "owns_database": bool(ownership[0]),
        "owns_schema": bool(ownership[1]),
        "owns_relation": bool(ownership[2]),
        "owns_function": bool(ownership[3]),
        "table_privileges": table_privileges,
        "update_columns": update_columns,
        "sequence_privileges": sequence_privileges,
        "executable_functions": executable_functions,
    }


def validate_shadow_database_roles(
    databases: ShadowDatabaseTargets,
) -> tuple[str, str]:
    """Prove that both DSNs use distinct exact-privilege login roles."""

    reference = _inspect_database_role(
        databases.reference_database_url,
        expected_role="vrp_reference_loader",
        forbidden_role="vrp_eod_shadow_writer",
    )
    eod = _inspect_database_role(
        databases.eod_database_url,
        expected_role="vrp_eod_shadow_writer",
        forbidden_role="vrp_reference_loader",
    )
    role_contracts = (
        (
            "reference",
            reference,
            "vrp_reference_loader",
            _REFERENCE_SELECT_TABLES,
            _REFERENCE_INSERT_TABLES,
            _REFERENCE_UPDATE_COLUMNS,
            _REFERENCE_EXECUTE_FUNCTIONS,
        ),
        (
            "EOD",
            eod,
            "vrp_eod_shadow_writer",
            _EOD_SELECT_TABLES,
            _EOD_INSERT_TABLES,
            _EOD_UPDATE_COLUMNS,
            frozenset(),
        ),
    )
    for (
        label,
        observed,
        expected_role,
        expected_select,
        expected_insert,
        expected_updates,
        expected_functions,
    ) in role_contracts:
        if any(
            observed[key]
            for key in (
                "is_superuser",
                "can_create_database",
                "can_create_role",
                "can_replicate",
                "can_bypass_rls",
                "has_forbidden_role",
                "can_create_in_schema",
                "can_create_in_database",
                "can_publish",
                "owns_database",
                "owns_schema",
                "owns_relation",
                "owns_function",
            )
        ):
            raise ValueError(f"{label} shadow database identity is over-privileged")
        if not observed["has_expected_role"]:
            raise ValueError(f"{label} shadow database identity lacks its capability role")
        if not observed["can_use_schema"]:
            raise ValueError(f"{label} shadow database identity lacks schema usage")
        if set(observed["memberships"]) != {expected_role}:
            raise ValueError(
                f"{label} shadow database identity has unexpected role memberships"
            )
        observed_table_privileges: dict[str, set[str]] = {
            privilege: set()
            for privilege in (
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
                "MAINTAIN",
            )
        }
        for table_name, privilege in observed["table_privileges"]:
            observed_table_privileges.setdefault(privilege, set()).add(table_name)
        expected_table_privileges = {
            "SELECT": set(expected_select),
            "INSERT": set(expected_insert),
            "UPDATE": set(),
            "DELETE": set(),
            "TRUNCATE": set(),
            "REFERENCES": set(),
            "TRIGGER": set(),
            "MAINTAIN": set(),
        }
        if observed_table_privileges != expected_table_privileges:
            raise ValueError(
                f"{label} shadow database identity has unexpected table privileges"
            )
        if set(observed["update_columns"]) != set(expected_updates):
            raise ValueError(
                f"{label} shadow database identity has unexpected update privileges"
            )
        if observed["sequence_privileges"]:
            raise ValueError(
                f"{label} shadow database identity has unexpected sequence privileges"
            )
        if set(observed["executable_functions"]) != set(expected_functions):
            raise ValueError(
                f"{label} shadow database identity has unexpected function privileges"
            )
    if not reference["can_insert_reference_rates"] or not reference[
        "can_insert_daily_features"
    ]:
        raise ValueError("reference shadow database identity lacks append privileges")
    if reference["can_insert_market_snapshots"] or reference[
        "can_insert_selected_signals"
    ]:
        raise ValueError("reference shadow database identity can write EOD outputs")
    if eod["can_insert_reference_rates"] or eod["can_insert_daily_features"]:
        raise ValueError("EOD shadow database identity can mutate reference history")
    if not eod["can_insert_market_snapshots"] or not eod[
        "can_insert_selected_signals"
    ]:
        raise ValueError("EOD shadow database identity lacks snapshot privileges")
    if reference["current_user"] == eod["current_user"]:
        raise ValueError("reference and EOD shadow database identities must be distinct")
    return str(reference["current_user"]), str(eod["current_user"])


def resolve_shadow_runtime_config(
    project_root: Path,
    runtime_config: Path | None,
) -> Path:
    """Resolve every file needed after publication before EOD starts."""

    root = project_root.resolve()
    candidate = runtime_config or (root / DEFAULT_RUNTIME_CONFIG_REL)
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("shadow runtime config must be inside the project checkout") from exc
    required = (candidate, root / REFERENCE_LOADER_REL, root / EOD_LOADER_REL)
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise ValueError(f"shadow runtime files are missing: {missing}")
    return candidate


def validate_completed_eod_manifest(
    manifest_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    """Validate that the exact emitted manifest represents a publication."""

    expected_audit_root = (project_root.resolve() / EOD_AUDIT_REL).resolve()
    try:
        manifest_path.resolve().relative_to(expected_audit_root)
    except ValueError as exc:
        raise ValueError("completed EOD manifest is outside the EOD audit root") from exc
    if not manifest_path.is_file():
        raise ValueError(f"completed EOD manifest does not exist: {manifest_path}")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"completed EOD manifest is unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("completed EOD manifest must contain a JSON object")
    if payload.get("status") != "PASS":
        raise ValueError("completed EOD manifest status is not PASS")
    if payload.get("final_health") != "PASS":
        raise ValueError("completed EOD manifest final_health is not PASS")
    if payload.get("publish_requested") is not True:
        raise ValueError("completed EOD manifest does not confirm publication")
    manifest_root = Path(str(payload.get("project_root", ""))).resolve()
    if manifest_root != project_root.resolve():
        raise ValueError("completed EOD manifest project_root does not match the request")
    if not isinstance(payload.get("published_outputs"), dict) or not payload[
        "published_outputs"
    ]:
        raise ValueError("completed EOD manifest has no published output set")
    return payload


def build_reference_loader_command(
    *,
    project_root: Path,
    runtime_config: Path,
    environment: str,
    identity: ShadowIdentity,
    python_executable: str | Path | None = None,
) -> list[str]:
    return [
        str(python_executable or sys.executable),
        "-u",
        str(project_root / REFERENCE_LOADER_REL),
        "all",
        "--project-root",
        str(project_root),
        "--runtime-config",
        str(runtime_config),
        "--environment",
        environment,
        "--code-version",
        identity.code_version,
        "--requested-by",
        identity.requested_by,
    ]


def build_eod_loader_command(
    *,
    project_root: Path,
    run_dir: Path,
    environment: str,
    identity: ShadowIdentity,
    python_executable: str | Path | None = None,
) -> list[str]:
    return [
        str(python_executable or sys.executable),
        "-u",
        str(project_root / EOD_LOADER_REL),
        "--project-root",
        str(project_root),
        "--run-dir",
        str(run_dir),
        "--environment",
        environment,
        "--code-version",
        identity.code_version,
        "--requested-by",
        identity.requested_by,
    ]


def _loader_environment(
    database_url: str,
    base_environment: Mapping[str, str] | None = None,
    *,
    statement_timeout_seconds: float,
) -> dict[str, str]:
    child = dict(os.environ if base_environment is None else base_environment)
    child.pop(REFERENCE_DATABASE_ENV, None)
    child.pop(EOD_DATABASE_ENV, None)
    child[LOADER_DATABASE_ENV] = database_url
    child.setdefault("PGCONNECT_TIMEOUT", str(DEFAULT_CONNECT_TIMEOUT_SECONDS))
    statement_ms = max(1, int(statement_timeout_seconds * 1000))
    lock_ms = min(statement_ms, DEFAULT_LOCK_TIMEOUT_SECONDS * 1000)
    timeout_options = (
        f"-c statement_timeout={statement_ms} -c lock_timeout={lock_ms}"
    )
    existing_options = child.get("PGOPTIONS", "").strip()
    child["PGOPTIONS"] = " ".join(
        item for item in (existing_options, timeout_options) if item
    )
    return child


def _run_loader(
    command: Sequence[str],
    *,
    project_root: Path,
    database_url: str,
    base_environment: Mapping[str, str] | None,
    timeout_seconds: float,
) -> tuple[int, Any | None]:
    completed = subprocess.run(
        list(command),
        cwd=project_root,
        env=_loader_environment(
            database_url,
            base_environment,
            statement_timeout_seconds=timeout_seconds,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n")
    if completed.stderr:
        print(
            completed.stderr,
            end="" if completed.stderr.endswith("\n") else "\n",
            file=sys.stderr,
        )
    parsed: Any | None = None
    if completed.returncode == 0 and completed.stdout.strip():
        try:
            parsed = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"shadow loader returned invalid JSON: {exc}") from exc
    return int(completed.returncode), parsed


def _write_status(path: Path, payload: Mapping[str, Any]) -> None:
    temp_path = path.with_name(f".{path.name}.tmp")
    rendered = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temp_path.write_text(rendered, encoding="utf-8")
    temp_path.replace(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def execute_post_publish_shadow(
    *,
    project_root: Path,
    manifest_path: Path,
    runtime_config: Path | None,
    environment: str,
    identity: ShadowIdentity,
    databases: ShadowDatabaseTargets,
    python_executable: str | Path | None = None,
    base_environment: Mapping[str, str] | None = None,
    loader_timeout_seconds: float = DEFAULT_LOADER_TIMEOUT_SECONDS,
    validated_database_users: tuple[str, str] | None = None,
) -> ShadowWriteResult:
    """Load references, then record the exact successful EOD snapshot."""

    root = project_root.resolve()
    if not isinstance(loader_timeout_seconds, (int, float)) or not (
        0 < float(loader_timeout_seconds) <= 3600
    ):
        raise ValueError("shadow loader timeout must be between 0 and 3600 seconds")
    manifest = validate_completed_eod_manifest(manifest_path, root)
    run_dir = manifest_path.parent.resolve()
    runtime_path = resolve_shadow_runtime_config(root, runtime_config)

    status_path = run_dir / SHADOW_STATUS_NAME
    status: dict[str, Any] = {
        "code_version": identity.code_version,
        "environment": environment,
        "eod_manifest": str(manifest_path),
        "file_pipeline_status": manifest["status"],
        "file_publication_retained": True,
        "requested_by": identity.requested_by,
        "shadow_authoritative": False,
        "shadow_status": "RUNNING",
        "started_at": _utc_now(),
    }
    if validated_database_users is not None:
        status["reference_database_user"] = validated_database_users[0]
        status["eod_database_user"] = validated_database_users[1]
    _write_status(status_path, status)
    try:
        validate_shadow_checkout(
            root,
            identity=identity,
            runtime_config=runtime_path,
        )
    except Exception as exc:
        error = (
            "shadow source checkout changed after canonical publication; "
            "database loaders were skipped"
        )
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "source_validation_error": type(exc).__name__,
                "shadow_status": "FAILED",
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)
    print("VRP_SHADOW|STARTING|Loading reference history before the EOD snapshot.")

    reference_command = build_reference_loader_command(
        project_root=root,
        runtime_config=runtime_path,
        environment=environment,
        identity=identity,
        python_executable=python_executable,
    )
    try:
        reference_code, reference_output = _run_loader(
            reference_command,
            project_root=root,
            database_url=databases.reference_database_url,
            base_environment=base_environment,
            timeout_seconds=float(loader_timeout_seconds),
        )
    except subprocess.TimeoutExpired:
        error = "reference-history shadow load timed out; EOD snapshot load was skipped"
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "reference_loader_return_code": None,
                "shadow_status": "FAILED",
                "snapshot_loader_return_code": None,
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)
    except Exception as exc:
        error = (
            "reference-history shadow load raised "
            f"{type(exc).__name__}; EOD snapshot load was skipped"
        )
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "reference_loader_return_code": None,
                "shadow_status": "FAILED",
                "snapshot_loader_return_code": None,
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)
    status["reference_loader_return_code"] = reference_code
    if reference_code != 0:
        error = "reference-history shadow load failed; EOD snapshot load was skipped"
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "shadow_status": "FAILED",
                "snapshot_loader_return_code": None,
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)

    status["reference_result"] = reference_output
    print("VRP_SHADOW|REFERENCE_PASS|Reference history is current.")
    print("VRP_SHADOW|SNAPSHOT_STARTING|Recording the exact EOD snapshot.")
    try:
        validate_shadow_checkout(
            root,
            identity=identity,
            runtime_config=runtime_path,
        )
    except Exception as exc:
        error = (
            "shadow source checkout changed after the reference load; "
            "EOD snapshot load was skipped"
        )
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "source_validation_error": type(exc).__name__,
                "shadow_status": "FAILED",
                "snapshot_loader_return_code": None,
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)
    snapshot_command = build_eod_loader_command(
        project_root=root,
        run_dir=run_dir,
        environment=environment,
        identity=identity,
        python_executable=python_executable,
    )
    try:
        snapshot_code, snapshot_output = _run_loader(
            snapshot_command,
            project_root=root,
            database_url=databases.eod_database_url,
            base_environment=base_environment,
            timeout_seconds=float(loader_timeout_seconds),
        )
    except subprocess.TimeoutExpired:
        error = "EOD PostgreSQL shadow snapshot load timed out"
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "shadow_status": "FAILED",
                "snapshot_loader_return_code": None,
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)
    except Exception as exc:
        error = f"EOD PostgreSQL shadow snapshot load raised {type(exc).__name__}"
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "shadow_status": "FAILED",
                "snapshot_loader_return_code": None,
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)
    status["snapshot_loader_return_code"] = snapshot_code
    if snapshot_code != 0:
        error = "EOD PostgreSQL shadow snapshot load failed"
        status.update(
            {
                "error": error,
                "finished_at": _utc_now(),
                "shadow_status": "FAILED",
            }
        )
        _write_status(status_path, status)
        return ShadowWriteResult(False, status_path, error)

    status.update(
        {
            "finished_at": _utc_now(),
            "shadow_status": "PASS",
            "snapshot_result": snapshot_output,
        }
    )
    _write_status(status_path, status)
    print("VRP_SHADOW|PASS|PostgreSQL shadow recording completed.")
    return ShadowWriteResult(True, status_path)
