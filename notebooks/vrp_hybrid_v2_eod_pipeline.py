from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

import pandas as pd

from vrp.orchestration.eod import resolve_clean_code_version
from vrp.orchestration.eod_bundle import load_eod_source_bundle
from vrp.orchestration.eod_finalization_gate import (
    assert_no_unresolved_eod_finalizations,
    require_canonical_eod_runtime_config,
    resolve_eod_audit_root,
)
from vrp.orchestration.eod_lock import (
    eod_writer_execution_lock,
    exclusive_eod_canonical_writer_lock,
)

from vrp_hybrid_v2_common import (
    DEFAULT_PROJECT_ROOT,
    EXPECTED_TENORS,
    LOCK_ID,
    atomic_copy,
    backup_files,
    first_existing_column,
    glob_all,
    glob_latest,
    latest_completed_xnys_session,
    load_json,
    load_runtime_config,
    normalize_dates,
    now_stamp,
    probe_tcp,
    progress,
    read_table,
    resolve_path,
    restore_files,
    sha256_file,
    utc_now_iso,
    write_csv_atomic,
    write_json,
    write_parquet_atomic,
)
from vrp_hybrid_v2_health_check import HealthConfig, run_health_check


RESULT_HANDOFF_CONTRACT = "vrp.hybrid_v2.eod_result"
RESULT_HANDOFF_SCHEMA_VERSION = 1
RESULT_HANDOFF_WRITE_FAILED_EXIT_CODE = 3


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    runtime_config_path: Path
    target_date: pd.Timestamp
    approved_nav: float
    skip_upstream: bool
    force_recalculate: bool
    recalc_start_override: pd.Timestamp | None
    publish: bool
    code_version: str | None
    postgres_postpass_required: bool
    postgres_environment: str | None
    postgres_postpass_bypass_reason: str | None
    run_timestamp: str
    run_dir: Path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Hybrid v2 gap-aware EOD production pipeline.")
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--runtime-config", type=Path, default=None)
    parser.add_argument("--target-date", default=None, help="YYYY-MM-DD or YYYYMMDD. Default: latest completed XNYS session.")
    parser.add_argument("--approved-nav", type=float, default=1_000_000.0)
    parser.add_argument("--skip-upstream", action="store_true", help="Skip external data builders and publish from existing inputs.")
    parser.add_argument("--force-recalculate", action="store_true", help="Force full upstream refresh behavior from the earliest detected gap.")
    parser.add_argument(
        "--recalc-start",
        default=None,
        help=(
            "Optional YYYY-MM-DD or YYYYMMDD override for the earliest downstream "
            "recalculation date. This forces the implied-variance window without "
            "forcing full market-data/Corsi rebuilds."
        ),
    )
    parser.add_argument("--no-publish", action="store_true")
    parser.add_argument(
        "--code-version",
        default=None,
        help=(
            "Optional full Git SHA assertion. Published runs always resolve and "
            "record the exact clean producing commit."
        ),
    )
    parser.add_argument(
        "--postgres-postpass-required",
        action="store_true",
        help="Record that this published file run requires the automatic DB post-pass.",
    )
    parser.add_argument("--postgres-environment", default=None)
    parser.add_argument(
        "--postgres-postpass-bypass-reason",
        choices=("explicit-no-postgres-shadow",),
        default=None,
        help="Audit an explicit wrapper-approved file-only publication.",
    )
    parser.add_argument(
        "--result-handoff",
        type=Path,
        default=None,
        help=(
            "Optional caller-owned JSON path that is atomically created only "
            "after the completed EOD run has passed."
        ),
    )
    return parser.parse_args(argv)


def build_result_handoff(
    config: PipelineConfig,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Build the exact completed-run contract consumed by the outer launcher."""

    run_dir = config.run_dir.resolve(strict=True)
    manifest_path = (run_dir / "run_manifest.json").resolve(strict=True)
    disk_manifest = load_json(manifest_path)

    if manifest.get("status") != "PASS" or disk_manifest.get("status") != "PASS":
        raise RuntimeError("result handoff requires an in-memory and on-disk PASS manifest")
    if manifest != disk_manifest:
        raise RuntimeError("in-memory and on-disk completed-run manifests disagree")

    target_date = str(config.target_date.date())
    if disk_manifest.get("target_date") != target_date:
        raise RuntimeError("completed-run manifest target date disagrees with the run configuration")
    if disk_manifest.get("final_health") != "PASS":
        raise RuntimeError("result handoff requires final_health=PASS")

    published_outputs = disk_manifest.get("published_outputs")
    if not isinstance(published_outputs, dict):
        raise RuntimeError("completed-run manifest published_outputs must be an object")
    actually_published = bool(published_outputs)
    if actually_published != bool(config.publish):
        raise RuntimeError("completed-run publication evidence disagrees with the run configuration")

    source_bundle = load_eod_source_bundle(run_dir)
    return {
        "contract": RESULT_HANDOFF_CONTRACT,
        "schema_version": RESULT_HANDOFF_SCHEMA_VERSION,
        "status": "PASS",
        "target_date": target_date,
        "code_version": disk_manifest.get("code_version"),
        "postgres_postpass_required": disk_manifest.get(
            "postgres_postpass_required"
        ),
        "postgres_environment": disk_manifest.get("postgres_environment"),
        "run_dir": str(run_dir),
        "run_manifest": {
            "path": str(manifest_path),
            "sha256": sha256_file(manifest_path),
        },
        "source_bundle": source_bundle.to_json_dict(),
        "published": actually_published,
    }


def write_result_handoff(
    destination: Path,
    config: PipelineConfig,
    manifest: dict[str, Any],
) -> Path:
    """Atomically publish one completed-run handoff without changing run evidence."""

    destination = destination.expanduser()
    if not destination.is_absolute():
        destination = Path.cwd() / destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.parent.resolve() / destination.name
    if os.path.lexists(destination):
        raise FileExistsError(
            f"result handoff destination already exists; refusing to replace it: {destination}"
        )
    payload = build_result_handoff(config, manifest)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = -1
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary_path, destination)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)
    return destination


def claim_run_directory(run_dir: Path) -> Path:
    """Atomically reserve one audit directory; concurrent runs may not share it."""

    run_dir = run_dir.resolve()
    try:
        run_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise RuntimeError(
            f"EOD audit directory already exists; refusing to reuse it: {run_dir}"
        ) from exc
    return run_dir


def parse_target(value: str | None, close_buffer: int) -> pd.Timestamp:
    if value:
        return pd.Timestamp(pd.to_datetime(value, errors="raise")).normalize()
    return latest_completed_xnys_session(close_buffer_minutes=close_buffer)


def resolve_script(project_root: Path, candidates: Sequence[str]) -> Path:
    for candidate in candidates:
        path = resolve_path(project_root, candidate)
        if path and path.exists():
            return path
    raise FileNotFoundError(f"None of the configured scripts exists: {list(candidates)}")


def run_command(
    *,
    label: str,
    command: list[str],
    log_dir: Path,
    progress_pct: int,
) -> dict[str, Any]:
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{label}_stdout.txt"
    stderr_path = log_dir / f"{label}_stderr.txt"
    progress(label, progress_pct, "Starting")
    print("RUN:", subprocess.list2cmdline(command), flush=True)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )
    stdout_lines: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        stdout_lines.append(line)
        print(line, end="", flush=True)
    return_code = process.wait()
    stdout_text = "".join(stdout_lines)
    stdout_path.write_text(stdout_text, encoding="utf-8")
    stderr_path.write_text(
        "stderr was merged into stdout to prevent subprocess pipe deadlocks.\n",
        encoding="utf-8",
    )
    if return_code != 0:
        raise RuntimeError(f"{label} failed with return code {return_code}. Logs: {stdout_path}, {stderr_path}")
    progress(label, progress_pct, "Completed")
    return {
        "step": label,
        "status": "PASS",
        "command": subprocess.list2cmdline(command),
        "return_code": return_code,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
    }


def parse_sofr_manifest_from_stdout(stdout_path: Path) -> tuple[Path, dict[str, Any]]:
    """Read the SOFR updater's machine-readable manifest marker."""
    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    marker = "SOFR_MANIFEST_PATH="
    matches = [
        line.split(marker, 1)[1].strip()
        for line in text.splitlines()
        if marker in line
    ]
    if not matches:
        raise RuntimeError(
            f"SOFR updater completed but did not emit {marker}. See {stdout_path}"
        )
    manifest_path = Path(matches[-1])
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"SOFR updater manifest marker points to a missing file: {manifest_path}"
        )
    manifest = load_json(manifest_path)
    status = str(manifest.get("status", ""))
    if status not in {"PUBLISHED", "NO_CHANGE", "CHECK_ONLY"}:
        raise RuntimeError(
            f"SOFR updater returned unacceptable status={status!r}. "
            f"Manifest: {manifest_path}"
        )
    if not bool(manifest.get("hard_checks_passed", False)):
        raise RuntimeError(
            f"SOFR updater hard checks did not pass. Manifest: {manifest_path}"
        )
    return manifest_path, manifest


def earliest_gap_from_health(frame: pd.DataFrame, target_date: pd.Timestamp) -> pd.Timestamp:
    dates: list[pd.Timestamp] = []
    for value in frame.get("missing_dates", pd.Series(dtype=object)).dropna():
        try:
            items = json.loads(value) if isinstance(value, str) else value
        except json.JSONDecodeError:
            items = []
        for item in items or []:
            try:
                dates.append(pd.Timestamp(item).normalize())
            except Exception:
                pass
    return min(dates) if dates else target_date


def file_max_date(path: Path) -> pd.Timestamp | None:
    try:
        frame = read_table(path)
        date_col = first_existing_column(frame, ("date", "trade_date"), required=False)
        if date_col is None:
            return None
        dates = normalize_dates(frame[date_col]).dropna()
        return dates.max() if len(dates) else None
    except Exception:
        return None


def inspect_implied_variance_surface(path: Path, target_date: pd.Timestamp) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "target_date": str(target_date.date()),
        "valid": False,
        "row_count": 0,
        "target_row_count": 0,
        "target_tenors": [],
        "duplicate_target_keys": 0,
        "invalid_target_values": 0,
        "max_date": None,
        "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
        "error": None,
    }
    if not path.exists():
        return record
    try:
        frame = read_table(path)
        record["row_count"] = int(len(frame))
        date_col = first_existing_column(frame, ("date", "trade_date"), label="implied-variance date")
        tenor_col = first_existing_column(
            frame, ("tenor", "target_dte", "dte", "target_days"), label="implied-variance tenor"
        )
        work = frame.copy()
        work["__date"] = normalize_dates(work[date_col])
        work["__tenor"] = pd.to_numeric(work[tenor_col], errors="coerce").round().astype("Int64")
        valid_dates = work["__date"].dropna()
        record["max_date"] = str(valid_dates.max().date()) if len(valid_dates) else None
        target = work.loc[work["__date"].eq(target_date) & work["__tenor"].isin(EXPECTED_TENORS)].copy()
        record["target_row_count"] = int(len(target))
        record["target_tenors"] = sorted(target["__tenor"].dropna().astype(int).unique().tolist())
        record["duplicate_target_keys"] = int(target.duplicated(["__date", "__tenor"], keep=False).sum())

        value_col = first_existing_column(
            target,
            (
                "implied_variance", "implied_variance_final", "vix_style_variance",
                "interpolated_variance", "target_variance", "annualized_variance",
                "implied_vol_pct", "vix_style_vol", "vix_style_volatility",
                "target_vol", "volatility",
            ),
            required=False,
        )
        if value_col is None:
            record["invalid_target_values"] = int(len(target))
        else:
            values = pd.to_numeric(target[value_col], errors="coerce")
            record["invalid_target_values"] = int((values.isna() | values.le(0)).sum())

        record["valid"] = bool(
            len(target) == len(EXPECTED_TENORS)
            and set(record["target_tenors"]) == set(EXPECTED_TENORS)
            and record["duplicate_target_keys"] == 0
            and record["invalid_target_values"] == 0
        )
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
    return record


def synchronize_implied_variance_handoff(
    project_root: Path,
    runtime: dict[str, Any],
    target_date: pd.Timestamp,
    audit_dir: Path,
) -> tuple[Path, dict[str, Any]]:
    canonical = resolve_path(project_root, runtime.get("canonical", {}).get("implied_variance"))
    refreshed = resolve_path(
        project_root,
        runtime.get("canonical", {}).get(
            "implied_variance_update_surface",
            "data/processed/implied_variance/spx_vix_style_implied_variance_surface_v1.parquet",
        ),
    )
    if canonical is None or refreshed is None:
        raise RuntimeError("Runtime config does not define implied-variance handoff paths.")

    candidates: list[Path] = []
    for candidate in (refreshed, canonical):
        if candidate not in candidates:
            candidates.append(candidate)
    audit_rows = [inspect_implied_variance_surface(path, target_date) for path in candidates]
    audit_frame = pd.DataFrame(audit_rows)
    write_csv_atomic(audit_frame, audit_dir / "implied_variance_handoff_audit.csv")

    valid = [row for row in audit_rows if row["valid"]]
    if not valid:
        raise RuntimeError(
            "No implied-variance artifact contains a complete positive nine-tenor grid on "
            f"{target_date.date()}. See {audit_dir / 'implied_variance_handoff_audit.csv'}"
        )

    # Prefer the updater's stable processed surface. If both are valid, newest mtime is the final tie-break.
    refreshed_key = str(refreshed.resolve())
    selected_row = max(
        valid,
        key=lambda row: (
            int(str(Path(row["path"]).resolve()) == refreshed_key),
            int(row.get("mtime_ns") or 0),
        ),
    )
    selected = Path(selected_row["path"])
    if selected.resolve() != canonical.resolve():
        atomic_copy(selected, canonical)

    canonical_check = inspect_implied_variance_surface(canonical, target_date)
    if not canonical_check["valid"]:
        raise RuntimeError(
            "The implied-variance canonical alias did not validate after synchronization: "
            f"{canonical_check}"
        )

    payload = {
        "target_date": str(target_date.date()),
        "selected_source": str(selected),
        "canonical_alias": str(canonical),
        "synchronized": bool(selected.resolve() != canonical.resolve()),
        "candidate_audit": audit_rows,
        "canonical_validation": canonical_check,
    }
    write_json(audit_dir / "implied_variance_handoff.json", payload)
    return canonical, payload


def select_base_before(paths: list[Path], recalc_start: pd.Timestamp) -> Path | None:
    dated: list[tuple[pd.Timestamp, Path]] = []
    for path in paths:
        max_date = file_max_date(path)
        if max_date is not None:
            dated.append((max_date, path))
    before = [(d, p) for d, p in dated if d < recalc_start]
    if before:
        return max(before, key=lambda item: (item[0], item[1].stat().st_mtime))[1]
    return min(dated, key=lambda item: item[0])[1] if dated else None


def truncate_table_before(path: Path, cutoff: pd.Timestamp, output_path: Path) -> Path:
    frame = read_table(path)
    date_col = first_existing_column(frame, ("date", "trade_date"), label="truncation date")
    dates = normalize_dates(frame[date_col])
    truncated = frame.loc[dates.lt(cutoff)].copy()
    if truncated.empty:
        raise RuntimeError(f"Cannot create a pre-gap base from {path}; no rows before {cutoff.date()}.")
    write_parquet_atomic(truncated, output_path)
    return output_path


def build_backup_targets(project_root: Path, runtime: dict[str, Any]) -> list[Path]:
    keys = [
        "spy_eod", "rv21d", "wilder_rsi", "implied_variance", "implied_variance_update_surface", "forecast_history",
        "signal_history", "latest_snapshot", "selected_decisions", "static_tiebreaks", "data_status", "execution_handoff",
    ]
    paths: list[Path] = []
    for key in keys:
        value = runtime.get("canonical", {}).get(key)
        if value:
            path = resolve_path(project_root, value)
            if path:
                paths.append(path)
    return paths


def stage_to_canonical(project_root: Path, runtime: dict[str, Any], staging_dir: Path) -> dict[str, str]:
    mapping = {
        "forecast_history": "vrp_hybrid_v2_forecast_history.parquet",
        "signal_history": "vrp_hybrid_v2_signal_history.parquet",
        "latest_snapshot": "vrp_hybrid_v2_latest_snapshot.parquet",
        "selected_decisions": "vrp_hybrid_v2_selected_decisions.parquet",
        "execution_handoff": "vrp_hybrid_v2_latest_execution_handoff.csv",
        "static_tiebreaks": "vrp_hybrid_v2_static_tiebreaks.csv",
    }
    published: dict[str, str] = {}
    for key, filename in mapping.items():
        source = staging_dir / filename
        if not source.exists():
            raise FileNotFoundError(f"Staged output missing: {source}")
        destination = resolve_path(project_root, runtime["canonical"][key])
        assert destination is not None
        atomic_copy(source, destination)
        published[key] = str(destination)
    return published


def pipeline(config: PipelineConfig) -> dict[str, Any]:
    runtime, runtime_path = load_runtime_config(config.project_root, config.runtime_config_path)
    audit_dir = config.run_dir
    log_dir = audit_dir / "logs"
    staging_dir = audit_dir / "staging"
    backup_dir = audit_dir / "backups"
    claim_run_directory(audit_dir)
    staging_dir.mkdir()
    step_rows: list[dict[str, Any]] = []

    lock_path = resolve_path(config.project_root, runtime["canonical"]["production_config"])
    assert lock_path is not None
    if not lock_path.exists():
        raise FileNotFoundError(f"Missing production config: {lock_path}")
    lock = load_json(lock_path)
    observed_release = lock.get("release_id") or lock.get("lock_id")
    if observed_release != LOCK_ID:
        raise RuntimeError(f"Wrong lock config: expected={LOCK_ID}, observed={observed_release}")
    if list(runtime.get("target_tenors", [])) != [9, 12, 15, 18, 21, 24, 27, 30, 33]:
        raise RuntimeError("Runtime tenor grid is not the exact locked nine-tenor grid.")

    td = runtime.get("thetadata", {})
    td_ok, td_detail = probe_tcp(str(td.get("host", "127.0.0.1")), int(td.get("port", 25503)))
    if not config.skip_upstream and not td_ok:
        raise RuntimeError(td_detail)
    progress("preflight", 3, f"Lock and runtime config validated; ThetaData={td_ok}")

    py = sys.executable
    scripts = runtime["scripts"]
    sofr_manifest_path: Path | None = None
    sofr_manifest: dict[str, Any] = {}
    sofr_changed_date: pd.Timestamp | None = None

    if not config.skip_upstream:
        sofr_script = resolve_script(config.project_root, scripts["sofr"])
        sofr_result = run_command(
            label="00_sofr_refresh",
            command=[
                py,
                "-u",
                str(sofr_script),
                "--project-root",
                str(config.project_root),
                "--write-canonical",
            ],
            log_dir=log_dir,
            progress_pct=6,
        )
        step_rows.append(sofr_result)
        sofr_manifest_path, sofr_manifest = parse_sofr_manifest_from_stdout(
            Path(sofr_result["stdout"])
        )
        changed_value = sofr_manifest.get("first_changed_sofr_date")
        if changed_value:
            sofr_changed_date = pd.Timestamp(
                pd.to_datetime(changed_value, errors="raise")
            ).normalize()
    else:
        step_rows.append({
            "step": "00_sofr_refresh",
            "status": "SKIP",
            "detail": "Skipped with --skip-upstream.",
        })
        progress("00_sofr_refresh", 6, "Skipped with --skip-upstream")

    initial_csv = audit_dir / "component_gap_report_before.csv"
    initial_json = audit_dir / "data_health_before.json"
    initial_health, initial_payload = run_health_check(HealthConfig(
        project_root=config.project_root,
        runtime_config_path=runtime_path,
        target_date=config.target_date,
        csv_out=initial_csv,
        json_out=initial_json,
        probe_thetadata=not config.skip_upstream,
    ))

    recalc_candidates = [
        earliest_gap_from_health(initial_health, config.target_date)
    ]
    if config.recalc_start_override is not None:
        recalc_candidates.append(config.recalc_start_override)
    if sofr_changed_date is not None:
        recalc_candidates.append(sofr_changed_date)

    recalc_start = min(recalc_candidates)
    if recalc_start > config.target_date:
        raise RuntimeError(
            f"Resolved recalc_start={recalc_start.date()} is after "
            f"target_date={config.target_date.date()}."
        )

    progress(
        "gap_scan",
        10,
        (
            f"Earliest recalc date={recalc_start.date()}; "
            f"override={config.recalc_start_override.date() if config.recalc_start_override is not None else None}; "
            f"SOFR_changed={sofr_changed_date.date() if sofr_changed_date is not None else None}"
        ),
    )

    backup_map = backup_files(build_backup_targets(config.project_root, runtime), backup_dir)
    manifest: dict[str, Any] = {
        "release_id": LOCK_ID,
        "run_timestamp": config.run_timestamp,
        "started_at": utc_now_iso(),
        "project_root": str(config.project_root),
        "runtime_config": str(runtime_path),
        "target_date": str(config.target_date.date()),
        "approved_nav": config.approved_nav,
        "code_version": config.code_version,
        "postgres_postpass_required": config.postgres_postpass_required,
        "postgres_environment": config.postgres_environment,
        "postgres_postpass_bypass_reason": (
            config.postgres_postpass_bypass_reason
        ),
        "skip_upstream": config.skip_upstream,
        "force_recalculate": config.force_recalculate,
        "recalc_start_override": (
            str(config.recalc_start_override.date())
            if config.recalc_start_override is not None
            else None
        ),
        "sofr_manifest": str(sofr_manifest_path) if sofr_manifest_path else None,
        "sofr_status": sofr_manifest.get("status"),
        "sofr_first_changed_date": (
            str(sofr_changed_date.date()) if sofr_changed_date is not None else None
        ),
        "publish_requested": config.publish,
        "recalc_start": str(recalc_start.date()),
        "backup_map": backup_map,
        "status": "RUNNING",
    }
    write_json(audit_dir / "run_manifest.json", manifest)

    try:
        if not config.skip_upstream:
            iv_script = resolve_script(config.project_root, scripts["implied_variance"])
            iv_cmd = [
                py, "-u", str(iv_script), "--project-root", str(config.project_root),
                "--write-canonical",
            ]
            force_iv_window = bool(
                config.force_recalculate
                or config.recalc_start_override is not None
                or sofr_changed_date is not None
            )
            if force_iv_window:
                iv_cmd += [
                    "--start-date",
                    recalc_start.strftime("%Y%m%d"),
                    "--end-date",
                    config.target_date.strftime("%Y%m%d"),
                    "--force-refresh",
                ]
            else:
                iv_cmd += [
                    "--update-missing",
                    "--end-date",
                    config.target_date.strftime("%Y%m%d"),
                ]
            step_rows.append(run_command(label="01_implied_variance", command=iv_cmd, log_dir=log_dir, progress_pct=16))

            market_script = resolve_script(config.project_root, scripts["market_data"])
            market_cmd = [
                py, "-u", str(market_script), "--project-root", str(config.project_root),
                "--start-date", str(runtime.get("history_start", "2018-01-01")).replace("-", ""),
                "--end-date", config.target_date.strftime("%Y%m%d"),
            ]
            if config.force_recalculate:
                market_cmd.append("--force-full-refresh")
            step_rows.append(run_command(label="02_market_data", command=market_cmd, log_dir=log_dir, progress_pct=26))

            rsi_script = resolve_script(config.project_root, scripts["wilder_rsi"])
            rsi_cmd = [
                py, "-u", str(rsi_script), "--project-root", str(config.project_root),
                "--end-date", config.target_date.strftime("%Y%m%d"),
            ]
            if config.force_recalculate:
                rsi_cmd.append("--force-full-refresh")
            step_rows.append(run_command(label="03_wilder_rsi", command=rsi_cmd, log_dir=log_dir, progress_pct=39))

            corsi_script = resolve_script(config.project_root, scripts["corsi_source"])
            corsi_cmd = [
                py, "-u", str(corsi_script), "--project-root", str(config.project_root),
                "--start-date", str(runtime.get("component_starts", {}).get("corsi_source", "2018-06-25")).replace("-", ""),
                "--end-date", config.target_date.strftime("%Y%m%d"),
            ]
            if config.force_recalculate:
                corsi_cmd.append("--force-refresh-theta")
            step_rows.append(run_command(label="04_corsi_source", command=corsi_cmd, log_dir=log_dir, progress_pct=50))

            feature_path = glob_latest(config.project_root, runtime["discovery"].get("feature_panel", []))
            feature_latest = file_max_date(feature_path) if feature_path else None
            feature_has_gap = bool(initial_health.loc[initial_health["component"].eq("feature_panel"), "status"].ne("PASS").any())
            if feature_latest != config.target_date or feature_has_gap or config.force_recalculate:
                source_panel = glob_latest(config.project_root, runtime["discovery"].get("corsi_source", []))
                if source_panel is None:
                    raise FileNotFoundError("No Corsi source panel found after update.")
                feature_candidates = glob_all(config.project_root, runtime["discovery"].get("feature_panel", []))
                old_feature = select_base_before(feature_candidates, recalc_start) if (feature_has_gap or config.force_recalculate) else feature_path
                if old_feature is None:
                    raise FileNotFoundError("No old feature panel found for locked update.")
                if (feature_has_gap or config.force_recalculate) and (
                    file_max_date(old_feature) is None or file_max_date(old_feature) >= recalc_start
                ):
                    old_feature = truncate_table_before(
                        old_feature,
                        recalc_start,
                        staging_dir / "feature_panel_base_before_recalc.parquet",
                    )
                feature_script = resolve_script(config.project_root, scripts["feature_panel"])
                feature_cmd = [
                    py, "-u", str(feature_script), "--project-root", str(config.project_root),
                    "--source-panel", str(source_panel), "--old-feature-panel", str(old_feature),
                    "--end-date", config.target_date.strftime("%Y%m%d"),
                ]
                step_rows.append(run_command(label="05_feature_panel", command=feature_cmd, log_dir=log_dir, progress_pct=60))
            else:
                step_rows.append({"step": "05_feature_panel", "status": "SKIP", "detail": "Already complete through target."})
                progress("05_feature_panel", 60, "Already complete through target")

        implied_source, implied_handoff = synchronize_implied_variance_handoff(
            config.project_root, runtime, config.target_date, audit_dir
        )
        step_rows.append({
            "step": "05b_implied_variance_handoff",
            "status": "PASS",
            "detail": (
                f"Validated target grid in {implied_handoff['selected_source']} and synchronized "
                f"canonical alias={implied_handoff['canonical_alias']}"
            ),
        })
        progress("05b_implied_variance_handoff", 68, "Fresh implied-variance surface validated and handed off")

        publisher_script = Path(__file__).with_name("vrp_hybrid_v2_signal_publish.py")
        feature_panel = glob_latest(config.project_root, runtime["discovery"].get("feature_panel", []))
        component_source = glob_latest(config.project_root, runtime["discovery"].get("corsi_source", []))
        if feature_panel is None:
            raise FileNotFoundError("No feature panel found for v2 publisher.")
        if component_source is None:
            raise FileNotFoundError("No Corsi component source found for v2 publisher.")
        publish_cmd = [
            sys.executable, "-u", str(publisher_script),
            "--project-root", str(config.project_root),
            "--runtime-config", str(runtime_path),
            "--lock-config", str(lock_path),
            "--target-date", config.target_date.strftime("%Y%m%d"),
            "--approved-nav", str(config.approved_nav),
            "--feature-panel", str(feature_panel),
            "--component-source", str(component_source),
            "--implied-variance", str(implied_source),
            "--staging-dir", str(staging_dir),
        ]
        step_rows.append(run_command(label="06_hybrid_v2_publish_stage", command=publish_cmd, log_dir=log_dir, progress_pct=76))

        if config.publish:
            published = stage_to_canonical(config.project_root, runtime, staging_dir)
            progress("07_atomic_publish", 86, "Staged v2 outputs copied to canonical paths")
        else:
            published = {}
            progress("07_atomic_publish", 86, "No-publish mode; canonical outputs unchanged")

        final_csv = audit_dir / "component_gap_report_after.csv"
        final_json = audit_dir / "data_health_after.json"
        final_health, final_payload = run_health_check(HealthConfig(
            project_root=config.project_root,
            runtime_config_path=runtime_path,
            target_date=config.target_date,
            csv_out=final_csv,
            json_out=final_json,
            probe_thetadata=not config.skip_upstream,
        ))
        if config.publish and final_payload["overall_status"] != "PASS":
            raise RuntimeError(
                "Final production health gate failed after publish. Canonical files will be restored. "
                f"See {final_csv}"
            )
        progress("08_final_health", 95, f"Final health={final_payload['overall_status']}")

        publish_manifest_path = staging_dir / "vrp_hybrid_v2_publish_manifest.json"
        publish_manifest = load_json(publish_manifest_path) if publish_manifest_path.exists() else {}
        status_payload = {
            "release_id": LOCK_ID,
            "generated_at": utc_now_iso(),
            "run_timestamp": config.run_timestamp,
            "target_date": str(config.target_date.date()),
            "status": "PASS" if final_payload["overall_status"] == "PASS" or not config.publish else "FAIL",
            "published": bool(config.publish),
            "latest_decision": publish_manifest.get("latest_decision", []),
            "data_health": final_payload,
            "audit_dir": str(audit_dir),
        }
        if config.publish:
            status_path = resolve_path(config.project_root, runtime["canonical"]["data_status"])
            assert status_path is not None
            write_json(status_path, status_payload)
        write_json(audit_dir / "run_status.json", status_payload)
        write_csv_atomic(pd.DataFrame(step_rows), audit_dir / "step_status.csv")
        manifest.update({
            "status": "PASS",
            "finished_at": utc_now_iso(),
            "published_outputs": published,
            "initial_health": initial_payload["overall_status"],
            "final_health": final_payload["overall_status"],
            "publish_manifest": str(publish_manifest_path),
        })
        write_json(audit_dir / "run_manifest.json", manifest)
        progress("complete", 100, "Hybrid v2 EOD pipeline completed")
        return manifest
    except Exception as exc:
        if config.publish or backup_map:
            restore_files(backup_map)
        error_path = audit_dir / "pipeline_error.txt"
        error_path.write_text(traceback.format_exc(), encoding="utf-8")
        write_csv_atomic(pd.DataFrame(step_rows), audit_dir / "step_status.csv")
        manifest.update({
            "status": "FAILED_NOT_PUBLISHED",
            "finished_at": utc_now_iso(),
            "error": str(exc),
            "error_trace": str(error_path),
            "rollback_attempted": True,
        })
        write_json(audit_dir / "run_manifest.json", manifest)
        progress("failed", 100, f"FAILED — NOT PUBLISHED: {exc}")
        raise


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    runtime_path = args.runtime_config.resolve() if args.runtime_config else project_root / "config/vrp_hybrid_v2_eod_runtime_config.json"
    runtime, _ = load_runtime_config(project_root, runtime_path)
    target = parse_target(args.target_date, int(runtime.get("close_buffer_minutes", 15)))
    if bool(args.postgres_postpass_required) != bool(args.postgres_environment):
        raise ValueError(
            "--postgres-postpass-required and --postgres-environment must be supplied together"
        )
    if (
        isinstance(args.postgres_environment, str)
        and args.postgres_environment != args.postgres_environment.strip()
    ):
        raise ValueError("--postgres-environment may not contain surrounding whitespace")
    if bool(args.postgres_postpass_required) and args.postgres_postpass_bypass_reason:
        raise ValueError("the PostgreSQL post-pass cannot be required and bypassed")
    code_version = None
    if not bool(args.no_publish) or args.code_version is not None:
        code_version = resolve_clean_code_version(
            source_root=REPOSITORY_ROOT,
            project_root=project_root,
            explicit=args.code_version,
        )
    recalc_start_override = (
        pd.Timestamp(pd.to_datetime(args.recalc_start, errors="raise")).normalize()
        if args.recalc_start
        else None
    )
    stamp = now_stamp()
    audit_rel = Path(runtime.get("outputs", {}).get("audit_dir", "data/audit/vrp_hybrid_v2_eod"))
    run_dir = project_root / audit_rel / stamp
    config = PipelineConfig(
        project_root=project_root,
        runtime_config_path=runtime_path,
        target_date=target,
        approved_nav=float(args.approved_nav),
        skip_upstream=bool(args.skip_upstream),
        force_recalculate=bool(args.force_recalculate),
        recalc_start_override=recalc_start_override,
        publish=not bool(args.no_publish),
        code_version=code_version,
        postgres_postpass_required=bool(args.postgres_postpass_required),
        postgres_environment=args.postgres_environment,
        postgres_postpass_bypass_reason=args.postgres_postpass_bypass_reason,
        run_timestamp=stamp,
        run_dir=run_dir,
    )
    if config.publish and config.skip_upstream:
        raise ValueError("--skip-upstream may only be used with --no-publish")
    if config.publish and not config.postgres_postpass_required and (
        config.postgres_postpass_bypass_reason != "explicit-no-postgres-shadow"
    ):
        raise ValueError(
            "published EOD requires the PostgreSQL post-pass unless the stable "
            "wrapper records an explicit bypass"
        )
    if not config.publish and config.postgres_postpass_bypass_reason is not None:
        raise ValueError("a no-publish diagnostic cannot record a publication bypass")
    if config.publish or not config.skip_upstream:
        require_canonical_eod_runtime_config(project_root, runtime_path)
    print("=" * 110)
    print("VRP Hybrid v2 EOD pipeline")
    print("=" * 110)
    print(f"Project root:      {project_root}")
    print(f"Target date:       {target.date()}")
    print(f"Approved NAV:      ${config.approved_nav:,.2f}")
    print(
        "Recalc override:   "
        + (
            str(config.recalc_start_override.date())
            if config.recalc_start_override is not None
            else "None"
        )
    )
    print(f"Audit directory:   {run_dir}")
    print(f"Publish:           {config.publish}")
    with eod_writer_execution_lock(project_root):
        if config.publish or not config.skip_upstream:
            assert_no_unresolved_eod_finalizations(
                resolve_eod_audit_root(project_root, None)
            )
        with exclusive_eod_canonical_writer_lock(project_root):
            manifest = pipeline(config)
            if args.result_handoff is not None:
                try:
                    handoff_path = write_result_handoff(
                        args.result_handoff,
                        config,
                        manifest,
                    )
                except Exception as exc:
                    print(
                        "PASS - Hybrid v2 EOD pipeline completed, but the result "
                        f"handoff failed: {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    print(
                        f"Healthy run retained at: {config.run_dir.resolve()}",
                        file=sys.stderr,
                        flush=True,
                    )
                    return RESULT_HANDOFF_WRITE_FAILED_EXIT_CODE
                print(f"Result handoff:    {handoff_path}")
    print("=" * 110)
    print("PASS — Hybrid v2 EOD pipeline completed.")
    print(f"Manifest: {run_dir / 'run_manifest.json'}")
    print("=" * 110)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
