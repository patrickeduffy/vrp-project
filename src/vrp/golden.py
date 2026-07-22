"""Capture and verify immutable EOD production examples.

Golden fixtures protect the locked signal contract while storage and
orchestration are reorganized. They intentionally read canonical production
artifacts instead of reimplementing model mathematics.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

FORMAT_VERSION = 1
VERIFICATION_FORMAT_VERSION = 1
EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
EXPECTED_COMPARISON = {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-10}
DEFAULT_FIXTURE_REL = Path("tests/golden/eod_v2_production_baseline.json")

CANONICAL_SIGNAL_REL = Path(
    "data/processed/vrp_hybrid_v2_eod/vrp_hybrid_v2_signal_history.parquet"
)
CANONICAL_DECISION_REL = Path(
    "data/processed/vrp_hybrid_v2_eod/vrp_hybrid_v2_selected_decisions.parquet"
)

SIGNAL_COLUMNS = [
    "date",
    "tenor",
    "test_year",
    "selected_alpha",
    "train_rows_used",
    "predicted_log_variance_candidate",
    "forecast_variance_candidate",
    "forecast_vol_pct",
    "forecast_model",
    "implied_variance",
    "implied_vol_pct",
    "model_vrp_log",
    "z_3m",
    "z_1y",
    "prior_63_rows",
    "prior_252_rows",
    "spy_close",
    "rsi14",
    "rsi_formula_version",
    "rv21d_vol_pct",
    "core_bucket",
    "core_threshold_vrp",
    "core_threshold_z3",
    "core_threshold_z1",
    "core_threshold_rsi",
    "core_threshold_rv",
    "core_rule_exists",
    "core_size_pct_nav",
    "core_pass",
    "core_failure_reason",
    "secondary_bucket",
    "secondary_threshold_vrp",
    "secondary_threshold_z3",
    "secondary_threshold_z1",
    "secondary_threshold_rsi",
    "secondary_threshold_rv",
    "secondary_rule_exists",
    "secondary_size_pct_nav",
    "secondary_pass",
    "secondary_failure_reason",
    "selected_layer",
    "selected_tenor",
    "selected_trade",
    "lock_id",
]

DECISION_COLUMNS = [
    "date",
    "layer",
    "bucket",
    "tenor",
    "size_pct_nav",
    "target_max_risk_dollars",
    "implied_variance",
    "implied_vol_pct",
    "forecast_variance_candidate",
    "forecast_vol_pct",
    "model_vrp_log",
    "z_3m",
    "z_1y",
    "rsi14",
    "rv21d_vol_pct",
    "continuous_quality_score",
    "research_sleeve_win_rate_pct",
    "research_sleeve_worst_1pct_mean_return",
    "threshold_vrp",
    "threshold_z3",
    "threshold_z1",
    "threshold_rsi",
    "threshold_rv",
    "selection_reason",
    "decision_status",
    "lock_id",
    "selection_rule",
    "approved_nav_dollars",
]


@dataclass(frozen=True)
class GoldenCase:
    case_id: str
    date: str
    description: str


DEFAULT_CASES = [
    GoldenCase(
        "latest_no_trade",
        "2026-07-21",
        "Latest accepted EOD no-trade decision at the production baseline.",
    ),
    GoldenCase(
        "dense_core_back_tiebreak",
        "2026-04-21",
        "Core Back selection when many Core and Secondary candidates qualify.",
    ),
    GoldenCase(
        "core_middle",
        "2026-03-16",
        "Representative Core Middle selection.",
    ),
    GoldenCase(
        "secondary_back",
        "2026-05-15",
        "Representative Secondary Back selection.",
    ),
    GoldenCase(
        "secondary_middle",
        "2026-05-12",
        "Representative Secondary Middle selection.",
    ),
    GoldenCase(
        "secondary_front",
        "2026-05-14",
        "Representative Secondary Front selection under the locked Hybrid v2 contract.",
    ),
]


def _sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _clean_git_head(source_root: Path) -> str:
    git_prefix = ["git", "-c", f"safe.directory={source_root}", "-C", str(source_root)]
    head_result = subprocess.run(
        [*git_prefix, "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if head_result.returncode != 0:
        raise ValueError(f"source_root is not a readable Git checkout: {source_root}")
    status_result = subprocess.run(
        [*git_prefix, "status", "--porcelain", "--untracked-files=no"],
        capture_output=True,
        text=True,
        check=False,
    )
    if status_result.returncode != 0:
        raise ValueError(f"could not inspect tracked Git state under source_root: {source_root}")
    if status_result.stdout.strip():
        raise ValueError("source_root has tracked changes; golden capture requires a clean checkout")
    return head_result.stdout.strip().lower()


def _json_scalar(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _records(frame: pd.DataFrame, columns: Sequence[str]) -> list[dict[str, Any]]:
    return [
        {column: _json_scalar(row[column]) for column in columns}
        for row in frame.loc[:, columns].to_dict(orient="records")
    ]


def _resolve_artifact_path(
    source_root: Path,
    override: Path | None,
    default_relative: Path,
) -> Path:
    if override is None:
        return source_root / default_relative
    return override if override.is_absolute() else source_root / override


def resolve_output_paths(
    source_root: Path,
    *,
    signal_history_path: Path | None = None,
    selected_decisions_path: Path | None = None,
) -> tuple[Path, Path]:
    if (signal_history_path is None) != (selected_decisions_path is None):
        raise ValueError(
            "signal_history_path and selected_decisions_path must be supplied together"
        )
    signal_path = _resolve_artifact_path(
        source_root,
        signal_history_path,
        CANONICAL_SIGNAL_REL,
    )
    decision_path = _resolve_artifact_path(
        source_root,
        selected_decisions_path,
        CANONICAL_DECISION_REL,
    )
    return signal_path.resolve(), decision_path.resolve()


def _load_outputs_from_paths(
    signal_path: Path,
    decision_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing_files = [path for path in (signal_path, decision_path) if not path.is_file()]
    if missing_files:
        rendered = ", ".join(str(path) for path in missing_files)
        raise FileNotFoundError(f"Missing EOD artifacts: {rendered}")

    signals = pd.read_parquet(signal_path)
    decisions = pd.read_parquet(decision_path)
    for label, frame, required in (
        ("signal history", signals, SIGNAL_COLUMNS),
        ("selected decisions", decisions, DECISION_COLUMNS),
    ):
        missing_columns = [column for column in required if column not in frame.columns]
        if missing_columns:
            raise ValueError(f"{label} is missing contract columns: {missing_columns}")
        frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    return signals, decisions


def _load_outputs(
    source_root: Path,
    *,
    signal_history_path: Path | None = None,
    selected_decisions_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, Path, Path]:
    signal_path, decision_path = resolve_output_paths(
        source_root,
        signal_history_path=signal_history_path,
        selected_decisions_path=selected_decisions_path,
    )
    signals, decisions = _load_outputs_from_paths(signal_path, decision_path)
    return signals, decisions, signal_path, decision_path


def _case_payload(
    signals: pd.DataFrame,
    decisions: pd.DataFrame,
    case: GoldenCase,
) -> dict[str, Any]:
    target = pd.Timestamp(case.date).normalize()
    signal_rows = signals.loc[signals["date"].eq(target)].sort_values("tenor")
    decision_rows = decisions.loc[decisions["date"].eq(target)]

    if signal_rows.empty:
        raise ValueError(f"Golden case {case.case_id!r} has no signal rows on {case.date}.")
    tenors = signal_rows["tenor"].astype(int).tolist()
    if tenors != EXPECTED_TENORS:
        raise ValueError(
            f"Golden case {case.case_id!r} expected tenors {EXPECTED_TENORS}, found {tenors}."
        )
    if len(decision_rows) != 1:
        raise ValueError(
            f"Golden case {case.case_id!r} expected one decision row, found {len(decision_rows)}."
        )

    return {
        "case_id": case.case_id,
        "date": target.date().isoformat(),
        "description": case.description,
        "signals": _records(signal_rows, SIGNAL_COLUMNS),
        "decision": _records(decision_rows, DECISION_COLUMNS)[0],
    }


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def capture_golden_contract(
    source_root: Path,
    fixture_path: Path,
    *,
    baseline_commit: str,
    cases: Sequence[GoldenCase] = DEFAULT_CASES,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Capture selected canonical dates as an immutable JSON contract."""

    source_root = source_root.resolve()
    fixture_path = fixture_path.resolve()
    if not re.fullmatch(r"[0-9a-f]{40}", baseline_commit):
        raise ValueError("baseline_commit must be a full lowercase 40-character Git SHA")
    source_commit = _clean_git_head(source_root)
    if source_commit != baseline_commit:
        raise ValueError(
            f"baseline_commit {baseline_commit} does not match source_root HEAD {source_commit}"
        )
    if fixture_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing golden fixture: {fixture_path}. "
            "An approved recapture must pass overwrite=True."
        )
    signals, decisions, signal_path, decision_path = _load_outputs(source_root)
    payload = {
        "format_version": FORMAT_VERSION,
        "baseline_commit": baseline_commit,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_contract": {
            "signal_history": {
                "path": CANONICAL_SIGNAL_REL.as_posix(),
                "sha256_at_capture": _sha256(signal_path),
            },
            "selected_decisions": {
                "path": CANONICAL_DECISION_REL.as_posix(),
                "sha256_at_capture": _sha256(decision_path),
            },
        },
        "comparison": EXPECTED_COMPARISON,
        "expected_tenors": EXPECTED_TENORS,
        "signal_columns": SIGNAL_COLUMNS,
        "decision_columns": DECISION_COLUMNS,
        "cases": [_case_payload(signals, decisions, case) for case in cases],
    }
    errors = validate_fixture_payload(payload)
    if errors:
        raise ValueError("Invalid captured fixture: " + "; ".join(errors))

    _write_json_atomic(fixture_path, payload)
    return payload


def load_fixture(fixture_path: Path) -> dict[str, Any]:
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def validate_fixture_payload(payload: Mapping[str, Any]) -> list[str]:
    """Validate fixture structure without requiring local production data."""

    errors: list[str] = []
    if payload.get("format_version") != FORMAT_VERSION:
        errors.append(f"format_version must equal {FORMAT_VERSION}")
    captured_at = payload.get("captured_at_utc")
    try:
        captured_timestamp = pd.Timestamp(captured_at)
        captured_is_utc = (
            captured_timestamp.tzinfo is not None
            and captured_timestamp.utcoffset() is not None
            and captured_timestamp.utcoffset().total_seconds() == 0
        )
    except (TypeError, ValueError):
        captured_is_utc = False
    if not captured_is_utc:
        errors.append("captured_at_utc must be a valid UTC timestamp")
    baseline_commit = payload.get("baseline_commit")
    if not isinstance(baseline_commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}", baseline_commit
    ):
        errors.append("baseline_commit must be a full lowercase 40-character Git SHA")
    comparison = payload.get("comparison")
    if comparison != EXPECTED_COMPARISON:
        errors.append(f"comparison must equal {EXPECTED_COMPARISON}")
    source_contract = payload.get("source_contract")
    if not isinstance(source_contract, dict):
        errors.append("source_contract must be an object")
    else:
        expected_sources = {
            "signal_history": CANONICAL_SIGNAL_REL.as_posix(),
            "selected_decisions": CANONICAL_DECISION_REL.as_posix(),
        }
        if set(source_contract) != set(expected_sources):
            errors.append(f"source_contract keys must equal {sorted(expected_sources)}")
        for key, expected_path in expected_sources.items():
            source = source_contract.get(key)
            if not isinstance(source, dict) or set(source) != {"path", "sha256_at_capture"}:
                errors.append(f"source_contract.{key} must contain path and sha256_at_capture")
                continue
            if source.get("path") != expected_path:
                errors.append(f"source_contract.{key}.path must equal {expected_path}")
            digest = source.get("sha256_at_capture")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                errors.append(f"source_contract.{key}.sha256_at_capture must be lowercase SHA-256")
    if payload.get("expected_tenors") != EXPECTED_TENORS:
        errors.append(f"expected_tenors must equal {EXPECTED_TENORS}")
    if payload.get("signal_columns") != SIGNAL_COLUMNS:
        errors.append("signal_columns do not match the locked fixture contract")
    if payload.get("decision_columns") != DECISION_COLUMNS:
        errors.append("decision_columns do not match the locked fixture contract")

    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        errors.append("at least one golden case is required")
        return errors

    identifiers: set[str] = set()
    dates: set[str] = set()
    for case_index, case in enumerate(cases):
        if not isinstance(case, dict):
            errors.append(f"case {case_index} must be an object")
            continue
        case_id = case.get("case_id")
        case_date = case.get("date")
        if not isinstance(case_id, str) or not case_id.strip():
            errors.append(f"case_id must be a non-empty string: {case_id!r}")
            case_id = f"case_{case_index}"
        elif case_id in identifiers:
            errors.append(f"case_id must be present and unique: {case_id!r}")
        identifiers.add(case_id)
        if not isinstance(case_date, str) or not case_date.strip():
            errors.append(f"case date must be a non-empty string: {case_date!r}")
            case_date = f"invalid_date_{case_index}"
        elif case_date in dates:
            errors.append(f"case date must be present and unique: {case_date!r}")
        dates.add(case_date)
        signal_rows = case.get("signals")
        if not isinstance(signal_rows, list) or len(signal_rows) != len(EXPECTED_TENORS):
            errors.append(f"{case_id}: expected {len(EXPECTED_TENORS)} signal rows")
            continue
        if not all(isinstance(row, dict) for row in signal_rows):
            errors.append(f"{case_id}: every signal row must be an object")
            continue
        tenors = [row.get("tenor") for row in signal_rows]
        if tenors != EXPECTED_TENORS:
            errors.append(f"{case_id}: tenor order mismatch: {tenors}")
        for index, row in enumerate(signal_rows):
            if not isinstance(row, dict) or set(row) != set(SIGNAL_COLUMNS):
                errors.append(f"{case_id}: signal row {index} keys do not match signal_columns")
                continue
            try:
                row_date = pd.Timestamp(row["date"]).date().isoformat()
            except (TypeError, ValueError):
                row_date = None
            if row_date != case_date:
                errors.append(f"{case_id}: signal row {index} date does not match case date")

        decision = case.get("decision")
        if not isinstance(decision, dict):
            errors.append(f"{case_id}: one decision object is required")
            continue
        if set(decision) != set(DECISION_COLUMNS):
            errors.append(f"{case_id}: decision keys do not match decision_columns")
            continue
        try:
            decision_date = pd.Timestamp(decision["date"]).date().isoformat()
        except (TypeError, ValueError):
            decision_date = None
        if decision_date != case_date:
            errors.append(f"{case_id}: decision date does not match case date")
        decision_status = decision.get("decision_status")
        selected_rows = [row for row in signal_rows if row.get("selected_trade") is True]
        if decision_status == "TRADE":
            if len(selected_rows) != 1:
                errors.append(f"{case_id}: TRADE decision must have exactly one selected signal row")
            elif selected_rows[0].get("tenor") != decision.get("tenor"):
                errors.append(f"{case_id}: selected signal tenor does not match decision tenor")
        elif decision_status == "NO_TRADE":
            if selected_rows:
                errors.append(f"{case_id}: NO_TRADE decision cannot have a selected signal row")
        else:
            errors.append(f"{case_id}: unsupported decision_status {decision_status!r}")
    return errors


def _compare_scalar(expected: Any, actual: Any, *, atol: float, rtol: float) -> bool:
    if expected is None or actual is None:
        return expected is actual
    if isinstance(expected, bool) or isinstance(actual, bool):
        return type(expected) is bool and type(actual) is bool and expected == actual
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return math.isclose(float(expected), float(actual), abs_tol=atol, rel_tol=rtol)
    return expected == actual


def _compare_record(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    *,
    location: str,
    columns: Iterable[str],
    atol: float,
    rtol: float,
) -> list[str]:
    mismatches: list[str] = []
    for column in columns:
        expected_value = expected[column]
        actual_value = actual[column]
        if not _compare_scalar(expected_value, actual_value, atol=atol, rtol=rtol):
            mismatches.append(
                f"{location}.{column}: expected {expected_value!r}, found {actual_value!r}"
            )
    return mismatches


def _verify_loaded_contract(
    fixture: Mapping[str, Any],
    signals: pd.DataFrame,
    decisions: pd.DataFrame,
) -> list[str]:
    structural_errors = validate_fixture_payload(fixture)
    if structural_errors:
        return [f"fixture: {error}" for error in structural_errors]
    comparison = fixture["comparison"]
    atol = float(comparison["absolute_tolerance"])
    rtol = float(comparison["relative_tolerance"])
    signal_columns = fixture["signal_columns"]
    decision_columns = fixture["decision_columns"]
    mismatches: list[str] = []

    for case in fixture["cases"]:
        case_id = case["case_id"]
        target = pd.Timestamp(case["date"]).normalize()
        signal_rows = signals.loc[signals["date"].eq(target)].sort_values("tenor")
        decision_rows = decisions.loc[decisions["date"].eq(target)]
        if len(signal_rows) != len(EXPECTED_TENORS):
            mismatches.append(
                f"{case_id}: expected {len(EXPECTED_TENORS)} signal rows, found {len(signal_rows)}"
            )
            continue
        if len(decision_rows) != 1:
            mismatches.append(f"{case_id}: expected one decision row, found {len(decision_rows)}")
            continue

        actual_signals = _records(signal_rows, signal_columns)
        for index, (expected_row, actual_row) in enumerate(
            zip(case["signals"], actual_signals, strict=True)
        ):
            tenor = expected_row.get("tenor", index)
            mismatches.extend(
                _compare_record(
                    expected_row,
                    actual_row,
                    location=f"{case_id}.signals[{tenor}]",
                    columns=signal_columns,
                    atol=atol,
                    rtol=rtol,
                )
            )

        actual_decision = _records(decision_rows, decision_columns)[0]
        mismatches.extend(
            _compare_record(
                case["decision"],
                actual_decision,
                location=f"{case_id}.decision",
                columns=decision_columns,
                atol=atol,
                rtol=rtol,
            )
        )
    return mismatches


def verify_golden_contract(
    source_root: Path,
    fixture_path: Path,
    *,
    signal_history_path: Path | None = None,
    selected_decisions_path: Path | None = None,
) -> list[str]:
    """Return all deviations from an existing golden fixture.

    This is a low-level comparison helper. By default it reads the canonical
    files under ``source_root``. Staged callers that may publish outputs must
    use :func:`verify_golden_contract_with_manifest` and then apply the
    publisher-side manifest validator.
    """

    fixture = load_fixture(fixture_path)
    signals, decisions, _, _ = _load_outputs(
        source_root.resolve(),
        signal_history_path=signal_history_path,
        selected_decisions_path=selected_decisions_path,
    )
    return _verify_loaded_contract(fixture, signals, decisions)


def _verification_identity_payload(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Return the path- and time-independent content identity for a manifest."""

    fixture = manifest["fixture"]
    artifacts = manifest["artifacts"]
    return {
        "format_version": manifest["format_version"],
        "status": manifest["status"],
        "mode": manifest["mode"],
        "fixture_sha256": fixture["sha256"],
        "baseline_commit": fixture["baseline_commit"],
        "signal_history_sha256": artifacts["signal_history"]["sha256"],
        "selected_decisions_sha256": artifacts["selected_decisions"]["sha256"],
        "case_ids": manifest["case_ids"],
        "mismatch_count": manifest["mismatch_count"],
        "mismatches": manifest["mismatches"],
    }


def _verification_id(manifest: Mapping[str, Any]) -> str:
    identity_bytes = json.dumps(
        _verification_identity_payload(manifest),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(identity_bytes).hexdigest()


def _try_sha256(path: Path, *, label: str, errors: list[str]) -> str | None:
    try:
        if not path.is_file():
            errors.append(f"{label}: file does not exist: {path}")
            return None
        return _sha256(path)
    except OSError as exc:
        errors.append(f"{label}: could not hash {path}: {exc}")
        return None


def verify_golden_contract_with_manifest(
    source_root: Path,
    fixture_path: Path,
    *,
    signal_history_path: Path | None = None,
    selected_decisions_path: Path | None = None,
) -> tuple[list[str], dict[str, Any]]:
    """Verify outputs and bind the result to their exact content digests."""

    source_root = source_root.resolve()
    fixture_path = fixture_path.resolve()
    signal_path, decision_path = resolve_output_paths(
        source_root,
        signal_history_path=signal_history_path,
        selected_decisions_path=selected_decisions_path,
    )
    mismatches: list[str] = []

    fixture: Mapping[str, Any] | None = None
    fixture_digest: str | None = None
    try:
        fixture_bytes = fixture_path.read_bytes()
        fixture_digest = _sha256_bytes(fixture_bytes)
        parsed_fixture = json.loads(fixture_bytes)
        if not isinstance(parsed_fixture, dict):
            mismatches.append("fixture: root value must be an object")
        else:
            fixture = parsed_fixture
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        mismatches.append(f"fixture: could not read valid JSON from {fixture_path}: {exc}")

    before = {
        "signal_history": _try_sha256(
            signal_path,
            label="signal_history",
            errors=mismatches,
        ),
        "selected_decisions": _try_sha256(
            decision_path,
            label="selected_decisions",
            errors=mismatches,
        ),
    }
    if fixture is not None and all(digest is not None for digest in before.values()):
        try:
            signals, decisions = _load_outputs_from_paths(signal_path, decision_path)
            mismatches.extend(_verify_loaded_contract(fixture, signals, decisions))
        except Exception as exc:  # noqa: BLE001 - a failed verifier must emit FAIL evidence
            mismatches.append(f"verification: {type(exc).__name__}: {exc}")

    after: dict[str, str | None] = {}
    for label, path in (
        ("signal_history", signal_path),
        ("selected_decisions", decision_path),
    ):
        if before[label] is None:
            after[label] = None
            continue
        post_errors: list[str] = []
        after[label] = _try_sha256(path, label=label, errors=post_errors)
        if post_errors:
            mismatches.extend(post_errors)
    if before != after:
        mismatches.append("candidate artifacts changed while golden verification was running")

    cases = fixture.get("cases") if fixture is not None else None
    case_ids = (
        [
            case["case_id"]
            for case in cases
            if isinstance(case, dict) and isinstance(case.get("case_id"), str)
        ]
        if isinstance(cases, list)
        else []
    )
    baseline_commit = fixture.get("baseline_commit") if fixture is not None else None
    manifest: dict[str, Any] = {
        "format_version": VERIFICATION_FORMAT_VERSION,
        "verified_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": "PASS" if not mismatches else "FAIL",
        "mode": "STAGED" if signal_history_path is not None else "CANONICAL",
        "fixture": {
            "path": str(fixture_path),
            "sha256": fixture_digest,
            "baseline_commit": baseline_commit,
        },
        "artifacts": {
            "signal_history": {
                "path": str(signal_path),
                "sha256": after["signal_history"],
            },
            "selected_decisions": {
                "path": str(decision_path),
                "sha256": after["selected_decisions"],
            },
        },
        "case_ids": case_ids,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }
    manifest["verification_id"] = _verification_id(manifest)
    return mismatches, manifest


def ensure_manifest_output_path(path: Path, protected_paths: Iterable[Path | str]) -> Path:
    """Reject a manifest destination that aliases any verification input."""

    resolved = path.resolve()
    for protected in protected_paths:
        protected_resolved = Path(protected).resolve()
        if resolved == protected_resolved:
            raise ValueError(
                f"verification manifest cannot overwrite protected input: {protected_resolved}"
            )
    return resolved


def write_verification_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    try:
        protected_paths = [
            manifest["fixture"]["path"],
            manifest["artifacts"]["signal_history"]["path"],
            manifest["artifacts"]["selected_decisions"]["path"],
        ]
    except (KeyError, TypeError) as exc:
        raise ValueError("verification manifest is missing protected input paths") from exc
    resolved = ensure_manifest_output_path(path, protected_paths)
    _write_json_atomic(resolved, manifest)


def validate_verification_manifest_for_publication(
    manifest_path: Path,
    *,
    accepted_baseline_commit: str,
    accepted_fixture_path: Path,
    accepted_fixture_sha256: str,
    allowed_signal_history_path: Path,
    allowed_selected_decisions_path: Path,
) -> list[str]:
    """Validate and rehash staged evidence immediately before publication."""

    try:
        manifest = load_fixture(manifest_path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [f"manifest: could not read valid JSON: {exc}"]
    if not isinstance(manifest, dict):
        return ["manifest: root value must be an object"]

    errors: list[str] = []
    expected_top = {
        "format_version",
        "verified_at_utc",
        "status",
        "mode",
        "fixture",
        "artifacts",
        "case_ids",
        "mismatch_count",
        "mismatches",
        "verification_id",
    }
    if set(manifest) != expected_top:
        errors.append("manifest: top-level keys do not match the verification contract")
    if manifest.get("format_version") != VERIFICATION_FORMAT_VERSION:
        errors.append(f"manifest: format_version must equal {VERIFICATION_FORMAT_VERSION}")
    if manifest.get("status") != "PASS":
        errors.append("manifest: status must be PASS")
    if manifest.get("mode") != "STAGED":
        errors.append("manifest: mode must be STAGED")
    if manifest.get("mismatch_count") != 0 or manifest.get("mismatches") != []:
        errors.append("manifest: PASS evidence cannot contain mismatches")

    verified_at = manifest.get("verified_at_utc")
    try:
        verified_timestamp = pd.Timestamp(verified_at)
        verified_is_utc = (
            verified_timestamp.tzinfo is not None
            and verified_timestamp.utcoffset() is not None
            and verified_timestamp.utcoffset().total_seconds() == 0
        )
    except (TypeError, ValueError):
        verified_is_utc = False
    if not verified_is_utc:
        errors.append("manifest: verified_at_utc must be a valid UTC timestamp")

    fixture = manifest.get("fixture")
    artifacts = manifest.get("artifacts")
    fixture_valid = isinstance(fixture, dict) and set(fixture) == {
        "path",
        "sha256",
        "baseline_commit",
    }
    artifacts_valid = (
        isinstance(artifacts, dict)
        and set(artifacts) == {"signal_history", "selected_decisions"}
        and all(
            isinstance(artifacts.get(label), dict)
            and set(artifacts[label]) == {"path", "sha256"}
            for label in ("signal_history", "selected_decisions")
        )
    )
    if not fixture_valid:
        errors.append("manifest: fixture block does not match the verification contract")
    if not artifacts_valid:
        errors.append("manifest: artifacts block does not match the verification contract")

    case_ids = manifest.get("case_ids")
    if (
        not isinstance(case_ids, list)
        or not case_ids
        or not all(isinstance(case_id, str) and case_id for case_id in case_ids)
        or len(case_ids) != len(set(case_ids))
    ):
        errors.append("manifest: case_ids must be a non-empty list of unique strings")

    verification_id = manifest.get("verification_id")
    if not isinstance(verification_id, str) or not re.fullmatch(
        r"[0-9a-f]{64}", verification_id
    ):
        errors.append("manifest: verification_id must be a lowercase SHA-256")
    elif fixture_valid and artifacts_valid and set(manifest) == expected_top:
        try:
            expected_id = _verification_id(manifest)
        except (KeyError, TypeError, ValueError):
            errors.append("manifest: could not calculate verification_id")
        else:
            if verification_id != expected_id:
                errors.append("manifest: verification_id does not match its content identity")

    if not isinstance(accepted_baseline_commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}", accepted_baseline_commit
    ):
        errors.append("policy: accepted_baseline_commit must be a lowercase full Git SHA")
    if not isinstance(accepted_fixture_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", accepted_fixture_sha256
    ):
        errors.append("policy: accepted_fixture_sha256 must be a lowercase SHA-256")

    if fixture_valid:
        if fixture.get("baseline_commit") != accepted_baseline_commit:
            errors.append("manifest: baseline commit is not the accepted production baseline")
        if fixture.get("sha256") != accepted_fixture_sha256:
            errors.append("manifest: fixture digest is not the accepted production fixture")

    path_contracts = []
    if fixture_valid:
        path_contracts.append(
            ("fixture", fixture["path"], accepted_fixture_path, fixture["sha256"])
        )
    if artifacts_valid:
        path_contracts.extend(
            [
                (
                    "signal_history",
                    artifacts["signal_history"]["path"],
                    allowed_signal_history_path,
                    artifacts["signal_history"]["sha256"],
                ),
                (
                    "selected_decisions",
                    artifacts["selected_decisions"]["path"],
                    allowed_selected_decisions_path,
                    artifacts["selected_decisions"]["sha256"],
                ),
            ]
        )
    for label, recorded_path, allowed_path, recorded_digest in path_contracts:
        if not isinstance(recorded_path, str):
            errors.append(f"manifest: {label} path must be a string")
            continue
        try:
            resolved_recorded = Path(recorded_path).resolve()
            resolved_allowed = Path(allowed_path).resolve()
        except (OSError, TypeError, ValueError) as exc:
            errors.append(f"manifest: {label} path could not be resolved: {exc}")
            continue
        if resolved_recorded != resolved_allowed:
            errors.append(f"manifest: {label} path is outside the allowed publication path")
            continue
        if not isinstance(recorded_digest, str) or not re.fullmatch(
            r"[0-9a-f]{64}", recorded_digest
        ):
            errors.append(f"manifest: {label} digest must be a lowercase SHA-256")
            continue
        rehash_errors: list[str] = []
        current_digest = _try_sha256(
            resolved_allowed,
            label=label,
            errors=rehash_errors,
        )
        errors.extend(f"publication: {error}" for error in rehash_errors)
        if current_digest is not None and current_digest != recorded_digest:
            errors.append(f"publication: {label} changed after verification")
    return errors
