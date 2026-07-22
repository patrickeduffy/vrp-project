"""Bind a completed EOD run to the exact SOFR updater evidence it consumed."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from vrp.reference_history.sources import normalize_sofr_csv


UPDATER_MANIFEST_NAME = "sofr_update_manifest.json"
UPDATER_TIMESTAMP = re.compile(r"^\d{8}_\d{6}_UTC$")


class SofrEvidenceError(ValueError):
    """The updater artifacts do not prove the SOFR input used by an EOD run."""


@dataclass(frozen=True)
class SofrUpdaterEvidence:
    """Immutable, content-addressed evidence for one accepted SOFR snapshot."""

    updater_manifest_path: Path
    updater_manifest_sha256: str
    refreshed_snapshot_path: Path
    refreshed_snapshot_sha256: str
    normalized_content_sha256: str
    start_date: date
    end_date: date
    row_count: int
    observation_date: date
    rate_decimal: Decimal
    row_sha256: str


def _fail(message: str) -> None:
    raise SofrEvidenceError(message)


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} must be a non-empty string")
    return value.strip()


def _portable_name(value: str, label: str) -> str:
    """Return a basename for either Windows or POSIX manifest paths."""

    normalized = value.strip().replace("\\", "/").rstrip("/")
    name = normalized.rsplit("/", 1)[-1]
    if not name or name in {".", ".."}:
        _fail(f"{label} does not identify a file")
    return name


def _sha256_file(path: Path, label: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise SofrEvidenceError(f"cannot read {label}: {path}: {exc}") from exc
    return digest.hexdigest()


def _stable_json(path: Path, label: str) -> tuple[dict[str, Any], str]:
    before = _sha256_file(path, label)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SofrEvidenceError(f"invalid {label}: {path}: {exc}") from exc
    in_memory = hashlib.sha256(raw).hexdigest()
    after = _sha256_file(path, label)
    if before != in_memory or before != after:
        _fail(f"{label} changed while it was being read")
    if not isinstance(payload, dict):
        _fail(f"{label} must contain a JSON object")
    return payload, before


def _manifest_date(value: Any, label: str) -> date:
    text = _required_text(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise SofrEvidenceError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != text:
        _fail(f"{label} must use canonical YYYY-MM-DD form")
    return parsed


def _resolve_updater_manifest(run_manifest: Mapping[str, Any]) -> Path:
    value = _required_text(run_manifest.get("sofr_manifest"), "run sofr_manifest")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        project_root = run_manifest.get("project_root")
        if isinstance(project_root, str) and project_root.strip():
            candidate = Path(project_root).expanduser() / candidate
    try:
        path = candidate.resolve(strict=True)
    except OSError as exc:
        raise SofrEvidenceError(f"missing SOFR updater manifest: {candidate}") from exc
    if not path.is_file():
        _fail(f"SOFR updater manifest is not a file: {path}")
    if path.name != UPDATER_MANIFEST_NAME:
        _fail(f"SOFR updater manifest must be named {UPDATER_MANIFEST_NAME}")
    if UPDATER_TIMESTAMP.fullmatch(path.parent.name) is None:
        _fail("SOFR updater manifest must be inside a timestamped updater directory")
    return path


def _validate_updater_manifest(payload: Mapping[str, Any], manifest_path: Path) -> None:
    required = {
        "status": "PUBLISHED",
        "published": True,
        "hard_checks_passed": True,
        "source": "FRED_SOFR",
    }
    for field, expected in required.items():
        observed = payload.get(field)
        if type(observed) is not type(expected) or observed != expected:
            _fail(f"SOFR updater manifest {field} must be {expected!r}")

    run_timestamp = _required_text(
        payload.get("run_timestamp_utc"),
        "SOFR updater run_timestamp_utc",
    )
    if UPDATER_TIMESTAMP.fullmatch(run_timestamp) is None:
        _fail("SOFR updater run_timestamp_utc is invalid")
    if run_timestamp != manifest_path.parent.name:
        _fail("SOFR updater manifest directory does not match run_timestamp_utc")

    recorded_audit_directory = _required_text(
        payload.get("audit_directory"),
        "SOFR updater audit_directory",
    )
    if _portable_name(recorded_audit_directory, "SOFR updater audit_directory") != run_timestamp:
        _fail("SOFR updater audit_directory does not match run_timestamp_utc")


def _resolve_refreshed_snapshot(
    payload: Mapping[str, Any],
    manifest_path: Path,
) -> Path:
    audit_files = payload.get("audit_files")
    if not isinstance(audit_files, Mapping):
        _fail("SOFR updater audit_files must be a JSON object")
    recorded = _required_text(
        audit_files.get("refreshed_snapshot"),
        "SOFR updater audit_files.refreshed_snapshot",
    )
    filename = _portable_name(
        recorded,
        "SOFR updater audit_files.refreshed_snapshot",
    )
    audit_directory = manifest_path.parent.resolve()
    candidate = audit_directory / filename
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise SofrEvidenceError(f"missing SOFR refreshed snapshot: {candidate}") from exc
    if not resolved.is_file() or resolved.parent != audit_directory:
        _fail("SOFR refreshed snapshot must be a file within the updater audit directory")
    return resolved


def load_sofr_updater_evidence(
    run_manifest: Mapping[str, Any],
    valuation_date: date,
) -> SofrUpdaterEvidence:
    """Validate and content-address the exact SOFR updater snapshot for an EOD run."""

    if not isinstance(run_manifest, Mapping):
        _fail("run_manifest must be a mapping")
    if type(valuation_date) is not date:
        _fail("valuation_date must be a date without a time component")

    manifest_path = _resolve_updater_manifest(run_manifest)
    payload, manifest_sha256 = _stable_json(
        manifest_path,
        "SOFR updater manifest",
    )
    _validate_updater_manifest(payload, manifest_path)
    snapshot_path = _resolve_refreshed_snapshot(payload, manifest_path)

    snapshot_sha256 = _sha256_file(snapshot_path, "SOFR refreshed snapshot")
    try:
        normalized = normalize_sofr_csv(snapshot_path)
    except Exception as exc:
        raise SofrEvidenceError(f"invalid SOFR refreshed snapshot: {exc}") from exc
    if _sha256_file(snapshot_path, "SOFR refreshed snapshot") != snapshot_sha256:
        _fail("SOFR refreshed snapshot changed while it was being normalized")

    start_date = _manifest_date(payload.get("new_min_date"), "SOFR new_min_date")
    end_date = _manifest_date(payload.get("new_max_date"), "SOFR new_max_date")
    row_count = payload.get("new_rows_total")
    if type(row_count) is not int or row_count <= 0:
        _fail("SOFR new_rows_total must be a positive integer")
    if start_date != normalized.start_date:
        _fail("SOFR manifest new_min_date does not match the normalized snapshot")
    if end_date != normalized.end_date:
        _fail("SOFR manifest new_max_date does not match the normalized snapshot")
    if row_count != len(normalized.rows):
        _fail("SOFR manifest new_rows_total does not match the normalized snapshot")
    if end_date >= valuation_date:
        _fail("SOFR snapshot end_date must be strictly before the EOD valuation_date")

    selected = normalized.rows[-1]
    if selected.observation_date != end_date:
        _fail("SOFR normalized history is not ordered through its declared end_date")

    # Reverify both evidence files after all parsing and normalization work.
    if _sha256_file(manifest_path, "SOFR updater manifest") != manifest_sha256:
        _fail("SOFR updater manifest changed while evidence was being validated")
    if _sha256_file(snapshot_path, "SOFR refreshed snapshot") != snapshot_sha256:
        _fail("SOFR refreshed snapshot changed while evidence was being validated")

    return SofrUpdaterEvidence(
        updater_manifest_path=manifest_path,
        updater_manifest_sha256=manifest_sha256,
        refreshed_snapshot_path=snapshot_path,
        refreshed_snapshot_sha256=snapshot_sha256,
        normalized_content_sha256=normalized.content_sha256,
        start_date=normalized.start_date,
        end_date=normalized.end_date,
        row_count=len(normalized.rows),
        observation_date=selected.observation_date,
        rate_decimal=selected.rate_decimal,
        row_sha256=selected.row_sha256,
    )


__all__ = [
    "SofrEvidenceError",
    "SofrUpdaterEvidence",
    "load_sofr_updater_evidence",
]
