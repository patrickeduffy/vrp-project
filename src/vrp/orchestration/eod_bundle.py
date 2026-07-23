"""Content-address one exact completed Hybrid v2 EOD source bundle."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping


EOD_SOURCE_BUNDLE_CONTRACT = "vrp.hybrid_v2.eod_source_bundle"
EOD_SOURCE_BUNDLE_SCHEMA_VERSION = 1
SOFR_UPDATER_MANIFEST_NAME = "sofr_update_manifest.json"
SOFR_REFRESHED_SNAPSHOT_NAME = "fred_sofr_history_refreshed_snapshot.csv"
SOFR_UPDATER_TIMESTAMP = re.compile(r"^\d{8}_\d{6}_UTC$")

_RUN_FILES = {
    "run_manifest": "run_manifest.json",
    "run_status": "run_status.json",
}
_STAGING_FILES = {
    "publish_manifest": "vrp_hybrid_v2_publish_manifest.json",
    "signal_history": "vrp_hybrid_v2_signal_history.parquet",
    "latest_snapshot": "vrp_hybrid_v2_latest_snapshot.parquet",
    "selected_decisions": "vrp_hybrid_v2_selected_decisions.parquet",
    "forecast_history": "vrp_hybrid_v2_forecast_history.parquet",
    "static_tiebreaks": "vrp_hybrid_v2_static_tiebreaks.csv",
}
EOD_SOURCE_BUNDLE_LABELS = tuple(
    sorted(
        (
            *_RUN_FILES,
            *_STAGING_FILES,
            "sofr_update_manifest",
            "sofr_refreshed_snapshot",
        )
    )
)


class EodSourceBundleError(ValueError):
    """The supplied run directory is not one stable, exact EOD source bundle."""


@dataclass(frozen=True)
class EodSourceBundle:
    """Path-independent labels and digests for one exact EOD source bundle."""

    schema_version: int
    content_sha256: str
    artifact_sha256: Mapping[str, str]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "contract": EOD_SOURCE_BUNDLE_CONTRACT,
            "schema_version": self.schema_version,
            "content_sha256": self.content_sha256,
            "artifact_sha256": dict(self.artifact_sha256),
        }


def _fail(message: str) -> None:
    raise EodSourceBundleError(message)


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} must be a non-empty string")
    return value.strip()


def _file_state(path: Path, label: str) -> tuple[int, int, int, int]:
    try:
        stat = path.stat()
    except OSError as exc:
        raise EodSourceBundleError(f"cannot stat {label}: {path}: {exc}") from exc
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _sha256_file(path: Path, label: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise EodSourceBundleError(f"cannot read {label}: {path}: {exc}") from exc
    return digest.hexdigest()


def _stable_sha256(path: Path, label: str) -> str:
    before = _file_state(path, label)
    first = _sha256_file(path, label)
    middle = _file_state(path, label)
    second = _sha256_file(path, label)
    after = _file_state(path, label)
    if before != middle or middle != after or first != second:
        _fail(f"{label} changed while it was being hashed")
    return first


def _stable_json(path: Path, label: str) -> tuple[dict[str, Any], str]:
    before = _stable_sha256(path, label)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EodSourceBundleError(f"invalid {label}: {path}: {exc}") from exc
    in_memory = hashlib.sha256(raw).hexdigest()
    after = _stable_sha256(path, label)
    if before != in_memory or before != after:
        _fail(f"{label} changed while it was being read")
    if not isinstance(payload, dict):
        _fail(f"{label} must contain a JSON object")
    return payload, before


def _fixed_file(directory: Path, filename: str, label: str) -> Path:
    try:
        resolved_directory = directory.resolve(strict=True)
        path = (resolved_directory / filename).resolve(strict=True)
    except OSError as exc:
        raise EodSourceBundleError(f"missing {label}: {directory / filename}") from exc
    if not resolved_directory.is_dir():
        _fail(f"{label} parent is not a directory: {resolved_directory}")
    if not path.is_file() or path.parent != resolved_directory or path.name != filename:
        _fail(f"{label} must be the fixed file {resolved_directory / filename}")
    return path


def _recorded_path(
    value: Any,
    *,
    base: Path,
    expected_name: str,
    label: str,
) -> Path:
    text = _required_text(value, label)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        path = candidate.resolve(strict=True)
    except OSError as exc:
        raise EodSourceBundleError(f"missing {label}: {candidate}") from exc
    if not path.is_file() or path.name != expected_name:
        _fail(f"{label} must identify {expected_name}")
    return path


def _canonical_digest(artifact_sha256: Mapping[str, str]) -> str:
    identity = {
        "contract": EOD_SOURCE_BUNDLE_CONTRACT,
        "schema_version": EOD_SOURCE_BUNDLE_SCHEMA_VERSION,
        "artifact_sha256": {
            label: artifact_sha256[label] for label in sorted(artifact_sha256)
        },
    }
    encoded = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_eod_source_bundle(run_dir: Path) -> EodSourceBundle:
    """Validate and hash every fixed file consumed by EOD shadow loading."""

    try:
        run_dir = Path(run_dir).expanduser().resolve(strict=True)
    except OSError as exc:
        raise EodSourceBundleError(f"run directory does not exist: {run_dir}") from exc
    if not run_dir.is_dir():
        _fail(f"run directory is not a directory: {run_dir}")

    run_paths = {
        label: _fixed_file(run_dir, filename, label)
        for label, filename in _RUN_FILES.items()
    }
    try:
        staging_dir = (run_dir / "staging").resolve(strict=True)
    except OSError as exc:
        raise EodSourceBundleError(f"missing staging directory: {run_dir / 'staging'}") from exc
    if (
        not staging_dir.is_dir()
        or staging_dir.parent != run_dir
        or staging_dir.name != "staging"
    ):
        _fail("staging must be the fixed directory directly beneath the run directory")
    staging_paths = {
        label: _fixed_file(staging_dir, filename, label)
        for label, filename in _STAGING_FILES.items()
    }

    run_manifest, run_manifest_sha = _stable_json(
        run_paths["run_manifest"], "run manifest"
    )
    run_status, run_status_sha = _stable_json(run_paths["run_status"], "run status")
    publish_manifest, publish_manifest_sha = _stable_json(
        staging_paths["publish_manifest"], "publish manifest"
    )

    if run_manifest.get("status") != "PASS" or run_manifest.get("final_health") != "PASS":
        _fail("run manifest must have status=PASS and final_health=PASS")
    if run_status.get("status") != "PASS":
        _fail("run status must have status=PASS")
    target_date = _required_text(run_manifest.get("target_date"), "run target_date")
    if _required_text(run_status.get("target_date"), "run-status target_date") != target_date:
        _fail("run manifest and run status target dates disagree")
    if _required_text(
        publish_manifest.get("target_date"), "publish-manifest target_date"
    ) != target_date:
        _fail("run and publish manifest target dates disagree")

    project_root_text = _required_text(run_manifest.get("project_root"), "project_root")
    project_root = Path(project_root_text).expanduser()
    if not project_root.is_absolute():
        _fail("run manifest project_root must be absolute")
    try:
        project_root = project_root.resolve(strict=True)
    except OSError as exc:
        raise EodSourceBundleError(
            f"run manifest project_root does not exist: {project_root}"
        ) from exc
    if not project_root.is_dir():
        _fail("run manifest project_root must be a directory")

    recorded_publish = _recorded_path(
        run_manifest.get("publish_manifest"),
        base=project_root,
        expected_name=_STAGING_FILES["publish_manifest"],
        label="run publish_manifest",
    )
    if recorded_publish != staging_paths["publish_manifest"]:
        _fail("run manifest does not name the fixed staged publish manifest")

    sofr_manifest_path = _recorded_path(
        run_manifest.get("sofr_manifest"),
        base=project_root,
        expected_name=SOFR_UPDATER_MANIFEST_NAME,
        label="run sofr_manifest",
    )
    if SOFR_UPDATER_TIMESTAMP.fullmatch(sofr_manifest_path.parent.name) is None:
        _fail("SOFR updater manifest must be inside a timestamped updater directory")
    sofr_manifest, sofr_manifest_sha = _stable_json(
        sofr_manifest_path, "SOFR updater manifest"
    )
    audit_files = sofr_manifest.get("audit_files")
    if not isinstance(audit_files, dict):
        _fail("SOFR updater manifest audit_files must be an object")
    sofr_snapshot_path = _recorded_path(
        audit_files.get("refreshed_snapshot"),
        base=sofr_manifest_path.parent,
        expected_name=SOFR_REFRESHED_SNAPSHOT_NAME,
        label="SOFR refreshed snapshot",
    )
    if sofr_snapshot_path.parent != sofr_manifest_path.parent:
        _fail("SOFR refreshed snapshot must be beside its updater manifest")

    paths = {
        **run_paths,
        **staging_paths,
        "sofr_update_manifest": sofr_manifest_path,
        "sofr_refreshed_snapshot": sofr_snapshot_path,
    }
    artifact_sha256 = {
        "run_manifest": run_manifest_sha,
        "run_status": run_status_sha,
        "publish_manifest": publish_manifest_sha,
        "sofr_update_manifest": sofr_manifest_sha,
    }
    for label, path in paths.items():
        if label not in artifact_sha256:
            artifact_sha256[label] = _stable_sha256(path, label.replace("_", " "))

    if tuple(sorted(artifact_sha256)) != EOD_SOURCE_BUNDLE_LABELS:
        _fail("EOD source bundle artifact labels are incomplete")

    # Close the resolution-to-return window for every file, including the JSON
    # documents used to discover the exact SOFR evidence paths.
    for label, path in paths.items():
        observed = _stable_sha256(path, label.replace("_", " "))
        if observed != artifact_sha256[label]:
            _fail(f"{label.replace('_', ' ')} changed while the bundle was assembled")

    canonical_mapping = {
        label: artifact_sha256[label] for label in EOD_SOURCE_BUNDLE_LABELS
    }
    return EodSourceBundle(
        schema_version=EOD_SOURCE_BUNDLE_SCHEMA_VERSION,
        content_sha256=_canonical_digest(canonical_mapping),
        artifact_sha256=MappingProxyType(canonical_mapping),
    )


__all__ = [
    "EOD_SOURCE_BUNDLE_CONTRACT",
    "EOD_SOURCE_BUNDLE_LABELS",
    "EOD_SOURCE_BUNDLE_SCHEMA_VERSION",
    "EodSourceBundle",
    "EodSourceBundleError",
    "load_eod_source_bundle",
]
