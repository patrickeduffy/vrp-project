"""Content-addressed snapshots for mutable canonical source files."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from urllib.request import url2pathname

from .canonical import canonical_json_bytes, sha256_bytes, sha256_file
from .ids import asset_id
from .models import (
    DataAsset,
    FileArtifact,
    FrozenInput,
    FrozenInputBatch,
    NormalizedHistory,
    PreparedHistory,
)

SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.=-]*$")
RESERVED_LOGICAL_NAMES = frozenset(
    {"normalized_history", "reference_history_qa_manifest"}
)


def _safe_component(value: str, label: str) -> str:
    if not isinstance(value, str) or SAFE_COMPONENT.fullmatch(value) is None:
        raise ValueError(f"{label} must contain only letters, digits, dot, dash, or underscore")
    if value in {".", ".."}:
        raise ValueError(f"{label} cannot be a relative-path marker")
    return value


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _snapshot_file(source: Path, destination: Path, expected_sha256: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if not destination.is_file() or destination.is_symlink():
            raise RuntimeError(f"content-addressed snapshot is not a regular file: {destination}")
        if sha256_file(destination) != expected_sha256:
            raise RuntimeError(f"content-addressed snapshot has unexpected bytes: {destination}")
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        shutil.copyfile(source, temporary)
        if sha256_file(temporary) != expected_sha256:
            raise RuntimeError(f"source changed while it was being snapshotted: {source}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _format_for(path: Path) -> str:
    return {
        ".csv": "CSV",
        ".json": "JSON",
        ".parquet": "PARQUET",
        ".pq": "PARQUET",
        ".txt": "TEXT",
    }.get(path.suffix.lower(), "OTHER")


class ContentAddressedArtifactStore:
    """Freeze exact inputs before normalization and verify every published byte."""

    def __init__(self, root: Path):
        candidate = Path(root).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        if candidate.is_symlink():
            raise ValueError("artifact-store root cannot be a symbolic link")
        self.root = candidate.resolve()

    def _path(self, *components: str) -> Path:
        safe = tuple(_safe_component(item, "artifact path component") for item in components)
        destination = self.root.joinpath(*safe)
        resolved = destination.resolve(strict=False)
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("artifact path escapes the configured root") from exc
        current = self.root
        for component in safe[:-1]:
            current = current / component
            if current.exists() and current.is_symlink():
                raise ValueError(f"artifact path traverses a symbolic link: {current}")
        return destination

    def freeze_inputs(self, source_paths: Mapping[str, Path]) -> FrozenInputBatch:
        """Snapshot a source set, returning only immutable paths for readers."""

        if not isinstance(source_paths, Mapping) or not source_paths:
            raise ValueError("source_paths must be a non-empty mapping")
        if RESERVED_LOGICAL_NAMES.intersection(source_paths):
            raise ValueError("source logical names collide with generated artifacts")
        retrieved_at = datetime.now(timezone.utc)
        frozen: dict[str, FrozenInput] = {}
        for logical_name in sorted(source_paths):
            _safe_component(logical_name, "logical_name")
            source_candidate = Path(source_paths[logical_name]).expanduser()
            if source_candidate.is_symlink():
                raise FileNotFoundError(
                    f"reference-history input cannot be a symbolic link: {source_candidate}"
                )
            source = source_candidate.resolve()
            if not source.is_file() or source.is_symlink():
                raise FileNotFoundError(f"reference-history input is not a regular file: {source}")
            before = source.stat()
            digest = sha256_file(source)
            destination = self._path(
                "inputs",
                logical_name,
                f"sha256={digest}",
                _safe_component(source.name, "source filename"),
            )
            _snapshot_file(source, destination, digest)
            after = source.stat()
            if (
                before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or sha256_file(source) != digest
            ):
                raise RuntimeError(f"source changed while it was being frozen: {source}")
            frozen[logical_name] = FrozenInput(
                logical_name=logical_name,
                original_path=source,
                path=destination,
                content_sha256=digest,
                captured_at=datetime.fromtimestamp(
                    destination.stat().st_mtime, tz=timezone.utc
                ),
                source_modified_at=datetime.fromtimestamp(before.st_mtime, tz=timezone.utc),
                byte_size=destination.stat().st_size,
            )
        batch = FrozenInputBatch(retrieved_at=retrieved_at, inputs=frozen)
        self.verify_frozen_inputs(batch)
        return batch

    def verify_frozen_inputs(self, batch: FrozenInputBatch) -> None:
        for item in batch.inputs.values():
            self._verify_path(
                item.path,
                expected_sha256=item.content_sha256,
                expected_size=item.byte_size,
            )

    def _verify_path(self, path: Path, *, expected_sha256: str, expected_size: int) -> None:
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"artifact is missing or is not a regular file: {path}")
        resolved = path.resolve()
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise RuntimeError(f"artifact is outside the configured store: {path}") from exc
        if resolved.stat().st_size != expected_size:
            raise RuntimeError(f"artifact size does not match its immutable record: {path}")
        if sha256_file(resolved) != expected_sha256:
            raise RuntimeError(f"artifact digest does not match its immutable record: {path}")

    def _input_artifact(
        self,
        frozen: FrozenInput,
        *,
        history: NormalizedHistory,
        row_count: int,
    ) -> FileArtifact:
        dataset_name = f"{history.dataset_key}_{frozen.logical_name.upper()}"
        storage_uri = frozen.path.resolve().as_uri()
        asset = DataAsset(
            data_asset_id=asset_id(dataset_name, storage_uri, frozen.content_sha256),
            dataset_name=dataset_name,
            asset_class=("STANDARDIZED" if frozen.path.suffix.lower() == ".csv" else "DERIVED"),
            asset_format=_format_for(frozen.path),
            storage_uri=storage_uri,
            content_sha256=frozen.content_sha256,
            captured_at=frozen.captured_at,
            source_system=history.source_system,
            trade_date_start=history.start_date,
            trade_date_end=history.end_date,
            row_count=row_count,
            byte_size=frozen.byte_size,
            metadata={"snapshot_policy": "exact-byte-before-normalization"},
        )
        return FileArtifact(
            logical_name=frozen.logical_name,
            path=frozen.path,
            asset=asset,
            usage_role="INPUT",
            lineage={
                "source_modified_at": frozen.source_modified_at.isoformat(),
                "source_original_path": str(frozen.original_path),
            },
        )

    def write_normalized(self, history: NormalizedHistory) -> FileArtifact:
        destination = self._path(
            "normalized",
            history.dataset_key.lower(),
            f"sha256={history.content_sha256}",
            "history.json",
        )
        if destination.exists():
            if sha256_file(destination) != history.content_sha256:
                raise RuntimeError(f"normalized artifact has unexpected bytes: {destination}")
        else:
            _atomic_write(destination, history.canonical_bytes)
        digest = sha256_file(destination)
        if digest != history.content_sha256 or digest != sha256_bytes(history.canonical_bytes):
            raise RuntimeError("normalized artifact digest does not match normalized rows")
        asset = DataAsset(
            data_asset_id=asset_id(
                f"{history.dataset_key}_NORMALIZED", destination.resolve().as_uri(), digest
            ),
            dataset_name=f"{history.dataset_key}_NORMALIZED",
            asset_class="DERIVED",
            asset_format="JSON",
            storage_uri=destination.resolve().as_uri(),
            content_sha256=digest,
            captured_at=datetime.fromtimestamp(destination.stat().st_mtime, tz=timezone.utc),
            schema_version=history.schema_version,
            source_system=history.source_system,
            trade_date_start=history.start_date,
            trade_date_end=history.end_date,
            row_count=len(history.rows),
            byte_size=destination.stat().st_size,
            metadata={"normalized_business_rows": True},
        )
        return FileArtifact(
            logical_name="normalized_history",
            path=destination,
            asset=asset,
            usage_role="OUTPUT",
        )

    def write_qa_manifest(
        self, history: NormalizedHistory, payload: Mapping[str, Any]
    ) -> FileArtifact:
        content = canonical_json_bytes(payload)
        digest = sha256_bytes(content)
        destination = self._path(
            "qa",
            history.dataset_key.lower(),
            f"sha256={digest}",
            "manifest.json",
        )
        if destination.exists():
            if sha256_file(destination) != digest:
                raise RuntimeError(f"QA manifest has unexpected bytes: {destination}")
        else:
            _atomic_write(destination, content)
        if sha256_file(destination) != digest:
            raise RuntimeError("QA manifest digest changed after publication")
        asset = DataAsset(
            data_asset_id=asset_id(
                f"{history.dataset_key}_LOAD_QA", destination.resolve().as_uri(), digest
            ),
            dataset_name=f"{history.dataset_key}_LOAD_QA",
            asset_class="MANIFEST",
            asset_format="JSON",
            storage_uri=destination.resolve().as_uri(),
            content_sha256=digest,
            captured_at=datetime.fromtimestamp(destination.stat().st_mtime, tz=timezone.utc),
            schema_version="reference-history-qa-v1",
            source_system="VRP_REFERENCE_HISTORY_LOADER",
            trade_date_start=history.start_date,
            trade_date_end=history.end_date,
            row_count=1,
            byte_size=destination.stat().st_size,
            metadata={"hard_gates_passed": True},
        )
        return FileArtifact(
            logical_name="reference_history_qa_manifest",
            path=destination,
            asset=asset,
            usage_role="QA_EVIDENCE",
        )

    def verify_artifact(self, artifact: FileArtifact) -> None:
        resolved = artifact.path.resolve()
        if resolved.as_uri() != artifact.asset.storage_uri:
            raise RuntimeError("artifact URI does not match its immutable asset record")
        self._verify_path(
            resolved,
            expected_sha256=artifact.asset.content_sha256,
            expected_size=int(artifact.asset.byte_size or 0),
        )

    def verify_storage_asset(
        self,
        *,
        storage_uri: str,
        content_sha256: str,
        byte_size: int,
    ) -> None:
        """Verify an already registered local asset, including prior-run QA evidence."""

        parsed = urlparse(storage_uri)
        if parsed.scheme != "file":
            raise RuntimeError(
                f"unsupported persisted artifact URI scheme: {parsed.scheme or '<none>'}"
            )
        path = Path(url2pathname(parsed.path))
        if parsed.netloc:
            path = Path(f"//{parsed.netloc}{url2pathname(parsed.path)}")
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"persisted artifact is missing or not a regular file: {path}")
        if path.stat().st_size != byte_size:
            raise RuntimeError(f"persisted artifact size is inconsistent: {path}")
        if sha256_file(path) != content_sha256:
            raise RuntimeError(f"persisted artifact digest is inconsistent: {path}")

    def verify_prepared(self, prepared: PreparedHistory) -> None:
        for artifact in (*prepared.source_artifacts, prepared.normalized_artifact):
            self.verify_artifact(artifact)
        if prepared.normalized_artifact.path.read_bytes() != prepared.normalized.canonical_bytes:
            raise RuntimeError("normalized artifact bytes do not match the prepared history")


def prepare_history(
    history: NormalizedHistory,
    *,
    frozen_inputs: FrozenInputBatch,
    store: ContentAddressedArtifactStore,
) -> PreparedHistory:
    """Build database-ready assets from inputs that were frozen before reading."""

    if set(frozen_inputs.inputs) != set(history.source_row_counts):
        raise ValueError("frozen input keys must exactly match normalized source row-count keys")
    store.verify_frozen_inputs(frozen_inputs)
    artifacts = tuple(
        store._input_artifact(
            frozen_inputs.inputs[logical_name],
            history=history,
            row_count=int(history.source_row_counts[logical_name]),
        )
        for logical_name in sorted(frozen_inputs.inputs)
    )
    prepared = PreparedHistory(
        normalized=history,
        retrieved_at=frozen_inputs.retrieved_at,
        source_artifacts=artifacts,
        normalized_artifact=store.write_normalized(history),
    )
    store.verify_prepared(prepared)
    return prepared
