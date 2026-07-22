"""Value objects shared by reference-history normalization and persistence."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

from vrp.storage.reference_data import (
    DailyMarketFeature,
    DailyMarketFeatureDefinition,
    ReferenceRateObservation,
)

SHA256 = re.compile(r"^[0-9a-f]{64}$")
HistoryRow = ReferenceRateObservation | DailyMarketFeature


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _digest(value: str, name: str) -> str:
    if not isinstance(value, str) or SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value


def _frozen_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("metadata must be a mapping")
    return MappingProxyType(dict(value))


@dataclass(frozen=True)
class DataAsset:
    data_asset_id: UUID
    dataset_name: str
    asset_class: str
    asset_format: str
    storage_uri: str
    content_sha256: str
    captured_at: datetime
    schema_version: str | None = None
    source_system: str | None = None
    trade_date_start: date | None = None
    trade_date_end: date | None = None
    row_count: int | None = None
    byte_size: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.data_asset_id, UUID):
            raise ValueError("data_asset_id must be a UUID")
        object.__setattr__(self, "dataset_name", _required_text(self.dataset_name, "dataset_name"))
        if self.asset_class not in {"RAW", "STANDARDIZED", "DERIVED", "MANIFEST", "REPORT"}:
            raise ValueError("asset_class is not supported by the operational schema")
        if self.asset_format not in {
            "PARQUET",
            "CSV",
            "JSON",
            "TEXT",
            "BINARY",
            "API_RESPONSE",
            "OTHER",
        }:
            raise ValueError("asset_format is not supported by the operational schema")
        object.__setattr__(self, "storage_uri", _required_text(self.storage_uri, "storage_uri"))
        object.__setattr__(self, "content_sha256", _digest(self.content_sha256, "content_sha256"))
        _aware(self.captured_at, "captured_at")
        if self.trade_date_start and self.trade_date_end and self.trade_date_end < self.trade_date_start:
            raise ValueError("trade_date_end cannot precede trade_date_start")
        if self.row_count is not None and (type(self.row_count) is not int or self.row_count < 0):
            raise ValueError("row_count must be a non-negative integer or None")
        if self.byte_size is not None and (type(self.byte_size) is not int or self.byte_size < 0):
            raise ValueError("byte_size must be a non-negative integer or None")
        object.__setattr__(self, "metadata", _frozen_mapping(self.metadata))


@dataclass(frozen=True)
class FileArtifact:
    logical_name: str
    path: Path
    asset: DataAsset
    usage_role: str
    lineage: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "logical_name", _required_text(self.logical_name, "logical_name"))
        if not isinstance(self.path, Path):
            raise ValueError("path must be a pathlib.Path")
        if not isinstance(self.asset, DataAsset):
            raise ValueError("asset must be a DataAsset")
        if self.usage_role not in {"INPUT", "OUTPUT", "INTERMEDIATE", "MANIFEST", "QA_EVIDENCE"}:
            raise ValueError("usage_role is not supported by the operational schema")
        object.__setattr__(self, "lineage", _frozen_mapping(self.lineage))


@dataclass(frozen=True)
class NormalizedHistory:
    dataset_key: str
    dataset_kind: str
    schema_version: str
    source_system: str
    vintage_kind: str
    rows: tuple[HistoryRow, ...]
    canonical_bytes: bytes
    content_sha256: str
    definition: DailyMarketFeatureDefinition | None = None
    source_row_counts: Mapping[str, int] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_key", _required_text(self.dataset_key, "dataset_key"))
        if self.dataset_kind not in {"REFERENCE_RATE", "DAILY_MARKET_FEATURES"}:
            raise ValueError("dataset_kind is invalid")
        object.__setattr__(self, "schema_version", _required_text(self.schema_version, "schema_version"))
        object.__setattr__(self, "source_system", _required_text(self.source_system, "source_system"))
        if self.vintage_kind not in {"POINT_IN_TIME", "LATEST_REVISED", "UNKNOWN"}:
            raise ValueError("vintage_kind is invalid")
        if not self.rows:
            raise ValueError("normalized history cannot be empty")
        if not isinstance(self.rows, tuple):
            raise ValueError("rows must be an immutable tuple")
        if not isinstance(self.canonical_bytes, bytes):
            raise ValueError("canonical_bytes must be bytes")
        object.__setattr__(self, "content_sha256", _digest(self.content_sha256, "content_sha256"))
        from hashlib import sha256

        if sha256(self.canonical_bytes).hexdigest() != self.content_sha256:
            raise ValueError("content_sha256 must hash canonical_bytes")
        if self.dataset_kind == "REFERENCE_RATE":
            if self.definition is not None:
                raise ValueError("reference-rate history cannot have a feature definition")
            if not all(isinstance(row, ReferenceRateObservation) for row in self.rows):
                raise ValueError("reference-rate history contains an invalid row type")
            row_dates = tuple(row.observation_date for row in self.rows)
        else:
            if not isinstance(self.definition, DailyMarketFeatureDefinition):
                raise ValueError("daily-market history requires a feature definition")
            if not all(isinstance(row, DailyMarketFeature) for row in self.rows):
                raise ValueError("daily-market history contains an invalid row type")
            row_dates = tuple(row.trade_date for row in self.rows)
        if row_dates != tuple(sorted(row_dates)) or len(row_dates) != len(set(row_dates)):
            raise ValueError("normalized rows must be sorted and unique by date")
        for name, count in self.source_row_counts.items():
            _required_text(name, "source row-count key")
            if type(count) is not int or count < 0:
                raise ValueError("source row counts must be non-negative integers")
        object.__setattr__(self, "source_row_counts", _frozen_mapping(self.source_row_counts))
        object.__setattr__(self, "metadata", _frozen_mapping(self.metadata))

    @property
    def start_date(self) -> date:
        first = self.rows[0]
        return first.observation_date if isinstance(first, ReferenceRateObservation) else first.trade_date

    @property
    def end_date(self) -> date:
        last = self.rows[-1]
        return last.observation_date if isinstance(last, ReferenceRateObservation) else last.trade_date


@dataclass(frozen=True)
class FrozenInput:
    """One exact-byte source snapshot created before normalization."""

    logical_name: str
    original_path: Path
    path: Path
    content_sha256: str
    captured_at: datetime
    source_modified_at: datetime
    byte_size: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "logical_name", _required_text(self.logical_name, "logical_name"))
        if not isinstance(self.original_path, Path) or not isinstance(self.path, Path):
            raise ValueError("frozen input paths must be pathlib.Path values")
        object.__setattr__(self, "content_sha256", _digest(self.content_sha256, "content_sha256"))
        _aware(self.captured_at, "captured_at")
        _aware(self.source_modified_at, "source_modified_at")
        if type(self.byte_size) is not int or self.byte_size < 0:
            raise ValueError("byte_size must be a non-negative integer")


@dataclass(frozen=True)
class FrozenInputBatch:
    """A logically simultaneous set of exact-byte inputs."""

    retrieved_at: datetime
    inputs: Mapping[str, FrozenInput]

    def __post_init__(self) -> None:
        _aware(self.retrieved_at, "retrieved_at")
        if not isinstance(self.inputs, Mapping) or not self.inputs:
            raise ValueError("inputs must be a non-empty mapping")
        copied = dict(self.inputs)
        if set(copied) != {item.logical_name for item in copied.values()}:
            raise ValueError("frozen input keys must match their logical names")
        object.__setattr__(self, "inputs", MappingProxyType(copied))


@dataclass(frozen=True)
class PreparedHistory:
    normalized: NormalizedHistory
    retrieved_at: datetime
    source_artifacts: tuple[FileArtifact, ...]
    normalized_artifact: FileArtifact

    def __post_init__(self) -> None:
        if not isinstance(self.normalized, NormalizedHistory):
            raise ValueError("normalized must be a NormalizedHistory")
        _aware(self.retrieved_at, "retrieved_at")
        if not isinstance(self.source_artifacts, tuple) or not self.source_artifacts:
            raise ValueError("source_artifacts must be a non-empty tuple")
        if not all(isinstance(item, FileArtifact) for item in self.source_artifacts):
            raise ValueError("source_artifacts contains an invalid item")
        if not isinstance(self.normalized_artifact, FileArtifact):
            raise ValueError("normalized_artifact must be a FileArtifact")


@dataclass(frozen=True)
class CurrentRow:
    natural_key: date
    record_id: UUID
    row_sha256: str

    def __post_init__(self) -> None:
        if type(self.natural_key) is not date:
            raise ValueError("natural_key must be a date")
        if not isinstance(self.record_id, UUID):
            raise ValueError("record_id must be a UUID")
        object.__setattr__(self, "row_sha256", _digest(self.row_sha256, "row_sha256"))


@dataclass(frozen=True)
class RowChange:
    natural_key: date
    row: HistoryRow
    supersedes_record_id: UUID | None

    @property
    def is_correction(self) -> bool:
        return self.supersedes_record_id is not None


@dataclass(frozen=True)
class ChangePlan:
    changes: tuple[RowChange, ...]
    unchanged_count: int
    current_count: int

    @property
    def new_count(self) -> int:
        return sum(not change.is_correction for change in self.changes)

    @property
    def correction_count(self) -> int:
        return sum(change.is_correction for change in self.changes)

    @property
    def persisted_count(self) -> int:
        return len(self.changes)


@dataclass(frozen=True)
class LoadResult:
    pipeline_run_id: UUID
    reference_data_release_id: UUID
    dataset_key: str
    content_sha256: str
    source_row_count: int
    persisted_row_count: int
    new_count: int
    correction_count: int
    unchanged_count: int
    no_op: bool
