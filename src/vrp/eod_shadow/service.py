"""Atomic orchestration for recording a completed EOD file run in PostgreSQL.

This module is intentionally a recorder, not a calculator or publisher.  The
caller must first construct a validated :class:`EodSnapshot` from immutable
staged outputs.  Successful writes and their reconciliation evidence share one
outer transaction owned here.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from vrp.eod_shadow.models import EodSnapshot
from vrp.storage.eod_postgres import (
    EodDataAsset,
    ForecastVarianceRow,
    ImpliedVarianceRow,
    MarketSnapshotRow,
    PostgresEodRepository,
    QaResultRow,
    SelectedSignalRow,
    SignalEvaluationRow,
    SignalFeatureRow,
)

ORCHESTRATOR_VERSION = "eod-postgres-shadow-v1"
STAGE_NAME = "RECORD_EOD_SHADOW"
EXPECTED_TENORS = (9, 12, 15, 18, 21, 24, 27, 30, 33)
PROJECTION_ABSOLUTE_TOLERANCE = 1e-12
PROJECTION_RELATIVE_TOLERANCE = 1e-10

_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://github.com/patrickeduffy/vrp-project/eod-postgres-shadow/v1",
)


@dataclass(frozen=True)
class EodShadowLoadResult:
    pipeline_run_id: UUID
    market_snapshot_id: UUID
    selected_signal_id: UUID
    valuation_date: date
    snapshot_at: datetime
    content_sha256: str
    database_projection_sha256: str
    database_readback_sha256: str
    implied_variance_count: int
    forecast_variance_count: int
    signal_feature_count: int
    signal_evaluation_count: int
    decision: str
    no_op: bool

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "content_sha256": self.content_sha256,
            "database_projection_sha256": self.database_projection_sha256,
            "database_readback_sha256": self.database_readback_sha256,
            "decision": self.decision,
            "forecast_variance_count": self.forecast_variance_count,
            "implied_variance_count": self.implied_variance_count,
            "market_snapshot_id": str(self.market_snapshot_id),
            "no_op": self.no_op,
            "pipeline_run_id": str(self.pipeline_run_id),
            "selected_signal_id": str(self.selected_signal_id),
            "signal_evaluation_count": self.signal_evaluation_count,
            "signal_feature_count": self.signal_feature_count,
            "snapshot_at": _utc_text(self.snapshot_at),
            "status": "COMPLETED",
            "valuation_date": self.valuation_date.isoformat(),
        }


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def validate_code_version(value: str) -> str:
    """Return an accepted immutable production code identity."""

    candidate = _required_text(value, "code_version")
    if re.fullmatch(r"[0-9a-f]{40}", candidate) is None:
        raise ValueError("code_version must be a full 40-character lowercase Git SHA")
    return candidate


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("EOD timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def _canonical(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _canonical(asdict(value))
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _utc_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical decimals must be finite")
        return "0" if value == 0 else format(value.normalize(), "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical floats must be finite")
        return "0" if value == 0 else format(value, ".17g")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    raise ValueError(f"unsupported canonical value: {type(value).__name__}")


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        _canonical(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def database_readback_fingerprint(projection: Mapping[str, Any]) -> str:
    """Hash the PostgreSQL-decoded projection exactly as later reads see it."""

    if not isinstance(projection, Mapping):
        raise ValueError("database projection must be a mapping")
    return _digest({"database_projection": _projection_value(projection)})


def _stable_id(kind: str, *parts: object) -> UUID:
    kind = _required_text(kind, "identity kind")
    rendered = [f"kind:{kind}"]
    for part in parts:
        if part is None:
            rendered.append("none:")
            continue
        text = str(part).strip()
        if not text:
            raise ValueError("stable identity parts cannot be empty")
        if "\x1f" in text:
            raise ValueError("stable identity parts cannot contain a unit separator")
        rendered.append(f"{type(part).__module__}.{type(part).__qualname__}:{text}")
    return uuid5(_NAMESPACE, "\x1f".join(rendered))


def _numeric_equal(left: Any, right: Any, *, atol: float, rtol: float) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is bool and type(right) is bool and left == right
    numeric = (int, float, Decimal)
    if isinstance(left, numeric) and isinstance(right, numeric):
        return math.isclose(float(left), float(right), abs_tol=atol, rel_tol=rtol)
    return False


def projection_mismatches(
    expected: Any,
    actual: Any,
    *,
    location: str = "projection",
    absolute_tolerance: float = PROJECTION_ABSOLUTE_TOLERANCE,
    relative_tolerance: float = PROJECTION_RELATIVE_TOLERANCE,
) -> list[str]:
    """Compare a database projection recursively with exact structural checks."""

    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return [f"{location}: expected an object, found {type(actual).__name__}"]
        expected_keys = set(expected)
        actual_keys = set(actual)
        mismatches: list[str] = []
        if expected_keys != actual_keys:
            missing = sorted(expected_keys - actual_keys)
            extra = sorted(actual_keys - expected_keys)
            mismatches.append(f"{location}: key mismatch missing={missing} extra={extra}")
        for key in sorted(expected_keys & actual_keys):
            mismatches.extend(
                projection_mismatches(
                    expected[key],
                    actual[key],
                    location=f"{location}.{key}",
                    absolute_tolerance=absolute_tolerance,
                    relative_tolerance=relative_tolerance,
                )
            )
        return mismatches
    if isinstance(expected, (tuple, list)):
        if not isinstance(actual, (tuple, list)):
            return [f"{location}: expected a list, found {type(actual).__name__}"]
        if len(expected) != len(actual):
            return [f"{location}: expected {len(expected)} rows, found {len(actual)}"]
        mismatches: list[str] = []
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
            mismatches.extend(
                projection_mismatches(
                    expected_item,
                    actual_item,
                    location=f"{location}[{index}]",
                    absolute_tolerance=absolute_tolerance,
                    relative_tolerance=relative_tolerance,
                )
            )
        return mismatches
    if isinstance(expected, bool) or isinstance(actual, bool):
        if type(expected) is bool and type(actual) is bool and expected == actual:
            return []
        return [f"{location}: expected {expected!r}, found {actual!r}"]
    if expected == actual:
        return []
    if _numeric_equal(
        expected,
        actual,
        atol=absolute_tolerance,
        rtol=relative_tolerance,
    ):
        return []
    return [f"{location}: expected {expected!r}, found {actual!r}"]


def _assert_projection(expected: Any, actual: Any) -> None:
    mismatches = projection_mismatches(expected, actual)
    if mismatches:
        preview = "; ".join(mismatches[:10])
        remainder = len(mismatches) - min(10, len(mismatches))
        if remainder:
            preview += f"; and {remainder} additional mismatches"
        raise RuntimeError(f"PostgreSQL shadow projection did not reconcile: {preview}")


def _require_idle_connection(connection: Any) -> None:
    if getattr(connection, "autocommit", False):
        raise ValueError("EOD shadow loading requires a non-autocommit connection")
    info = getattr(connection, "info", None)
    status = getattr(info, "transaction_status", None)
    if status is not None and getattr(status, "name", None) != "IDLE":
        raise ValueError("EOD shadow loading requires an idle dedicated connection")


def _assert_file_integrity(
    path: Path,
    *,
    expected_sha256: str,
    label: str,
    expected_byte_size: int | None = None,
) -> None:
    """Fail closed unless one evidence file still has its recorded bytes."""

    if (
        not isinstance(expected_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
    ):
        raise RuntimeError(f"{label} has invalid recorded SHA-256 evidence")
    try:
        stat = path.stat()
        digest = hashlib.sha256()
        observed_byte_size = 0
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
                observed_byte_size += len(chunk)
    except OSError as exc:
        raise RuntimeError(f"{label} is no longer readable: {path}") from exc
    if not path.is_file():
        raise RuntimeError(f"{label} is no longer a regular file: {path}")
    if stat.st_size != observed_byte_size:
        raise RuntimeError(f"{label} changed while its integrity was checked")
    if expected_byte_size is not None and observed_byte_size != expected_byte_size:
        raise RuntimeError(
            f"{label} byte size changed: expected {expected_byte_size}, "
            f"found {observed_byte_size}"
        )
    observed_sha256 = digest.hexdigest()
    if observed_sha256 != expected_sha256:
        raise RuntimeError(
            f"{label} SHA-256 changed: expected {expected_sha256}, "
            f"found {observed_sha256}"
        )


def _assert_snapshot_files_unchanged(snapshot: EodSnapshot) -> None:
    """Revalidate every file-backed input used by a shadow load."""

    for artifact in snapshot.artifacts:
        _assert_file_integrity(
            artifact.path,
            expected_sha256=artifact.sha256,
            expected_byte_size=artifact.byte_size,
            label=f"snapshot artifact {artifact.logical_name}",
        )

    _assert_file_integrity(
        snapshot.model_identity.path,
        expected_sha256=snapshot.model_identity.sha256,
        label="model-lock document",
    )
    _assert_file_integrity(
        snapshot.golden_evidence.fixture_path,
        expected_sha256=snapshot.golden_evidence.fixture_sha256,
        label="golden EOD fixture",
    )

    configuration = snapshot.configuration_identity
    runtime_sha256 = configuration.payload.get("runtime_configuration_sha256")
    if runtime_sha256 is not None:
        _assert_file_integrity(
            configuration.path,
            expected_sha256=runtime_sha256,
            label="runtime configuration",
        )

    production_sha256 = configuration.payload.get(
        "production_configuration_sha256"
    )
    if production_sha256 is not None:
        lock_config = snapshot.publish_manifest.get("lock_config")
        if not isinstance(lock_config, str) or not lock_config.strip():
            raise RuntimeError(
                "production configuration digest is present but lock_config is missing"
            )
        _assert_file_integrity(
            Path(lock_config).resolve(),
            expected_sha256=production_sha256,
            label="production configuration",
        )


# The implementation below consumes the concrete snapshot DTOs without
# recalculating them.  Keeping the repository factory injectable makes the
# transaction and no-op evidence paths independently testable.
RepositoryFactory = Callable[[Any], PostgresEodRepository]


# Explicit production definition contract accepted by the initial migration-0002
# load.  This is a formula/seed-state identity, not a file-location fingerprint.
# A new definition requires a reviewed contract and code change here.
SPY_DAILY_DEFINITION_CONTRACT = {
    "definition_key": "SPY_SIGNAL_FEATURES",
    "version_label": "v1-71854988797d",
    "content_sha256": (
        "71854988797daedd685fa8d9a140fdcbd8dddaa8c2d7cd1c59c23e7f97a0371d"
    ),
}


@dataclass(frozen=True)
class _Identifiers:
    pipeline_run_id: UUID
    pipeline_stage_id: UUID
    model_version_id: UUID
    configuration_version_id: UUID
    market_snapshot_id: UUID
    implied: Mapping[int, UUID]
    forecast: Mapping[int, UUID]
    features: Mapping[int, UUID]
    evaluations: Mapping[str, UUID]
    selected_signal_id: UUID
    golden_qa_result_id: UUID
    readback_qa_result_id: UUID


@dataclass(frozen=True)
class _AssetInput:
    logical_name: str
    path: Path
    relative_path: str
    asset_format: str
    content_sha256: str
    byte_size: int
    row_count: int | None
    usage_role: str
    asset_class: str
    identity_input: bool
    metadata: Mapping[str, Any]
    trade_date_start: date | None
    trade_date_end: date | None


def _effective_input(
    snapshot: EodSnapshot,
    *,
    environment: str,
    code_version: str,
    sofr: Any,
    spy: Any,
) -> tuple[str, str]:
    payload = {
        "code_version": code_version,
        "configuration_content_sha256": snapshot.configuration_identity.sha256,
        "environment": environment,
        "model_content_sha256": snapshot.model_identity.sha256,
        "run_kind": "EOD",
        "snapshot_at": _utc_text(snapshot.snapshot_at),
        "snapshot_content_sha256": snapshot.content_sha256,
        "data_cutoff_at": _utc_text(snapshot.data_cutoff_at),
        "valuation_date": snapshot.valuation_date.isoformat(),
        "ordered_artifact_digests": [
            {
                "logical_name": artifact.logical_name,
                "sha256": artifact.sha256,
            }
            for artifact in sorted(snapshot.artifacts, key=lambda item: item.logical_name)
            if artifact.identity_input
        ],
        "spy_daily_definition_content_sha256": SPY_DAILY_DEFINITION_CONTRACT[
            "content_sha256"
        ],
        "reference_pins": _reference_pin_identity(sofr, spy),
    }
    effective_sha256 = _digest(payload)
    return effective_sha256, (
        f"eod-postgres-shadow/v1/{snapshot.valuation_date.isoformat()}/"
        f"{effective_sha256}"
    )


def _identifiers(
    snapshot: EodSnapshot,
    *,
    environment: str,
    idempotency_key: str,
) -> _Identifiers:
    pipeline_run_id = _stable_id("run", environment, idempotency_key)
    market_snapshot_id = _stable_id("market-snapshot", pipeline_run_id)
    return _Identifiers(
        pipeline_run_id=pipeline_run_id,
        pipeline_stage_id=_stable_id("stage", pipeline_run_id, STAGE_NAME),
        model_version_id=_stable_id(
            "model-version",
            snapshot.model_identity.key,
            snapshot.model_identity.sha256,
        ),
        configuration_version_id=_stable_id(
            "configuration-version",
            snapshot.configuration_identity.key,
            snapshot.configuration_identity.sha256,
        ),
        market_snapshot_id=market_snapshot_id,
        implied={
            tenor: _stable_id("implied-variance", market_snapshot_id, tenor)
            for tenor in EXPECTED_TENORS
        },
        forecast={
            tenor: _stable_id("forecast-variance", market_snapshot_id, tenor)
            for tenor in EXPECTED_TENORS
        },
        features={
            tenor: _stable_id("signal-feature", market_snapshot_id, tenor)
            for tenor in EXPECTED_TENORS
        },
        evaluations={
            item.evaluation_key: _stable_id(
                "signal-evaluation", market_snapshot_id, item.evaluation_key
            )
            for item in snapshot.signal_evaluations
        },
        selected_signal_id=_stable_id("selected-signal", market_snapshot_id),
        golden_qa_result_id=_stable_id(
            "qa", pipeline_run_id, "golden_eod_contract", "run"
        ),
        readback_qa_result_id=_stable_id(
            "qa", pipeline_run_id, "postgres_projection_reconciliation", "run"
        ),
    )


def _relative_label(path: Path, run_dir: Path) -> str:
    try:
        return path.resolve().relative_to(run_dir.resolve()).as_posix()
    except ValueError:
        return path.name


def _asset_inputs(snapshot: EodSnapshot) -> tuple[_AssetInput, ...]:
    assets: list[_AssetInput] = []
    for item in snapshot.artifacts:
        role = (
            "MANIFEST"
            if item.logical_name in {"run_manifest", "publish_manifest"}
            else "INPUT"
        )
        assets.append(
            _AssetInput(
                logical_name=item.logical_name,
                path=item.path,
                relative_path=item.relative_path,
                asset_format=item.asset_format,
                content_sha256=item.sha256,
                byte_size=item.byte_size,
                row_count=item.row_count,
                usage_role=role,
                asset_class="MANIFEST" if role == "MANIFEST" else "DERIVED",
                identity_input=item.identity_input,
                metadata=dict(item.metadata),
                trade_date_start=item.trade_date_start,
                trade_date_end=item.trade_date_end,
            )
        )
    fixture = snapshot.golden_evidence.fixture_path
    assets.append(
        _AssetInput(
            logical_name="golden_eod_fixture",
            path=fixture,
            relative_path=_relative_label(fixture, snapshot.run_dir),
            asset_format="JSON",
            content_sha256=snapshot.golden_evidence.fixture_sha256,
            byte_size=fixture.stat().st_size,
            row_count=None,
            usage_role="QA_EVIDENCE",
            asset_class="REPORT",
            identity_input=True,
            metadata={"source": "accepted_golden_eod_contract"},
            trade_date_start=None,
            trade_date_end=None,
        )
    )
    logical_names = [item.logical_name for item in assets]
    if len(logical_names) != len(set(logical_names)):
        raise ValueError("EOD shadow evidence contains duplicate logical artifact names")
    return tuple(sorted(assets, key=lambda item: (item.usage_role, item.logical_name)))


def _file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _parse_source_timestamp(value: Any, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _asset_captured_at(asset: _AssetInput, snapshot: EodSnapshot) -> datetime:
    if asset.logical_name == "golden_eod_fixture":
        try:
            fixture = json.loads(asset.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("golden EOD fixture is not readable JSON") from exc
        if not isinstance(fixture, Mapping):
            raise ValueError("golden EOD fixture must contain an object")
        return _parse_source_timestamp(
            fixture.get("captured_at_utc"), "golden fixture captured_at_utc"
        )
    return _parse_source_timestamp(
        snapshot.run_manifest.get("finished_at"), "run manifest finished_at"
    )


def _asset_record(asset: _AssetInput, snapshot: EodSnapshot) -> EodDataAsset:
    storage_uri = _file_uri(asset.path)
    dataset_name = f"vrp_eod_shadow/{asset.logical_name}"
    captured_at = _asset_captured_at(asset, snapshot)
    return EodDataAsset(
        data_asset_id=_stable_id(
            "asset", dataset_name, storage_uri, asset.content_sha256
        ),
        dataset_name=dataset_name,
        asset_class=asset.asset_class,
        asset_format=asset.asset_format,
        storage_uri=storage_uri,
        content_sha256=asset.content_sha256,
        schema_version="eod-shadow-v1",
        source_system="VRP_HYBRID_V2_EOD",
        captured_at=captured_at,
        trade_date_start=asset.trade_date_start,
        trade_date_end=asset.trade_date_end,
        row_count=asset.row_count,
        byte_size=asset.byte_size,
        metadata={
            **dict(asset.metadata),
            "identity_input": asset.identity_input,
            "logical_name": asset.logical_name,
            "relative_path": asset.relative_path,
        },
    )


def _model_locked_at(snapshot: EodSnapshot) -> datetime:
    value = snapshot.model_identity.payload.get("lock_date")
    try:
        locked_date = date.fromisoformat(str(value))
    except (TypeError, ValueError):
        locked_date = snapshot.valuation_date
    return datetime.combine(locked_date, time.min, tzinfo=timezone.utc)


def _run_contract(
    snapshot: EodSnapshot,
    *,
    effective_input_sha256: str,
    code_version: str,
    sofr: Any,
    spy: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    invocation = {
        "artifact_digests": [
            {
                "logical_name": artifact.logical_name,
                "sha256": artifact.sha256,
            }
            for artifact in sorted(snapshot.artifacts, key=lambda item: item.logical_name)
            if artifact.identity_input
        ],
        "effective_input_sha256": effective_input_sha256,
        "golden_verification_id": snapshot.golden_evidence.verification_id,
        "output_fingerprint": snapshot.output_fingerprint,
        "reference_pins": _reference_pin_identity(sofr, spy),
        "snapshot_content_sha256": snapshot.content_sha256,
        "spy_daily_definition": SPY_DAILY_DEFINITION_CONTRACT,
    }
    metadata = {
        "authoritative": False,
        "code_version": code_version,
        "lock_id": snapshot.lock_id,
        "publishes_signal": False,
        "shadow_recorder": True,
    }
    return invocation, metadata


def _reference_pin_identity(sofr: Any, spy: Any) -> dict[str, Any]:
    return {
        "sofr": {
            "observation_id": str(sofr.observation_id),
            "reference_data_release_id": str(sofr.reference_data_release_id),
            "observation_date": sofr.observation_date.isoformat(),
            "row_sha256": sofr.row_sha256,
            "normalized_content_sha256": sofr.normalized_content_sha256,
        },
        "spy_daily_feature": {
            "daily_market_feature_id": str(spy.daily_market_feature_id),
            "daily_market_feature_definition_id": str(
                spy.daily_market_feature_definition_id
            ),
            "reference_data_release_id": str(spy.reference_data_release_id),
            "trade_date": spy.trade_date.isoformat(),
            "row_sha256": spy.row_sha256,
            "definition_content_sha256": SPY_DAILY_DEFINITION_CONTRACT[
                "content_sha256"
            ],
        },
    }


def _validate_reference_pins(snapshot: EodSnapshot, sofr: Any, spy: Any) -> None:
    evidence = snapshot.sofr_evidence
    if sofr.normalized_content_sha256 != evidence.normalized_content_sha256:
        raise RuntimeError("resolved SOFR release does not match the run's normalized snapshot")
    if sofr.observation_date != evidence.observation_date:
        raise RuntimeError("resolved SOFR observation date does not match the run evidence")
    if sofr.rate_decimal != evidence.rate_decimal:
        raise RuntimeError("resolved SOFR rate does not match the run evidence")
    if sofr.row_sha256 != evidence.row_sha256:
        raise RuntimeError("resolved SOFR row digest does not match the run evidence")
    if sofr.observation_date >= snapshot.valuation_date:
        raise RuntimeError("resolved SOFR observation is not strictly before valuation date")
    if spy.trade_date != snapshot.valuation_date:
        raise RuntimeError("resolved SPY feature is not on the exact valuation date")
    if (spy.calculation_status, spy.quality_status) != ("AVAILABLE", "PASS"):
        raise RuntimeError("resolved SPY feature is not AVAILABLE/PASS")
    if Decimal(str(snapshot.market_snapshot.spy_price)) != spy.spy_close:
        raise RuntimeError("staged SPY close does not exactly match the accepted database row")
    expected_rsi = snapshot.signal_features[0].rsi14
    expected_rv_variance = snapshot.signal_features[0].rv21d_variance
    expected_rv_volatility = snapshot.signal_features[0].rv21d_volatility_pct
    if any(
        not math.isclose(
            value,
            expected,
            abs_tol=PROJECTION_ABSOLUTE_TOLERANCE,
            rel_tol=PROJECTION_RELATIVE_TOLERANCE,
        )
        for value, expected in (
            (spy.rsi14, expected_rsi),
            (spy.rv21d_variance, expected_rv_variance),
            (spy.rv21d_volatility_pct, expected_rv_volatility),
        )
        if value is not None
    ) or any(
        value is None for value in (spy.rsi14, spy.rv21d_variance, spy.rv21d_volatility_pct)
    ):
        raise RuntimeError("staged RSI/RV21D values do not match the accepted SPY feature")
    for feature in snapshot.signal_features:
        if not (
            math.isclose(feature.rsi14, expected_rsi, abs_tol=0.0, rel_tol=0.0)
            and math.isclose(
                feature.rv21d_variance, expected_rv_variance, abs_tol=0.0, rel_tol=0.0
            )
            and math.isclose(
                feature.rv21d_volatility_pct,
                expected_rv_volatility,
                abs_tol=0.0,
                rel_tol=0.0,
            )
        ):
            raise RuntimeError("staged daily features vary across tenors")


def _database_rows(
    snapshot: EodSnapshot,
    ids: _Identifiers,
    sofr: Any,
    spy: Any,
) -> tuple[
    MarketSnapshotRow,
    tuple[ImpliedVarianceRow, ...],
    tuple[ForecastVarianceRow, ...],
    tuple[SignalFeatureRow, ...],
    tuple[SignalEvaluationRow, ...],
    SelectedSignalRow,
]:
    market = MarketSnapshotRow(
        market_snapshot_id=ids.market_snapshot_id,
        pipeline_run_id=ids.pipeline_run_id,
        valuation_date=snapshot.valuation_date,
        snapshot_at=snapshot.snapshot_at,
        source_latest_at=snapshot.data_cutoff_at,
        freshness_status=snapshot.market_snapshot.freshness_status,
        spx_spot=None,
        spy_price=spy.spy_close,
        sofr_rate=sofr.rate_decimal,
        sofr_observation_date=sofr.observation_date,
        sofr_observation_id=sofr.observation_id,
        daily_market_feature_id=spy.daily_market_feature_id,
        details={
            **dict(snapshot.market_snapshot.details),
            "lock_id": snapshot.lock_id,
            "shadow_recorder": ORCHESTRATOR_VERSION,
        },
    )
    implied = tuple(
        ImpliedVarianceRow(
            implied_variance_id=ids.implied[item.tenor_days],
            pipeline_run_id=ids.pipeline_run_id,
            market_snapshot_id=ids.market_snapshot_id,
            tenor_days=item.tenor_days,
            target_expiration=item.target_expiration,
            effective_dte=item.effective_dte,
            annualized_variance=item.annualized_variance,
            annualized_volatility_pct=item.annualized_volatility_pct,
            calculation_status=item.calculation_status,
            quality_status=item.quality_status,
            source_quote_at=None,
            quality_details=item.quality_details,
        )
        for item in snapshot.implied_variance
    )
    forecast = tuple(
        ForecastVarianceRow(
            forecast_variance_id=ids.forecast[item.tenor_days],
            pipeline_run_id=ids.pipeline_run_id,
            market_snapshot_id=ids.market_snapshot_id,
            tenor_days=item.tenor_days,
            forecast_as_of_date=item.forecast_as_of_date,
            predicted_log_variance=item.predicted_log_variance,
            annualized_variance=item.annualized_variance,
            annualized_volatility_pct=item.annualized_volatility_pct,
            calculation_status=item.calculation_status,
            quality_status=item.quality_status,
            quality_details=item.quality_details,
        )
        for item in snapshot.forecast_variance
    )
    features = tuple(
        SignalFeatureRow(
            signal_feature_id=ids.features[item.tenor_days],
            pipeline_run_id=ids.pipeline_run_id,
            market_snapshot_id=ids.market_snapshot_id,
            tenor_days=item.tenor_days,
            tenor_bucket=item.tenor_bucket,
            implied_variance_id=ids.implied[item.tenor_days],
            forecast_variance_id=ids.forecast[item.tenor_days],
            vrp_log=item.vrp_log,
            vrp_3m_prior_mean=item.vrp_3m_prior_mean,
            vrp_3m_prior_sample_std=item.vrp_3m_prior_sample_std,
            vrp_1y_prior_mean=item.vrp_1y_prior_mean,
            vrp_1y_prior_sample_std=item.vrp_1y_prior_sample_std,
            zscore_3m=item.zscore_3m,
            zscore_1y=item.zscore_1y,
            rsi14=spy.rsi14,
            rv21d_variance=spy.rv21d_variance,
            rv21d_volatility_pct=spy.rv21d_volatility_pct,
            zscore_3m_sample_count=item.zscore_3m_sample_count,
            zscore_1y_sample_count=item.zscore_1y_sample_count,
            history_through_date=item.history_through_date,
            is_complete=item.is_complete,
            daily_market_feature_id=spy.daily_market_feature_id,
            details=item.details,
        )
        for item in snapshot.signal_features
    )
    evaluations = tuple(
        SignalEvaluationRow(
            signal_evaluation_id=ids.evaluations[item.evaluation_key],
            pipeline_run_id=ids.pipeline_run_id,
            market_snapshot_id=ids.market_snapshot_id,
            signal_feature_id=ids.features[item.tenor_days],
            tenor_days=item.tenor_days,
            tenor_bucket=item.tenor_bucket,
            signal_layer=item.signal_layer,
            evaluation_status=item.evaluation_status,
            qualifies=item.qualifies,
            vrp_pass=item.vrp_pass,
            zscore_3m_pass=item.zscore_3m_pass,
            zscore_1y_pass=item.zscore_1y_pass,
            rsi14_pass=item.rsi14_pass,
            rv21d_pass=item.rv21d_pass,
            threshold_values=item.threshold_values,
            comparison_results=item.comparison_results,
            failed_checks=item.failed_checks,
            rank_position=item.rank_position,
            rank_score=item.rank_score,
            target_size_pct_nav=item.target_size_pct_nav,
            details={"evaluation_key": item.evaluation_key, **dict(item.details)},
        )
        for item in snapshot.signal_evaluations
    )
    selected_key = snapshot.selected_signal.selected_evaluation_key
    selected = SelectedSignalRow(
        selected_signal_id=ids.selected_signal_id,
        pipeline_run_id=ids.pipeline_run_id,
        market_snapshot_id=ids.market_snapshot_id,
        selected_evaluation_id=(
            None if selected_key is None else ids.evaluations[selected_key]
        ),
        decision=snapshot.selected_signal.decision,
        signal_state=snapshot.selected_signal.signal_state,
        selection_rule_id=snapshot.selected_signal.selection_rule_id,
        no_trade_reason=snapshot.selected_signal.no_trade_reason,
        approved_nav_dollars=snapshot.selected_signal.approved_nav_dollars,
        target_max_risk_dollars=snapshot.selected_signal.target_max_risk_dollars,
        first_observed_at=None,
        consecutive_snapshots=None,
        selection_trace=snapshot.selected_signal.selection_trace,
    )
    return market, implied, forecast, features, evaluations, selected


def _projection_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _projection_value(asdict(value))
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _utc_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _projection_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_projection_value(item) for item in value]
    return value


def _market_projection(row: MarketSnapshotRow) -> dict[str, Any]:
    return _projection_value(
        {
            "market_snapshot_id": row.market_snapshot_id,
            "pipeline_run_id": row.pipeline_run_id,
            "valuation_date": row.valuation_date,
            "snapshot_at": row.snapshot_at,
            "snapshot_kind": "EOD_OFFICIAL",
            "market_session": "CLOSED",
            "source_latest_at": row.source_latest_at,
            "freshness_status": row.freshness_status,
            "spx_spot": row.spx_spot,
            "spy_price": row.spy_price,
            "sofr_rate": row.sofr_rate,
            "sofr_observation_date": row.sofr_observation_date,
            "details": row.details,
            "sofr_observation_id": row.sofr_observation_id,
            "daily_market_feature_id": row.daily_market_feature_id,
        }
    )


def _rows_projection(rows: Sequence[Any]) -> list[dict[str, Any]]:
    return [_projection_value(asdict(row)) for row in rows]


def _expected_projection(
    *,
    ids: _Identifiers,
    environment: str,
    idempotency_key: str,
    snapshot: EodSnapshot,
    code_version: str,
    invocation: Mapping[str, Any],
    metadata: Mapping[str, Any],
    market: MarketSnapshotRow,
    implied: Sequence[ImpliedVarianceRow],
    forecast: Sequence[ForecastVarianceRow],
    features: Sequence[SignalFeatureRow],
    evaluations: Sequence[SignalEvaluationRow],
    selected: SelectedSignalRow,
    assets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "run": _projection_value(
            {
                "pipeline_run_id": ids.pipeline_run_id,
                "environment": environment,
                "idempotency_key": idempotency_key,
                "run_kind": "EOD",
                "valuation_date": snapshot.valuation_date,
                "snapshot_at": snapshot.snapshot_at,
                "data_cutoff_at": snapshot.data_cutoff_at,
                "model_version_id": ids.model_version_id,
                "configuration_version_id": ids.configuration_version_id,
                "code_version": code_version,
                "orchestrator_version": ORCHESTRATOR_VERSION,
                "invocation": invocation,
                "metadata": metadata,
            }
        ),
        "assets": [_projection_value(item) for item in assets],
        "market_snapshot": _market_projection(market),
        "implied_variances": _rows_projection(implied),
        "forecast_variances": _rows_projection(forecast),
        "signal_features": _rows_projection(features),
        "signal_evaluations": _rows_projection(evaluations),
        "selected_signal": _projection_value(asdict(selected)),
    }


def _asset_projection(
    *,
    record: EodDataAsset,
    usage_role: str,
    logical_name: str,
    lineage: Mapping[str, Any],
) -> dict[str, Any]:
    return _projection_value(
        {
            "data_asset_id": record.data_asset_id,
            "dataset_name": record.dataset_name,
            "asset_class": record.asset_class,
            "asset_format": record.asset_format,
            "content_sha256": record.content_sha256,
            "storage_uri": record.storage_uri,
            "schema_version": record.schema_version,
            "source_system": record.source_system,
            "observation_start_at": record.observation_start_at,
            "observation_end_at": record.observation_end_at,
            "trade_date_start": record.trade_date_start,
            "trade_date_end": record.trade_date_end,
            "row_count": record.row_count,
            "byte_size": record.byte_size,
            "is_immutable": True,
            "metadata": record.metadata,
            "usage_role": usage_role,
            "logical_name": logical_name,
            "stage_name": STAGE_NAME,
            "is_required": True,
            "lineage": lineage,
        }
    )


_ASSET_EVIDENCE_FIELDS = (
    "data_asset_id",
    "dataset_name",
    "asset_class",
    "asset_format",
    "content_sha256",
    "storage_uri",
    "schema_version",
    "source_system",
    "observation_start_at",
    "observation_end_at",
    "trade_date_start",
    "trade_date_end",
    "row_count",
    "byte_size",
    "is_immutable",
    "metadata",
    "usage_role",
    "logical_name",
    "stage_name",
    "is_required",
    "lineage",
)


def _asset_evidence_contract(assets: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [
            _projection_value({field: item.get(field) for field in _ASSET_EVIDENCE_FIELDS})
            for item in assets
            if isinstance(item.get("lineage"), Mapping)
            and item["lineage"].get("identity_input") is True
        ],
        key=lambda item: (item["usage_role"], item["logical_name"]),
    )


def _asset_lineage(asset: _AssetInput) -> dict[str, Any]:
    return {
        "content_sha256": asset.content_sha256,
        "identity_input": asset.identity_input,
        "relative_path": asset.relative_path,
    }


def _expected_asset_evidence(
    inputs: Sequence[_AssetInput],
    snapshot: EodSnapshot,
) -> list[dict[str, Any]]:
    return _asset_evidence_contract(
        [
            _asset_projection(
                record=_asset_record(item, snapshot),
                usage_role=item.usage_role,
                logical_name=item.logical_name,
                lineage=_asset_lineage(item),
            )
            for item in inputs
            if item.identity_input
        ]
    )


def _assert_run_state(
    state: Any,
    *,
    ids: _Identifiers,
    environment: str,
    idempotency_key: str,
    snapshot: EodSnapshot,
    code_version: str,
    invocation: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> None:
    expected = {
        "pipeline_run_id": ids.pipeline_run_id,
        "environment": environment,
        "idempotency_key": idempotency_key,
        "valuation_date": snapshot.valuation_date,
        "snapshot_at": snapshot.snapshot_at,
        "data_cutoff_at": snapshot.data_cutoff_at,
        "model_version_id": ids.model_version_id,
        "configuration_version_id": ids.configuration_version_id,
        "code_version": code_version,
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "invocation": invocation,
        "metadata": metadata,
    }
    actual = {key: getattr(state, key) for key in expected}
    _assert_projection(_projection_value(expected), _projection_value(actual))


def _golden_qa(snapshot: EodSnapshot, ids: _Identifiers) -> QaResultRow:
    golden = snapshot.golden_evidence
    evidence = {
        "fixture_sha256": golden.fixture_sha256,
        "selected_decisions_sha256": golden.selected_decisions_sha256,
        "signal_history_sha256": golden.signal_history_sha256,
        "verification_id": golden.verification_id,
    }
    return QaResultRow(
        qa_result_id=ids.golden_qa_result_id,
        pipeline_run_id=ids.pipeline_run_id,
        stage_name=STAGE_NAME,
        check_code="golden_eod_contract",
        scope_key="run",
        severity="ERROR",
        outcome="PASS",
        is_hard_gate=True,
        message="Staged EOD artifacts pass the accepted golden calculation contract.",
        observed_value={"status": golden.status},
        expected_value={"status": "PASS"},
        evidence=evidence,
    )


def _readback_qa(
    ids: _Identifiers,
    *,
    output_fingerprint: str,
    metrics: Mapping[str, Any],
) -> QaResultRow:
    return QaResultRow(
        qa_result_id=ids.readback_qa_result_id,
        pipeline_run_id=ids.pipeline_run_id,
        stage_name=STAGE_NAME,
        check_code="postgres_projection_reconciliation",
        scope_key="run",
        severity="ERROR",
        outcome="PASS",
        is_hard_gate=True,
        message="The complete PostgreSQL EOD projection matches the staged snapshot.",
        observed_value={"output_fingerprint": output_fingerprint, **dict(metrics)},
        expected_value={"output_fingerprint": output_fingerprint, **dict(metrics)},
        evidence={
            "absolute_tolerance": PROJECTION_ABSOLUTE_TOLERANCE,
            "relative_tolerance": PROJECTION_RELATIVE_TOLERANCE,
        },
    )


def _result(
    snapshot: EodSnapshot,
    ids: _Identifiers,
    *,
    effective_input_sha256: str,
    database_projection_sha256: str,
    database_readback_sha256: str,
    no_op: bool,
) -> EodShadowLoadResult:
    return EodShadowLoadResult(
        pipeline_run_id=ids.pipeline_run_id,
        market_snapshot_id=ids.market_snapshot_id,
        selected_signal_id=ids.selected_signal_id,
        valuation_date=snapshot.valuation_date,
        snapshot_at=snapshot.snapshot_at,
        content_sha256=effective_input_sha256,
        database_projection_sha256=database_projection_sha256,
        database_readback_sha256=database_readback_sha256,
        implied_variance_count=len(snapshot.implied_variance),
        forecast_variance_count=len(snapshot.forecast_variance),
        signal_feature_count=len(snapshot.signal_features),
        signal_evaluation_count=len(snapshot.signal_evaluations),
        decision=snapshot.selected_signal.decision,
        no_op=no_op,
    )


def _stable_reconciliation_evidence(repository: Any, pipeline_run_id: UUID) -> Mapping[str, Any]:
    fetch = getattr(repository, "fetch_reconciliation_evidence", None)
    if fetch is not None:
        return fetch(pipeline_run_id)
    cursor = getattr(repository, "cursor", None)
    if cursor is None:
        raise RuntimeError("repository cannot re-read stage and QA evidence")
    cursor.execute(
        """
        SELECT stage_name, status, input_fingerprint, output_fingerprint, metrics
        FROM vrp.pipeline_run_stages
        WHERE pipeline_run_id = %s
        ORDER BY stage_order
        """,
        (pipeline_run_id,),
    )
    stages = [
        {
            "stage_name": row[0],
            "status": row[1],
            "input_fingerprint": row[2],
            "output_fingerprint": row[3],
            "metrics": row[4],
        }
        for row in cursor.fetchall()
    ]
    cursor.execute(
        """
        SELECT check_code, scope_key, outcome, is_hard_gate, evidence
        FROM vrp.qa_results
        WHERE pipeline_run_id = %s
        ORDER BY check_code, scope_key
        """,
        (pipeline_run_id,),
    )
    qa_results = [
        {
            "check_code": row[0],
            "scope_key": row[1],
            "outcome": row[2],
            "is_hard_gate": row[3],
            "evidence": row[4],
        }
        for row in cursor.fetchall()
    ]
    cursor.execute(
        "SELECT COUNT(*) FROM vrp.signal_publications WHERE pipeline_run_id = %s",
        (pipeline_run_id,),
    )
    publication_row = cursor.fetchone()
    return {
        "stages": stages,
        "qa_results": qa_results,
        "signal_publication_count": int(publication_row[0]),
    }


def _assert_completed_evidence(
    repository: Any,
    pipeline_run_id: UUID,
    *,
    effective_input_sha256: str,
    output_fingerprint: str,
    golden_verification_id: str,
) -> None:
    evidence = _stable_reconciliation_evidence(repository, pipeline_run_id)
    if evidence.get("signal_publication_count") != 0:
        raise RuntimeError("EOD shadow run must not have a signal publication")
    stages = evidence.get("stages") if isinstance(evidence, Mapping) else None
    qa_results = evidence.get("qa_results") if isinstance(evidence, Mapping) else None
    if not isinstance(stages, Sequence) or isinstance(stages, (str, bytes)):
        raise RuntimeError("completed EOD run is missing stable stage evidence")
    if not isinstance(qa_results, Sequence) or isinstance(qa_results, (str, bytes)):
        raise RuntimeError("completed EOD run is missing stable QA evidence")
    matching_stages = [item for item in stages if item.get("stage_name") == STAGE_NAME]
    if len(matching_stages) != 1 or matching_stages[0].get("status") != "COMPLETED":
        raise RuntimeError("completed EOD run does not have one completed shadow stage")
    stage = matching_stages[0]
    if (
        stage.get("input_fingerprint") != effective_input_sha256
        or stage.get("output_fingerprint") != output_fingerprint
    ):
        raise RuntimeError("completed EOD stage fingerprints do not match supplied evidence")
    observed = {
        (item.get("check_code"), item.get("scope_key")): (
            item.get("outcome"),
            item.get("is_hard_gate"),
        )
        for item in qa_results
    }
    expected = {
        ("golden_eod_contract", "run"): ("PASS", True),
        ("postgres_projection_reconciliation", "run"): ("PASS", True),
    }
    if observed != expected:
        raise RuntimeError(
            f"completed EOD run QA evidence is incomplete or unexpected: {observed}"
        )
    by_code = {item.get("check_code"): item for item in qa_results}
    golden_detail = by_code["golden_eod_contract"].get("evidence")
    readback_detail = by_code["postgres_projection_reconciliation"].get("evidence")
    if not isinstance(golden_detail, Mapping) or (
        golden_detail.get("verification_id") != golden_verification_id
    ):
        raise RuntimeError("stored golden QA evidence does not match supplied evidence")
    if not isinstance(readback_detail, Mapping) or (
        readback_detail.get("absolute_tolerance") != PROJECTION_ABSOLUTE_TOLERANCE
        or readback_detail.get("relative_tolerance") != PROJECTION_RELATIVE_TOLERANCE
    ):
        raise RuntimeError("stored read-back QA tolerance contract is inconsistent")


def execute_eod_shadow_load(
    connection: Any,
    snapshot: EodSnapshot,
    *,
    environment: str,
    code_version: str,
    requested_by: str,
    repository_factory: RepositoryFactory = PostgresEodRepository,
) -> EodShadowLoadResult:
    """Atomically record and reconcile one validated completed EOD snapshot."""

    _require_idle_connection(connection)
    environment = _required_text(environment, "environment")
    code_version = validate_code_version(code_version)
    requested_by = _required_text(requested_by, "requested_by")
    if snapshot.market_snapshot.snapshot_kind != "EOD_OFFICIAL":
        raise ValueError("the shadow recorder accepts only official EOD snapshots")
    if tuple(item.tenor_days for item in snapshot.implied_variance) != EXPECTED_TENORS:
        raise ValueError("implied variance does not use the exact locked tenor grid")

    # The staged-output loader already validated these files.  Recheck before
    # opening a database cursor so a caller cannot pass stale in-memory evidence.
    _assert_snapshot_files_unchanged(snapshot)
    asset_inputs = _asset_inputs(snapshot)

    try:
        with connection.cursor() as cursor:
            repository = repository_factory(cursor)
            sofr = repository.resolve_sofr_before(
                snapshot.valuation_date,
                normalized_content_sha256=(
                    snapshot.sofr_evidence.normalized_content_sha256
                ),
            )
            spy = repository.resolve_spy_feature(
                valuation_date=snapshot.valuation_date,
                definition_key=SPY_DAILY_DEFINITION_CONTRACT["definition_key"],
                version_label=SPY_DAILY_DEFINITION_CONTRACT["version_label"],
                definition_content_sha256=SPY_DAILY_DEFINITION_CONTRACT[
                    "content_sha256"
                ],
            )
            _validate_reference_pins(snapshot, sofr, spy)
            effective_input_sha256, idempotency_key = _effective_input(
                snapshot,
                environment=environment,
                code_version=code_version,
                sofr=sofr,
                spy=spy,
            )
            ids = _identifiers(
                snapshot,
                environment=environment,
                idempotency_key=idempotency_key,
            )
            invocation, metadata = _run_contract(
                snapshot,
                effective_input_sha256=effective_input_sha256,
                code_version=code_version,
                sofr=sofr,
                spy=spy,
            )
            repository.acquire_logical_run_lock(
                environment=environment,
                idempotency_key=idempotency_key,
            )
            model_version_id = repository.register_model_version(
                model_version_id=ids.model_version_id,
                model_key=snapshot.model_identity.key,
                version_label=snapshot.model_identity.version_label,
                content_sha256=snapshot.model_identity.sha256,
                manifest=snapshot.model_identity.payload,
                locked_at=_model_locked_at(snapshot),
            )
            configuration_version_id = repository.register_configuration_version(
                configuration_version_id=ids.configuration_version_id,
                configuration_key=snapshot.configuration_identity.key,
                version_label=snapshot.configuration_identity.version_label,
                content_sha256=snapshot.configuration_identity.sha256,
                configuration=snapshot.configuration_identity.payload,
            )
            if (
                model_version_id != ids.model_version_id
                or configuration_version_id != ids.configuration_version_id
            ):
                raise RuntimeError("registered version IDs differ from deterministic IDs")
            market, implied, forecast, features, evaluations, selected = _database_rows(
                snapshot, ids, sofr, spy
            )

            existing = repository.find_run(
                environment=environment,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                if not existing.is_completed_pass:
                    raise RuntimeError(
                        "existing EOD shadow run is not COMPLETED/PASS; "
                        "explicit recovery is required"
                    )
                _assert_run_state(
                    existing,
                    ids=ids,
                    environment=environment,
                    idempotency_key=idempotency_key,
                    snapshot=snapshot,
                    code_version=code_version,
                    invocation=invocation,
                    metadata=metadata,
                )
                actual = repository.fetch_run_projection(ids.pipeline_run_id)
                actual_assets = actual.get("assets")
                if not isinstance(actual_assets, Sequence) or isinstance(
                    actual_assets, (str, bytes)
                ):
                    raise RuntimeError("completed EOD run is missing asset evidence")
                _assert_projection(
                    _expected_asset_evidence(asset_inputs, snapshot),
                    _asset_evidence_contract(actual_assets),
                )
                expected = _expected_projection(
                    ids=ids,
                    environment=environment,
                    idempotency_key=idempotency_key,
                    snapshot=snapshot,
                    code_version=code_version,
                    invocation=invocation,
                    metadata=metadata,
                    market=market,
                    implied=implied,
                    forecast=forecast,
                    features=features,
                    evaluations=evaluations,
                    selected=selected,
                    assets=actual_assets,
                )
                _assert_projection(expected, _projection_value(actual))
                output_fingerprint = _digest({"database_projection": expected})
                readback_fingerprint = database_readback_fingerprint(actual)
                _assert_completed_evidence(
                    repository,
                    ids.pipeline_run_id,
                    effective_input_sha256=effective_input_sha256,
                    output_fingerprint=output_fingerprint,
                    golden_verification_id=snapshot.golden_evidence.verification_id,
                )
                result = _result(
                    snapshot,
                    ids,
                    effective_input_sha256=effective_input_sha256,
                    database_projection_sha256=output_fingerprint,
                    database_readback_sha256=readback_fingerprint,
                    no_op=True,
                )
            else:
                run_state = repository.begin_run(
                    pipeline_run_id=ids.pipeline_run_id,
                    environment=environment,
                    idempotency_key=idempotency_key,
                    valuation_date=snapshot.valuation_date,
                    snapshot_at=snapshot.snapshot_at,
                    data_cutoff_at=snapshot.data_cutoff_at,
                    model_version_id=ids.model_version_id,
                    configuration_version_id=ids.configuration_version_id,
                    code_version=code_version,
                    orchestrator_version=ORCHESTRATOR_VERSION,
                    requested_by=requested_by,
                    invocation=invocation,
                    metadata=metadata,
                )
                if not run_state.inserted:
                    raise RuntimeError(
                        "logical EOD shadow run appeared after its advisory lock was acquired"
                    )
                repository.start_shadow_import_stage(
                    pipeline_stage_id=ids.pipeline_stage_id,
                    pipeline_run_id=ids.pipeline_run_id,
                    stage_name=STAGE_NAME,
                    input_fingerprint=effective_input_sha256,
                )

                expected_assets: list[Mapping[str, Any]] = []
                for item in asset_inputs:
                    asset_record = _asset_record(item, snapshot)
                    stored = repository.register_asset(asset_record)
                    if stored.record_id != asset_record.data_asset_id:
                        raise RuntimeError(
                            "registered asset ID differs from its deterministic identity"
                        )
                    lineage = _asset_lineage(item)
                    repository.link_asset(
                        pipeline_run_id=ids.pipeline_run_id,
                        data_asset_id=stored.record_id,
                        usage_role=item.usage_role,
                        logical_name=item.logical_name,
                        stage_name=STAGE_NAME,
                        lineage=lineage,
                    )
                    expected_assets.append(
                        _asset_projection(
                            record=asset_record,
                            usage_role=item.usage_role,
                            logical_name=item.logical_name,
                            lineage=lineage,
                        )
                    )
                expected_assets.sort(
                    key=lambda item: (
                        item["usage_role"], item["logical_name"], item["data_asset_id"]
                    )
                )

                repository.insert_market_snapshot(market)
                repository.insert_implied_variances(implied)
                repository.insert_forecast_variances(forecast)
                repository.insert_signal_features(features)
                repository.insert_signal_evaluations(evaluations)
                repository.insert_selected_signal(selected)
                repository.record_qa(_golden_qa(snapshot, ids))

                expected = _expected_projection(
                    ids=ids,
                    environment=environment,
                    idempotency_key=idempotency_key,
                    snapshot=snapshot,
                    code_version=code_version,
                    invocation=invocation,
                    metadata=metadata,
                    market=market,
                    implied=implied,
                    forecast=forecast,
                    features=features,
                    evaluations=evaluations,
                    selected=selected,
                    assets=expected_assets,
                )
                actual = repository.fetch_run_projection(ids.pipeline_run_id)
                _assert_projection(expected, _projection_value(actual))
                output_fingerprint = _digest({"database_projection": expected})
                metrics = {
                    "forecast_variance_count": len(forecast),
                    "implied_variance_count": len(implied),
                    "signal_evaluation_count": len(evaluations),
                    "signal_feature_count": len(features),
                    "selected_signal_count": 1,
                }
                repository.record_qa(
                    _readback_qa(
                        ids,
                        output_fingerprint=output_fingerprint,
                        metrics=metrics,
                    )
                )
                repository.complete_shadow_import_stage(
                    pipeline_run_id=ids.pipeline_run_id,
                    stage_name=STAGE_NAME,
                    output_fingerprint=output_fingerprint,
                    metrics=metrics,
                )
                repository.finalize_run(ids.pipeline_run_id)
                final_projection = repository.fetch_run_projection(ids.pipeline_run_id)
                _assert_projection(expected, _projection_value(final_projection))
                readback_fingerprint = database_readback_fingerprint(
                    final_projection
                )
                _assert_completed_evidence(
                    repository,
                    ids.pipeline_run_id,
                    effective_input_sha256=effective_input_sha256,
                    output_fingerprint=output_fingerprint,
                    golden_verification_id=snapshot.golden_evidence.verification_id,
                )
                result = _result(
                    snapshot,
                    ids,
                    effective_input_sha256=effective_input_sha256,
                    database_projection_sha256=output_fingerprint,
                    database_readback_sha256=readback_fingerprint,
                    no_op=False,
                )
        # Close the validation-to-write window as tightly as possible.  A file
        # mutation during database work must abort the transaction, including
        # the revalidated no-op path, rather than committing stale evidence.
        _assert_snapshot_files_unchanged(snapshot)
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
