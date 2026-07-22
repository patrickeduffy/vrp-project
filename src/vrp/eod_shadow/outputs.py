"""Fail-closed normalization of an accepted staged Hybrid v2 EOD run."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

try:
    import pandas_market_calendars as mcal
except ImportError:  # pragma: no cover - production requirements pin this dependency
    mcal = None  # type: ignore[assignment]

from vrp.golden import DEFAULT_FIXTURE_REL, verify_golden_contract_with_manifest

from .models import (
    ArtifactMetadata,
    EodSnapshot,
    ForecastVarianceRecord,
    GoldenVerificationEvidence,
    ImpliedVarianceRecord,
    MarketSnapshotRecord,
    SelectedSignalRecord,
    SignalEvaluationRecord,
    SignalFeatureRecord,
    TARGET_TENORS,
    VersionedDocument,
)
from .sofr_evidence import SofrEvidenceError, load_sofr_updater_evidence


RUN_MANIFEST_NAME = "run_manifest.json"
STAGING_DIR_NAME = "staging"
PUBLISH_MANIFEST_NAME = "vrp_hybrid_v2_publish_manifest.json"
SIGNAL_HISTORY_NAME = "vrp_hybrid_v2_signal_history.parquet"
LATEST_SNAPSHOT_NAME = "vrp_hybrid_v2_latest_snapshot.parquet"
SELECTED_DECISIONS_NAME = "vrp_hybrid_v2_selected_decisions.parquet"
FORECAST_HISTORY_NAME = "vrp_hybrid_v2_forecast_history.parquet"
STATIC_TIEBREAKS_NAME = "vrp_hybrid_v2_static_tiebreaks.csv"
REQUIRED_STAGED_NAMES = (
    PUBLISH_MANIFEST_NAME,
    SIGNAL_HISTORY_NAME,
    LATEST_SNAPSHOT_NAME,
    SELECTED_DECISIONS_NAME,
    FORECAST_HISTORY_NAME,
    STATIC_TIEBREAKS_NAME,
)


class EodOutputContractError(ValueError):
    """A completed-run directory does not satisfy the shadow-write contract."""


def _fail(message: str) -> None:
    raise EodOutputContractError(message)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        _fail(f"missing {label}: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail(f"invalid {label}: {exc}")
    if not isinstance(payload, dict):
        _fail(f"{label} must contain a JSON object")
    return payload


def _read_stable_json(path: Path, label: str) -> tuple[dict[str, Any], str]:
    before = _hash(path)
    payload = _read_json(path, label)
    after = _hash(path)
    if before != after:
        _fail(f"{label} changed while it was being read")
    return payload, after


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _as_date(value: Any, label: str) -> date:
    if value is None or (isinstance(value, str) and not value.strip()):
        _fail(f"missing {label}")
    try:
        return pd.Timestamp(value).date()
    except (TypeError, ValueError) as exc:
        raise EodOutputContractError(f"invalid {label}: {value!r}") from exc


def _as_utc_datetime(value: Any, label: str) -> datetime:
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise EodOutputContractError(f"invalid {label}: {value!r}") from exc
    if stamp.tzinfo is None:
        _fail(f"{label} must include a timezone")
    return stamp.tz_convert("UTC").to_pydatetime()


def _float(value: Any, label: str, *, positive: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise EodOutputContractError(f"{label} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        _fail(f"{label} must be finite" + (" and positive" if positive else ""))
    return result


def _bool(value: Any, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if type(value).__name__ == "bool_":
        return bool(value)
    _fail(f"{label} must be boolean")


def _same_float(values: pd.Series, label: str) -> float:
    clean = pd.to_numeric(values, errors="coerce")
    if clean.isna().any() or clean.nunique(dropna=False) != 1:
        _fail(f"target rows do not contain one identical {label}")
    return _float(clean.iloc[0], label)


def _bucket(tenor: int) -> str:
    return "FRONT" if tenor <= 18 else "MIDDLE" if tenor <= 24 else "BACK"


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    missing = sorted(set(columns).difference(frame.columns))
    if missing:
        _fail(f"{label} is missing columns: {missing}")


def _normalize_tenors(
    frame: pd.DataFrame,
    label: str,
    *,
    column: str = "tenor",
    allow_null: bool = False,
) -> None:
    _require_columns(frame, (column,), label)
    normalized: list[int | None] = []
    for index, value in frame[column].items():
        if pd.isna(value):
            if allow_null:
                normalized.append(None)
                continue
            _fail(f"{label} has a null {column} at row {index}")
        if isinstance(value, bool):
            _fail(f"{label} has a boolean {column} at row {index}")
        numeric = _float(value, f"{label} {column}")
        if not numeric.is_integer():
            _fail(f"{label} has a non-integral {column}: {value!r}")
        normalized.append(int(numeric))
    frame[column] = normalized


def _versioned_document(path_value: Any, key: str, version: str) -> VersionedDocument:
    path = Path(str(path_value)).resolve()
    payload, digest = _read_stable_json(path, key)
    return VersionedDocument(
        key=key,
        version_label=version,
        path=path,
        sha256=digest,
        payload=payload,
    )


def _configuration_identity(
    runtime_path_value: Any,
    production_path_value: Any,
    version: str,
    approved_nav: float,
) -> tuple[VersionedDocument, dict[str, Any], Path]:
    runtime_path = Path(str(runtime_path_value)).resolve()
    production_path = Path(str(production_path_value)).resolve()
    runtime, runtime_sha256 = _read_stable_json(runtime_path, "runtime configuration")
    production, production_sha256 = _read_stable_json(
        production_path, "production configuration"
    )
    production_version = production.get("release_id") or production.get("lock_id")
    if production_version != version:
        _fail("production configuration does not match the completed run lock ID")
    payload = {
        "runtime_configuration": runtime,
        "runtime_configuration_sha256": runtime_sha256,
        "production_configuration": production,
        "production_configuration_sha256": production_sha256,
        "approved_nav": approved_nav,
    }
    content_sha256 = _canonical_hash(payload)
    identity = VersionedDocument(
        key="hybrid_v2_eod_configuration",
        version_label=f"{version}:{content_sha256[:16]}",
        path=runtime_path,
        sha256=content_sha256,
        payload=payload,
    )
    return identity, runtime, runtime_path


def _resolve_model_lock_path(
    runtime: Mapping[str, Any],
    runtime_path: Path,
    source_root: Path,
) -> Path:
    canonical = runtime.get("canonical")
    value = canonical.get("lock_manifest") if isinstance(canonical, dict) else None
    if not isinstance(value, str) or not value.strip():
        _fail("runtime configuration does not identify canonical.lock_manifest")
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.resolve()
    runtime_project_candidate = runtime_path.parent.parent / candidate
    if runtime_project_candidate.is_file():
        return runtime_project_candidate.resolve()
    return (source_root / candidate).resolve()


def _artifact(
    path: Path,
    logical_name: str,
    relative_path: str,
    row_count: int | None = None,
    *,
    identity_input: bool = True,
    verified_sha256: str | None = None,
    trade_date_start: date | None = None,
    trade_date_end: date | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ArtifactMetadata:
    suffix = path.suffix.lower()
    asset_format = {".parquet": "PARQUET", ".csv": "CSV", ".json": "JSON"}.get(suffix)
    if asset_format is None:
        _fail(f"unsupported staged artifact format: {path}")
    return ArtifactMetadata(
        logical_name=logical_name,
        path=path,
        asset_format=asset_format,
        sha256=verified_sha256 or _hash(path),
        byte_size=path.stat().st_size,
        row_count=row_count,
        relative_path=relative_path,
        identity_input=identity_input,
        metadata=(
            {"source": "completed_legacy_eod_staging", "immutable_after_validation": True}
            if metadata is None
            else dict(metadata)
        ),
        trade_date_start=trade_date_start,
        trade_date_end=trade_date_end,
    )


def _artifact_date_coverage(frame: pd.DataFrame, label: str) -> tuple[date, date]:
    _require_columns(frame, ("date",), label)
    if frame.empty:
        _fail(f"{label} cannot be empty")
    try:
        parsed = pd.to_datetime(frame["date"], errors="raise")
    except (TypeError, ValueError) as exc:
        raise EodOutputContractError(f"{label} contains an invalid date") from exc
    if parsed.isna().any():
        _fail(f"{label} contains a missing date")
    dates = parsed.dt.date
    return min(dates), max(dates)


def _official_xnys_close(valuation_date: date) -> datetime:
    """Resolve the official close, including early closes, and fail closed."""

    if mcal is None:
        _fail("pandas-market-calendars is required to resolve the official XNYS close")
    try:
        schedule = mcal.get_calendar("XNYS").schedule(
            start_date=valuation_date.isoformat(),
            end_date=valuation_date.isoformat(),
        )
    except Exception as exc:  # pragma: no cover - dependency/calendar failure
        raise EodOutputContractError(
            f"could not resolve XNYS session close for {valuation_date}: {exc}"
        ) from exc
    if len(schedule) != 1 or "market_close" not in schedule:
        _fail(f"valuation date is not exactly one XNYS session: {valuation_date}")
    close = pd.Timestamp(schedule.iloc[0]["market_close"])
    if close.tzinfo is None:
        _fail("XNYS calendar returned a timezone-naive session close")
    return close.tz_convert("UTC").to_pydatetime()


def _reconcile_manifest_paths(
    run_manifest: Mapping[str, Any],
    publish_manifest: Mapping[str, Any],
    fixed_paths: Mapping[str, Path],
) -> None:
    recorded_publish = run_manifest.get("publish_manifest")
    if not isinstance(recorded_publish, str) or Path(recorded_publish).resolve() != fixed_paths["publish_manifest"]:
        _fail("run manifest publish_manifest does not name the fixed staged manifest")
    recorded = publish_manifest.get("staged_outputs")
    if not isinstance(recorded, dict):
        _fail("publish manifest staged_outputs must be an object")
    mapping = {
        "manifest": "publish_manifest",
        "signal_history": "signal_history",
        "latest_snapshot": "latest_snapshot",
        "selected_decisions": "selected_decisions",
        "forecast_history": "forecast_history",
        "tiebreaks": "static_tiebreaks",
    }
    for manifest_key, fixed_key in mapping.items():
        value = recorded.get(manifest_key)
        if not isinstance(value, str) or Path(value).resolve() != fixed_paths[fixed_key]:
            _fail(f"publish manifest {manifest_key} path does not match the fixed staging artifact")


def _target_frames(
    signals: pd.DataFrame,
    latest: pd.DataFrame,
    decisions: pd.DataFrame,
    valuation_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    for frame, label in ((signals, "signal history"), (latest, "latest snapshot"), (decisions, "decisions")):
        _require_columns(frame, ("date",), label)
        frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.date
        if frame.empty or max(frame["date"]) != valuation_date:
            _fail(f"{label} does not end on the run target date")
    target = signals.loc[signals["date"].eq(valuation_date)].sort_values("tenor").copy()
    latest_target = latest.loc[latest["date"].eq(valuation_date)].sort_values("tenor").copy()
    tenor_list = target["tenor"].astype(int).tolist() if "tenor" in target else []
    if tenor_list != list(TARGET_TENORS) or target["tenor"].duplicated().any():
        _fail(f"target signal grid must be exactly {list(TARGET_TENORS)}; found {tenor_list}")
    latest_tenors = latest_target["tenor"].astype(int).tolist() if "tenor" in latest_target else []
    if len(latest) != 9 or latest_tenors != list(TARGET_TENORS):
        _fail("fixed latest snapshot is not the target nine-tenor grid")
    if set(latest_target.columns) != set(target.columns):
        missing = sorted(set(target.columns).difference(latest_target.columns))
        extra = sorted(set(latest_target.columns).difference(target.columns))
        _fail(
            "latest snapshot schema does not match the target signal-history schema: "
            f"missing={missing}, extra={extra}"
        )
    required_compare = list(target.columns)
    left = target[required_compare].reset_index(drop=True)
    right = latest_target[required_compare].reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-12)
    except AssertionError as exc:
        _fail(f"latest snapshot does not reconcile to signal history: {exc}")
    decision = decisions.loc[decisions["date"].eq(valuation_date)].copy()
    if len(decision) != 1:
        _fail(f"target date must have exactly one decision; found {len(decision)}")
    return target, decision.iloc[[0]]


def _validate_full_history_grids(
    signals: pd.DataFrame,
    forecasts: pd.DataFrame,
    decisions: pd.DataFrame,
) -> None:
    for frame, label in ((signals, "signal history"), (forecasts, "forecast history")):
        _require_columns(frame, ("date", "tenor"), label)
        if frame[["date", "tenor"]].isna().any().any():
            _fail(f"{label} contains null date/tenor keys")
        if frame.duplicated(["date", "tenor"]).any():
            _fail(f"{label} contains duplicate date/tenor keys")
        bad_dates = [
            str(group_date)
            for group_date, group in frame.groupby("date", sort=False)
            if sorted(group["tenor"].tolist()) != list(TARGET_TENORS)
        ]
        if bad_dates:
            _fail(f"{label} has incomplete or unexpected tenor grids: {bad_dates[:5]}")
    _require_columns(decisions, ("date",), "decision history")
    if decisions["date"].isna().any() or decisions.duplicated("date").any():
        _fail("decision history must contain one unique non-null row per date")
    signal_dates = set(signals["date"])
    if set(forecasts["date"]) != signal_dates or set(decisions["date"]) != signal_dates:
        _fail("signal, forecast, and decision histories do not cover identical dates")


def _prior_stats(
    signals: pd.DataFrame,
    target: pd.DataFrame,
    valuation_date: date,
    *,
    atol: float,
    rtol: float,
) -> dict[int, tuple[float, float, float, float, date]]:
    _require_columns(signals, ("date", "tenor", "model_vrp_log", "z_3m", "z_1y"), "signal history")
    if signals.duplicated(["date", "tenor"]).any():
        _fail("signal history contains duplicate date/tenor keys")
    result: dict[int, tuple[float, float, float, float, date]] = {}
    for row in target.itertuples(index=False):
        tenor = int(row.tenor)
        history = signals.loc[
            signals["tenor"].astype(int).eq(tenor) & (signals["date"] < valuation_date),
            ["date", "model_vrp_log"],
        ].sort_values("date")
        values = pd.to_numeric(history["model_vrp_log"], errors="coerce")
        if values.isna().any() or len(values) < 252:
            _fail(f"{tenor}D does not have 252 complete prior VRP observations")
        prior63 = values.iloc[-63:]
        prior252 = values.iloc[-252:]
        mean63, std63 = float(prior63.mean()), float(prior63.std(ddof=1))
        mean252, std252 = float(prior252.mean()), float(prior252.std(ddof=1))
        if not all(math.isfinite(value) for value in (mean63, std63, mean252, std252)) or std63 <= 0 or std252 <= 0:
            _fail(f"{tenor}D prior z-score windows are degenerate")
        vrp = _float(row.model_vrp_log, f"{tenor}D VRP")
        expected3 = (vrp - mean63) / std63
        expected1 = (vrp - mean252) / std252
        if not math.isclose(expected3, _float(row.z_3m, "z_3m"), abs_tol=atol, rel_tol=rtol):
            _fail(f"{tenor}D stored z_3m does not match the prior-only 63-session window")
        if not math.isclose(expected1, _float(row.z_1y, "z_1y"), abs_tol=atol, rel_tol=rtol):
            _fail(f"{tenor}D stored z_1y does not match the prior-only 252-session window")
        if int(row.prior_63_rows) != 63 or int(row.prior_252_rows) != 252:
            _fail(f"{tenor}D stored prior row counts are not 63/252")
        history_through = history["date"].iloc[-1]
        if not isinstance(history_through, date):
            _fail(f"{tenor}D prior history does not have a valid terminal date")
        result[tenor] = mean63, std63, mean252, std252, history_through
    return result


CHECK_ORDER = (
    ("VRP", "vrp_pass", "model_vrp_log", "threshold_vrp", ">"),
    ("ZSCORE_3M", "zscore_3m_pass", "z_3m", "threshold_z3", ">"),
    ("ZSCORE_1Y", "zscore_1y_pass", "z_1y", "threshold_z1", ">"),
    ("RSI14", "rsi14_pass", "rsi14", "threshold_rsi", "<"),
    ("RV21D", "rv21d_pass", "rv21d_vol_pct", "threshold_rv", ">"),
)

LOCKED_OPERATOR_MAP = {
    "vrp_log": ">",
    "z_3m": ">",
    "z_1y": ">",
    "rsi14": "<",
    "rv21d_vol_pct": ">",
}

LOCKED_SELECTION_ORDER = (
    "size_desc",
    "Core_before_Secondary",
    "continuous_quality_desc",
    "research_win_rate_desc",
    "research_tail_desc",
    "tenor_desc",
)

CONFIG_THRESHOLD_FIELDS = {
    "threshold_vrp": "vrp_log",
    "threshold_z3": "z_3m",
    "threshold_z1": "z_1y",
    "threshold_rsi": "rsi_cap",
    "threshold_rv": "rv21d_floor",
}


def _configuration_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        _fail(f"production configuration {label} must be an object")
    return value


def _configuration_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(f"production configuration {label} must be a list")
    return value


def _configuration_tenor(value: Any, label: str) -> int:
    if isinstance(value, bool):
        _fail(f"production configuration {label} must be an integral target tenor")
    numeric = _float(value, f"production configuration {label}")
    if not numeric.is_integer() or int(numeric) not in TARGET_TENORS:
        _fail(f"production configuration {label} must be an integral target tenor")
    return int(numeric)


def _configuration_float(value: Any, label: str) -> float:
    if isinstance(value, bool):
        _fail(f"production configuration {label} must be numeric")
    return _float(value, f"production configuration {label}")


def _locked_rule_map(
    production: Mapping[str, Any],
) -> dict[tuple[str, int], dict[str, Any]]:
    rules: dict[tuple[str, int], dict[str, Any]] = {}
    sections = (
        ("CORE", "core_thresholds", "tenor"),
        ("SECONDARY", "secondary_thresholds", "tenors"),
    )
    for layer, section_name, tenor_field in sections:
        entries = _configuration_list(production.get(section_name), section_name)
        for entry_index, raw_entry in enumerate(entries):
            label = f"{section_name}[{entry_index}]"
            entry = _configuration_mapping(raw_entry, label)
            configured_layer = entry.get("layer")
            if not isinstance(configured_layer, str) or configured_layer.upper() != layer:
                _fail(f"production configuration {label}.layer must be {layer.title()}")
            configured_bucket = entry.get("bucket")
            if not isinstance(configured_bucket, str) or not configured_bucket.strip():
                _fail(f"production configuration {label}.bucket must be a non-empty string")
            if tenor_field == "tenor":
                tenors = [_configuration_tenor(entry.get("tenor"), f"{label}.tenor")]
            else:
                raw_tenors = _configuration_list(entry.get("tenors"), f"{label}.tenors")
                if not raw_tenors:
                    _fail(f"production configuration {label}.tenors cannot be empty")
                tenors = [
                    _configuration_tenor(value, f"{label}.tenors[{index}]")
                    for index, value in enumerate(raw_tenors)
                ]
                if len(tenors) != len(set(tenors)):
                    _fail(f"production configuration {label}.tenors contains duplicates")
            thresholds = {
                target_name: _configuration_float(
                    entry.get(config_name), f"{label}.{config_name}"
                )
                for target_name, config_name in CONFIG_THRESHOLD_FIELDS.items()
            }
            for tenor in tenors:
                bucket = _bucket(tenor)
                if configured_bucket.upper() != bucket:
                    _fail(
                        f"production configuration {layer}:{tenor} bucket does not "
                        "match the deterministic tenor bucket"
                    )
                key = (layer, tenor)
                if key in rules:
                    _fail(f"production configuration contains duplicate {layer}:{tenor} rules")
                rules[key] = {"bucket": bucket, "thresholds": thresholds}

    sizes: dict[tuple[str, int], float] = {}
    for entry_index, raw_entry in enumerate(
        _configuration_list(production.get("sizes"), "sizes")
    ):
        label = f"sizes[{entry_index}]"
        entry = _configuration_mapping(raw_entry, label)
        raw_layer = entry.get("layer")
        if not isinstance(raw_layer, str) or raw_layer.upper() not in {"CORE", "SECONDARY"}:
            _fail(f"production configuration {label}.layer must be Core or Secondary")
        layer = raw_layer.upper()
        tenor = _configuration_tenor(entry.get("tenor"), f"{label}.tenor")
        bucket = entry.get("bucket")
        if not isinstance(bucket, str) or bucket.upper() != _bucket(tenor):
            _fail(
                f"production configuration {layer}:{tenor} size bucket does not "
                "match the deterministic tenor bucket"
            )
        size = _configuration_float(entry.get("size_pct_nav"), f"{label}.size_pct_nav")
        if not 0 < size <= 1:
            _fail(f"production configuration {label}.size_pct_nav must be in (0, 1]")
        key = (layer, tenor)
        if key in sizes:
            _fail(f"production configuration contains duplicate {layer}:{tenor} sizes")
        sizes[key] = size
    if set(sizes) != set(rules):
        missing = sorted(set(rules).difference(sizes))
        unexpected = sorted(set(sizes).difference(rules))
        _fail(
            "production configuration rule and size maps disagree: "
            f"missing_sizes={missing}, unexpected_sizes={unexpected}"
        )
    for key, size in sizes.items():
        rules[key]["size_pct_nav"] = size

    inactive = _configuration_mapping(production.get("inactive"), "inactive")
    required_inactive_keys = {"Core_Front", "tenor_9D"}
    if set(inactive) != required_inactive_keys or any(
        not isinstance(inactive[key], bool) for key in required_inactive_keys
    ):
        _fail(
            "production configuration inactive must contain exactly boolean "
            "Core_Front and tenor_9D flags"
        )
    core_front_is_inactive = not any(
        layer == "CORE" and _bucket(tenor) == "FRONT" for layer, tenor in rules
    )
    tenor_9d_is_inactive = not any(tenor == 9 for _, tenor in rules)
    if inactive["Core_Front"] != core_front_is_inactive:
        _fail("production configuration Core_Front inactive flag contradicts its rule map")
    if inactive["tenor_9D"] != tenor_9d_is_inactive:
        _fail("production configuration tenor_9D inactive flag contradicts its rule map")
    return rules


def _validate_locked_production_configuration(
    target: pd.DataFrame,
    decision_row: Any,
    production: Mapping[str, Any],
) -> dict[tuple[str, int], dict[str, Any]]:
    forecast_model = production.get("forecast_model")
    if not isinstance(forecast_model, str) or not forecast_model.strip():
        _fail("production configuration forecast_model must be a non-empty string")
    observed_models = set(target["forecast_model"].dropna().astype(str))
    if observed_models != {forecast_model} or target["forecast_model"].isna().any():
        _fail("target forecast model does not match the locked production configuration")

    operators = _configuration_mapping(
        production.get("strict_operators"), "strict_operators"
    )
    if dict(operators) != LOCKED_OPERATOR_MAP:
        _fail(
            "production configuration strict_operators do not match the "
            "implemented fail-closed comparisons"
        )

    rules = _locked_rule_map(production)
    for layer in ("CORE", "SECONDARY"):
        prefix = layer.lower()
        observed_active = {
            int(row.tenor)
            for row in target.itertuples(index=False)
            if _bool(getattr(row, f"{prefix}_rule_exists"), f"{layer} rule_exists")
        }
        expected_active = {tenor for rule_layer, tenor in rules if rule_layer == layer}
        if observed_active != expected_active:
            _fail(
                f"target {layer} active tenor map does not match the locked "
                f"production configuration: expected={sorted(expected_active)}, "
                f"observed={sorted(observed_active)}"
            )

    for row in target.itertuples(index=False):
        tenor = int(row.tenor)
        for layer in ("CORE", "SECONDARY"):
            prefix = layer.lower()
            rule = rules.get((layer, tenor))
            if rule is None:
                continue
            observed_bucket = getattr(row, f"{prefix}_bucket")
            if not isinstance(observed_bucket, str) or observed_bucket.upper() != rule["bucket"]:
                _fail(
                    f"target {layer}:{tenor} bucket does not match the locked "
                    "production configuration"
                )
            for target_name, expected in rule["thresholds"].items():
                observed = _float(
                    getattr(row, f"{prefix}_{target_name}"),
                    f"target {layer}:{tenor} {target_name}",
                )
                if not math.isclose(observed, expected, rel_tol=1e-12, abs_tol=1e-12):
                    _fail(
                        f"target {layer}:{tenor} {target_name} does not match the "
                        "locked production configuration"
                    )
            observed_size = _float(
                getattr(row, f"{prefix}_size_pct_nav"),
                f"target {layer}:{tenor} size_pct_nav",
            )
            if not math.isclose(
                observed_size, rule["size_pct_nav"], rel_tol=1e-12, abs_tol=1e-12
            ):
                _fail(
                    f"target {layer}:{tenor} size_pct_nav does not match the locked "
                    "production configuration"
                )

    selection = _configuration_mapping(production.get("selection"), "selection")
    rule_id = selection.get("rule_id")
    if not isinstance(rule_id, str) or not rule_id.strip():
        _fail("production configuration selection.rule_id must be a non-empty string")
    if getattr(decision_row, "selection_rule", None) != rule_id:
        _fail("staged decision selection_rule does not match the locked production configuration")
    order = selection.get("order")
    if not isinstance(order, list) or tuple(order) != LOCKED_SELECTION_ORDER:
        _fail(
            "production configuration selection.order does not match the "
            "implemented locked winner ranking"
        )
    if selection.get("one_trade_per_day") is not True:
        _fail("production configuration selection.one_trade_per_day must be true")
    return rules


def _static_tiebreak_map(
    frame: pd.DataFrame,
    rules: Mapping[tuple[str, int], Mapping[str, Any]],
) -> dict[tuple[str, int], dict[str, float]]:
    _require_columns(
        frame,
        (
            "layer",
            "bucket",
            "tenor",
            "continuous_quality_score",
            "research_sleeve_win_rate_pct",
            "research_sleeve_worst_1pct_mean_return",
        ),
        "staged static tiebreaks",
    )
    _normalize_tenors(frame, "staged static tiebreaks")
    records: dict[tuple[str, int], dict[str, float]] = {}
    for row in frame.itertuples(index=False):
        raw_layer = row.layer
        if not isinstance(raw_layer, str) or raw_layer.upper() not in {"CORE", "SECONDARY"}:
            _fail("staged static tiebreaks layer must be Core or Secondary")
        layer = raw_layer.upper()
        tenor = int(row.tenor)
        key = (layer, tenor)
        if key in records:
            _fail(f"staged static tiebreaks contains duplicate {layer}:{tenor} rows")
        rule = rules.get(key)
        if rule is None:
            _fail(f"staged static tiebreaks contains unexpected {layer}:{tenor} row")
        if not isinstance(row.bucket, str) or row.bucket.upper() != rule["bucket"]:
            _fail(f"staged static tiebreaks {layer}:{tenor} bucket disagrees with its rule")
        records[key] = {
            "continuous_quality_score": _float(
                row.continuous_quality_score,
                f"staged static tiebreaks {layer}:{tenor} continuous_quality_score",
            ),
            "research_sleeve_win_rate_pct": _float(
                row.research_sleeve_win_rate_pct,
                f"staged static tiebreaks {layer}:{tenor} research_sleeve_win_rate_pct",
            ),
            "research_sleeve_worst_1pct_mean_return": _float(
                row.research_sleeve_worst_1pct_mean_return,
                f"staged static tiebreaks {layer}:{tenor} research_sleeve_worst_1pct_mean_return",
            ),
        }
    if set(records) != set(rules):
        missing = sorted(set(rules).difference(records))
        _fail(f"staged static tiebreaks is missing locked rule rows: {missing}")
    return records


def _validate_locked_winner(
    decision_row: Any,
    evaluations: tuple[SignalEvaluationRecord, ...],
    tiebreaks: Mapping[tuple[str, int], Mapping[str, float]],
) -> None:
    qualified = [item for item in evaluations if item.qualifies]
    status = str(decision_row.decision_status)
    if not qualified:
        if status != "NO_TRADE":
            _fail("locked winner ranking found no qualified candidate for a TRADE decision")
        return
    if status != "TRADE":
        _fail("locked winner ranking found qualified candidates for a NO_TRADE decision")

    def ranking(item: SignalEvaluationRecord) -> tuple[float, int, float, float, float, int]:
        if item.target_size_pct_nav is None:
            _fail(f"qualified {item.evaluation_key} is missing its locked target size")
        metrics = tiebreaks[(item.signal_layer, item.tenor_days)]
        return (
            item.target_size_pct_nav,
            1 if item.signal_layer == "CORE" else 0,
            metrics["continuous_quality_score"],
            metrics["research_sleeve_win_rate_pct"],
            metrics["research_sleeve_worst_1pct_mean_return"],
            item.tenor_days,
        )

    winner = max(qualified, key=ranking)
    if pd.isna(getattr(decision_row, "layer", None)) or pd.isna(
        getattr(decision_row, "tenor", None)
    ):
        _fail("TRADE decision is missing its locked winner key")
    selected_key = f"{str(decision_row.layer).upper()}:{int(decision_row.tenor)}"
    if selected_key != winner.evaluation_key:
        _fail(
            "staged TRADE decision is not the winner under locked selection.order: "
            f"expected={winner.evaluation_key}, observed={selected_key}"
        )
    winner_metrics = tiebreaks[(winner.signal_layer, winner.tenor_days)]
    for field, expected in winner_metrics.items():
        observed = _float(getattr(decision_row, field, None), f"decision {field}")
        if not math.isclose(observed, expected, rel_tol=1e-12, abs_tol=1e-12):
            _fail(f"TRADE decision {field} does not match the staged static tiebreak row")


def _evaluation(row: Any, layer: str) -> SignalEvaluationRecord:
    prefix = layer.lower()
    tenor = int(row.tenor)
    bucket = _bucket(tenor)
    active = _bool(getattr(row, f"{prefix}_rule_exists"), f"{layer} rule_exists")
    key = f"{layer.upper()}:{tenor}"
    if not active:
        inactive_fields = (
            f"{prefix}_bucket",
            f"{prefix}_threshold_vrp",
            f"{prefix}_threshold_z3",
            f"{prefix}_threshold_z1",
            f"{prefix}_threshold_rsi",
            f"{prefix}_threshold_rv",
            f"{prefix}_size_pct_nav",
        )
        if any(not pd.isna(getattr(row, name)) for name in inactive_fields):
            _fail(f"{key} inactive rule contains bucket, threshold, or size values")
        if _bool(getattr(row, f"{prefix}_pass"), f"{key} stored pass"):
            _fail(f"{key} inactive rule cannot pass")
        legacy_reason = str(getattr(row, f"{prefix}_failure_reason"))
        if legacy_reason.strip().lower() != "inactive":
            _fail(f"{key} inactive rule has a contradictory failure reason")
        return SignalEvaluationRecord(
            key, tenor, bucket, layer.upper(), "INACTIVE", False,
            None, None, None, None, None, {}, {"rule_exists": False},
            ("RULE_INACTIVE",), None, None, None,
            {"legacy_failure_reason": legacy_reason},
        )
    observed_bucket = str(getattr(row, f"{prefix}_bucket")).upper()
    if observed_bucket != bucket:
        _fail(f"{key} stored bucket does not match the deterministic tenor bucket")
    passes: dict[str, bool] = {}
    thresholds: dict[str, float | None] = {}
    comparisons: dict[str, Any] = {"rule_exists": True}
    failed: list[str] = []
    for code, pass_name, value_name, threshold_name, operator in CHECK_ORDER:
        value = _float(getattr(row, value_name), f"{key} {value_name}")
        threshold = _float(getattr(row, f"{prefix}_{threshold_name}"), f"{key} {threshold_name}")
        passed = value > threshold if operator == ">" else value < threshold
        passes[pass_name] = passed
        thresholds[threshold_name.removeprefix("threshold_")] = threshold
        comparisons[code] = {"value": value, "operator": operator, "threshold": threshold, "passed": passed}
        if not passed:
            failed.append(code)
    qualifies = not failed
    stored_pass = _bool(getattr(row, f"{prefix}_pass"), f"{key} stored pass")
    if stored_pass != qualifies:
        _fail(f"{key} stored qualification does not match normalized checks")
    legacy_reason = str(getattr(row, f"{prefix}_failure_reason"))
    if qualifies and legacy_reason != "PASS":
        _fail(f"{key} qualified rule must have legacy failure reason PASS")
    if not qualifies and (not legacy_reason.strip() or legacy_reason == "PASS"):
        _fail(f"{key} non-qualifying rule must retain a non-empty failure reason")
    size_value = getattr(row, f"{prefix}_size_pct_nav")
    if pd.isna(size_value):
        _fail(f"{key} active rule is missing its target size")
    size = _float(size_value, f"{key} size")
    if not 0 < size <= 1:
        _fail(f"{key} active rule size must be in (0, 1]")
    return SignalEvaluationRecord(
        evaluation_key=key,
        tenor_days=tenor,
        tenor_bucket=bucket,
        signal_layer=layer.upper(),
        evaluation_status="QUALIFIED" if qualifies else "NOT_QUALIFIED",
        qualifies=qualifies,
        vrp_pass=passes["vrp_pass"],
        zscore_3m_pass=passes["zscore_3m_pass"],
        zscore_1y_pass=passes["zscore_1y_pass"],
        rsi14_pass=passes["rsi14_pass"],
        rv21d_pass=passes["rv21d_pass"],
        threshold_values=thresholds,
        comparison_results=comparisons,
        failed_checks=tuple(failed),
        rank_position=None,
        rank_score=None,
        target_size_pct_nav=size,
        details={"legacy_failure_reason": legacy_reason},
    )


def _selected_signal(
    decision_row: Any,
    evaluations: tuple[SignalEvaluationRecord, ...],
    approved_nav: float,
) -> SelectedSignalRecord:
    status = str(decision_row.decision_status)
    selection_rule = str(decision_row.selection_rule)
    decision_nav = _float(decision_row.approved_nav_dollars, "decision approved_nav_dollars")
    if not math.isclose(decision_nav, approved_nav, rel_tol=0, abs_tol=0):
        _fail("decision approved NAV does not match the completed run")
    qualified = [evaluation for evaluation in evaluations if evaluation.qualifies]
    states = {
        evaluation.evaluation_key: {
            "status": evaluation.evaluation_status,
            "failed_checks": list(evaluation.failed_checks),
            "target_size_pct_nav": evaluation.target_size_pct_nav,
        }
        for evaluation in evaluations
    }
    if status == "NO_TRADE":
        if qualified:
            _fail("NO_TRADE decision conflicts with qualified Core/Secondary evaluations")
        reason = "NO_CORE_OR_SECONDARY_SIGNAL_QUALIFIED"
        return SelectedSignalRecord(
            decision="NO_TRADE",
            signal_state="EOD_OFFICIAL",
            selection_rule_id=selection_rule,
            selected_evaluation_key=None,
            no_trade_reason=reason,
            approved_nav_dollars=approved_nav,
            target_max_risk_dollars=None,
            selection_trace={
                "outcome": reason,
                "qualified_candidates": [],
                "evaluation_states": states,
                "tie_break_applied": False,
            },
        )
    if status != "TRADE":
        _fail(f"unsupported decision_status: {status!r}")
    if pd.isna(decision_row.tenor) or pd.isna(decision_row.layer):
        _fail("TRADE decision is missing layer or tenor")
    key = f"{str(decision_row.layer).upper()}:{int(decision_row.tenor)}"
    selected = next((evaluation for evaluation in qualified if evaluation.evaluation_key == key), None)
    if selected is None:
        _fail("TRADE decision does not identify a qualifying normalized evaluation")
    marked = [evaluation.evaluation_key for evaluation in evaluations if evaluation.qualifies]
    target_risk = _float(decision_row.target_max_risk_dollars, "target_max_risk_dollars")
    decision_size = _float(decision_row.size_pct_nav, "decision size_pct_nav")
    if selected.target_size_pct_nav is None or not math.isclose(
        decision_size, selected.target_size_pct_nav, rel_tol=0, abs_tol=1e-12
    ):
        _fail("TRADE decision size does not match the selected evaluation")
    if not math.isclose(target_risk, approved_nav * decision_size, rel_tol=1e-12, abs_tol=0.01):
        _fail("TRADE target risk does not equal approved NAV times selected size")
    return SelectedSignalRecord(
        decision="TRADE",
        signal_state="EOD_OFFICIAL",
        selection_rule_id=selection_rule,
        selected_evaluation_key=key,
        no_trade_reason=None,
        approved_nav_dollars=approved_nav,
        target_max_risk_dollars=target_risk,
        selection_trace={
            "selected": key,
            "qualified_candidates": marked,
            "legacy_selection_reason": None if pd.isna(decision_row.selection_reason) else str(decision_row.selection_reason),
            "continuous_quality_score": _optional_float(
                getattr(decision_row, "continuous_quality_score", None),
                "continuous_quality_score",
            ),
            "research_sleeve_win_rate_pct": _optional_float(
                getattr(decision_row, "research_sleeve_win_rate_pct", None),
                "research_sleeve_win_rate_pct",
            ),
            "research_sleeve_worst_1pct_mean_return": _optional_float(
                getattr(decision_row, "research_sleeve_worst_1pct_mean_return", None),
                "research_sleeve_worst_1pct_mean_return",
            ),
            "evaluation_states": states,
            "tie_break_applied": len(marked) > 1,
        },
    )


def _validate_selection_markers(target: pd.DataFrame, decision_row: Any) -> None:
    _require_columns(
        target,
        ("selected_trade", "selected_layer", "selected_tenor"),
        "target signal grid",
    )
    marked = target.loc[target["selected_trade"].map(lambda value: _bool(value, "selected_trade"))]
    if str(decision_row.decision_status) == "NO_TRADE":
        if not marked.empty:
            _fail("NO_TRADE target grid contains a selected_trade marker")
        if target["selected_layer"].notna().any() or target["selected_tenor"].notna().any():
            _fail("NO_TRADE target grid contains selected layer/tenor markers")
        return
    if len(marked) != 1:
        _fail("TRADE target grid must contain exactly one selected_trade marker")
    marked_row = marked.iloc[0]
    decision_tenor = int(decision_row.tenor)
    decision_layer = str(decision_row.layer)
    if int(marked_row["tenor"]) != decision_tenor:
        _fail("selected_trade marker tenor does not match decision")
    if target["selected_tenor"].isna().any() or target["selected_layer"].isna().any():
        _fail("TRADE target grid must duplicate selection markers across all nine rows")
    selected_tenors = target["selected_tenor"].tolist()
    selected_layers = target["selected_layer"].astype(str).tolist()
    if selected_tenors != [decision_tenor] * 9 or selected_layers != [decision_layer] * 9:
        _fail("target selected layer/tenor markers do not match decision")


def _optional_float(value: Any, label: str) -> float | None:
    return None if value is None or pd.isna(value) else _float(value, label)


def _is_null_scalar(value: Any) -> bool:
    if value is None:
        return True
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return (isinstance(result, bool) or type(result).__name__ == "bool_") and bool(result)


def _manifest_scalar_matches(left: Any, right: Any) -> bool:
    left_null = _is_null_scalar(left)
    right_null = _is_null_scalar(right)
    if left_null or right_null:
        return left_null and right_null
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is bool and type(right) is bool and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)
    return str(left) == str(right)


def _reconcile_manifest_decision(
    manifest_row: Mapping[str, Any],
    decision_row: Mapping[str, Any],
) -> None:
    if set(manifest_row) != set(decision_row):
        missing = sorted(set(decision_row).difference(manifest_row))
        extra = sorted(set(manifest_row).difference(decision_row))
        _fail(
            "publish manifest latest decision schema disagrees with the staged "
            f"decision: missing={missing}, extra={extra}"
        )
    for key, expected in decision_row.items():
        observed = manifest_row[key]
        if key == "date":
            if _as_date(observed, "manifest decision date") != _as_date(
                expected, "staged decision date"
            ):
                _fail("publish manifest latest decision date disagrees with the staged decision")
        elif not _manifest_scalar_matches(observed, expected):
            _fail(
                f"publish manifest latest decision {key} disagrees with the staged decision"
            )


NO_TRADE_NULL_FIELDS = (
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
)


def _validate_trade_decision_payload(
    target: pd.DataFrame,
    decision_row: Any,
    evaluations: tuple[SignalEvaluationRecord, ...],
) -> None:
    if str(decision_row.decision_status) != "TRADE":
        non_null = [
            field
            for field in NO_TRADE_NULL_FIELDS
            if not _is_null_scalar(getattr(decision_row, field, None))
        ]
        if non_null:
            _fail(f"NO_TRADE decision contains trade-only values: {non_null}")
        return
    tenor = int(decision_row.tenor)
    layer = str(decision_row.layer).upper()
    target_rows = target.loc[target["tenor"].astype(int).eq(tenor)]
    if len(target_rows) != 1:
        _fail("TRADE decision tenor is not unique in the target signal grid")
    signal = target_rows.iloc[0]
    evaluation = next(
        (item for item in evaluations if item.evaluation_key == f"{layer}:{tenor}"),
        None,
    )
    if evaluation is None:
        _fail("TRADE decision has no corresponding normalized evaluation")
    if str(decision_row.bucket).upper() != evaluation.tenor_bucket:
        _fail("TRADE decision bucket does not match the normalized tenor bucket")
    numeric_pairs = (
        ("implied_variance", "implied_variance"),
        ("implied_vol_pct", "implied_vol_pct"),
        ("forecast_variance_candidate", "forecast_variance_candidate"),
        ("forecast_vol_pct", "forecast_vol_pct"),
        ("model_vrp_log", "model_vrp_log"),
        ("z_3m", "z_3m"),
        ("z_1y", "z_1y"),
        ("rsi14", "rsi14"),
        ("rv21d_vol_pct", "rv21d_vol_pct"),
    )
    for decision_name, signal_name in numeric_pairs:
        expected = _float(signal[signal_name], f"target {signal_name}")
        observed = _float(getattr(decision_row, decision_name), f"decision {decision_name}")
        if not math.isclose(observed, expected, rel_tol=1e-10, abs_tol=1e-12):
            _fail(f"TRADE decision {decision_name} does not match the target signal row")
    prefix = layer.lower()
    for short_name in ("vrp", "z3", "z1", "rsi", "rv"):
        expected = _float(signal[f"{prefix}_threshold_{short_name}"], f"target threshold_{short_name}")
        observed = _float(getattr(decision_row, f"threshold_{short_name}"), f"decision threshold_{short_name}")
        if not math.isclose(observed, expected, rel_tol=1e-10, abs_tol=1e-12):
            _fail(f"TRADE decision threshold_{short_name} does not match its evaluation")


def load_staged_eod_snapshot(
    run_dir: Path,
    source_root: Path,
    *,
    fixture_path: Path | None = None,
    zscore_atol: float = 1e-12,
    zscore_rtol: float = 1e-10,
) -> EodSnapshot:
    """Load and validate one explicit successful run without writing anything."""

    run_dir = Path(run_dir).resolve()
    source_root = Path(source_root).resolve()
    if not run_dir.is_dir():
        _fail(f"run directory does not exist: {run_dir}")
    staging = run_dir / STAGING_DIR_NAME
    fixed_paths = {
        "run_manifest": run_dir / RUN_MANIFEST_NAME,
        "publish_manifest": staging / PUBLISH_MANIFEST_NAME,
        "signal_history": staging / SIGNAL_HISTORY_NAME,
        "latest_snapshot": staging / LATEST_SNAPSHOT_NAME,
        "selected_decisions": staging / SELECTED_DECISIONS_NAME,
        "forecast_history": staging / FORECAST_HISTORY_NAME,
        "static_tiebreaks": staging / STATIC_TIEBREAKS_NAME,
    }
    for name, path in fixed_paths.items():
        if not path.is_file():
            _fail(f"missing fixed {name} artifact: {path}")
    initial_hashes = {name: _hash(path) for name, path in fixed_paths.items()}

    run_manifest = _read_json(fixed_paths["run_manifest"], "run manifest")
    publish_manifest = _read_json(fixed_paths["publish_manifest"], "publish manifest")
    if run_manifest.get("status") != "PASS" or run_manifest.get("final_health") != "PASS":
        _fail("run manifest must have status=PASS and final_health=PASS")
    valuation_date = _as_date(run_manifest.get("target_date"), "run target_date")
    try:
        sofr_evidence = load_sofr_updater_evidence(run_manifest, valuation_date)
    except SofrEvidenceError as exc:
        _fail(f"SOFR updater evidence failed validation: {exc}")
    publish_date = _as_date(publish_manifest.get("target_date"), "publish target_date")
    if valuation_date != publish_date:
        _fail("run and publish target dates disagree")
    lock_id = str(run_manifest.get("release_id", ""))
    if not lock_id or publish_manifest.get("release_id") != lock_id:
        _fail("run and publish lock IDs disagree")
    approved_nav = _float(run_manifest.get("approved_nav"), "approved_nav", positive=True)
    if not math.isclose(approved_nav, _float(publish_manifest.get("approved_nav"), "publish approved_nav"), rel_tol=0, abs_tol=0):
        _fail("run and publish approved NAV values disagree")
    manifest_decision = publish_manifest.get("latest_decision")
    if not isinstance(manifest_decision, list) or len(manifest_decision) != 1:
        _fail("publish manifest must contain exactly one latest_decision")
    manifest_decision_row = manifest_decision[0]
    if not isinstance(manifest_decision_row, dict):
        _fail("publish manifest latest_decision must be an object")
    if _as_date(manifest_decision_row.get("date"), "manifest decision date") != valuation_date:
        _fail("publish manifest latest decision date disagrees with the run target")
    if manifest_decision_row.get("lock_id") != lock_id:
        _fail("publish manifest latest decision lock ID disagrees with the run")
    _reconcile_manifest_paths(run_manifest, publish_manifest, fixed_paths)

    signals = pd.read_parquet(fixed_paths["signal_history"])
    latest = pd.read_parquet(fixed_paths["latest_snapshot"])
    decisions = pd.read_parquet(fixed_paths["selected_decisions"])
    forecast_history = pd.read_parquet(fixed_paths["forecast_history"])
    static_tiebreaks = pd.read_csv(fixed_paths["static_tiebreaks"])
    _normalize_tenors(signals, "signal history")
    _normalize_tenors(latest, "latest snapshot")
    _normalize_tenors(decisions, "decision history", allow_null=True)
    _normalize_tenors(forecast_history, "forecast history")
    _validate_full_history_grids(signals, forecast_history, decisions)
    declared_counts = publish_manifest.get("row_counts")
    if not isinstance(declared_counts, dict):
        _fail("publish manifest row_counts must be an object")
    observed_counts = {
        "signal_history": len(signals),
        "latest_snapshot": len(latest),
        "decision_history": len(decisions),
        "forecast_history": len(forecast_history),
    }
    missing_counts = sorted(set(observed_counts).difference(declared_counts))
    if missing_counts:
        _fail(f"publish manifest row_counts is missing required keys: {missing_counts}")
    for key, observed in observed_counts.items():
        declared = _float(declared_counts[key], f"row_counts.{key}")
        if not declared.is_integer() or int(declared) != observed:
            _fail(f"publish manifest row count for {key} does not match the artifact")
    target, decision = _target_frames(signals, latest, decisions, valuation_date)
    _normalize_tenors(target, "target signal grid", column="selected_tenor", allow_null=True)
    _require_columns(forecast_history, ("date", "tenor"), "forecast history")
    forecast_dates = pd.to_datetime(forecast_history["date"], errors="raise").dt.date
    if forecast_history.empty or max(forecast_dates) != valuation_date:
        _fail("forecast history does not end on the run target date")
    forecast_target_tenors = sorted(
        forecast_history.loc[forecast_dates.eq(valuation_date), "tenor"].astype(int).tolist()
    )
    if forecast_target_tenors != list(TARGET_TENORS):
        _fail("forecast history target date is not the exact nine-tenor grid")
    forecast_target = forecast_history.loc[forecast_dates.eq(valuation_date)].sort_values("tenor")
    forecast_columns = (
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "forecast_vol_pct",
        "forecast_model",
    )
    _require_columns(forecast_target, forecast_columns, "forecast history target grid")
    _require_columns(
        target,
        (
            "tenor", "predicted_log_variance_candidate", "forecast_variance_candidate",
            "forecast_vol_pct", "implied_variance", "implied_vol_pct", "model_vrp_log",
            "z_3m", "z_1y", "prior_63_rows", "prior_252_rows", "spy_close",
            "rsi14", "rv21d_vol_pct", "lock_id", "core_rule_exists",
            "rsi_formula_version",
            "secondary_rule_exists", "selected_trade", "selected_layer", "selected_tenor",
            "core_bucket", "core_threshold_vrp", "core_threshold_z3",
            "core_threshold_z1", "core_threshold_rsi", "core_threshold_rv",
            "core_size_pct_nav", "core_pass", "core_failure_reason",
            "secondary_bucket", "secondary_threshold_vrp", "secondary_threshold_z3",
            "secondary_threshold_z1", "secondary_threshold_rsi", "secondary_threshold_rv",
            "secondary_size_pct_nav", "secondary_pass", "secondary_failure_reason",
        ),
        "target signal grid",
    )
    observed_locks = set(target["lock_id"].dropna().astype(str)) | set(decision["lock_id"].dropna().astype(str))
    if observed_locks != {lock_id}:
        _fail(f"target output lock IDs disagree: {sorted(observed_locks)}")
    decision_row = next(decision.itertuples(index=False))
    _reconcile_manifest_decision(
        manifest_decision_row,
        decision.iloc[0].to_dict(),
    )
    _validate_selection_markers(target, decision_row)
    stats = _prior_stats(signals, target, valuation_date, atol=zscore_atol, rtol=zscore_rtol)
    joined_forecast = target[["tenor", *forecast_columns]].merge(
        forecast_target[["tenor", *forecast_columns]],
        on="tenor",
        how="inner",
        suffixes=("_signal", "_forecast"),
        validate="one_to_one",
    )
    if len(joined_forecast) != 9:
        _fail("forecast history does not reconcile one-to-one to target signals")
    for row in joined_forecast.itertuples(index=False):
        tenor = int(row.tenor)
        for column in forecast_columns:
            left = getattr(row, f"{column}_signal")
            right = getattr(row, f"{column}_forecast")
            if column == "forecast_model":
                if str(left) != str(right):
                    _fail(f"{tenor}D forecast model disagrees between staged artifacts")
            elif not math.isclose(
                _float(left, f"{tenor}D signal {column}"),
                _float(right, f"{tenor}D forecast {column}"),
                rel_tol=1e-10,
                abs_tol=1e-12,
            ):
                _fail(f"{tenor}D {column} disagrees between staged artifacts")

    fixture = (fixture_path or (Path(__file__).resolve().parents[3] / DEFAULT_FIXTURE_REL)).resolve()
    mismatches, golden_manifest = verify_golden_contract_with_manifest(
        source_root,
        fixture,
        signal_history_path=fixed_paths["signal_history"],
        selected_decisions_path=fixed_paths["selected_decisions"],
    )
    if mismatches or golden_manifest.get("status") != "PASS" or golden_manifest.get("mode") != "STAGED":
        _fail("staged golden verification failed: " + "; ".join(mismatches[:10]))
    golden = GoldenVerificationEvidence(
        status="PASS",
        verification_id=str(golden_manifest["verification_id"]),
        fixture_path=fixture,
        fixture_sha256=str(golden_manifest["fixture"]["sha256"]),
        signal_history_sha256=str(golden_manifest["artifacts"]["signal_history"]["sha256"]),
        selected_decisions_sha256=str(golden_manifest["artifacts"]["selected_decisions"]["sha256"]),
        manifest=golden_manifest,
    )

    configuration_identity, runtime_payload, runtime_path = _configuration_identity(
        publish_manifest.get("runtime_config"),
        publish_manifest.get("lock_config"),
        lock_id,
        approved_nav,
    )
    production_payload = configuration_identity.payload["production_configuration"]
    locked_rules = _validate_locked_production_configuration(
        target,
        decision_row,
        production_payload,
    )
    tiebreaks = _static_tiebreak_map(static_tiebreaks, locked_rules)
    accepted_rsi_versions = runtime_payload.get("accepted_rsi_versions")
    if (
        not isinstance(accepted_rsi_versions, list)
        or not accepted_rsi_versions
        or not all(isinstance(value, str) and value for value in accepted_rsi_versions)
    ):
        _fail("runtime configuration must provide accepted_rsi_versions")
    observed_rsi_versions = set(target["rsi_formula_version"].dropna().astype(str))
    if len(observed_rsi_versions) != 1 or not observed_rsi_versions.issubset(
        set(accepted_rsi_versions)
    ):
        _fail("target RSI formula version is not one accepted runtime version")
    model_lock_path = _resolve_model_lock_path(runtime_payload, runtime_path, source_root)
    model_identity = _versioned_document(model_lock_path, "hybrid_v2_model_lock", lock_id)
    observed_model_lock = model_identity.payload.get("lock_id") or model_identity.payload.get("release_id")
    if observed_model_lock != lock_id:
        _fail("canonical model-lock document does not match the completed run lock ID")
    generated_at = _as_utc_datetime(publish_manifest.get("generated_at"), "generated_at")
    finished_at = _as_utc_datetime(run_manifest.get("finished_at"), "finished_at")
    snapshot_at = _official_xnys_close(valuation_date)
    if generated_at < snapshot_at:
        _fail("publish artifact was generated before the official XNYS close")
    if finished_at < generated_at:
        _fail("completed run finished before its staged publish artifact was generated")
    if run_manifest.get("started_at") is not None:
        started_at = _as_utc_datetime(run_manifest.get("started_at"), "started_at")
        if started_at > generated_at:
            _fail("completed run started after its staged publish artifact was generated")
        if started_at > finished_at:
            _fail("completed run started after it finished")
    data_cutoff_at = snapshot_at
    spy_price = _same_float(target["spy_close"], "spy_close")
    market = MarketSnapshotRecord(
        valuation_date=valuation_date,
        snapshot_at=snapshot_at,
        data_cutoff_at=data_cutoff_at,
        snapshot_kind="EOD_OFFICIAL",
        market_session="CLOSED",
        freshness_status="FRESH",
        spy_price=spy_price,
        details={
            "cutoff_semantics": "official_xnys_session_close",
            "calendar": "XNYS",
        },
    )

    implied: list[ImpliedVarianceRecord] = []
    forecasts: list[ForecastVarianceRecord] = []
    features: list[SignalFeatureRecord] = []
    evaluations: list[SignalEvaluationRecord] = []
    for row in target.itertuples(index=False):
        tenor = int(row.tenor)
        implied_variance = _float(row.implied_variance, f"{tenor}D implied variance", positive=True)
        forecast_variance = _float(row.forecast_variance_candidate, f"{tenor}D forecast variance", positive=True)
        vrp = _float(row.model_vrp_log, f"{tenor}D VRP")
        implied_vol = _float(row.implied_vol_pct, f"{tenor}D implied vol", positive=True)
        forecast_vol = _float(row.forecast_vol_pct, f"{tenor}D forecast vol", positive=True)
        predicted_log = _float(row.predicted_log_variance_candidate, f"{tenor}D predicted log variance")
        if not math.isclose(vrp, math.log(implied_variance / forecast_variance), rel_tol=1e-10, abs_tol=1e-12):
            _fail(f"{tenor}D stored VRP does not equal log(implied/forecast)")
        if not math.isclose(implied_vol, 100.0 * math.sqrt(implied_variance), rel_tol=1e-10, abs_tol=1e-12):
            _fail(f"{tenor}D implied volatility does not equal 100*sqrt(variance)")
        if not math.isclose(forecast_vol, 100.0 * math.sqrt(forecast_variance), rel_tol=1e-10, abs_tol=1e-12):
            _fail(f"{tenor}D forecast volatility does not equal 100*sqrt(variance)")
        if not math.isclose(predicted_log, math.log(forecast_variance), rel_tol=1e-10, abs_tol=1e-12):
            _fail(f"{tenor}D predicted log variance does not equal log(forecast variance)")
        expiration = valuation_date + timedelta(days=tenor)
        horizon_metadata = {
            "expiration_semantics": "synthetic_target_horizon_not_option_expiration",
            "target_horizon_rule": "valuation_date_plus_tenor_calendar_days",
            "effective_dte_rule": "locked_target_tenor_days",
        }
        implied.append(ImpliedVarianceRecord(
            tenor, expiration, float(tenor), implied_variance,
            implied_vol,
            quality_details=horizon_metadata,
        ))
        forecasts.append(ForecastVarianceRecord(
            tenor, valuation_date,
            predicted_log,
            forecast_variance,
            forecast_vol,
            quality_details={"forecast_model": str(row.forecast_model)},
        ))
        mean63, std63, mean252, std252, history_through = stats[tenor]
        rv_vol = _float(row.rv21d_vol_pct, "rv21d_vol_pct")
        features.append(SignalFeatureRecord(
            tenor_days=tenor,
            tenor_bucket=_bucket(tenor),
            vrp_log=vrp,
            vrp_3m_prior_mean=mean63,
            vrp_3m_prior_sample_std=std63,
            vrp_1y_prior_mean=mean252,
            vrp_1y_prior_sample_std=std252,
            zscore_3m=_float(row.z_3m, "z_3m"),
            zscore_1y=_float(row.z_1y, "z_1y"),
            rsi14=_float(row.rsi14, "rsi14"),
            rv21d_variance=(rv_vol / 100.0) ** 2,
            rv21d_volatility_pct=rv_vol,
            zscore_3m_sample_count=63,
            zscore_1y_sample_count=252,
            history_through_date=history_through,
            details={
                "history_semantics": "strictly_prior_signal_observations",
                "rv21d_variance_formula": "(rv21d_volatility_pct / 100)^2",
            },
        ))
        evaluations.extend((_evaluation(row, "Core"), _evaluation(row, "Secondary")))

    evaluation_tuple = tuple(evaluations)
    _validate_trade_decision_payload(target, decision_row, evaluation_tuple)
    _validate_locked_winner(decision_row, evaluation_tuple, tiebreaks)
    if str(decision_row.decision_status) == "TRADE":
        selected_key = f"{str(decision_row.layer).upper()}:{int(decision_row.tenor)}"
        rank_score = _optional_float(
            getattr(decision_row, "continuous_quality_score", None),
            "continuous_quality_score",
        )
        evaluation_tuple = tuple(
            replace(item, rank_position=1, rank_score=rank_score)
            if item.evaluation_key == selected_key
            else item
            for item in evaluation_tuple
        )
    selected = _selected_signal(decision_row, evaluation_tuple, approved_nav)
    row_counts = {
        "signal_history": len(signals),
        "latest_snapshot": len(latest),
        "selected_decisions": len(decisions),
        "forecast_history": len(forecast_history),
        "static_tiebreaks": len(static_tiebreaks),
    }
    relative_paths = {
        "run_manifest": RUN_MANIFEST_NAME,
        "publish_manifest": f"{STAGING_DIR_NAME}/{PUBLISH_MANIFEST_NAME}",
        "signal_history": f"{STAGING_DIR_NAME}/{SIGNAL_HISTORY_NAME}",
        "latest_snapshot": f"{STAGING_DIR_NAME}/{LATEST_SNAPSHOT_NAME}",
        "selected_decisions": f"{STAGING_DIR_NAME}/{SELECTED_DECISIONS_NAME}",
        "forecast_history": f"{STAGING_DIR_NAME}/{FORECAST_HISTORY_NAME}",
        "static_tiebreaks": f"{STAGING_DIR_NAME}/{STATIC_TIEBREAKS_NAME}",
    }
    final_hashes = {name: _hash(path) for name, path in fixed_paths.items()}
    if final_hashes != initial_hashes:
        _fail("fixed staged artifacts changed while the EOD contract was being loaded")
    if _hash(model_identity.path) != model_identity.sha256:
        _fail("model-lock document changed while the EOD contract was being loaded")
    if _hash(configuration_identity.path) != configuration_identity.payload[
        "runtime_configuration_sha256"
    ]:
        _fail("runtime configuration changed while the EOD contract was being loaded")
    production_path = Path(str(publish_manifest["lock_config"])).resolve()
    if _hash(production_path) != configuration_identity.payload[
        "production_configuration_sha256"
    ]:
        _fail("production configuration changed while the EOD contract was being loaded")
    if _hash(fixture) != golden.fixture_sha256:
        _fail("golden fixture changed during staged verification")
    if golden.signal_history_sha256 != final_hashes["signal_history"]:
        _fail("golden evidence is not bound to the final signal-history bytes")
    if golden.selected_decisions_sha256 != final_hashes["selected_decisions"]:
        _fail("golden evidence is not bound to the final selected-decision bytes")
    if _hash(sofr_evidence.updater_manifest_path) != sofr_evidence.updater_manifest_sha256:
        _fail("SOFR updater manifest changed after evidence validation")
    if _hash(sofr_evidence.refreshed_snapshot_path) != sofr_evidence.refreshed_snapshot_sha256:
        _fail("SOFR refreshed snapshot changed after evidence validation")
    artifact_coverage = {
        "signal_history": _artifact_date_coverage(signals, "signal history"),
        "latest_snapshot": _artifact_date_coverage(latest, "latest snapshot"),
        "selected_decisions": _artifact_date_coverage(decisions, "decision history"),
        "forecast_history": _artifact_date_coverage(forecast_history, "forecast history"),
    }
    artifacts = tuple(
        _artifact(
            fixed_paths[key],
            key,
            relative_paths[key],
            row_counts.get(key),
            identity_input=key not in {"run_manifest", "publish_manifest"},
            verified_sha256=final_hashes[key],
            trade_date_start=(artifact_coverage[key][0] if key in artifact_coverage else None),
            trade_date_end=(artifact_coverage[key][1] if key in artifact_coverage else None),
        )
        for key in (
            "run_manifest", "publish_manifest", "signal_history", "latest_snapshot",
            "selected_decisions", "forecast_history", "static_tiebreaks",
        )
    ) + (
        _artifact(
            sofr_evidence.updater_manifest_path,
            "sofr_update_manifest",
            "sofr_evidence/sofr_update_manifest.json",
            verified_sha256=sofr_evidence.updater_manifest_sha256,
            metadata={
                "source": "FRED_SOFR",
                "evidence_role": "updater_manifest",
                "immutable_after_validation": True,
            },
        ),
        _artifact(
            sofr_evidence.refreshed_snapshot_path,
            "sofr_refreshed_snapshot",
            "sofr_evidence/fred_sofr_history_refreshed_snapshot.csv",
            row_count=sofr_evidence.row_count,
            verified_sha256=sofr_evidence.refreshed_snapshot_sha256,
            metadata={
                "source": "FRED_SOFR",
                "evidence_role": "frozen_refreshed_snapshot",
                "normalized_content_sha256": sofr_evidence.normalized_content_sha256,
                "observation_start_date": sofr_evidence.start_date.isoformat(),
                "observation_end_date": sofr_evidence.end_date.isoformat(),
                "immutable_after_validation": True,
            },
        ),
    )
    fingerprint = _canonical_hash({
        "valuation_date": valuation_date.isoformat(),
        "lock_id": lock_id,
        "golden_verification_id": golden.verification_id,
        "artifacts": [
            (item.logical_name, item.sha256) for item in artifacts if item.identity_input
        ],
        "model_sha256": model_identity.sha256,
        "configuration_sha256": configuration_identity.sha256,
        "decision": selected.decision,
        "selected_evaluation_key": selected.selected_evaluation_key,
        "no_trade_reason": selected.no_trade_reason,
    })
    return EodSnapshot(
        run_dir=run_dir,
        valuation_date=valuation_date,
        lock_id=lock_id,
        approved_nav=approved_nav,
        run_manifest=run_manifest,
        publish_manifest=publish_manifest,
        model_identity=model_identity,
        configuration_identity=configuration_identity,
        sofr_evidence=sofr_evidence,
        market_snapshot=market,
        implied_variance=tuple(implied),
        forecast_variance=tuple(forecasts),
        signal_features=tuple(features),
        signal_evaluations=evaluation_tuple,
        selected_signal=selected,
        artifacts=artifacts,
        golden_evidence=golden,
        output_fingerprint=fingerprint,
    )
