"""Validated, database-shaped values extracted from a completed EOD run.

The records in this module deliberately contain no persistence behaviour.  They
are the immutable hand-off between the accepted file pipeline and the
PostgreSQL shadow writer.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from .sofr_evidence import SofrUpdaterEvidence


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TARGET_TENORS = (9, 12, 15, 18, 21, 24, 27, 30, 33)


def _nonempty(value: str, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")


def _sha256(value: str, label: str) -> None:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _finite(value: float | None, label: str, *, positive: bool = False) -> None:
    if value is None:
        return
    if not math.isfinite(float(value)) or (positive and float(value) <= 0):
        qualifier = "finite and positive" if positive else "finite"
        raise ValueError(f"{label} must be {qualifier}")


@dataclass(frozen=True)
class ArtifactMetadata:
    logical_name: str
    path: Path
    asset_format: str
    sha256: str
    byte_size: int
    row_count: int | None = None
    relative_path: str = ""
    identity_input: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)
    trade_date_start: date | None = None
    trade_date_end: date | None = None

    def __post_init__(self) -> None:
        _nonempty(self.logical_name, "logical_name")
        if not self.path.is_absolute():
            raise ValueError("artifact path must be absolute")
        if self.asset_format not in {"PARQUET", "CSV", "JSON"}:
            raise ValueError("unsupported artifact format")
        _sha256(self.sha256, "sha256")
        if self.byte_size < 0 or (self.row_count is not None and self.row_count < 0):
            raise ValueError("artifact sizes and row counts cannot be negative")
        _nonempty(self.relative_path, "relative_path")
        if Path(self.relative_path).is_absolute() or ".." in Path(self.relative_path).parts:
            raise ValueError("relative_path must be a safe relative path")
        if (self.trade_date_start is None) != (self.trade_date_end is None):
            raise ValueError("artifact trade-date coverage must provide both bounds")
        if self.trade_date_start is not None and (
            type(self.trade_date_start) is not date
            or type(self.trade_date_end) is not date
        ):
            raise ValueError("artifact trade-date coverage must use dates")
        if (
            self.trade_date_start is not None
            and self.trade_date_end is not None
            and self.trade_date_start > self.trade_date_end
        ):
            raise ValueError("artifact trade-date coverage is reversed")


@dataclass(frozen=True)
class VersionedDocument:
    key: str
    version_label: str
    path: Path
    sha256: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        _nonempty(self.key, "key")
        _nonempty(self.version_label, "version_label")
        if not self.path.is_absolute():
            raise ValueError("versioned document path must be absolute")
        _sha256(self.sha256, "sha256")


@dataclass(frozen=True)
class MarketSnapshotRecord:
    valuation_date: date
    snapshot_at: datetime
    data_cutoff_at: datetime
    snapshot_kind: str
    market_session: str
    freshness_status: str
    spy_price: float
    details: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.snapshot_at.tzinfo is None or self.data_cutoff_at.tzinfo is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        if self.data_cutoff_at > self.snapshot_at:
            raise ValueError("data_cutoff_at cannot be after snapshot_at")
        if self.snapshot_kind != "EOD_OFFICIAL":
            raise ValueError("staged EOD snapshots must be EOD_OFFICIAL")
        if self.market_session != "CLOSED" or self.freshness_status != "FRESH":
            raise ValueError("a successful staged EOD snapshot must be closed and fresh")
        _finite(self.spy_price, "spy_price", positive=True)


@dataclass(frozen=True)
class ImpliedVarianceRecord:
    tenor_days: int
    target_expiration: date
    effective_dte: float
    annualized_variance: float
    annualized_volatility_pct: float
    calculation_status: str = "AVAILABLE"
    quality_status: str = "PASS"
    quality_details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.tenor_days not in TARGET_TENORS:
            raise ValueError("unexpected implied-variance tenor")
        _finite(self.effective_dte, "effective_dte", positive=True)
        _finite(self.annualized_variance, "annualized_variance", positive=True)
        _finite(self.annualized_volatility_pct, "annualized_volatility_pct", positive=True)
        if self.calculation_status != "AVAILABLE" or self.quality_status != "PASS":
            raise ValueError("successful staged implied variance must be AVAILABLE/PASS")


@dataclass(frozen=True)
class ForecastVarianceRecord:
    tenor_days: int
    forecast_as_of_date: date
    predicted_log_variance: float
    annualized_variance: float
    annualized_volatility_pct: float
    calculation_status: str = "AVAILABLE"
    quality_status: str = "PASS"
    quality_details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.tenor_days not in TARGET_TENORS:
            raise ValueError("unexpected forecast tenor")
        for label in (
            "predicted_log_variance",
            "annualized_variance",
            "annualized_volatility_pct",
        ):
            _finite(getattr(self, label), label, positive=label != "predicted_log_variance")
        if self.calculation_status != "AVAILABLE" or self.quality_status != "PASS":
            raise ValueError("successful staged forecast must be AVAILABLE/PASS")


@dataclass(frozen=True)
class SignalFeatureRecord:
    tenor_days: int
    tenor_bucket: str
    vrp_log: float
    vrp_3m_prior_mean: float
    vrp_3m_prior_sample_std: float
    vrp_1y_prior_mean: float
    vrp_1y_prior_sample_std: float
    zscore_3m: float
    zscore_1y: float
    rsi14: float
    rv21d_variance: float
    rv21d_volatility_pct: float
    zscore_3m_sample_count: int
    zscore_1y_sample_count: int
    history_through_date: date
    is_complete: bool = True
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.tenor_days not in TARGET_TENORS:
            raise ValueError("unexpected signal tenor")
        if self.tenor_bucket not in {"FRONT", "MIDDLE", "BACK"}:
            raise ValueError("unexpected tenor bucket")
        for label in (
            "vrp_log", "vrp_3m_prior_mean", "vrp_3m_prior_sample_std",
            "vrp_1y_prior_mean", "vrp_1y_prior_sample_std", "zscore_3m",
            "zscore_1y", "rsi14", "rv21d_variance", "rv21d_volatility_pct",
        ):
            _finite(getattr(self, label), label)
        if self.vrp_3m_prior_sample_std <= 0 or self.vrp_1y_prior_sample_std <= 0:
            raise ValueError("prior sample standard deviations must be positive")
        if not 0 <= self.rsi14 <= 100 or self.rv21d_variance < 0:
            raise ValueError("signal feature is outside its valid range")
        if self.zscore_3m_sample_count != 63 or self.zscore_1y_sample_count != 252:
            raise ValueError("official EOD z-scores require complete 63/252 prior windows")
        if not self.is_complete:
            raise ValueError("successful staged signal features must be complete")


@dataclass(frozen=True)
class SignalEvaluationRecord:
    evaluation_key: str
    tenor_days: int
    tenor_bucket: str
    signal_layer: str
    evaluation_status: str
    qualifies: bool
    vrp_pass: bool | None
    zscore_3m_pass: bool | None
    zscore_1y_pass: bool | None
    rsi14_pass: bool | None
    rv21d_pass: bool | None
    threshold_values: Mapping[str, float | None]
    comparison_results: Mapping[str, Any]
    failed_checks: tuple[str, ...]
    rank_position: int | None
    rank_score: float | None
    target_size_pct_nav: float | None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _nonempty(self.evaluation_key, "evaluation_key")
        if self.tenor_days not in TARGET_TENORS or self.tenor_bucket not in {"FRONT", "MIDDLE", "BACK"}:
            raise ValueError("invalid evaluation tenor or bucket")
        if self.signal_layer not in {"CORE", "SECONDARY"}:
            raise ValueError("only Core and Secondary are normalized by this contract")
        if self.evaluation_status not in {"QUALIFIED", "NOT_QUALIFIED", "INACTIVE"}:
            raise ValueError("invalid evaluation status")
        if self.qualifies != (self.evaluation_status == "QUALIFIED"):
            raise ValueError("qualifies and evaluation_status disagree")
        if self.rank_position is not None and self.rank_position <= 0:
            raise ValueError("rank_position must be positive")
        _finite(self.rank_score, "rank_score")
        if self.target_size_pct_nav is not None and not 0 <= self.target_size_pct_nav <= 1:
            raise ValueError("target_size_pct_nav must be a fraction between zero and one")
        if self.evaluation_status == "INACTIVE":
            if self.failed_checks != ("RULE_INACTIVE",):
                raise ValueError("inactive evaluations must use the deterministic inactive reason")
        elif not self.qualifies and not self.failed_checks:
            raise ValueError("non-qualifying active evaluations require failed checks")


@dataclass(frozen=True)
class SelectedSignalRecord:
    decision: str
    signal_state: str
    selection_rule_id: str
    selected_evaluation_key: str | None
    no_trade_reason: str | None
    approved_nav_dollars: float
    target_max_risk_dollars: float | None
    selection_trace: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.decision not in {"TRADE", "NO_TRADE"} or self.signal_state != "EOD_OFFICIAL":
            raise ValueError("invalid official EOD decision")
        _nonempty(self.selection_rule_id, "selection_rule_id")
        _finite(self.approved_nav_dollars, "approved_nav_dollars", positive=True)
        _finite(self.target_max_risk_dollars, "target_max_risk_dollars")
        if self.decision == "TRADE":
            if self.selected_evaluation_key is None or self.no_trade_reason is not None:
                raise ValueError("TRADE must identify an evaluation and have no no-trade reason")
        elif self.selected_evaluation_key is not None or not self.no_trade_reason:
            raise ValueError("NO_TRADE must have an explicit reason and no selected evaluation")


@dataclass(frozen=True)
class GoldenVerificationEvidence:
    status: str
    verification_id: str
    fixture_path: Path
    fixture_sha256: str
    signal_history_sha256: str
    selected_decisions_sha256: str
    manifest: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.status != "PASS":
            raise ValueError("golden verification must pass")
        for label in (
            "verification_id", "fixture_sha256", "signal_history_sha256",
            "selected_decisions_sha256",
        ):
            _sha256(getattr(self, label), label)
        if not self.fixture_path.is_absolute():
            raise ValueError("fixture path must be absolute")


@dataclass(frozen=True)
class EodSnapshot:
    run_dir: Path
    valuation_date: date
    lock_id: str
    approved_nav: float
    run_manifest: Mapping[str, Any]
    publish_manifest: Mapping[str, Any]
    model_identity: VersionedDocument
    configuration_identity: VersionedDocument
    sofr_evidence: SofrUpdaterEvidence
    market_snapshot: MarketSnapshotRecord
    implied_variance: tuple[ImpliedVarianceRecord, ...]
    forecast_variance: tuple[ForecastVarianceRecord, ...]
    signal_features: tuple[SignalFeatureRecord, ...]
    signal_evaluations: tuple[SignalEvaluationRecord, ...]
    selected_signal: SelectedSignalRecord
    artifacts: tuple[ArtifactMetadata, ...]
    golden_evidence: GoldenVerificationEvidence
    output_fingerprint: str

    def __post_init__(self) -> None:
        if not self.run_dir.is_absolute():
            raise ValueError("run_dir must be absolute")
        _nonempty(self.lock_id, "lock_id")
        _finite(self.approved_nav, "approved_nav", positive=True)
        _sha256(self.output_fingerprint, "output_fingerprint")
        if not isinstance(self.sofr_evidence, SofrUpdaterEvidence):
            raise ValueError("sofr_evidence must be validated updater evidence")
        for records, count, label in (
            (self.implied_variance, 9, "implied variance"),
            (self.forecast_variance, 9, "forecast variance"),
            (self.signal_features, 9, "signal features"),
            (self.signal_evaluations, 18, "signal evaluations"),
        ):
            if len(records) != count:
                raise ValueError(f"{label} must contain exactly {count} records")
        for records in (self.implied_variance, self.forecast_variance, self.signal_features):
            if tuple(record.tenor_days for record in records) != TARGET_TENORS:
                raise ValueError("normalized term structures must use the exact target tenor order")

    @property
    def snapshot_at(self) -> datetime:
        """Stable official XNYS close used by the database idempotency key."""

        return self.market_snapshot.snapshot_at

    @property
    def data_cutoff_at(self) -> datetime:
        return self.market_snapshot.data_cutoff_at

    def projection(self) -> dict[str, Any]:
        """Return the path- and run-time-independent database value projection."""

        def clean(value: Any) -> Any:
            if dataclasses.is_dataclass(value):
                return {
                    field.name: clean(getattr(value, field.name))
                    for field in dataclasses.fields(value)
                    if field.name not in {"path", "metadata"}
                }
            if isinstance(value, Mapping):
                return {str(key): clean(item) for key, item in sorted(value.items())}
            if isinstance(value, (tuple, list)):
                return [clean(item) for item in value]
            if isinstance(value, (date, datetime)):
                return value.isoformat()
            if isinstance(value, Path):
                return value.as_posix()
            return value

        return {
            "valuation_date": self.valuation_date.isoformat(),
            "lock_id": self.lock_id,
            "approved_nav": self.approved_nav,
            "sofr_evidence": {
                "updater_manifest_sha256": self.sofr_evidence.updater_manifest_sha256,
                "refreshed_snapshot_sha256": self.sofr_evidence.refreshed_snapshot_sha256,
                "normalized_content_sha256": self.sofr_evidence.normalized_content_sha256,
                "start_date": self.sofr_evidence.start_date.isoformat(),
                "end_date": self.sofr_evidence.end_date.isoformat(),
                "row_count": self.sofr_evidence.row_count,
                "observation_date": self.sofr_evidence.observation_date.isoformat(),
                "rate_decimal": str(self.sofr_evidence.rate_decimal),
                "row_sha256": self.sofr_evidence.row_sha256,
            },
            "market_snapshot": clean(self.market_snapshot),
            "implied_variance": clean(self.implied_variance),
            "forecast_variance": clean(self.forecast_variance),
            "signal_features": clean(self.signal_features),
            "signal_evaluations": clean(self.signal_evaluations),
            "selected_signal": clean(self.selected_signal),
        }

    @property
    def content_identity(self) -> dict[str, Any]:
        """Canonical semantic identity; excludes host paths and volatile manifests."""

        return {
            "format_version": 1,
            "projection": self.projection(),
            "model_sha256": self.model_identity.sha256,
            "configuration_sha256": self.configuration_identity.sha256,
            "golden_verification_id": self.golden_evidence.verification_id,
            "artifacts": [
                {
                    "logical_name": artifact.logical_name,
                    "relative_path": artifact.relative_path,
                    "sha256": artifact.sha256,
                }
                for artifact in self.artifacts
                if artifact.identity_input
            ],
        }

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self.content_identity,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
