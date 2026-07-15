from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import socket
import tempfile
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
ACTIVE_TENORS = [12, 15, 18, 21, 24, 27, 30, 33]
LOCK_ID = "vrp_corsi_intraday_hybrid_v2"
DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_RUNTIME_CONFIG_REL = Path("config/vrp_hybrid_v2_eod_runtime_config.json")
DEFAULT_LOCK_CONFIG_REL = Path("config/vrp_corsi_intraday_hybrid_v2_production_config.json")
DEFAULT_PROCESSED_REL = Path("data/processed/vrp_hybrid_v2_eod")
DEFAULT_AUDIT_REL = Path("data/audit/vrp_hybrid_v2_eod")
DATE_ALIASES = ("date", "trade_date", "observation_date", "timestamp")
TENOR_ALIASES = ("tenor", "target_dte", "dte", "target_days")


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return json_safe(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.DataFrame):
        return [json_safe(row) for row in value.to_dict(orient="records")]
    if isinstance(value, pd.Series):
        return [json_safe(v) for v in value.tolist()]
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    if pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
        return None
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def first_existing_column(
    frame: pd.DataFrame,
    candidates: Sequence[str],
    *,
    required: bool = True,
    label: str = "column",
) -> str | None:
    exact = {str(c): str(c) for c in frame.columns}
    lower = {str(c).lower(): str(c) for c in frame.columns}
    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    if required:
        raise KeyError(f"Could not resolve {label}. Tried {list(candidates)}. Available={list(frame.columns)}")
    return None


def normalize_dates(values: pd.Series) -> pd.Series:
    """Normalize heterogeneous date columns without misreading YYYYMMDD integers as epoch nanoseconds.

    Supported inputs include pandas/NumPy datetimes, Python/date strings, integer or float YYYYMMDD values,
    common Unix epoch units, and Excel serial dates. Unparseable values become ``NaT``. The input index is
    preserved.
    """
    series = values.copy() if isinstance(values, pd.Series) else pd.Series(values)
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        parsed = pd.to_datetime(series, errors="coerce")
        if isinstance(parsed.dtype, pd.DatetimeTZDtype):
            parsed = parsed.dt.tz_convert(None)
        return parsed.dt.normalize()

    non_null = series.notna()
    numeric = pd.to_numeric(series, errors="coerce")
    integer_like = numeric.notna() & np.isclose(numeric, np.round(numeric), rtol=0.0, atol=1e-9)

    # First handle numeric YYYYMMDD explicitly. Passing 20260710 directly to pd.to_datetime would otherwise
    # interpret it as nanoseconds after 1970-01-01 and collapse every row to 1970-01-01 after normalization.
    ymd_numeric = integer_like & numeric.between(19000101, 21001231)
    if ymd_numeric.any():
        ymd_text = numeric.loc[ymd_numeric].round().astype("Int64").astype("string")
        result.loc[ymd_numeric] = pd.to_datetime(ymd_text, format="%Y%m%d", errors="coerce").to_numpy()

    text = series.astype("string").str.strip()
    ymd_text_mask = non_null & result.isna() & text.str.fullmatch(r"\d{8}", na=False)
    if ymd_text_mask.any():
        result.loc[ymd_text_mask] = pd.to_datetime(
            text.loc[ymd_text_mask], format="%Y%m%d", errors="coerce"
        ).to_numpy()

    remaining_numeric = non_null & result.isna() & numeric.notna()
    numeric_abs = numeric.abs()
    unit_bands = [
        (numeric_abs.ge(1e17), "ns"),
        (numeric_abs.ge(1e14) & numeric_abs.lt(1e17), "us"),
        (numeric_abs.ge(1e11) & numeric_abs.lt(1e14), "ms"),
        (numeric_abs.ge(1e9) & numeric_abs.lt(1e11), "s"),
    ]
    for band, unit in unit_bands:
        mask = remaining_numeric & band
        if mask.any():
            parsed = pd.to_datetime(numeric.loc[mask], unit=unit, errors="coerce", utc=True)
            result.loc[mask] = parsed.dt.tz_convert(None).to_numpy()

    excel_mask = remaining_numeric & result.isna() & numeric.between(20000, 80000)
    if excel_mask.any():
        result.loc[excel_mask] = pd.to_datetime(
            numeric.loc[excel_mask], unit="D", origin="1899-12-30", errors="coerce"
        ).to_numpy()

    general_mask = non_null & result.isna() & numeric.isna()
    if general_mask.any():
        parsed = pd.to_datetime(text.loc[general_mask], errors="coerce", format="mixed")
        if isinstance(parsed.dtype, pd.DatetimeTZDtype):
            parsed = parsed.dt.tz_convert(None)
        result.loc[general_mask] = parsed.to_numpy()

    return result.dt.normalize()


def normalize_tenor(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").round().astype("Int64")


def read_table(path: Path, columns: Sequence[str] | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path, columns=list(columns) if columns else None)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(path, usecols=list(columns) if columns else None)
    if suffix in {".pkl", ".pickle"}:
        frame = pd.read_pickle(path)
        return frame[list(columns)] if columns else frame
    raise ValueError(f"Unsupported table format: {path}")


def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.to_csv(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        shutil.copy2(source, tmp)
        os.replace(tmp, destination)
    finally:
        tmp.unlink(missing_ok=True)


def resolve_path(project_root: Path, value: str | Path | None, default_rel: Path | None = None) -> Path | None:
    if value is None:
        return project_root / default_rel if default_rel is not None else None
    path = Path(value)
    if path.is_absolute():
        return path
    return project_root / path


def glob_latest(project_root: Path, patterns: str | Sequence[str]) -> Path | None:
    pats = [patterns] if isinstance(patterns, str) else list(patterns)
    matches: list[Path] = []
    for pattern in pats:
        p = Path(pattern)
        if p.is_absolute():
            base = p.anchor
            relative = str(p)[len(base):].lstrip("\\/")
            matches.extend(Path(base).glob(relative))
        else:
            matches.extend(project_root.glob(pattern))
    files = [p for p in matches if p.is_file()]
    return max(files, key=lambda p: (p.stat().st_mtime, p.name)) if files else None


def glob_all(project_root: Path, patterns: str | Sequence[str]) -> list[Path]:
    pats = [patterns] if isinstance(patterns, str) else list(patterns)
    matches: list[Path] = []
    for pattern in pats:
        p = Path(pattern)
        if p.is_absolute():
            base = p.anchor
            relative = str(p)[len(base):].lstrip("\\/")
            matches.extend(Path(base).glob(relative))
        else:
            matches.extend(project_root.glob(pattern))
    return sorted({p.resolve() for p in matches if p.is_file()})


def parse_path_from_output(text: str, suffixes: Iterable[str] = (".parquet", ".csv", ".json")) -> list[Path]:
    suffix_pattern = "|".join(re.escape(s) for s in suffixes)
    pattern = rf"([A-Za-z]:\\[^\r\n\t<>\"|?*]+?(?:{suffix_pattern}))"
    found: list[Path] = []
    for match in re.findall(pattern, text, flags=re.IGNORECASE):
        candidate = Path(match.strip())
        if candidate not in found:
            found.append(candidate)
    return found


def progress(step: str, percent: int, message: str) -> None:
    clean = str(message).replace("\n", " ").replace("|", "/")
    print(f"VRP_PROGRESS|{step}|{int(percent)}|{clean}", flush=True)


def probe_tcp(host: str, port: int, timeout: float = 2.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"Connected to {host}:{port}"
    except OSError as exc:
        return False, f"Could not connect to {host}:{port}: {exc}"


def _calendar_schedule(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    try:
        import pandas_market_calendars as mcal
    except ImportError as exc:
        raise RuntimeError(
            "pandas_market_calendars is required for XNYS session handling. "
            "Install it with: py -m pip install pandas_market_calendars"
        ) from exc
    calendar = mcal.get_calendar("XNYS")
    return calendar.schedule(start_date=start.date(), end_date=end.date())


def xnys_sessions(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    schedule = _calendar_schedule(pd.Timestamp(start), pd.Timestamp(end))
    return pd.DatetimeIndex(schedule.index).tz_localize(None).normalize()


def latest_completed_xnys_session(
    now: pd.Timestamp | None = None,
    close_buffer_minutes: int = 15,
) -> pd.Timestamp:
    now_ts = pd.Timestamp.now(tz="America/New_York") if now is None else pd.Timestamp(now)
    if now_ts.tzinfo is None:
        now_ts = now_ts.tz_localize("America/New_York")
    else:
        now_ts = now_ts.tz_convert("America/New_York")
    start = (now_ts - pd.Timedelta(days=14)).normalize()
    end = (now_ts + pd.Timedelta(days=1)).normalize()
    schedule = _calendar_schedule(start, end)
    completed: list[pd.Timestamp] = []
    for session, row in schedule.iterrows():
        close_utc = pd.Timestamp(row["market_close"])
        close_ny = close_utc.tz_convert("America/New_York")
        if now_ts >= close_ny + pd.Timedelta(minutes=close_buffer_minutes):
            completed.append(pd.Timestamp(session).tz_localize(None).normalize())
    if not completed:
        raise RuntimeError("No completed XNYS session found in the previous 14 calendar days.")
    return max(completed)


def date_coverage(
    actual_dates: Iterable[pd.Timestamp],
    expected_sessions: pd.DatetimeIndex,
) -> dict[str, Any]:
    actual = pd.DatetimeIndex(pd.to_datetime(list(actual_dates), errors="coerce")).dropna().normalize().unique()
    expected = pd.DatetimeIndex(expected_sessions).normalize().unique()
    missing = expected.difference(actual)
    extra = actual.difference(expected)
    latest_actual = actual.max() if len(actual) else pd.NaT
    earliest_actual = actual.min() if len(actual) else pd.NaT
    interior = missing[(missing > earliest_actual) & (missing < latest_actual)] if len(actual) else missing
    recent = missing[missing > latest_actual] if len(actual) else missing
    return {
        "actual_count": int(len(actual)),
        "expected_count": int(len(expected)),
        "earliest_actual": earliest_actual,
        "latest_actual": latest_actual,
        "missing_count": int(len(missing)),
        "interior_missing_count": int(len(interior)),
        "recent_missing_count": int(len(recent)),
        "missing_dates": [str(x.date()) for x in missing],
        "interior_missing_dates": [str(x.date()) for x in interior],
        "recent_missing_dates": [str(x.date()) for x in recent],
        "extra_dates": [str(x.date()) for x in extra],
    }


def load_runtime_config(project_root: Path, config_path: Path | None = None) -> tuple[dict[str, Any], Path]:
    path = config_path or (project_root / DEFAULT_RUNTIME_CONFIG_REL)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing EOD runtime config: {path}. Copy vrp_hybrid_v2_eod_runtime_config.json "
            "into the project config directory."
        )
    return load_json(path), path


def backup_files(paths: Iterable[Path], backup_dir: Path) -> dict[str, str]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    mapping: dict[str, str] = {}
    for source in paths:
        if not source.exists():
            mapping[str(source)] = "__ABSENT__"
            continue
        if not source.is_file():
            raise RuntimeError(f"Transactional backup target is not a regular file: {source}")
        safe_name = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:12] + "_" + source.name
        destination = backup_dir / safe_name
        shutil.copy2(source, destination)
        mapping[str(source)] = str(destination)
    write_json(backup_dir / "backup_map.json", mapping)
    return mapping


def restore_files(mapping: dict[str, str]) -> None:
    for original, backup in mapping.items():
        original_path = Path(original)
        if backup == "__ABSENT__":
            original_path.unlink(missing_ok=True)
            continue
        backup_path = Path(backup)
        if backup_path.exists():
            atomic_copy(backup_path, original_path)


def dataframe_file_summary(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "sha256": sha256_file(path) if stat.st_size <= 500 * 1024 * 1024 else None,
    }
