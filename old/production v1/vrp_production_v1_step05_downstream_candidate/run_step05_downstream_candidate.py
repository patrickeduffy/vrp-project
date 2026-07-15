from __future__ import annotations

import argparse
import json
import math
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

FRED_SP500_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500"
TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_project_date_series(s: pd.Series) -> pd.Series:
    """Parse common project date encodings without turning YYYYMMDD ints into 1970 dates."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s).dt.normalize()
    # Convert to string while preserving integer YYYYMMDD.
    ss = s.astype(str).str.strip()
    ss = ss.str.replace(r"\.0$", "", regex=True)
    yyyymmdd_mask = ss.str.fullmatch(r"\d{8}")
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if yyyymmdd_mask.any():
        out.loc[yyyymmdd_mask] = pd.to_datetime(ss.loc[yyyymmdd_mask], format="%Y%m%d", errors="coerce")
    if (~yyyymmdd_mask).any():
        out.loc[~yyyymmdd_mask] = pd.to_datetime(ss.loc[~yyyymmdd_mask], errors="coerce")
    return out.dt.normalize()


def to_trade_date_int(dt: pd.Series) -> pd.Series:
    return pd.to_datetime(dt).dt.strftime("%Y%m%d").astype(int)


def infer_date_col(df: pd.DataFrame) -> str:
    for c in ["trade_date", "date", "observation_date", "DATE"]:
        if c in df.columns:
            return c
    raise ValueError(f"Could not infer date column. Columns: {list(df.columns)}")


def infer_tenor_col(df: pd.DataFrame) -> str:
    for c in ["target_days", "tenor", "dte", "tenor_days"]:
        if c in df.columns:
            return c
    raise ValueError(f"Could not infer tenor column. Columns: {list(df.columns)}")


def standardize_implied_panel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    date_col = infer_date_col(df)
    tenor_col = infer_tenor_col(df)
    parsed = parse_project_date_series(df[date_col])
    if parsed.isna().any():
        bad = df.loc[parsed.isna(), date_col].head(10).tolist()
        raise ValueError(f"Failed to parse implied date values. Examples: {bad}")
    df["date"] = parsed
    df["trade_date"] = to_trade_date_int(df["date"])
    if tenor_col != "target_days":
        df["target_days"] = pd.to_numeric(df[tenor_col], errors="coerce").astype("Int64")
    else:
        df["target_days"] = pd.to_numeric(df["target_days"], errors="coerce").astype("Int64")
    df["target_days"] = df["target_days"].astype(int)
    if "implied_variance" not in df.columns:
        raise ValueError("Canonical implied panel must contain implied_variance.")
    if "vix_style_vol" not in df.columns:
        df["vix_style_vol"] = np.sqrt(pd.to_numeric(df["implied_variance"], errors="coerce")) * 100.0
    df = df.sort_values(["trade_date", "target_days"]).reset_index(drop=True)
    return df


def backup_file(path: Path, backup_dir: Path, stamp: str) -> Optional[Path]:
    if not path.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{path.stem}_backup_before_step05_{stamp}{path.suffix}"
    shutil.copy2(path, backup_path)
    return backup_path


def load_or_refresh_spx(external_dir: Path, backup_dir: Path, refresh_spx: bool, stamp: str) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    raw_path = external_dir / "spx_index_daily_fred.csv"
    norm_path = external_dir / "spx_index_daily_fred_normalized.csv"
    meta: Dict[str, Optional[str]] = {
        "raw_path": str(raw_path),
        "normalized_path": str(norm_path),
        "refreshed": str(bool(refresh_spx)),
        "raw_backup": None,
        "normalized_backup": None,
    }
    external_dir.mkdir(parents=True, exist_ok=True)

    if refresh_spx or not raw_path.exists():
        meta["raw_backup"] = str(backup_file(raw_path, backup_dir, stamp)) if raw_path.exists() else None
        print("Refreshing FRED SPX history...")
        raw_df = pd.read_csv(FRED_SP500_URL)
        raw_df.to_csv(raw_path, index=False)
    else:
        print("Loading existing local FRED SPX history...")
        raw_df = pd.read_csv(raw_path)

    spx_df = normalize_spx_raw(raw_df)
    if norm_path.exists():
        meta["normalized_backup"] = str(backup_file(norm_path, backup_dir, stamp))
    spx_df.to_csv(norm_path, index=False)
    meta["rows"] = str(len(spx_df))
    meta["latest_trade_date"] = str(int(spx_df["trade_date"].max())) if len(spx_df) else None
    return spx_df, meta


def normalize_spx_raw(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    date_col = "observation_date" if "observation_date" in df.columns else "DATE" if "DATE" in df.columns else None
    if date_col is None:
        # fall back to first column if it looks like dates
        date_col = df.columns[0]
    price_col = None
    for c in ["SP500", "spx_close", "close", "Close", "PX_LAST"]:
        if c in df.columns:
            price_col = c
            break
    if price_col is None:
        # FRED CSV normally has DATE,SP500. Use second column if available.
        if len(df.columns) >= 2:
            price_col = df.columns[1]
        else:
            raise ValueError(f"Could not find SPX price column. Columns: {list(df.columns)}")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], errors="coerce")
    out["spx_close"] = pd.to_numeric(df[price_col], errors="coerce")
    out = out.dropna(subset=["date", "spx_close"]).copy()
    out["date"] = out["date"].dt.normalize()
    out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    out["trade_date"] = out["date"].dt.strftime("%Y%m%d").astype(int)
    out["spx_log_return"] = np.log(out["spx_close"] / out["spx_close"].shift(1))
    return out[["trade_date", "date", "spx_close", "spx_log_return"]]


def calculate_wilder_rsi(price_series: pd.Series, window: int = 14) -> pd.Series:
    price = pd.to_numeric(price_series, errors="coerce")
    delta = price.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    rsi = rsi.mask((avg_loss == 0) & (avg_gain == 0), 50.0)
    return rsi


def compute_rv21d(close: pd.Series, window: int = 21, annualization: int = 252) -> pd.Series:
    log_return = np.log(pd.to_numeric(close, errors="coerce") / pd.to_numeric(close, errors="coerce").shift(1))
    return log_return.rolling(window=window, min_periods=window).std() * math.sqrt(annualization) * 100.0


def realized_variance_over_calendar_window(
    returns_df: pd.DataFrame,
    trade_date: int,
    tenor_days: int,
    direction: str,
) -> Dict[str, object]:
    trade_ts = pd.to_datetime(str(int(trade_date)), format="%Y%m%d")
    min_available_date = returns_df["date"].min()
    max_available_date = returns_df["date"].max()
    if direction == "trailing":
        start_ts = trade_ts - pd.Timedelta(days=int(tenor_days))
        end_ts = trade_ts
        is_complete = bool(start_ts >= min_available_date)
    elif direction == "forward":
        start_ts = trade_ts
        end_ts = trade_ts + pd.Timedelta(days=int(tenor_days))
        is_complete = bool(end_ts <= max_available_date)
    else:
        raise ValueError("direction must be trailing or forward")

    mask = (returns_df["date"] > start_ts) & (returns_df["date"] <= end_ts)
    window = returns_df.loc[mask].copy()
    clean_returns = pd.to_numeric(window["spx_log_return"], errors="coerce").dropna()
    num_returns = int(clean_returns.shape[0])
    if num_returns == 0 or not is_complete:
        variance = np.nan
        vol = np.nan
    else:
        variance = float((clean_returns.pow(2).sum()) * 365.0 / float(tenor_days))
        vol = float(math.sqrt(variance) * 100.0) if variance >= 0 else np.nan
    return {
        f"{direction}_start_date": int(start_ts.strftime("%Y%m%d")),
        f"{direction}_end_date": int(end_ts.strftime("%Y%m%d")),
        f"{direction}_num_returns": num_returns,
        f"{direction}_window_complete": is_complete,
        f"{direction}_realized_variance": variance,
        f"{direction}_realized_vol": vol,
    }


def build_realized_panel(implied_df: pd.DataFrame, spx_df: pd.DataFrame) -> pd.DataFrame:
    implied_keys = implied_df[["trade_date", "target_days"]].drop_duplicates().sort_values(["trade_date", "target_days"])
    rows: List[Dict[str, object]] = []
    for _, row in implied_keys.iterrows():
        trade_date = int(row["trade_date"])
        tenor = int(row["target_days"])
        trailing = realized_variance_over_calendar_window(spx_df, trade_date, tenor, "trailing")
        forward = realized_variance_over_calendar_window(spx_df, trade_date, tenor, "forward")
        rows.append({"trade_date": trade_date, "target_days": tenor, **trailing, **forward})
    return pd.DataFrame(rows).sort_values(["trade_date", "target_days"]).reset_index(drop=True)


def build_vrp_panel(implied_df: pd.DataFrame, realized_df: pd.DataFrame, spx_df: pd.DataFrame) -> pd.DataFrame:
    df = implied_df.merge(realized_df, on=["trade_date", "target_days"], how="left", validate="one_to_one")
    denom = pd.to_numeric(df["trailing_realized_variance"], errors="coerce")
    numer = pd.to_numeric(df["implied_variance"], errors="coerce")
    ratio = numer / denom
    df["vrp_trailing_variance_ratio"] = ratio.where((numer > 0) & (denom > 0))
    df["vrp_trailing_log_variance_ratio"] = np.log(df["vrp_trailing_variance_ratio"])
    df["primary_vrp_signal"] = df["vrp_trailing_log_variance_ratio"]

    spx_features = spx_df.copy().sort_values("date")
    spx_features["spx_rsi_14"] = calculate_wilder_rsi(spx_features["spx_close"], window=14)
    # Keep log return from normalized file; recompute in case raw file had odd indexing.
    spx_features["spx_log_return"] = np.log(spx_features["spx_close"] / spx_features["spx_close"].shift(1))
    spx_features = spx_features[["trade_date", "spx_close", "spx_log_return", "spx_rsi_14"]]
    df = df.drop(columns=[c for c in ["spx_close", "spx_log_return", "spx_rsi_14"] if c in df.columns])
    df = df.merge(spx_features, on="trade_date", how="left", validate="many_to_one")
    return df.sort_values(["trade_date", "target_days"]).reset_index(drop=True)


def tenor_group_from_tenor(tenor: int) -> str:
    tenor = int(tenor)
    if tenor in (9, 12, 15):
        return "front"
    if tenor in (18, 21, 24):
        return "middle"
    if tenor in (27, 30, 33):
        return "back"
    return "unknown"


def rolling_z_by_tenor(panel: pd.DataFrame, value_col: str, window: int, use_prior_window: bool = True) -> pd.Series:
    out = pd.Series(index=panel.index, dtype="float64")
    for tenor, g in panel.groupby("tenor", sort=False):
        x = pd.to_numeric(g[value_col], errors="coerce")
        base = x.shift(1) if use_prior_window else x
        mean = base.rolling(window=window, min_periods=window).mean()
        std = base.rolling(window=window, min_periods=window).std()
        z = (x - mean) / std.replace(0, np.nan)
        out.loc[g.index] = z
    return out


def build_feature_panel(vrp_df: pd.DataFrame, spx_df: pd.DataFrame) -> pd.DataFrame:
    df = vrp_df.copy()
    df["date"] = parse_project_date_series(df["trade_date"])
    df["tenor"] = pd.to_numeric(df["target_days"], errors="coerce").astype(int)
    df["tenor_group"] = df["tenor"].map(tenor_group_from_tenor)
    df["forecast_variance"] = pd.to_numeric(df["trailing_realized_variance"], errors="coerce")
    df["forecast_vol"] = np.sqrt(df["forecast_variance"]) * 100.0
    df["vrp_log"] = pd.to_numeric(df["primary_vrp_signal"], errors="coerce")

    # Recompute signal-time market filters from SPX close history.
    market_features = spx_df.copy().sort_values("date")
    market_features["rsi14"] = calculate_wilder_rsi(market_features["spx_close"], window=14)
    market_features["rv21d"] = compute_rv21d(market_features["spx_close"], window=21, annualization=252)
    market_features = market_features[["date", "rsi14", "rv21d"]]
    df = df.drop(columns=[c for c in ["rsi14", "rv21d"] if c in df.columns])
    df = df.merge(market_features, on="date", how="left", validate="many_to_one")

    df = df.sort_values(["date", "tenor"]).reset_index(drop=True)
    df["vrp_z_3m"] = rolling_z_by_tenor(df, "vrp_log", 63, use_prior_window=True)
    df["vrp_z_1y"] = rolling_z_by_tenor(df, "vrp_log", 252, use_prior_window=True)

    # Keep a clean, stable column order first, then preserve useful audit columns afterwards.
    preferred = [
        "date", "trade_date", "tenor", "target_days", "tenor_group",
        "spx_close", "spx_log_return", "implied_variance", "vix_style_vol",
        "trailing_realized_variance", "trailing_realized_vol",
        "forecast_variance", "forecast_vol", "primary_vrp_signal", "vrp_log",
        "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14",
        "forward_realized_variance", "forward_realized_vol",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    return df[cols].sort_values(["date", "tenor"]).reset_index(drop=True)


def panel_basic_qa(df: pd.DataFrame, date_col: str, tenor_col: str, label: str) -> Dict[str, object]:
    out: Dict[str, object] = {"label": label, "rows": int(len(df))}
    if len(df) == 0:
        return out
    dates = parse_project_date_series(df[date_col])
    tenors = pd.to_numeric(df[tenor_col], errors="coerce")
    out["start_date"] = str(dates.min().date()) if dates.notna().any() else None
    out["end_date"] = str(dates.max().date()) if dates.notna().any() else None
    out["unique_dates"] = int(dates.nunique())
    out["duplicate_date_tenor_rows"] = int(df.assign(_d=dates, _t=tenors).duplicated(["_d", "_t"]).sum())
    counts = df.assign(_d=dates).groupby("_d").size()
    out["dates_not_9_rows"] = int((counts != 9).sum())
    return out


def compare_overlap(existing_path: Path, candidate: pd.DataFrame, key_cols: List[str], numeric_cols: List[str], label: str) -> Dict[str, object]:
    info: Dict[str, object] = {"label": label, "existing_path": str(existing_path), "exists": existing_path.exists()}
    if not existing_path.exists():
        return info
    try:
        old = pd.read_parquet(existing_path) if existing_path.suffix.lower() == ".parquet" else pd.read_csv(existing_path)
        old = old.copy()
        cand = candidate.copy()
        # Standardize date/tenor aliases if needed.
        for df in [old, cand]:
            if "trade_date" in key_cols and "trade_date" in df.columns:
                df["trade_date"] = to_trade_date_int(parse_project_date_series(df["trade_date"]))
            if "target_days" in key_cols and "target_days" in df.columns:
                df["target_days"] = pd.to_numeric(df["target_days"], errors="coerce").astype("Int64")
            if "tenor" in key_cols and "tenor" in df.columns:
                df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")
        common_cols = [c for c in numeric_cols if c in old.columns and c in cand.columns]
        if not common_cols:
            info["common_numeric_cols"] = []
            return info
        merged = old[key_cols + common_cols].merge(
            cand[key_cols + common_cols], on=key_cols, how="inner", suffixes=("_old", "_new")
        )
        info["overlap_rows"] = int(len(merged))
        diffs = {}
        for c in common_cols:
            a = pd.to_numeric(merged[f"{c}_old"], errors="coerce")
            b = pd.to_numeric(merged[f"{c}_new"], errors="coerce")
            diffs[c] = None if len(merged) == 0 else float((a - b).abs().max(skipna=True))
        info["max_abs_diff_by_col"] = diffs
    except Exception as e:
        info["error"] = str(e)
    return info


def write_table(df: pd.DataFrame, path_csv: Path, path_parquet: Path) -> None:
    path_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path_csv, index=False)
    df.to_parquet(path_parquet, index=False)


def write_report(audit_dir: Path, payload: Dict[str, object], qa_df: pd.DataFrame, overlap_df: pd.DataFrame, stamp: str) -> Tuple[Path, Path]:
    audit_dir.mkdir(parents=True, exist_ok=True)
    json_path = audit_dir / f"step05_downstream_candidate_{stamp}.json"
    md_path = audit_dir / f"step05_downstream_candidate_{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    lines = []
    lines.append("# VRP Production v1 - Step 05 Downstream Candidate Build")
    lines.append("")
    lines.append(f"Run timestamp: `{payload['run_timestamp']}`")
    lines.append(f"Project root: `{payload['project_root']}`")
    lines.append("")
    lines.append("## Summary")
    for k in [
        "spx_latest_trade_date", "implied_rows", "implied_latest_trade_date",
        "realized_rows", "vrp_rows", "feature_rows", "all_checks_green"
    ]:
        lines.append(f"- **{k}**: `{payload.get(k)}`")
    lines.append("")
    lines.append("## Output files")
    for k, v in payload.get("output_files", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## QA")
    lines.append(qa_df.to_markdown(index=False))
    lines.append("")
    lines.append("## Existing-overlap comparison")
    lines.append(overlap_df.to_markdown(index=False))
    lines.append("")
    lines.append("## Notes")
    lines.append("Candidate files were written under `data/processed/staging/`. Official realized/VRP/feature panels were not overwritten.")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--refresh-spx", action="store_true", help="Refresh FRED SPX history from FRED before rebuilding.")
    args = parser.parse_args()

    stamp = now_stamp()
    project_root = Path(args.project_root).expanduser().resolve()
    data_dir = project_root / "data"
    processed_dir = data_dir / "processed"
    external_dir = data_dir / "external"
    staging_dir = processed_dir / "staging"
    backup_dir = processed_dir / "backups" / "production_v1"
    audit_dir = data_dir / "audit" / "production_v1"

    implied_path = processed_dir / "vix_term_structure_history_v0_7_1_repaired_total_variance.parquet"
    if not implied_path.exists():
        raise FileNotFoundError(f"Missing canonical implied history: {implied_path}")

    print("Loading canonical implied term-structure history...")
    implied_df = standardize_implied_panel(pd.read_parquet(implied_path))
    implied_latest = int(implied_df["trade_date"].max())
    implied_start = int(implied_df["trade_date"].min())

    print("Loading/refreshing SPX daily closes...")
    spx_df, spx_meta = load_or_refresh_spx(external_dir, backup_dir, args.refresh_spx, stamp)
    spx_latest = int(spx_df["trade_date"].max()) if len(spx_df) else None

    implied_dates = set(implied_df["trade_date"].astype(int).unique())
    spx_dates = set(spx_df["trade_date"].astype(int).unique())
    missing_spx_dates = sorted(list(implied_dates - spx_dates))
    if missing_spx_dates:
        raise RuntimeError(
            "SPX close coverage is incomplete for implied panel dates. "
            f"Missing count={len(missing_spx_dates)}, first examples={missing_spx_dates[:10]}. "
            "Try rerunning with --refresh-spx after FRED has updated."
        )

    print("Building realized variance panel candidate...")
    realized_df = build_realized_panel(implied_df, spx_df)

    print("Building VRP panel candidate...")
    vrp_df = build_vrp_panel(implied_df, realized_df, spx_df)

    print("Building production feature panel candidate...")
    feature_panel = build_feature_panel(vrp_df, spx_df)

    start_end = f"{implied_start}_{implied_latest}"
    output_files = {
        "realized_csv": staging_dir / f"realized_variance_panel_v0_1_candidate_{start_end}.csv",
        "realized_parquet": staging_dir / f"realized_variance_panel_v0_1_candidate_{start_end}.parquet",
        "vrp_csv": staging_dir / f"vrp_panel_v0_1_candidate_{start_end}.csv",
        "vrp_parquet": staging_dir / f"vrp_panel_v0_1_candidate_{start_end}.parquet",
        "feature_csv": staging_dir / f"production_feature_panel_v0_1_candidate_{start_end}.csv",
        "feature_parquet": staging_dir / f"production_feature_panel_v0_1_candidate_{start_end}.parquet",
        "latest_snapshot_csv": staging_dir / f"production_feature_panel_latest_snapshot_v0_1_candidate_{implied_latest}.csv",
    }

    latest_snapshot = feature_panel.loc[feature_panel["trade_date"] == implied_latest].copy()

    print("Writing candidate files...")
    write_table(realized_df, output_files["realized_csv"], output_files["realized_parquet"])
    write_table(vrp_df, output_files["vrp_csv"], output_files["vrp_parquet"])
    write_table(feature_panel, output_files["feature_csv"], output_files["feature_parquet"])
    latest_snapshot.to_csv(output_files["latest_snapshot_csv"], index=False)

    qa_rows = []
    qa_rows.append(panel_basic_qa(implied_df, "trade_date", "target_days", "implied_canonical"))
    qa_rows.append(panel_basic_qa(realized_df, "trade_date", "target_days", "realized_candidate"))
    qa_rows.append(panel_basic_qa(vrp_df, "trade_date", "target_days", "vrp_candidate"))
    qa_rows.append(panel_basic_qa(feature_panel, "trade_date", "tenor", "feature_candidate"))
    qa_df = pd.DataFrame(qa_rows)

    # Missing critical fields. Early z/rsi/rv21d can be naturally missing, so don't fail on those.
    critical_checks = {
        "realized_missing_trailing_variance": int(realized_df["trailing_realized_variance"].isna().sum()),
        "vrp_missing_implied_variance": int(vrp_df["implied_variance"].isna().sum()),
        "vrp_missing_trailing_realized_variance": int(vrp_df["trailing_realized_variance"].isna().sum()),
        "vrp_missing_primary_signal": int(vrp_df["primary_vrp_signal"].isna().sum()),
        "vrp_missing_spx_close": int(vrp_df["spx_close"].isna().sum()),
        "feature_missing_vrp_log": int(feature_panel["vrp_log"].isna().sum()),
        "feature_missing_spx_close": int(feature_panel["spx_close"].isna().sum()),
        "feature_latest_snapshot_rows": int(len(latest_snapshot)),
    }

    overlap_infos = []
    overlap_infos.append(compare_overlap(
        processed_dir / "realized_variance_panel_v0_1.parquet",
        realized_df,
        ["trade_date", "target_days"],
        ["trailing_realized_variance", "trailing_realized_vol", "forward_realized_variance", "forward_realized_vol"],
        "realized_existing_overlap",
    ))
    overlap_infos.append(compare_overlap(
        processed_dir / "vrp_panel_v0_1.parquet",
        vrp_df,
        ["trade_date", "target_days"],
        ["implied_variance", "trailing_realized_variance", "primary_vrp_signal", "spx_close", "spx_rsi_14"],
        "vrp_existing_overlap",
    ))
    overlap_infos.append(compare_overlap(
        processed_dir / "production_feature_panel_v0_1.parquet",
        feature_panel,
        ["trade_date", "tenor"],
        ["implied_variance", "forecast_variance", "vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14"],
        "feature_existing_overlap",
    ))
    overlap_df = pd.DataFrame(overlap_infos)

    all_rows_match = len(realized_df) == len(implied_df) == len(vrp_df) == len(feature_panel)
    all_dates_9 = bool((qa_df["dates_not_9_rows"].fillna(0).astype(int) == 0).all())
    no_dupes = bool((qa_df["duplicate_date_tenor_rows"].fillna(0).astype(int) == 0).all())
    critical_ok = all(v == 0 for k, v in critical_checks.items() if k != "feature_latest_snapshot_rows") and critical_checks["feature_latest_snapshot_rows"] == 9
    all_checks_green = bool(all_rows_match and all_dates_9 and no_dupes and critical_ok)

    payload: Dict[str, object] = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "refresh_spx": bool(args.refresh_spx),
        "spx_meta": spx_meta,
        "spx_latest_trade_date": spx_latest,
        "implied_path": str(implied_path),
        "implied_rows": int(len(implied_df)),
        "implied_start_trade_date": implied_start,
        "implied_latest_trade_date": implied_latest,
        "realized_rows": int(len(realized_df)),
        "vrp_rows": int(len(vrp_df)),
        "feature_rows": int(len(feature_panel)),
        "critical_checks": critical_checks,
        "all_rows_match": all_rows_match,
        "all_dates_9_rows": all_dates_9,
        "no_duplicates": no_dupes,
        "all_checks_green": all_checks_green,
        "output_files": {k: str(v) for k, v in output_files.items()},
        "qa_rows": qa_rows,
        "overlap_comparison": overlap_infos,
    }
    json_path, md_path = write_report(audit_dir, payload, qa_df, overlap_df, stamp)

    print("\nStep 05 downstream candidate build complete.")
    print(f"SPX latest date:    {spx_latest}")
    print(f"Implied rows:       {len(implied_df)}")
    print(f"Realized rows:      {len(realized_df)}")
    print(f"VRP rows:           {len(vrp_df)}")
    print(f"Feature rows:       {len(feature_panel)}")
    print(f"Feature latest rows:{len(latest_snapshot)}")
    print(f"All checks green:   {all_checks_green}")
    print(f"Feature candidate:  {output_files['feature_parquet']}")
    print(f"Report:             {md_path}")
    if not all_checks_green:
        print("\nWARNING: QA did not come back fully green. Review the report before promotion.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
