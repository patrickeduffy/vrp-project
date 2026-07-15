
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_date_arg(x: str) -> pd.Timestamp:
    s = str(x).strip().replace("-", "")
    return pd.to_datetime(s, format="%Y%m%d").normalize()


def yyyymmdd(dt: pd.Timestamp) -> str:
    return pd.Timestamp(dt).strftime("%Y%m%d")


def parse_date_like(s: pd.Series) -> pd.Series:
    raw = pd.Series(s, index=s.index)
    if pd.api.types.is_datetime64_any_dtype(raw):
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    if as_str.str.fullmatch(r"\d{8}").mean() > 0.80:
        return pd.to_datetime(as_str, format="%Y%m%d", errors="coerce").dt.normalize()

    return pd.to_datetime(raw, errors="coerce").dt.normalize()


def add_work_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["_work_date"] = parse_date_like(out["date"])
    elif "trade_date" in out.columns:
        out["_work_date"] = parse_date_like(out["trade_date"])
    else:
        raise KeyError(f"No date/trade_date column. columns={out.columns.tolist()}")

    if "tenor" in out.columns:
        out["_work_tenor"] = pd.to_numeric(out["tenor"], errors="coerce").astype("Int64")
    elif "target_days" in out.columns:
        out["_work_tenor"] = pd.to_numeric(out["target_days"], errors="coerce").astype("Int64")
    else:
        raise KeyError(f"No tenor/target_days column. columns={out.columns.tolist()}")

    out["_work_trade_date"] = out["_work_date"].dt.strftime("%Y%m%d").astype("Int64")
    return out


def latest_valid_panel(
    folder: Path,
    pattern: str,
    target_date: pd.Timestamp | None = None,
    required_tenors: list[int] | None = None,
    exclude_names: set[str] | None = None,
    latest_before: pd.Timestamp | None = None,
) -> Path:
    exclude_names = exclude_names or set()
    candidates = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)

    good: list[tuple[pd.Timestamp, float, Path]] = []

    for p in candidates:
        if p.name in exclude_names:
            continue
        try:
            df = pd.read_parquet(p)
            wk = add_work_keys(df)
            max_date = wk["_work_date"].max()
            if pd.isna(max_date):
                continue

            if latest_before is not None and max_date >= latest_before:
                continue

            if target_date is not None and max_date < target_date:
                continue

            if required_tenors is not None:
                date_to_check = target_date if target_date is not None else max_date
                tenors = sorted(
                    wk.loc[wk["_work_date"].eq(date_to_check), "_work_tenor"]
                    .dropna()
                    .astype(int)
                    .unique()
                    .tolist()
                )
                if tenors != required_tenors:
                    continue

            good.append((pd.Timestamp(max_date), p.stat().st_mtime, p))
        except Exception:
            continue

    if not good:
        raise FileNotFoundError(
            f"No valid panel found in {folder} matching {pattern} "
            f"target_date={target_date} required_tenors={required_tenors}"
        )

    good = sorted(good, key=lambda x: (x[0], x[1]), reverse=True)
    return good[0][2]


def run_cmd(
    label: str,
    cmd: list[str],
    cwd: Path,
    log_dir: Path,
    fail_tokens: list[str] | None = None,
    pass_token: str | None = None,
) -> subprocess.CompletedProcess:
    banner(f"RUN — {label}")
    print(" ".join([f'"{x}"' if " " in str(x) else str(x) for x in cmd]))

    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        check=False,
        capture_output=True,
        text=True,
    )

    stdout_path = log_dir / f"{label}_stdout.txt"
    stderr_path = log_dir / f"{label}_stderr.txt"
    stdout_path.write_text(result.stdout or "", encoding="utf-8")
    stderr_path.write_text(result.stderr or "", encoding="utf-8")

    print("RETURN CODE:", result.returncode)
    print("\nSTDOUT")
    print(result.stdout)
    print("\nSTDERR")
    print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"{label} failed with return code {result.returncode}. Logs: {stdout_path}, {stderr_path}")

    for tok in fail_tokens or []:
        if tok in (result.stdout or "") or tok in (result.stderr or ""):
            raise RuntimeError(f"{label} produced failure token {tok!r}. Logs: {stdout_path}, {stderr_path}")

    if pass_token is not None and pass_token not in (result.stdout or ""):
        raise RuntimeError(f"{label} missing pass token {pass_token!r}. Logs: {stdout_path}, {stderr_path}")

    return result


def coalesce_cols(df: pd.DataFrame, candidates, default=np.nan):
    out = pd.Series(default, index=df.index)
    for c in candidates:
        if c in df.columns:
            out = out.where(out.notna(), df[c])
    return out


def validate_latest(path: Path, target_date: pd.Timestamp, label: str, expected_tenors: list[int] = EXPECTED_TENORS) -> None:
    df = pd.read_parquet(path)
    wk = add_work_keys(df)
    latest = wk["_work_date"].max()
    tenors = sorted(
        wk.loc[wk["_work_date"].eq(target_date), "_work_tenor"]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )

    if latest < target_date:
        raise RuntimeError(f"{label} latest date {latest.date()} < target {target_date.date()}")
    if tenors != expected_tenors:
        raise RuntimeError(f"{label} target-date tenors mismatch: {tenors}")

    print(f"{label} validated: latest={latest.date()} target_tenors={tenors}")


def repair_forecast_schema(
    *,
    forecast_panel: Path,
    old_forecast_panel: Path,
    target_date: pd.Timestamp,
    audit_dir: Path,
    run_ts: str,
) -> Path:
    banner("Forecast schema repair")

    old = pd.read_parquet(old_forecast_panel)
    new = pd.read_parquet(forecast_panel)

    if "date" not in new.columns:
        raise RuntimeError(f"Forecast panel missing date column: {forecast_panel}")
    if "trade_date" not in new.columns:
        raise RuntimeError(f"Forecast panel missing trade_date column: {forecast_panel}")

    new["date"] = pd.to_datetime(new["date"], errors="coerce").dt.normalize()

    if pd.api.types.is_integer_dtype(old["trade_date"]):
        new["trade_date"] = new["date"].dt.strftime("%Y%m%d").astype(old["trade_date"].dtype)
    elif pd.api.types.is_datetime64_any_dtype(old["trade_date"]):
        new["trade_date"] = new["date"].astype(old["trade_date"].dtype)
    else:
        sample = old["trade_date"].dropna().head(20).astype(str)
        if len(sample) and sample.str.match(r"^\d{4}-\d{2}-\d{2}").mean() > 0.50:
            new["trade_date"] = new["date"].dt.strftime("%Y-%m-%d").astype(object)
        else:
            new["trade_date"] = new["date"].dt.strftime("%Y%m%d").astype(object)

    old_cols = old.columns.tolist()
    extra_cols = [c for c in new.columns if c not in old_cols]
    new = new[[c for c in old_cols if c in new.columns] + extra_cols].copy()

    out = forecast_panel.with_name(
        forecast_panel.stem + f"_{run_ts}_schema_repair.parquet"
    )

    wk = add_work_keys(new)
    latest = wk["_work_date"].max()
    tenors = sorted(
        wk.loc[wk["_work_date"].eq(target_date), "_work_tenor"]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )

    if latest != target_date:
        raise RuntimeError(f"Repaired forecast latest date mismatch: {latest} vs {target_date}")
    if tenors != EXPECTED_TENORS:
        raise RuntimeError(f"Repaired forecast tenors mismatch: {tenors}")

    new.to_parquet(out, index=False)

    manifest = {
        "forecast_panel": str(forecast_panel),
        "old_forecast_panel": str(old_forecast_panel),
        "repaired_forecast_panel": str(out),
        "target_date": str(target_date.date()),
        "latest_tenors": tenors,
    }
    (audit_dir / f"forecast_schema_repair_manifest_{run_ts}.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print("Repaired forecast:", out)
    return out


def cast_trade_date_like_schema(series: pd.Series, schema_series: pd.Series) -> pd.Series:
    parsed = parse_date_like(series)
    dtype = schema_series.dtype

    if pd.api.types.is_datetime64_any_dtype(dtype):
        return parsed.astype(dtype)

    if pd.api.types.is_integer_dtype(dtype):
        return parsed.dt.strftime("%Y%m%d").astype(dtype)

    sample = schema_series.dropna().head(20).astype(str)
    if len(sample) and sample.str.match(r"^\d{4}-\d{2}-\d{2}").mean() > 0.50:
        return parsed.dt.strftime("%Y-%m-%d").astype(object)

    return parsed.dt.strftime("%Y%m%d").astype(object)


def publish_canonical_final(
    *,
    base_final_panel: Path,
    canonical_final: Path,
    canonical_snapshot: Path,
    canonical_selected: Path,
    target_date: pd.Timestamp,
    audit_dir: Path,
    run_ts: str,
) -> dict:
    banner("Publish canonical final signal schema")

    backup_dir = audit_dir / f"canonical_publish_backup_{run_ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    for p in [canonical_final, canonical_snapshot, canonical_selected]:
        if p.exists():
            shutil.copy2(p, backup_dir / p.name)

    schema_df = pd.read_parquet(canonical_final)
    schema_cols = schema_df.columns.tolist()

    old = add_work_keys(schema_df)
    base = add_work_keys(pd.read_parquet(base_final_panel))

    new_rows = base[base["_work_date"].eq(target_date)].copy().sort_values("_work_tenor")
    latest_tenors = sorted(new_rows["_work_tenor"].dropna().astype(int).unique().tolist())

    if latest_tenors != EXPECTED_TENORS:
        raise RuntimeError(f"Base final target-date tenors mismatch: {latest_tenors}")
    if len(new_rows) != len(EXPECTED_TENORS):
        raise RuntimeError(f"Base final target-date row count mismatch: {len(new_rows)}")

    old_without_target = old[~old["_work_date"].eq(target_date)].copy()
    old_without_target = old_without_target.drop(columns=[c for c in old_without_target.columns if c.startswith("_work_")])

    append = pd.DataFrame(index=new_rows.index)
    for c in schema_cols:
        append[c] = np.nan

    for c in schema_cols:
        if c in new_rows.columns:
            append[c] = new_rows[c].values

    if "date" in append.columns:
        if pd.api.types.is_datetime64_any_dtype(schema_df["date"]):
            append["date"] = target_date
        else:
            append["date"] = target_date.strftime("%Y-%m-%d")

    if "trade_date" in append.columns:
        if pd.api.types.is_datetime64_any_dtype(schema_df["trade_date"]):
            append["trade_date"] = target_date
        elif pd.api.types.is_integer_dtype(schema_df["trade_date"]):
            append["trade_date"] = int(target_date.strftime("%Y%m%d"))
        else:
            append["trade_date"] = target_date.strftime("%Y%m%d")

    if "tenor" in append.columns:
        append["tenor"] = new_rows["_work_tenor"].astype(int).values
    if "target_days" in append.columns:
        append["target_days"] = new_rows["_work_tenor"].astype(int).values

    if "forecast_variance_final" in append.columns:
        append["forecast_variance_final"] = coalesce_cols(new_rows, ["forecast_variance_final", "forecast_variance_candidate"]).values

    if "forecast_vol_final" in append.columns:
        append["forecast_vol_final"] = coalesce_cols(new_rows, ["forecast_vol_final", "candidate_forecast_vol_pct"]).values

    if "implied_variance_final" in append.columns:
        append["implied_variance_final"] = coalesce_cols(new_rows, ["implied_variance_final", "implied_variance"]).values

    if "vix_style_vol_final" in append.columns:
        vix = coalesce_cols(new_rows, ["vix_style_vol_final", "vix_style_vol", "implied_vol_final"])
        if vix.isna().any():
            ivar = coalesce_cols(new_rows, ["implied_variance_final", "implied_variance"])
            vix = vix.where(vix.notna(), np.sqrt(pd.to_numeric(ivar, errors="coerce")) * 100.0)
        append["vix_style_vol_final"] = vix.values

    if "model_vrp_log_final" in append.columns:
        mv = coalesce_cols(new_rows, ["model_vrp_log_final", "model_vrp_log"])
        if mv.isna().any():
            ivar = coalesce_cols(new_rows, ["implied_variance_final", "implied_variance"])
            fvar = coalesce_cols(new_rows, ["forecast_variance_final", "forecast_variance_candidate"])
            mv = mv.where(mv.notna(), np.log(pd.to_numeric(ivar, errors="coerce") / pd.to_numeric(fvar, errors="coerce")))
        append["model_vrp_log_final"] = mv.values

    if "z_3m_final" in append.columns:
        append["z_3m_final"] = coalesce_cols(new_rows, ["z_3m_final", "model_vrp_z_3m"]).values
    if "z_1y_final" in append.columns:
        append["z_1y_final"] = coalesce_cols(new_rows, ["z_1y_final", "model_vrp_z_1y"]).values
    if "rsi14_final" in append.columns:
        append["rsi14_final"] = coalesce_cols(new_rows, ["rsi14_final", "RSI14"]).values
    if "rv21d_vol_pct_final" in append.columns:
        append["rv21d_vol_pct_final"] = coalesce_cols(new_rows, ["rv21d_vol_pct_final", "rv21d_vol_pct"]).values

    bool_mappings = {
        "core_pass": ["core_pass", "core_signal_final"],
        "secondary_pass": ["secondary_pass", "secondary_signal_final"],
        "selected": ["selected", "selected_trade_final"],
    }
    for out_col, candidates in bool_mappings.items():
        if out_col in append.columns:
            append[out_col] = coalesce_cols(new_rows, candidates, default=False).fillna(False).astype(bool).values

    alias_mappings = {
        "selected_layer": ["selected_layer", "selected_tier_final", "selected_tier"],
        "selected_tenor_bucket": ["selected_tenor_bucket", "selected_bucket_final"],
        "selected_tenor": ["selected_tenor", "selected_tenor_final"],
        "selected_sleeve_id": ["selected_sleeve_id", "selected_label_final", "selected_label"],
        "selected_size_pct": ["selected_size_pct", "selected_size_pct_final"],
        "selected_size_label": ["selected_size_label", "selected_size_label_final"],
        "selected_sizing_quality_score": ["selected_sizing_quality_score", "selected_sizing_quality_score_final"],
        "selected_win_rate": ["selected_win_rate", "selected_win_rate_final"],
        "selected_1pct_expected_loss_positive": ["selected_1pct_expected_loss_positive", "selected_1pct_expected_loss_positive_final"],
        "selection_rank": ["selection_rank", "selection_rank_final"],
    }
    for out_col, candidates in alias_mappings.items():
        if out_col in append.columns:
            append[out_col] = coalesce_cols(new_rows, candidates).values

    combined = pd.concat([old_without_target[schema_cols], append[schema_cols]], ignore_index=True, sort=False)

    fixed = combined[schema_cols].copy()
    for c in schema_cols:
        old_dtype = schema_df[c].dtype

        if c == "trade_date":
            fixed[c] = cast_trade_date_like_schema(fixed[c], schema_df[c])
        elif c in ["tenor", "target_days", "selected_tenor", "selection_rank"]:
            vals = pd.to_numeric(fixed[c], errors="coerce")
            if pd.api.types.is_integer_dtype(old_dtype) and not vals.isna().any():
                fixed[c] = vals.astype(old_dtype)
            else:
                fixed[c] = vals
        elif pd.api.types.is_bool_dtype(old_dtype):
            fixed[c] = fixed[c].fillna(False).astype(bool)
        elif pd.api.types.is_float_dtype(old_dtype):
            fixed[c] = pd.to_numeric(fixed[c], errors="coerce").astype(old_dtype)
        elif pd.api.types.is_integer_dtype(old_dtype):
            vals = pd.to_numeric(fixed[c], errors="coerce")
            if vals.isna().any():
                fixed[c] = vals.astype("Int64")
            else:
                fixed[c] = vals.astype(old_dtype)
        elif pd.api.types.is_datetime64_any_dtype(old_dtype):
            fixed[c] = pd.to_datetime(fixed[c], errors="coerce").astype(old_dtype)
        else:
            fixed[c] = fixed[c].where(pd.notna(fixed[c]), None)

    wk = add_work_keys(fixed)
    latest_mask = wk["_work_date"].eq(target_date)

    latest = wk["_work_date"].max()
    tenors = sorted(wk.loc[latest_mask, "_work_tenor"].dropna().astype(int).unique().tolist())
    dups = int(wk.duplicated(["_work_trade_date", "_work_tenor"]).sum())

    if latest != target_date:
        raise RuntimeError(f"Canonical final latest mismatch: {latest} vs {target_date}")
    if tenors != EXPECTED_TENORS:
        raise RuntimeError(f"Canonical final latest tenors mismatch: {tenors}")
    if dups != 0:
        raise RuntimeError(f"Canonical final duplicate trade_date/tenor rows: {dups}")

    for c in ["implied_variance_final", "vix_style_vol_final", "core_pass", "secondary_pass", "selected"]:
        if c not in fixed.columns:
            raise RuntimeError(f"Canonical final missing required col {c}")
        if wk.loc[latest_mask, c].isna().any():
            raise RuntimeError(f"Canonical latest has null {c}")

    fv = pd.to_numeric(wk.loc[latest_mask, "forecast_variance_final"], errors="coerce")
    vol = pd.to_numeric(wk.loc[latest_mask, "forecast_vol_final"], errors="coerce")
    max_vol_diff = float((np.sqrt(fv) * 100.0 - vol).abs().max())
    if max_vol_diff >= 1e-10:
        raise RuntimeError(f"Forecast vol reconstruction diff too large: {max_vol_diff}")

    tmp_final = canonical_final.with_name(f"{canonical_final.stem}_tmp_{run_ts}.parquet")
    fixed.to_parquet(tmp_final, index=False)
    shutil.move(str(tmp_final), str(canonical_final))

    latest_snapshot = fixed.loc[latest_mask].copy()
    sort_col = "tenor" if "tenor" in latest_snapshot.columns else "target_days"
    latest_snapshot = latest_snapshot.sort_values(sort_col)

    tmp_snapshot = canonical_snapshot.with_name(f"{canonical_snapshot.stem}_tmp_{run_ts}.parquet")
    latest_snapshot.to_parquet(tmp_snapshot, index=False)
    shutil.move(str(tmp_snapshot), str(canonical_snapshot))

    selected_count = int(latest_snapshot["selected"].fillna(False).astype(bool).sum())

    manifest = {
        "target_date": str(target_date.date()),
        "base_final_panel": str(base_final_panel),
        "canonical_final": str(canonical_final),
        "canonical_snapshot": str(canonical_snapshot),
        "canonical_selected_left_unchanged": str(canonical_selected),
        "backup_dir": str(backup_dir),
        "latest_tenors": tenors,
        "duplicate_date_tenor_rows": dups,
        "selected_count_latest": selected_count,
        "max_forecast_vol_reconstruction_diff": max_vol_diff,
    }
    (audit_dir / f"canonical_publish_manifest_{run_ts}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Canonical final:", canonical_final)
    print("Canonical snapshot:", canonical_snapshot)
    print("Latest date:", latest.date())
    print("Latest tenors:", tenors)
    print("Selected count latest:", selected_count)

    return manifest


def patch_health_check_forecast_pointer(health_script: Path, forecast_path: Path, audit_dir: Path, run_ts: str) -> None:
    banner("Patch health-check forecast pointer")

    text = health_script.read_text(encoding="utf-8")
    backup = audit_dir / f"{health_script.name}.backup_{run_ts}"
    shutil.copy2(health_script, backup)

    pattern = r"07A_unified_fds_no_min_return_oos_forecast_panel_[A-Za-z0-9_\-]+\.parquet"
    matches = re.findall(pattern, text)

    if matches:
        for m in sorted(set(matches), key=len, reverse=True):
            text = text.replace(m, forecast_path.name)
        health_script.write_text(text, encoding="utf-8")
        print("Patched forecast filename(s):", sorted(set(matches)))
    elif forecast_path.name in text:
        print("Health check already points to:", forecast_path.name)
    else:
        print("WARNING: no forecast filename pattern found in health script. Wrapper health check will still use explicit path.")

    print("Health script backup:", backup)


def final_signal_zscore_only_failure(stdout: str, stderr: str) -> bool:
    txt = (stdout or "") + "\n" + (stderr or "")
    return (
        "latest_final_fields_non_null" in txt
        and "z_3m_final" in txt
        and "z_1y_final" in txt
        and "Hard checks failed: 1" in txt
        and "Output written" in txt
        and "Final signal panel:" in txt
        and (
            "Old signal panel rows preserved exactly" in txt
            or "old_overlap_exact   PASS" in txt
            or "old_overlap_exact" in txt
        )
    )


def tenor_bucket_for_final_signal(tenor: int) -> str | None:
    if tenor in [12, 15, 18]:
        return "Front"
    if tenor in [21, 24]:
        return "Middle"
    if tenor in [27, 30, 33]:
        return "Back"
    return None


def locked_signal_rule(layer: str, bucket: str, tenor: int) -> dict | None:
    # 9D inactive. Core Front inactive.
    if tenor == 9:
        return None

    if layer == "Core":
        if bucket == "Middle":
            return {
                "model_vrp_log_min": 0.65,
                "z_3m_min": 0.70,
                "z_1y_min": 0.70,
                "rsi14_max": 70.0,
                "rv21d_min": 8.5,
            }
        if bucket == "Back":
            return {
                "model_vrp_log_min": 0.70,
                "z_3m_min": 0.70,
                "z_1y_min": 0.70,
                "rsi14_max": 70.0,
                "rv21d_min": 8.5,
            }
        return None

    if layer == "Secondary":
        if bucket == "Front":
            return {
                "model_vrp_log_min": 0.65,
                "z_3m_min": 0.20,
                "z_1y_min": 0.20,
                "rsi14_max": 75.0,
                "rv21d_min": 7.0,
            }
        if bucket == "Middle":
            return {
                "model_vrp_log_min": 0.65,
                "z_3m_min": 0.20,
                "z_1y_min": 0.20,
                "rsi14_max": 76.0,
                "rv21d_min": 7.0,
            }
        if bucket == "Back":
            return {
                "model_vrp_log_min": 0.65,
                "z_3m_min": 0.00,
                "z_1y_min": 0.00,
                "rsi14_max": 77.0,
                "rv21d_min": 6.5,
            }
        return None

    return None


LOCKED_SIZE_PCT_BY_LABEL = {
    "Core_Middle_21D": 3.50,
    "Core_Middle_24D": 4.25,
    "Core_Back_27D": 4.50,
    "Core_Back_30D": 4.75,
    "Core_Back_33D": 5.00,
    "Secondary_Front_12D": 1.50,
    "Secondary_Front_15D": 2.00,
    "Secondary_Front_18D": 2.75,
    "Secondary_Middle_21D": 3.50,
    "Secondary_Middle_24D": 3.75,
    "Secondary_Back_27D": 4.00,
    "Secondary_Back_30D": 4.25,
    "Secondary_Back_33D": 4.50,
}


def locked_size_label(layer: str, bucket: str, tenor: int) -> str:
    return f"{layer}_{bucket}_{tenor}D"


def rule_passes(row: pd.Series, rule: dict) -> bool:
    vals = {
        "model_vrp_log_final": pd.to_numeric(pd.Series([row.get("model_vrp_log_final")]), errors="coerce").iloc[0],
        "z_3m_final": pd.to_numeric(pd.Series([row.get("z_3m_final")]), errors="coerce").iloc[0],
        "z_1y_final": pd.to_numeric(pd.Series([row.get("z_1y_final")]), errors="coerce").iloc[0],
        "rsi14_final": pd.to_numeric(pd.Series([row.get("rsi14_final")]), errors="coerce").iloc[0],
        "rv21d_vol_pct_final": pd.to_numeric(pd.Series([row.get("rv21d_vol_pct_final")]), errors="coerce").iloc[0],
    }

    if not all(np.isfinite(v) for v in vals.values()):
        return False

    return (
        vals["model_vrp_log_final"] > rule["model_vrp_log_min"]
        and vals["z_3m_final"] > rule["z_3m_min"]
        and vals["z_1y_final"] > rule["z_1y_min"]
        and vals["rsi14_final"] < rule["rsi14_max"]
        and vals["rv21d_vol_pct_final"] > rule["rv21d_min"]
    )


def infer_selected_size_scale(df: pd.DataFrame) -> float:
    if "selected_size_pct_final" not in df.columns:
        return 1.0

    vals = pd.to_numeric(df["selected_size_pct_final"], errors="coerce").dropna()
    if vals.empty:
        return 1.0

    # Existing production files may store either 5.00 or 0.0500.
    return 0.01 if float(vals.quantile(0.90)) <= 1.0 else 1.0


def lookup_existing_selected_metadata(df: pd.DataFrame, label: str, col: str):
    if "selected_label_final" not in df.columns or col not in df.columns:
        return np.nan

    mask = df["selected_label_final"].astype("string").eq(label) & df[col].notna()
    if not mask.any():
        return np.nan

    return df.loc[mask, col].dropna().iloc[-1]


def repair_final_signal_zscores_from_forecast(
    *,
    failed_final_panel: Path,
    forecast_panel: Path,
    target_date: pd.Timestamp,
    audit_dir: Path,
    run_ts: str,
) -> Path:
    banner("Repair final signal z-scores from forecast history")

    df_original = pd.read_parquet(failed_final_panel)
    df = add_work_keys(df_original)

    fc = add_work_keys(pd.read_parquet(forecast_panel))

    required_fc = ["implied_variance", "forecast_variance_candidate"]
    missing_fc = [c for c in required_fc if c not in fc.columns]
    if missing_fc:
        raise RuntimeError(f"Forecast panel missing required z-score source columns: {missing_fc}")

    required_final = [
        "model_vrp_log_final",
        "z_3m_final",
        "z_1y_final",
        "rsi14_final",
        "rv21d_vol_pct_final",
    ]
    missing_final = [c for c in required_final if c not in df.columns]
    if missing_final:
        raise RuntimeError(f"Final panel missing required repair columns: {missing_final}")

    fc["_z_source_model_vrp_log"] = np.log(
        pd.to_numeric(fc["implied_variance"], errors="coerce")
        / pd.to_numeric(fc["forecast_variance_candidate"], errors="coerce")
    )

    target_mask = df["_work_date"].eq(target_date)
    latest = df["_work_date"].max()
    latest_tenors = sorted(
        df.loc[target_mask, "_work_tenor"].dropna().astype(int).unique().tolist()
    )
    dups = int(df.duplicated(["_work_trade_date", "_work_tenor"]).sum())

    if latest != target_date:
        raise RuntimeError(f"Failed final panel latest date mismatch: {latest} vs {target_date}")
    if latest_tenors != EXPECTED_TENORS:
        raise RuntimeError(f"Failed final panel target tenors mismatch: {latest_tenors}")
    if dups != 0:
        raise RuntimeError(f"Failed final panel duplicate date/tenor rows: {dups}")

    repair_rows = []

    for tenor in EXPECTED_TENORS:
        final_row_mask = target_mask & df["_work_tenor"].eq(tenor)
        if int(final_row_mask.sum()) != 1:
            raise RuntimeError(f"Expected one target final row for tenor {tenor}; got {int(final_row_mask.sum())}")

        idx = df.index[final_row_mask][0]

        fc_row = fc[(fc["_work_date"].eq(target_date)) & (fc["_work_tenor"].eq(tenor))]
        if len(fc_row) != 1:
            raise RuntimeError(f"Expected one target forecast row for tenor {tenor}; got {len(fc_row)}")

        current_vrp_final = float(pd.to_numeric(pd.Series([df.loc[idx, "model_vrp_log_final"]]), errors="coerce").iloc[0])
        current_vrp_forecast = float(fc_row["_z_source_model_vrp_log"].iloc[0])

        if abs(current_vrp_final - current_vrp_forecast) >= 1e-10:
            raise RuntimeError(
                f"Target VRP mismatch for tenor {tenor}: final={current_vrp_final}, forecast={current_vrp_forecast}"
            )

        hist = (
            fc.loc[
                (fc["_work_tenor"].eq(tenor))
                & (fc["_work_date"] < target_date)
                & np.isfinite(fc["_z_source_model_vrp_log"]),
                ["_work_date", "_z_source_model_vrp_log"],
            ]
            .sort_values("_work_date")
            .copy()
        )

        prior_63 = hist["_z_source_model_vrp_log"].tail(63)
        prior_252 = hist["_z_source_model_vrp_log"].tail(252)

        if len(prior_63) != 63:
            raise RuntimeError(f"Tenor {tenor}: only {len(prior_63)} prior forecast rows for 3m z")
        if len(prior_252) != 252:
            raise RuntimeError(f"Tenor {tenor}: only {len(prior_252)} prior forecast rows for 1y z")

        z3 = (current_vrp_final - prior_63.mean()) / prior_63.std(ddof=1)
        z1 = (current_vrp_final - prior_252.mean()) / prior_252.std(ddof=1)

        if not np.isfinite(z3) or not np.isfinite(z1):
            raise RuntimeError(f"Non-finite repaired z-score for tenor {tenor}: z3={z3}, z1={z1}")

        df.loc[idx, "z_3m_final"] = z3
        df.loc[idx, "z_1y_final"] = z1

        if "source_model_vrp_log" in df.columns:
            df.loc[idx, "source_model_vrp_log"] = current_vrp_final
        if "source_model_vrp_z_3m" in df.columns:
            df.loc[idx, "source_model_vrp_z_3m"] = z3
        if "source_model_vrp_z_1y" in df.columns:
            df.loc[idx, "source_model_vrp_z_1y"] = z1
        if "source_vs_final_model_vrp_log_diff" in df.columns:
            df.loc[idx, "source_vs_final_model_vrp_log_diff"] = 0.0

        repair_rows.append({
            "date": str(target_date.date()),
            "tenor": tenor,
            "model_vrp_log_final": current_vrp_final,
            "z_3m_final_repaired": z3,
            "z_1y_final_repaired": z1,
            "prior_63_rows": len(prior_63),
            "prior_252_rows": len(prior_252),
            "prior_63_first_date": str(hist.tail(63)["_work_date"].min().date()),
            "prior_63_last_date": str(hist.tail(63)["_work_date"].max().date()),
            "prior_252_first_date": str(hist.tail(252)["_work_date"].min().date()),
            "prior_252_last_date": str(hist.tail(252)["_work_date"].max().date()),
        })

    # Recompute latest-date pass flags and selection after z repair.
    size_scale = infer_selected_size_scale(df)
    candidates = []

    for idx in df.index[target_mask]:
        tenor = int(df.loc[idx, "_work_tenor"])
        bucket = tenor_bucket_for_final_signal(tenor)
        row = df.loc[idx]

        core_pass = False
        secondary_pass = False

        for layer in ["Core", "Secondary"]:
            if bucket is None:
                continue

            rule = locked_signal_rule(layer, bucket, tenor)
            if rule is None:
                continue

            passed = rule_passes(row, rule)
            label = locked_size_label(layer, bucket, tenor)
            size_pct_raw = LOCKED_SIZE_PCT_BY_LABEL.get(label, np.nan)

            if layer == "Core":
                core_pass = bool(passed)
            else:
                secondary_pass = bool(passed)

            if passed:
                quality = lookup_existing_selected_metadata(df, label, "selected_sizing_quality_score_final")
                win_rate = lookup_existing_selected_metadata(df, label, "selected_win_rate_final")
                expected_loss = lookup_existing_selected_metadata(df, label, "selected_1pct_expected_loss_positive_final")

                candidates.append({
                    "idx": idx,
                    "layer": layer,
                    "bucket": bucket,
                    "tenor": tenor,
                    "label": label,
                    "size_pct_raw": size_pct_raw,
                    "size_pct_stored": size_pct_raw * size_scale if np.isfinite(size_pct_raw) else np.nan,
                    "quality": float(pd.to_numeric(pd.Series([quality]), errors="coerce").fillna(0.0).iloc[0]),
                    "win_rate": float(pd.to_numeric(pd.Series([win_rate]), errors="coerce").fillna(0.0).iloc[0]),
                    "expected_loss": float(pd.to_numeric(pd.Series([expected_loss]), errors="coerce").fillna(np.inf).iloc[0]),
                    "layer_priority": 1 if layer == "Core" else 0,
                })

        for c in ["core_signal_final", "core_pass"]:
            if c in df.columns:
                df.loc[idx, c] = core_pass
        for c in ["secondary_signal_final", "secondary_pass"]:
            if c in df.columns:
                df.loc[idx, c] = secondary_pass

    # Clear selected fields for target date before applying the selected candidate.
    selected_bool_cols = ["selected_trade_final", "selected"]
    for c in selected_bool_cols:
        if c in df.columns:
            df.loc[target_mask, c] = False

    selected_blank_cols = [
        "selected_label_final",
        "selected_size_pct_final",
        "selected_size_label_final",
        "selected_layer_final",
        "selected_tier_final",
        "selected_tenor_bucket_final",
        "selected_bucket_final",
        "selected_tenor_final",
        "selected_sleeve_id_final",
        "selected_sizing_quality_score_final",
        "selected_win_rate_final",
        "selected_1pct_expected_loss_positive_final",
        "selection_rank_final",
    ]
    for c in selected_blank_cols:
        if c in df.columns:
            df.loc[target_mask, c] = np.nan

    selected_candidate = None
    if candidates:
        selected_candidate = sorted(
            candidates,
            key=lambda x: (
                x["size_pct_raw"],
                x["layer_priority"],
                x["quality"],
                x["win_rate"],
                -x["expected_loss"],
                x["tenor"],
            ),
            reverse=True,
        )[0]

        idx = selected_candidate["idx"]

        for c in selected_bool_cols:
            if c in df.columns:
                df.loc[idx, c] = True

        label = selected_candidate["label"]
        layer = selected_candidate["layer"]
        bucket = selected_candidate["bucket"]
        tenor = selected_candidate["tenor"]

        for c in ["selected_label_final", "selected_sleeve_id_final"]:
            if c in df.columns:
                df.loc[idx, c] = label

        if "selected_size_pct_final" in df.columns:
            df.loc[idx, "selected_size_pct_final"] = selected_candidate["size_pct_stored"]
        if "selected_size_label_final" in df.columns:
            df.loc[idx, "selected_size_label_final"] = f"{selected_candidate['size_pct_raw']:.2f}%"
        if "selected_layer_final" in df.columns:
            df.loc[idx, "selected_layer_final"] = layer
        if "selected_tier_final" in df.columns:
            df.loc[idx, "selected_tier_final"] = layer
        if "selected_tenor_bucket_final" in df.columns:
            df.loc[idx, "selected_tenor_bucket_final"] = bucket
        if "selected_bucket_final" in df.columns:
            df.loc[idx, "selected_bucket_final"] = bucket
        if "selected_tenor_final" in df.columns:
            df.loc[idx, "selected_tenor_final"] = tenor
        if "selection_rank_final" in df.columns:
            df.loc[idx, "selection_rank_final"] = 1

        metadata_cols = [
            "selected_sizing_quality_score_final",
            "selected_win_rate_final",
            "selected_1pct_expected_loss_positive_final",
        ]
        for c in metadata_cols:
            if c in df.columns:
                v = lookup_existing_selected_metadata(df, label, c)
                if pd.notna(v):
                    df.loc[idx, c] = v

    # Validate non-target rows unchanged exactly.
    non_target = ~target_mask
    for c in df_original.columns:
        a = df_original.loc[non_target, c].reset_index(drop=True)
        b = df.loc[non_target, c].reset_index(drop=True)

        if pd.api.types.is_numeric_dtype(a) or pd.api.types.is_bool_dtype(a):
            ok = a.equals(b) or np.allclose(
                pd.to_numeric(a, errors="coerce"),
                pd.to_numeric(b, errors="coerce"),
                equal_nan=True,
            )
        else:
            ok = a.astype("string").fillna("<NA>").equals(b.astype("string").fillna("<NA>"))

        if not ok:
            raise RuntimeError(f"Non-target row changed unexpectedly in column {c}")

    if df.loc[target_mask, "z_3m_final"].isna().any() or df.loc[target_mask, "z_1y_final"].isna().any():
        raise RuntimeError("Z-score repair did not eliminate target-date null z-scores.")

    out = failed_final_panel.with_name(f"{failed_final_panel.stem}_zscore_repair_from_forecast_{run_ts}.parquet")
    df[df_original.columns.tolist()].to_parquet(out, index=False)

    repair_audit = pd.DataFrame(repair_rows)
    repair_audit_path = audit_dir / f"final_signal_zscore_repair_from_forecast_{run_ts}.csv"
    repair_audit.to_csv(repair_audit_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "target_date": str(target_date.date()),
        "failed_final_panel_input": str(failed_final_panel),
        "forecast_panel": str(forecast_panel),
        "repaired_final_panel_output": str(out),
        "z_source": "forecast panel implied_variance / forecast_variance_candidate",
        "z_method": "prior-only by tenor; 63 and 252 prior forecast rows; std ddof=1",
        "selected_candidate": selected_candidate,
        "repair_audit_csv": str(repair_audit_path),
    }
    manifest_path = audit_dir / f"final_signal_zscore_repair_from_forecast_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print("Final signal z-score repair output:", out)
    print("Repair audit:", repair_audit_path)
    print(repair_audit.to_string(index=False))

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Daily VRP production update wrapper v1.")
    ap.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    ap.add_argument("--end-date", required=True, help="YYYYMMDD or YYYY-MM-DD")
    ap.add_argument("--start-date", default="20180625")
    ap.add_argument("--venue", default="utp_cta")
    ap.add_argument("--skip-implied-variance", action="store_true")
    ap.add_argument("--skip-market-data", action="store_true")
    ap.add_argument("--skip-corsi-source", action="store_true")
    ap.add_argument("--skip-health-pointer-patch", action="store_true")
    ap.add_argument("--no-require-thetadata", action="store_true")
    args = ap.parse_args()

    project_root = Path(args.project_root)
    notebooks = project_root / "notebooks"
    target_date = parse_date_arg(args.end_date)
    end_yyyymmdd = yyyymmdd(target_date)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    audit_dir = project_root / "data" / "audit" / "daily_production_update" / run_ts
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP daily production update v1")
    print("Project root:", project_root)
    print("End date:", target_date.date())
    print("Run timestamp:", run_ts)
    print("Audit dir:", audit_dir)

    fit_log = project_root / "data" / "audit" / "vrp_front_middle_corsi_forecast_repair_v1" / "07A_unified_fit_log_20260704_203242.csv"

    canonical_dir = project_root / "data" / "processed" / "vrp_final_signal"
    canonical_final = canonical_dir / "vrp_final_corsi_signal_base_panel_v1.parquet"
    canonical_snapshot = canonical_dir / "vrp_final_corsi_latest_snapshot_v1.parquet"
    canonical_selected = canonical_dir / "vrp_final_corsi_selected_trades_v1.parquet"

    forecast_dir = project_root / "data" / "processed" / "vrp_front_middle_corsi_forecast_repair_v1"
    core_signal_dir = project_root / "data" / "processed" / "vrp_core_bucket_parameters_v1"

    if not args.skip_implied_variance:
        cmd = [
            "py", "-u", str(notebooks / "vrp_implied_variance_eod_update_v1.py"),
            "--project-root", str(project_root),
            "--single-date", end_yyyymmdd,
        ]
        if args.no_require_thetadata:
            cmd.append("--no-require-thetadata")
        run_cmd(
            "01_implied_variance",
            cmd,
            cwd=notebooks,
            log_dir=audit_dir,
            fail_tokens=["failed validation", "FAILED", "Traceback"],
        )

    if not args.skip_market_data:
        run_cmd(
            "02_market_data",
            [
                "py", "-u", str(notebooks / "vrp_market_data_build_v1.py"),
                "--project-root", str(project_root),
                "--end-date", end_yyyymmdd,
            ],
            cwd=notebooks,
            log_dir=audit_dir,
            fail_tokens=["failed validation", "FAILED", "Traceback"],
        )

    if not args.skip_corsi_source:
        run_cmd(
            "03_corsi_source",
            [
                "py", "-u", str(notebooks / "vrp_corsi_source_update_v1.py"),
                "--project-root", str(project_root),
                "--start-date", args.start_date,
                "--end-date", end_yyyymmdd,
                "--venue", args.venue,
            ],
            cwd=notebooks,
            log_dir=audit_dir,
            pass_token="CORSI_SOURCE_UPDATE_PASS: True",
        )

    corsi_source = latest_valid_panel(
        project_root / "data" / "processed" / "forecast_model_corsi_v1",
        "corsi_model_feature_panel_v1_*.parquet",
        target_date=target_date,
        required_tenors=EXPECTED_TENORS,
    )
    validate_latest(corsi_source, target_date, "Corsi source")

    old_feature_panel = latest_valid_panel(
        forecast_dir,
        "04_front_middle_candidate_feature_panel_*.parquet",
        required_tenors=EXPECTED_TENORS,
        latest_before=target_date,
    )

    run_cmd(
        "04_locked_cell4_features",
        [
            "py", "-u", str(notebooks / "vrp_locked_cell4_feature_panel_update_v1.py"),
            "--project-root", str(project_root),
            "--source-panel", str(corsi_source),
            "--old-feature-panel", str(old_feature_panel),
            "--end-date", end_yyyymmdd,
        ],
        cwd=notebooks,
        log_dir=audit_dir,
        pass_token="LOCKED_CELL4_FEATURE_UPDATE_PASS: True",
    )

    feature_panel = latest_valid_panel(
        forecast_dir,
        "04_front_middle_candidate_feature_panel_*.parquet",
        target_date=target_date,
        required_tenors=EXPECTED_TENORS,
        exclude_names={old_feature_panel.name},
    )
    validate_latest(feature_panel, target_date, "Locked Cell 4 feature panel")

    old_forecast_panel = latest_valid_panel(
        forecast_dir,
        "07A_unified_fds_no_min_return_oos_forecast_panel_*.parquet",
        required_tenors=EXPECTED_TENORS,
        latest_before=target_date,
    )

    run_cmd(
        "05_locked_unified_fds_forecast",
        [
            "py", "-u", str(notebooks / "vrp_locked_unified_fds_forecast_update_v1.py"),
            "--project-root", str(project_root),
            "--feature-panel", str(feature_panel),
            "--old-forecast-panel", str(old_forecast_panel),
            "--fit-log", str(fit_log),
            "--end-date", end_yyyymmdd,
        ],
        cwd=notebooks,
        log_dir=audit_dir,
        pass_token="LOCKED_UNIFIED_FDS_FORECAST_UPDATE_PASS: True",
    )

    forecast_panel_raw = latest_valid_panel(
        forecast_dir,
        "07A_unified_fds_no_min_return_oos_forecast_panel_*.parquet",
        target_date=target_date,
        required_tenors=EXPECTED_TENORS,
        exclude_names={old_forecast_panel.name},
    )
    validate_latest(forecast_panel_raw, target_date, "Forecast panel raw")

    repaired_forecast_panel = repair_forecast_schema(
        forecast_panel=forecast_panel_raw,
        old_forecast_panel=old_forecast_panel,
        target_date=target_date,
        audit_dir=audit_dir,
        run_ts=run_ts,
    )

    old_signal_panel = latest_valid_panel(
        core_signal_dir,
        "02C_cross_tenor_core_signal_base_panel_*.parquet",
        required_tenors=EXPECTED_TENORS,
        latest_before=target_date,
    )

    try:
        run_cmd(
            "06_final_signal_panel",
            [
                "py", "-u", str(notebooks / "vrp_final_signal_panel_update_v1.py"),
                "--project-root", str(project_root),
                "--forecast-panel", str(repaired_forecast_panel),
                "--old-signal-panel", str(old_signal_panel),
                "--end-date", end_yyyymmdd,
            ],
            cwd=notebooks,
            log_dir=audit_dir,
            pass_token="FINAL_SIGNAL_PANEL_UPDATE_PASS: True",
        )

        final_signal_panel = latest_valid_panel(
            core_signal_dir,
            "02C_cross_tenor_core_signal_base_panel_*.parquet",
            target_date=target_date,
            required_tenors=EXPECTED_TENORS,
            exclude_names={old_signal_panel.name},
        )

    except RuntimeError:
        stdout_path = audit_dir / "06_final_signal_panel_stdout.txt"
        stderr_path = audit_dir / "06_final_signal_panel_stderr.txt"

        stdout_text = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else ""
        stderr_text = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else ""

        if not final_signal_zscore_only_failure(stdout_text, stderr_text):
            raise

        banner("Detected z-score-only final signal failure; repairing from forecast history")

        failed_final_signal_panel = latest_valid_panel(
            core_signal_dir,
            "02C_cross_tenor_core_signal_base_panel_*.parquet",
            target_date=target_date,
            required_tenors=EXPECTED_TENORS,
            exclude_names={old_signal_panel.name},
        )

        final_signal_panel = repair_final_signal_zscores_from_forecast(
            failed_final_panel=failed_final_signal_panel,
            forecast_panel=repaired_forecast_panel,
            target_date=target_date,
            audit_dir=audit_dir,
            run_ts=run_ts,
        )

    validate_latest(final_signal_panel, target_date, "Final signal base panel")

    publish_manifest = publish_canonical_final(
        base_final_panel=final_signal_panel,
        canonical_final=canonical_final,
        canonical_snapshot=canonical_snapshot,
        canonical_selected=canonical_selected,
        target_date=target_date,
        audit_dir=audit_dir,
        run_ts=run_ts,
    )

    health_script = notebooks / "vrp_production_health_check_v1.py"
    if not args.skip_health_pointer_patch:
        patch_health_check_forecast_pointer(
            health_script=health_script,
            forecast_path=repaired_forecast_panel,
            audit_dir=audit_dir,
            run_ts=run_ts,
        )

    run_cmd(
        "07_production_health_check",
        [
            "py", "-u", str(health_script),
            "--project-root", str(project_root),
            "--forecast-source-path", str(repaired_forecast_panel),
            "--final-panel-path", str(canonical_final),
            "--latest-snapshot-path", str(canonical_snapshot),
            "--selected-trades-path", str(canonical_selected),
            "--expected-date", end_yyyymmdd,
        ],
        cwd=notebooks,
        log_dir=audit_dir,
        pass_token="PRODUCTION HEALTH: PASS",
    )

    manifest = {
        "run_ts": run_ts,
        "target_date": str(target_date.date()),
        "project_root": str(project_root),
        "corsi_source": str(corsi_source),
        "old_feature_panel": str(old_feature_panel),
        "feature_panel": str(feature_panel),
        "old_forecast_panel": str(old_forecast_panel),
        "forecast_panel_raw": str(forecast_panel_raw),
        "repaired_forecast_panel": str(repaired_forecast_panel),
        "old_signal_panel": str(old_signal_panel),
        "final_signal_panel": str(final_signal_panel),
        "canonical_final": str(canonical_final),
        "canonical_snapshot": str(canonical_snapshot),
        "canonical_selected": str(canonical_selected),
        "publish_manifest": publish_manifest,
        "audit_dir": str(audit_dir),
    }

    manifest_path = audit_dir / f"daily_production_update_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    banner("DAILY PRODUCTION UPDATE PASS")
    print("Official production date:", target_date.date())
    print("Audit dir:", audit_dir)
    print("Manifest:", manifest_path)


if __name__ == "__main__":
    main()
