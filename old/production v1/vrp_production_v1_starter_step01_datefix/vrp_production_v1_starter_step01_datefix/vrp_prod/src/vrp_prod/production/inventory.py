from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from vrp_prod.production.calendar_utils import latest_completed_eod_date, xnys_trading_days
from vrp_prod.qa.schema import check_repaired_term_structure, first_existing, latest_date_in_table
from vrp_prod.utils.paths import ProjectPaths

CHAIN_FILE_RE = re.compile(r"^(?P<root>SPXW?|spxw?)_(?P<trade_date>\d{8})_(?P<expiration>\d{8})_(?P<quote_time>\d{6})\.pkl$")


def _iso(d: date | None) -> str | None:
    return d.isoformat() if d else None


def inventory_chain_cache(cache_dir: Path) -> Dict[str, Any]:
    if not cache_dir.exists():
        return {"path": str(cache_dir), "exists": False, "file_count": 0, "latest_trade_date": None, "earliest_trade_date": None}

    records: List[Dict[str, Any]] = []
    unmatched = 0
    for file in cache_dir.glob("*.pkl"):
        m = CHAIN_FILE_RE.match(file.name)
        if not m:
            unmatched += 1
            continue
        gd = m.groupdict()
        records.append({
            "root": gd["root"].upper(),
            "trade_date": pd.Timestamp(gd["trade_date"]).date(),
            "expiration": pd.Timestamp(gd["expiration"]).date(),
            "quote_time": gd["quote_time"],
            "filename": file.name,
        })

    if not records:
        return {"path": str(cache_dir), "exists": True, "file_count": 0, "matched_file_count": 0, "unmatched_file_count": unmatched, "latest_trade_date": None, "earliest_trade_date": None}

    df = pd.DataFrame(records)
    by_date = df.groupby("trade_date").size().rename("chain_files").reset_index()
    latest = max(df["trade_date"])
    earliest = min(df["trade_date"])
    return {
        "path": str(cache_dir),
        "exists": True,
        "file_count": int(len(list(cache_dir.glob('*.pkl')))),
        "matched_file_count": int(len(df)),
        "unmatched_file_count": int(unmatched),
        "latest_trade_date": latest.isoformat(),
        "earliest_trade_date": earliest.isoformat(),
        "unique_trade_dates": int(df["trade_date"].nunique()),
        "latest_date_chain_files": int(by_date.loc[by_date["trade_date"] == latest, "chain_files"].iloc[0]),
    }


def build_inventory(project_root: str | Path, as_of: str | None = None) -> Dict[str, Any]:
    paths = ProjectPaths.load(project_root)
    as_of_date = pd.Timestamp(as_of).date() if as_of else pd.Timestamp.today().date()
    latest_eod = latest_completed_eod_date(as_of_date)

    repaired_path = first_existing([
        paths.get("processed", "repaired_vix_term_structure_parquet"),
        paths.get("processed", "repaired_vix_term_structure_csv"),
    ]) or paths.get("processed", "repaired_vix_term_structure_parquet")

    repaired_check = check_repaired_term_structure(repaired_path)
    latest_term_date = pd.Timestamp(repaired_check.latest_date).date() if repaired_check.latest_date else None

    missing_eod_dates: List[str] = []
    if latest_term_date:
        missing = xnys_trading_days(latest_term_date + pd.Timedelta(days=1), latest_eod)
        missing_eod_dates = [d.isoformat() for d in missing]

    external = paths.config.get("external", {})
    processed = paths.config.get("processed", {})

    files_to_check = {
        "sofr_history_csv": paths.p(external["sofr_history_csv"]),
        "spx_fred_csv": paths.p(external["spx_fred_csv"]),
        "spx_fred_normalized_csv": paths.p(external["spx_fred_normalized_csv"]),
        "realized_variance_panel": first_existing([paths.p(processed["realized_variance_panel_parquet"]), paths.p(processed["realized_variance_panel_csv"])]),
        "vrp_panel": first_existing([paths.p(processed["vrp_panel_parquet"]), paths.p(processed["vrp_panel_csv"])]),
        "production_feature_panel": paths.p(processed["production_feature_panel_parquet"]),
        "naked_atm_put_eod_panel": paths.p(processed["naked_atm_put_eod_panel_parquet"]),
    }

    file_inventory = {}
    for name, path in files_to_check.items():
        if path is None:
            file_inventory[name] = {"exists": False, "file": None}
        else:
            file_inventory[name] = latest_date_in_table(path)

    report: Dict[str, Any] = {
        "as_of_date": as_of_date.isoformat(),
        "latest_completed_eod_date": latest_eod.isoformat(),
        "project_root": str(Path(project_root).resolve()),
        "repaired_term_structure_check": asdict(repaired_check),
        "raw_chain_cache_inventory": inventory_chain_cache(paths.p(paths.config["raw_chain_cache"])),
        "downstream_file_inventory": file_inventory,
        "missing_eod_term_structure_dates": missing_eod_dates,
        "next_step": "Run append-only EOD updater for missing_eod_term_structure_dates; do not overwrite repaired history.",
    }
    return report


def write_inventory_report(report: Dict[str, Any], project_root: str | Path) -> Dict[str, str]:
    paths = ProjectPaths.load(project_root)
    out_dir = paths.get("audit", "production_v1_dir")
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"step01_inventory_{stamp}.json"
    md_path = out_dir / f"step01_inventory_{stamp}.md"

    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    repaired = report["repaired_term_structure_check"]
    chain = report["raw_chain_cache_inventory"]
    missing = report["missing_eod_term_structure_dates"]

    md_lines = [
        "# VRP Production v1 — Step 01 Inventory",
        "",
        f"As-of date: `{report['as_of_date']}`",
        f"Latest completed EOD trading date: `{report['latest_completed_eod_date']}`",
        "",
        "## Repaired term-structure history",
        f"File: `{repaired['file']}`",
        f"Exists: `{repaired['exists']}`",
        f"Rows: `{repaired.get('rows')}`",
        f"Date range: `{repaired.get('earliest_date')}` to `{repaired.get('latest_date')}`",
        f"Missing required columns: `{repaired.get('missing_required_columns')}`",
        f"Duplicate date/tenor rows: `{repaired.get('duplicate_date_tenor_rows')}`",
        f"Dates not having 9 tenors: `{repaired.get('dates_with_not_9_tenors')}`",
        f"Invalid variance rows: `{repaired.get('invalid_variance_rows')}`",
        "",
        "## Raw ThetaData chain cache",
        f"Path: `{chain['path']}`",
        f"Exists: `{chain['exists']}`",
        f"Matched chain files: `{chain.get('matched_file_count')}`",
        f"Date range: `{chain.get('earliest_trade_date')}` to `{chain.get('latest_trade_date')}`",
        "",
        "## Missing EOD term-structure dates",
    ]

    if missing:
        md_lines.extend([f"- {d}" for d in missing])
    else:
        md_lines.append("No missing completed EOD dates detected.")

    md_lines.extend([
        "",
        "## Downstream files",
    ])
    for name, inv in report["downstream_file_inventory"].items():
        md_lines.append(f"- **{name}**: exists={inv.get('exists')}, rows={inv.get('rows')}, latest={inv.get('latest_date')}, file=`{inv.get('file')}`")

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return {"json_report": str(json_path), "markdown_report": str(md_path)}
