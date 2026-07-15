from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCRIPT_NAMES = {
    "step01": "run_step01_inventory.py",
    "step02": "run_step02_eod_staging.py",
    "step03": "run_step03_append_candidate.py",
    "step04": "run_step04_promote_candidate.py",
    "step05": "run_step05_downstream_candidate.py",
    "step06": "run_step06_promote_downstream.py",
    "step07": "run_step07_signal_snapshot.py",
}

PREFERRED_PATH_SNIPPETS = {
    "step01": ["datefix", "starter"],
    "step02": ["step02_eod_staging"],
    "step03": ["step03_append_candidate"],
    "step04": ["step04_promote_candidate"],
    "step05": ["step05_downstream_candidate"],
    "step06": ["step06_promote_downstream"],
    "step07": ["step07_signal_snapshot"],
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def iso_to_yyyymmdd(d: str) -> str:
    return d.replace("-", "")


def latest_date_from_inventory(report: Dict[str, Any], name: str) -> Optional[str]:
    inv = report.get("downstream_file_inventory", {}).get(name, {})
    return inv.get("latest_date")


def term_latest_date(report: Dict[str, Any]) -> Optional[str]:
    return report.get("repaired_term_structure_check", {}).get("latest_date")


def project_production_dir(project_root: Path) -> Path:
    return project_root / "production v1"


def score_script_path(step: str, p: Path) -> Tuple[int, str]:
    s = str(p).lower()
    score = 0
    for i, snippet in enumerate(PREFERRED_PATH_SNIPPETS.get(step, [])):
        if snippet.lower() in s:
            score += 100 - i
    # Prefer shorter/nested extracted working script path if otherwise tied.
    return score, str(p)


def find_script(project_root: Path, step: str) -> Path:
    prod_dir = project_production_dir(project_root)
    script_name = SCRIPT_NAMES[step]
    roots = [prod_dir, project_root]
    candidates: List[Path] = []
    for root in roots:
        if root.exists():
            candidates.extend(root.rglob(script_name))
    # remove duplicates while preserving path identity
    unique = sorted(set(candidates), key=lambda p: str(p).lower())
    if not unique:
        searched = ", ".join(str(r) for r in roots)
        raise FileNotFoundError(f"Could not locate {script_name}. Searched under: {searched}")
    unique.sort(key=lambda p: score_script_path(step, p), reverse=True)
    return unique[0]


def cmd_str(cmd: List[str]) -> str:
    return " ".join(f'"{c}"' if " " in c else c for c in cmd)


def run_capture_json(cmd: List[str], cwd: Optional[Path], log_lines: List[str]) -> Dict[str, Any]:
    log_lines.append(f"\n$ {cmd_str(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    )
    if proc.stdout:
        print(proc.stdout, end="")
        log_lines.append(proc.stdout)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {proc.returncode}: {cmd_str(cmd)}")
    text = proc.stdout.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Expected JSON in command output, but could not find a JSON object.")
    return json.loads(text[start : end + 1])


def run_stream(cmd: List[str], cwd: Optional[Path], log_lines: List[str]) -> None:
    log_lines.append(f"\n$ {cmd_str(cmd)}")
    print(f"\n$ {cmd_str(cmd)}")
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        log_lines.append(line)
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"Command failed with exit code {rc}: {cmd_str(cmd)}")


def downstream_is_stale(report: Dict[str, Any]) -> bool:
    t = term_latest_date(report)
    if not t:
        return True
    for name in ["realized_variance_panel", "vrp_panel", "production_feature_panel"]:
        if latest_date_from_inventory(report, name) != t:
            return True
    return False


def write_master_reports(project_root: Path, stamp: str, payload: Dict[str, Any], log_lines: List[str]) -> Tuple[Path, Path, Path]:
    audit_dir = project_root / "data" / "audit" / "production_v1"
    audit_dir.mkdir(parents=True, exist_ok=True)
    json_path = audit_dir / f"step08_daily_pipeline_{stamp}.json"
    md_path = audit_dir / f"step08_daily_pipeline_{stamp}.md"
    log_path = audit_dir / f"step08_daily_pipeline_{stamp}.log"

    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log_path.write_text("".join(log_lines), encoding="utf-8")

    lines = [
        "# VRP Production v1 - Step 08 Daily Pipeline",
        "",
        f"Run timestamp: `{stamp}`",
        f"Project root: `{project_root}`",
        f"As-of: `{payload.get('as_of')}`",
        f"NAV: `{payload.get('nav')}`",
        "",
        "## Summary",
        "",
        f"- **initial_latest_completed_eod_date**: `{payload.get('initial_latest_completed_eod_date')}`",
        f"- **initial_missing_eod_term_structure_dates**: `{payload.get('initial_missing_eod_term_structure_dates')}`",
        f"- **term_update_ran**: `{payload.get('term_update_ran')}`",
        f"- **downstream_update_ran**: `{payload.get('downstream_update_ran')}`",
        f"- **signal_ran**: `{payload.get('signal_ran')}`",
        f"- **signal_date**: `{payload.get('signal_date')}`",
        f"- **final_term_latest_date**: `{payload.get('final_term_latest_date')}`",
        f"- **final_realized_latest_date**: `{payload.get('final_realized_latest_date')}`",
        f"- **final_vrp_latest_date**: `{payload.get('final_vrp_latest_date')}`",
        f"- **final_feature_latest_date**: `{payload.get('final_feature_latest_date')}`",
        f"- **all_green**: `{payload.get('all_green')}`",
        "",
        "## Script paths",
        "",
    ]
    for k, v in payload.get("script_paths", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.extend([
        "",
        "## Output files",
        "",
        f"- JSON: `{json_path}`",
        f"- Markdown: `{md_path}`",
        f"- Log: `{log_path}`",
        "",
        "## Notes",
        "",
        "This wrapper calls the already-tested Steps 01-07. Term-structure and downstream promotions only run when needed unless forced.",
    ])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, log_path


def main() -> int:
    ap = argparse.ArgumentParser(description="VRP Production v1 Step 08 daily pipeline wrapper")
    ap.add_argument("--project-root", required=True, help="VRP project root, e.g. C:\\Users\\patri\\vrp_project")
    ap.add_argument("--as-of", default=None, help="As-of date YYYY-MM-DD. Default: Step 01 default/today.")
    ap.add_argument("--nav", type=float, default=1_000_000.0, help="NAV used for signal max-risk dollars. Default 1,000,000.")
    ap.add_argument("--force-downstream", action="store_true", help="Rebuild and promote downstream panels even if inventory says they are current.")
    ap.add_argument("--force-signal-date", default=None, help="Optional signal date YYYYMMDD. Default: final term latest date.")
    ap.add_argument("--skip-signal", action="store_true", help="Skip Step 07 signal snapshot.")
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    stamp = now_stamp()
    log_lines: List[str] = []
    payload: Dict[str, Any] = {
        "run_timestamp": stamp,
        "project_root": str(project_root),
        "as_of": args.as_of,
        "nav": args.nav,
        "term_update_ran": False,
        "downstream_update_ran": False,
        "signal_ran": False,
        "all_green": False,
        "script_paths": {},
    }

    try:
        scripts = {step: find_script(project_root, step) for step in SCRIPT_NAMES}
        payload["script_paths"] = {k: str(v) for k, v in scripts.items()}

        print("Located scripts:")
        for step, path in scripts.items():
            print(f"  {step}: {path}")

        # Initial inventory
        step01_cmd = [sys.executable, str(scripts["step01"]), "--project-root", str(project_root)]
        if args.as_of:
            step01_cmd += ["--as-of", args.as_of]
        initial = run_capture_json(step01_cmd, scripts["step01"].parent, log_lines)
        missing = initial.get("missing_eod_term_structure_dates", []) or []
        payload["initial_latest_completed_eod_date"] = initial.get("latest_completed_eod_date")
        payload["initial_latest_term_structure_date"] = initial.get("latest_term_structure_date")
        payload["initial_missing_eod_term_structure_dates"] = missing

        if missing:
            start = iso_to_yyyymmdd(min(missing))
            end = iso_to_yyyymmdd(max(missing))
            payload["term_update_ran"] = True
            payload["term_update_start_date"] = start
            payload["term_update_end_date"] = end
            run_stream([sys.executable, str(scripts["step02"]), "--project-root", str(project_root), "--start-date", start, "--end-date", end, "--refresh-sofr"], scripts["step02"].parent, log_lines)
            run_stream([sys.executable, str(scripts["step03"]), "--project-root", str(project_root), "--start-date", start, "--end-date", end], scripts["step03"].parent, log_lines)
            run_stream([sys.executable, str(scripts["step04"]), "--project-root", str(project_root), "--start-date", start, "--end-date", end, "--confirm-promote"], scripts["step04"].parent, log_lines)
        else:
            print("No missing EOD term-structure dates. Skipping Steps 02-04.")

        # Mid inventory after term update.
        mid = run_capture_json(step01_cmd, scripts["step01"].parent, log_lines)
        payload["mid_latest_term_structure_date"] = mid.get("latest_term_structure_date")
        payload["mid_missing_eod_term_structure_dates"] = mid.get("missing_eod_term_structure_dates", [])

        if args.force_downstream or downstream_is_stale(mid):
            payload["downstream_update_ran"] = True
            expected_end = iso_to_yyyymmdd(term_latest_date(mid))
            run_stream([sys.executable, str(scripts["step05"]), "--project-root", str(project_root), "--refresh-spx"], scripts["step05"].parent, log_lines)
            run_stream([sys.executable, str(scripts["step06"]), "--project-root", str(project_root), "--expected-end-date", expected_end, "--confirm-promote"], scripts["step06"].parent, log_lines)
        else:
            print("Downstream panels already match term-structure latest date. Skipping Steps 05-06.")

        # Final inventory
        final = run_capture_json(step01_cmd, scripts["step01"].parent, log_lines)
        final_term = final.get("latest_term_structure_date")
        payload["final_latest_completed_eod_date"] = final.get("latest_completed_eod_date")
        payload["final_term_latest_date"] = final_term
        payload["final_missing_eod_term_structure_dates"] = final.get("missing_eod_term_structure_dates", [])
        payload["final_realized_latest_date"] = latest_date_from_inventory(final, "realized_variance_panel")
        payload["final_vrp_latest_date"] = latest_date_from_inventory(final, "vrp_panel")
        payload["final_feature_latest_date"] = latest_date_from_inventory(final, "production_feature_panel")

        if not args.skip_signal:
            signal_date = args.force_signal_date or iso_to_yyyymmdd(final_term)
            payload["signal_date"] = signal_date
            payload["signal_ran"] = True
            run_stream([sys.executable, str(scripts["step07"]), "--project-root", str(project_root), "--signal-date", signal_date, "--nav", str(args.nav)], scripts["step07"].parent, log_lines)
        else:
            payload["signal_date"] = None

        payload["all_green"] = (
            not payload.get("final_missing_eod_term_structure_dates")
            and payload.get("final_term_latest_date") == payload.get("final_realized_latest_date")
            and payload.get("final_term_latest_date") == payload.get("final_vrp_latest_date")
            and payload.get("final_term_latest_date") == payload.get("final_feature_latest_date")
        )
        json_path, md_path, log_path = write_master_reports(project_root, stamp, payload, log_lines)

        print("\nStep 08 daily pipeline complete.")
        print(f"Initial missing dates: {payload.get('initial_missing_eod_term_structure_dates')}")
        print(f"Term update ran:       {payload.get('term_update_ran')}")
        print(f"Downstream ran:        {payload.get('downstream_update_ran')}")
        print(f"Signal ran:            {payload.get('signal_ran')}")
        print(f"Signal date:           {payload.get('signal_date')}")
        print(f"Final term latest:     {payload.get('final_term_latest_date')}")
        print(f"Final feature latest:  {payload.get('final_feature_latest_date')}")
        print(f"All green:             {payload.get('all_green')}")
        print(f"Report:                {md_path}")
        print(f"Log:                   {log_path}")
        return 0

    except Exception as e:
        payload["error"] = str(e)
        try:
            json_path, md_path, log_path = write_master_reports(project_root, stamp, payload, log_lines)
            print("\nStep 08 daily pipeline FAILED.")
            print(f"Error:  {e}")
            print(f"Report: {md_path}")
            print(f"Log:    {log_path}")
        except Exception:
            print("\nStep 08 daily pipeline FAILED.")
            print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
