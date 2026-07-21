from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DEFAULT_SCAN_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_ARCHIVE_ROOT = Path(r"C:\Users\patri\VRP_Archive")
DEFAULT_AUDIT_RELATIVE = Path(r"data\audit\vrp_storage_inventory")

TIMESTAMP_RE = re.compile(r"(?:19|20)\d{6}(?:[_-]\d{6})?")

PROTECTED_RELATIVE_PATHS = {
    Path(r"launch_vrp_hybrid_v2_streamlit.bat"),
    Path(r"requirements_vrp_hybrid_v2_eod.txt"),
    Path(r"data\processed\market_data\spy_eod_prices_v1.parquet"),
    Path(r"data\processed\market_data\spy_realized_vol_history_v1.parquet"),
    Path(r"data\processed\market_data\spy_wilder_rsi14_history_v1.parquet"),
    Path(r"data\processed\implied_variance\spx_vix_style_implied_variance_surface_v1.parquet"),
    Path(r"data\processed\implied_variance\spx_vix_style_implied_variance_latest_snapshot_v1.parquet"),
    Path(r"data\processed\vix_term_structure_history_v0_7_1_repaired_total_variance.parquet"),
    Path(r"data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_forecast_history.parquet"),
    Path(r"data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_signal_history.parquet"),
    Path(r"data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_selected_decisions.parquet"),
    Path(r"data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_latest_snapshot.parquet"),
    Path(r"data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_data_status.json"),
}

PROTECTED_GLOBS = (
    "notebooks/vrp_hybrid_v2_*.py",
    "notebooks/streamlit_vrp_hybrid_v2_eod.py",
    "docs/*Hybrid*v2*",
    "docs/*hybrid*v2*",
    "*Hybrid*v2*Model*Lock*",
    "*Hybrid*v2*Production*Runbook*",
    "*hybrid_v2*production_config*.json",
    "*hybrid_v2*lock*.json",
)

SKIP_DIR_NAMES = {"$RECYCLE.BIN", "System Volume Information"}
TEMP_DIR_NAMES = {"__pycache__", ".ipynb_checkpoints", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
SMOKE_TEST_TOKENS = ("smoke", "synthetic", "test_artifact", "contract_test", "tmp_test")
RAW_OPTION_TOKENS = (
    "option_chain", "option-chains", "optionchains", "thetadata_cache", "theta_cache",
    "raw_options", "spxw_cache", "spx_cache",
)
BACKUP_TOKENS = (".backup_", "_backup_", ".bak", ".old", ".orig", ".tmp", "~$")
PACKAGE_TOKENS = ("patch_", "package_", "reproduction_package")


@dataclass
class FileRecord:
    path: str
    relative_path: str
    size_bytes: int
    size_mb: float
    modified_utc: str
    extension: str
    age_days: float
    protected: bool
    protection_reason: str
    category: str
    recommended_action: str
    confidence: str
    recommendation_reason: str
    sha256: str = ""
    duplicate_group: str = ""
    duplicate_keep_path: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only storage inventory for the VRP project. This tool never moves or deletes files."
    )
    parser.add_argument("--scan-root", type=Path, default=DEFAULT_SCAN_ROOT)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--top-folders", type=int, default=500)
    parser.add_argument("--min-hash-bytes", type=int, default=1_048_576)
    parser.add_argument("--raw-retention-days", type=int, default=90)
    parser.add_argument("--failed-audit-retention-days", type=int, default=14)
    parser.add_argument("--general-audit-retention-days", type=int, default=60)
    args, _unknown = parser.parse_known_args()
    return args


def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def age_days(ts: float) -> float:
    return max(0.0, (datetime.now(timezone.utc).timestamp() - ts) / 86400.0)


def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:,.2f} {unit}"
        amount /= 1024
    return f"{value:,} B"


def normalize_rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def iter_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from iter_strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from iter_strings(item)


def discover_manifest_protected_paths(root: Path) -> dict[Path, str]:
    protected: dict[Path, str] = {}
    candidates: list[Path] = []
    for pattern in ("**/*hybrid_v2*lock*.json", "**/*hybrid_v2*production_config*.json"):
        candidates.extend(root.glob(pattern))

    for json_path in candidates:
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for text in iter_strings(data):
            if not re.match(r"^[A-Za-z]:\\", text):
                continue
            p = Path(text)
            try:
                p.resolve().relative_to(root.resolve())
            except Exception:
                continue
            protected[p.resolve()] = f"Referenced by {json_path.name}"
        protected[json_path.resolve()] = "Hybrid v2 lock/config manifest"
    return protected


def discover_pattern_protected_paths(root: Path) -> dict[Path, str]:
    protected: dict[Path, str] = {}
    for rel in PROTECTED_RELATIVE_PATHS:
        p = (root / rel).resolve()
        if p.exists():
            protected[p] = "Known Hybrid v2 canonical production path"
    for pattern in PROTECTED_GLOBS:
        for p in root.glob(pattern):
            if p.is_file():
                protected[p.resolve()] = f"Matched protected pattern: {pattern}"
    return protected


def latest_successful_eod_run(root: Path) -> Path | None:
    audit_root = root / "data" / "audit" / "vrp_hybrid_v2_eod"
    if not audit_root.exists():
        return None

    runs = [p for p in audit_root.iterdir() if p.is_dir()]
    runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    for run in runs:
        if (run / "pipeline_error.txt").exists():
            continue
        report = run / "component_gap_report_after.csv"
        if report.exists():
            try:
                with report.open("r", encoding="utf-8-sig", newline="") as f:
                    rows = list(csv.DictReader(f))
                bad = False
                for row in rows:
                    joined = " ".join(str(v) for v in row.values()).lower()
                    if any(token in joined for token in ("fail", "error", "invalid", "missing critical")):
                        bad = True
                        break
                if not bad:
                    return run.resolve()
            except Exception:
                pass

        for manifest_name in ("run_manifest.json", "manifest.json", "pipeline_manifest.json"):
            manifest = run / manifest_name
            if not manifest.exists():
                continue
            try:
                data = json.loads(manifest.read_text(encoding="utf-8"))
                text = json.dumps(data).lower()
                if '"published": true' in text or '"status": "pass"' in text or '"healthy": true' in text:
                    return run.resolve()
            except Exception:
                pass

    return runs[0].resolve() if runs else None


def gather_files(root: Path, output_root: Path, archive_root: Path) -> tuple[list[Path], list[dict[str, str]]]:
    files: list[Path] = []
    errors: list[dict[str, str]] = []
    root_resolved = root.resolve()
    output_resolved = output_root.resolve()
    archive_resolved = archive_root.resolve()

    for current, dirs, names in os.walk(root_resolved):
        current_path = Path(current)
        filtered_dirs = []
        for d in dirs:
            candidate = (current_path / d).resolve()
            if d in SKIP_DIR_NAMES:
                continue
            if candidate == output_resolved or output_resolved in candidate.parents:
                continue
            if candidate == archive_resolved or archive_resolved in candidate.parents:
                continue
            filtered_dirs.append(d)
        dirs[:] = filtered_dirs

        for name in names:
            p = current_path / name
            try:
                if p.is_symlink():
                    continue
                if p.is_file():
                    files.append(p)
            except Exception as exc:
                errors.append({"path": str(p), "error": repr(exc)})
    return files, errors


def classify_file(
    path: Path,
    root: Path,
    protected_paths: dict[Path, str],
    latest_success_run: Path | None,
    args: argparse.Namespace,
) -> tuple[bool, str, str, str, str, str]:
    resolved = path.resolve()
    rel = normalize_rel(path, root)
    lower = rel.lower()
    name_lower = path.name.lower()
    age = age_days(path.stat().st_mtime)

    if resolved in protected_paths:
        reason = protected_paths[resolved]
        return True, reason, "canonical", "KEEP_CANONICAL", "HIGH", reason

    if latest_success_run and (resolved == latest_success_run or latest_success_run in resolved.parents):
        return True, "Latest successful Hybrid v2 EOD audit run", "production_audit", "KEEP_CANONICAL", "HIGH", "Latest successful production audit"

    if any(part in TEMP_DIR_NAMES for part in path.parts):
        return False, "", "temporary", "SAFE_DELETE_AFTER_ARCHIVE", "HIGH", "Python/Jupyter/test cache directory"

    if any(token in lower for token in SMOKE_TEST_TOKENS):
        return False, "", "test_artifact", "SAFE_DELETE_AFTER_ARCHIVE", "HIGH", "Smoke/synthetic/contract-test artifact"

    if any(token in name_lower for token in BACKUP_TOKENS):
        return False, "", "backup", "ARCHIVE", "HIGH", "Backup or temporary copy"

    if path.suffix.lower() in {".zip", ".7z", ".rar"} and any(token in name_lower for token in PACKAGE_TOKENS):
        return False, "", "package", "ARCHIVE", "MEDIUM", "Patch/reproduction package; retain only current release after review"

    if "data/audit/vrp_hybrid_v2_eod/" in lower:
        run_dir = path
        while run_dir.parent.name != "vrp_hybrid_v2_eod" and run_dir.parent != root:
            run_dir = run_dir.parent
        failed = (run_dir / "pipeline_error.txt").exists()
        if failed and age > args.failed_audit_retention_days:
            return False, "", "failed_production_audit", "ARCHIVE", "HIGH", f"Failed EOD audit older than {args.failed_audit_retention_days} days"
        if age > args.general_audit_retention_days:
            return False, "", "production_audit", "REVIEW", "MEDIUM", "Older EOD audit; retain milestones only"

    if "/data/audit/" in f"/{lower}" and age > args.general_audit_retention_days:
        return False, "", "research_audit", "ARCHIVE", "MEDIUM", f"Research audit older than {args.general_audit_retention_days} days"

    if any(token in lower for token in RAW_OPTION_TOKENS):
        if age > args.raw_retention_days:
            return False, "", "raw_option_data", "ARCHIVE", "HIGH", f"Raw option/cache data older than {args.raw_retention_days} days"
        return False, "", "raw_option_data", "REVIEW", "MEDIUM", "Recent raw option/cache data"

    if "/data/processed/" in f"/{lower}" and TIMESTAMP_RE.search(path.name):
        return False, "", "timestamped_derived", "REVIEW", "MEDIUM", "Timestamped processed output; likely superseded but verify canonical lineage"

    if path.suffix.lower() in {".parquet", ".csv", ".feather", ".pkl"} and "/data/processed/" in f"/{lower}":
        return False, "", "derived_data", "REVIEW", "LOW", "Processed data not explicitly protected"

    if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".html"} and "/data/audit/" in f"/{lower}":
        return False, "", "generated_report", "ARCHIVE", "MEDIUM", "Generated audit/report artifact"

    if path.suffix.lower() in {".py", ".ipynb", ".json", ".md", ".txt", ".docx", ".bat", ".ps1", ".toml", ".yaml", ".yml"}:
        return False, "", "code_or_documentation", "REVIEW", "LOW", "Code/document not explicitly part of protected Hybrid v2 manifest"

    return False, "", "other", "REVIEW", "LOW", "Unclassified file; manual review required"


def hash_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        if fieldnames:
            with path.open("w", encoding="utf-8-sig", newline="") as f:
                csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        else:
            path.write_text("", encoding="utf-8")
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    root = args.scan_root.expanduser().resolve()
    archive_root = args.archive_root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Scan root does not exist: {root}")
    if root == archive_root or root in archive_root.parents:
        raise RuntimeError("Archive root must not be the scan root or a parent of it.")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = args.output_root.expanduser().resolve() if args.output_root else (root / DEFAULT_AUDIT_RELATIVE / stamp).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    print("=" * 110)
    print("VRP project storage inventory v1 — READ ONLY")
    print("=" * 110)
    print(f"Scan root:       {root}")
    print(f"Archive root:    {archive_root}")
    print(f"Output root:     {output_root}")
    print("Move/delete mode: DISABLED")
    print("=" * 110)

    protected_paths = discover_pattern_protected_paths(root)
    protected_paths.update(discover_manifest_protected_paths(root))
    success_run = latest_successful_eod_run(root)

    files, scan_errors = gather_files(root, output_root, archive_root)
    print(f"Files discovered: {len(files):,}")

    records: list[FileRecord] = []
    folder_sizes: dict[Path, int] = defaultdict(int)
    folder_counts: dict[Path, int] = defaultdict(int)
    stat_errors = list(scan_errors)

    for idx, path in enumerate(files, start=1):
        try:
            st = path.stat()
            protected, protection_reason, category, action, confidence, reason = classify_file(
                path, root, protected_paths, success_run, args
            )
            record = FileRecord(
                path=str(path),
                relative_path=normalize_rel(path, root),
                size_bytes=st.st_size,
                size_mb=round(st.st_size / 1024 / 1024, 6),
                modified_utc=iso_utc(st.st_mtime),
                extension=path.suffix.lower(),
                age_days=round(age_days(st.st_mtime), 2),
                protected=protected,
                protection_reason=protection_reason,
                category=category,
                recommended_action=action,
                confidence=confidence,
                recommendation_reason=reason,
            )
            records.append(record)

            parent = path.parent.resolve()
            while True:
                folder_sizes[parent] += st.st_size
                folder_counts[parent] += 1
                if parent == root:
                    break
                if root not in parent.parents:
                    break
                parent = parent.parent
        except Exception as exc:
            stat_errors.append({"path": str(path), "error": repr(exc)})

        if idx % 5000 == 0:
            print(f"Inventoried {idx:,}/{len(files):,} files...")

    print("Detecting exact duplicates among same-sized files...")
    size_groups: dict[int, list[FileRecord]] = defaultdict(list)
    for record in records:
        if record.size_bytes >= args.min_hash_bytes:
            size_groups[record.size_bytes].append(record)

    hash_candidates = [r for group in size_groups.values() if len(group) > 1 for r in group]
    print(f"Files requiring SHA-256: {len(hash_candidates):,}")

    for idx, record in enumerate(hash_candidates, start=1):
        try:
            record.sha256 = hash_file(Path(record.path))
        except Exception as exc:
            stat_errors.append({"path": record.path, "error": f"Hash error: {exc!r}"})
        if idx % 100 == 0:
            print(f"Hashed {idx:,}/{len(hash_candidates):,} duplicate-size candidates...")

    duplicate_groups: dict[tuple[int, str], list[FileRecord]] = defaultdict(list)
    for record in hash_candidates:
        if record.sha256:
            duplicate_groups[(record.size_bytes, record.sha256)].append(record)

    duplicate_rows: list[dict[str, Any]] = []
    group_num = 0
    reclaimable_duplicate_bytes = 0

    for (_size, sha), group in sorted(duplicate_groups.items(), key=lambda item: item[0][0], reverse=True):
        if len(group) < 2:
            continue
        group_num += 1
        group_id = f"DUP-{group_num:05d}"
        protected_group = [r for r in group if r.protected]
        keeper = max(protected_group if protected_group else group, key=lambda r: r.modified_utc)

        for record in group:
            record.duplicate_group = group_id
            record.duplicate_keep_path = keeper.path
            is_keeper = record.path == keeper.path
            if not is_keeper and not record.protected:
                reclaimable_duplicate_bytes += record.size_bytes
                record.recommended_action = "SAFE_DELETE_AFTER_ARCHIVE"
                record.confidence = "HIGH"
                record.recommendation_reason = f"Exact duplicate of {keeper.path}"
            duplicate_rows.append({
                "duplicate_group": group_id,
                "is_keeper": is_keeper,
                "protected": record.protected,
                "size_bytes": record.size_bytes,
                "size_mb": record.size_mb,
                "sha256": sha,
                "path": record.path,
                "relative_path": record.relative_path,
                "recommended_action": record.recommended_action,
                "keep_path": keeper.path,
            })

    record_rows = [asdict(r) for r in records]
    largest_files = sorted(record_rows, key=lambda r: r["size_bytes"], reverse=True)

    folder_rows = []
    for folder, size in folder_sizes.items():
        try:
            rel = "." if folder == root else folder.relative_to(root).as_posix()
        except Exception:
            rel = str(folder)
        folder_rows.append({
            "folder": str(folder),
            "relative_folder": rel,
            "size_bytes": size,
            "size_gb": round(size / 1024**3, 6),
            "file_count": folder_counts[folder],
        })
    folder_rows.sort(key=lambda r: r["size_bytes"], reverse=True)

    candidate_rows = [
        row for row in record_rows
        if row["recommended_action"] in {"ARCHIVE", "REVIEW", "SAFE_DELETE_AFTER_ARCHIVE"}
    ]
    candidate_rows.sort(key=lambda r: (
        {"SAFE_DELETE_AFTER_ARCHIVE": 0, "ARCHIVE": 1, "REVIEW": 2}.get(r["recommended_action"], 9),
        -r["size_bytes"],
    ))
    protected_rows = sorted(
        [row for row in record_rows if row["protected"]],
        key=lambda r: r["size_bytes"], reverse=True,
    )

    write_csv(output_root / "all_files_inventory.csv", largest_files)
    write_csv(output_root / "largest_files.csv", largest_files[:1000])
    write_csv(output_root / "largest_folders.csv", folder_rows[: args.top_folders])
    write_csv(output_root / "duplicate_files.csv", duplicate_rows)
    write_csv(output_root / "archive_candidates.csv", candidate_rows)
    write_csv(output_root / "protected_files.csv", protected_rows)
    write_csv(output_root / "scan_errors.csv", stat_errors, fieldnames=["path", "error"])

    total_bytes = sum(r.size_bytes for r in records)
    protected_bytes = sum(r.size_bytes for r in records if r.protected)
    action_bytes: dict[str, int] = defaultdict(int)
    action_counts: dict[str, int] = defaultdict(int)
    category_bytes: dict[str, int] = defaultdict(int)
    for r in records:
        action_bytes[r.recommended_action] += r.size_bytes
        action_counts[r.recommended_action] += 1
        category_bytes[r.category] += r.size_bytes

    summary = {
        "tool": "VRP project storage inventory v1",
        "read_only": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "scan_root": str(root),
        "archive_root": str(archive_root),
        "output_root": str(output_root),
        "file_count": len(records),
        "total_bytes": total_bytes,
        "total_human": human_bytes(total_bytes),
        "protected_file_count": len(protected_rows),
        "protected_bytes": protected_bytes,
        "protected_human": human_bytes(protected_bytes),
        "duplicate_group_count": group_num,
        "duplicate_reclaimable_bytes": reclaimable_duplicate_bytes,
        "duplicate_reclaimable_human": human_bytes(reclaimable_duplicate_bytes),
        "action_counts": dict(action_counts),
        "action_bytes": dict(action_bytes),
        "action_human": {k: human_bytes(v) for k, v in action_bytes.items()},
        "category_human": {k: human_bytes(v) for k, v in sorted(category_bytes.items(), key=lambda x: x[1], reverse=True)},
        "latest_successful_eod_run": str(success_run) if success_run else None,
        "scan_error_count": len(stat_errors),
        "hash_minimum_bytes": args.min_hash_bytes,
        "files_written": [
            "all_files_inventory.csv", "largest_files.csv", "largest_folders.csv",
            "duplicate_files.csv", "archive_candidates.csv", "protected_files.csv",
            "scan_errors.csv", "summary.json", "summary.txt",
        ],
    }
    (output_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        "VRP PROJECT STORAGE INVENTORY v1 — READ ONLY",
        "=" * 72,
        f"Created:                    {summary['created_at']}",
        f"Scan root:                 {root}",
        f"Proposed archive root:     {archive_root}",
        f"Files scanned:             {len(records):,}",
        f"Total project size:        {human_bytes(total_bytes)}",
        f"Protected files:           {len(protected_rows):,} ({human_bytes(protected_bytes)})",
        f"Exact duplicate groups:    {group_num:,}",
        f"Duplicate reclaimable:     {human_bytes(reclaimable_duplicate_bytes)}",
        f"Scan/stat/hash errors:     {len(stat_errors):,}",
        "",
        "RECOMMENDATIONS BY ACTION",
        "-" * 72,
    ]
    for action in sorted(action_counts):
        lines.append(f"{action:<28} {action_counts[action]:>8,} files  {human_bytes(action_bytes[action]):>14}")
    lines += [
        "",
        "IMPORTANT",
        "-" * 72,
        "No files were moved, copied, renamed, or deleted.",
        "Review protected_files.csv first.",
        "Then review archive_candidates.csv and duplicate_files.csv.",
        "A separate archive-move tool should be built only after the candidate list is approved.",
    ]
    (output_root / "summary.txt").write_text("\n".join(lines), encoding="utf-8")

    print("\n" + "\n".join(lines))
    print("\nOutput files:")
    for file_name in summary["files_written"]:
        print(f"  {output_root / file_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
