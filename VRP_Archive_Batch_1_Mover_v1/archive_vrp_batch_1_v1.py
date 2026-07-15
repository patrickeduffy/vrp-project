from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_SOURCE_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_ARCHIVE_ROOT = Path(r"C:\Users\patri\VRP_Archive\batch_1_historical_research_20260712")
DEFAULT_MANIFEST = Path(__file__).with_name("VRP_Archive_Batch_1_Approved.csv")
CONFIRM_PHRASE = "ARCHIVE BATCH 1"
CHUNK_SIZE = 16 * 1024 * 1024
PROGRESS_EVERY_BYTES = 256 * 1024 * 1024

CANONICAL_FILENAMES = {
    "launch_vrp_hybrid_v2_streamlit.bat",
    "streamlit_vrp_hybrid_v2_eod.py",
    "vrp_hybrid_v2_common.py",
    "vrp_hybrid_v2_health_check.py",
    "vrp_hybrid_v2_wilder_rsi_update.py",
    "vrp_hybrid_v2_signal_publish.py",
    "vrp_hybrid_v2_eod_pipeline.py",
    "vrp_hybrid_v2_forecast_history.parquet",
    "vrp_hybrid_v2_signal_history.parquet",
    "vrp_hybrid_v2_selected_decisions.parquet",
    "vrp_hybrid_v2_latest_snapshot.parquet",
    "vrp_hybrid_v2_data_status.json",
    "spy_eod_prices_v1.parquet",
    "spy_realized_vol_history_v1.parquet",
    "spy_wilder_rsi14_history_v1.parquet",
    "spx_vix_style_implied_variance_surface_v1.parquet",
}

PROTECTED_PATH_TOKENS = (
    "data/processed/vrp_hybrid_v2_eod/",
    "data/audit/vrp_hybrid_v2_eod/",
    "config/vrp_hybrid_v2",
    "vrp_corsi_intraday_hybrid_v2",
)

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def human_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:,.2f} {unit}"
        amount /= 1024
    return f"{value:,} B"

def is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely archive the approved VRP Batch 1 manifest."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="Perform verified archive moves.")
    mode.add_argument("--dry-run", action="store_true", help="Validate only; this is the default.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--yes", action="store_true", help="Skip typed confirmation; intended for automated tests only.")
    parser.add_argument("--no-prune-empty-dirs", action="store_true")
    return parser.parse_args()

def read_manifest(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Approved manifest not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise RuntimeError("Approved manifest is empty.")

    required = {
        "approved_for_batch_1",
        "source_path",
        "relative_path",
        "archive_path",
        "size_bytes",
        "category",
        "reason",
        "sha256_if_known",
        "duplicate_keep_path",
    }
    missing = required - set(rows[0])
    if missing:
        raise RuntimeError(f"Manifest is missing columns: {sorted(missing)}")
    if any(row["approved_for_batch_1"].strip().upper() != "APPROVED" for row in rows):
        raise RuntimeError("Every manifest row must be marked APPROVED.")
    return rows

def metadata_paths(archive_root: Path) -> dict[str, Path]:
    root = archive_root / "_archive_metadata"
    return {
        "root": root,
        "approved_manifest": root / "approved_manifest.csv",
        "progress": root / "archive_progress.jsonl",
        "completed": root / "completed_manifest.csv",
        "summary": root / "archive_summary.json",
        "errors": root / "archive_errors.csv",
    }

def load_events(progress_path: Path) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    if not progress_path.exists():
        return states
    with progress_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except Exception as exc:
                raise RuntimeError(f"Invalid progress log at line {line_no}: {exc}") from exc
            source = event.get("source_path")
            if not source:
                continue
            state = states.setdefault(source, {})
            state[event["event"]] = event
    return states

def append_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(event, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())

def append_error(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp_utc", "source_path", "archive_path", "error"])
        if not exists:
            writer.writeheader()
        writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())

def path_is_protected(source: Path, relative_path: str) -> str | None:
    rel = relative_path.replace("\\", "/").lower()
    name = source.name.lower()
    if name in CANONICAL_FILENAMES:
        return f"Canonical production filename: {source.name}"
    for token in PROTECTED_PATH_TOKENS:
        if token in rel:
            return f"Protected production path token: {token}"
    if "notebooks/" in rel and ("vrp_hybrid_v2_" in name or name == "streamlit_vrp_hybrid_v2_eod.py"):
        return "Hybrid v2 production notebook/script"
    return None

def validate_manifest_paths(
    rows: list[dict[str, str]],
    source_root: Path,
    archive_root: Path,
) -> list[str]:
    errors: list[str] = []
    seen_sources: set[str] = set()
    seen_destinations: set[str] = set()

    if source_root.resolve() == archive_root.resolve():
        errors.append("Archive root cannot equal source root.")
    if is_within(archive_root, source_root):
        errors.append("Archive root must be outside the active project root.")

    for i, row in enumerate(rows, start=2):
        source = Path(row["source_path"])
        relative = Path(row["relative_path"])
        destination = Path(row["archive_path"])

        expected_source = source_root / relative
        expected_destination = archive_root / relative

        if os.path.normcase(str(source)) != os.path.normcase(str(expected_source)):
            errors.append(f"Row {i}: source_path does not match source_root + relative_path.")
        if os.path.normcase(str(destination)) != os.path.normcase(str(expected_destination)):
            errors.append(f"Row {i}: archive_path does not match archive_root + relative_path.")
        if not is_within(source, source_root):
            errors.append(f"Row {i}: source escapes project root: {source}")
        if not is_within(destination, archive_root):
            errors.append(f"Row {i}: destination escapes archive root: {destination}")

        protected_reason = path_is_protected(source, row["relative_path"])
        if protected_reason:
            errors.append(f"Row {i}: protected source rejected: {source} — {protected_reason}")

        source_key = os.path.normcase(str(source))
        destination_key = os.path.normcase(str(destination))
        if source_key in seen_sources:
            errors.append(f"Row {i}: duplicate source in manifest: {source}")
        if destination_key in seen_destinations:
            errors.append(f"Row {i}: duplicate destination in manifest: {destination}")
        seen_sources.add(source_key)
        seen_destinations.add(destination_key)

        try:
            expected_size = int(row["size_bytes"])
            if expected_size < 0:
                raise ValueError
        except Exception:
            errors.append(f"Row {i}: invalid size_bytes: {row['size_bytes']!r}")

        known_hash = row["sha256_if_known"].strip().lower()
        if known_hash and (len(known_hash) != 64 or any(c not in "0123456789abcdef" for c in known_hash)):
            errors.append(f"Row {i}: invalid known SHA-256.")

    return errors

def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()

def preflight(
    rows: list[dict[str, str]],
    source_root: Path,
    archive_root: Path,
    states: dict[str, dict[str, Any]],
) -> tuple[list[str], dict[str, int]]:
    errors = validate_manifest_paths(rows, source_root, archive_root)
    counts = {
        "pending": 0,
        "resumable_archived_verified": 0,
        "complete": 0,
        "bytes_pending": 0,
        "largest_pending_file": 0,
    }

    source_set = {os.path.normcase(row["source_path"]) for row in rows}

    for row in rows:
        source = Path(row["source_path"])
        destination = Path(row["archive_path"])
        expected_size = int(row["size_bytes"])
        state = states.get(str(source), {})
        verified_event = state.get("archived_verified")
        removed_event = state.get("source_removed")

        source_exists = source.exists()
        destination_exists = destination.exists()

        if source_exists:
            if not source.is_file():
                errors.append(f"Source is not a regular file: {source}")
                continue
            actual_size = source.stat().st_size
            if actual_size != expected_size:
                errors.append(
                    f"Source size changed: {source}; expected {expected_size}, found {actual_size}"
                )

        if destination_exists:
            if not destination.is_file():
                errors.append(f"Destination exists but is not a file: {destination}")
                continue
            if not verified_event:
                errors.append(f"Unexpected existing archive destination: {destination}")
                continue
            if destination.stat().st_size != expected_size:
                errors.append(f"Verified destination size changed: {destination}")

        if source_exists and not destination_exists:
            counts["pending"] += 1
            counts["bytes_pending"] += expected_size
            counts["largest_pending_file"] = max(counts["largest_pending_file"], expected_size)
        elif source_exists and destination_exists and verified_event:
            counts["resumable_archived_verified"] += 1
        elif not source_exists and destination_exists and verified_event:
            if removed_event:
                counts["complete"] += 1
            else:
                counts["resumable_archived_verified"] += 1
        elif not source_exists and not destination_exists:
            errors.append(f"Source is missing and no verified archive copy exists: {source}")

        keeper_text = row.get("duplicate_keep_path", "").strip()
        if keeper_text:
            keeper_key = os.path.normcase(keeper_text)
            if keeper_key not in source_set and not Path(keeper_text).exists():
                errors.append(f"Duplicate keeper is missing: {keeper_text}")

    if counts["largest_pending_file"] > 0:
        existing_parent = archive_root
        while not existing_parent.exists() and existing_parent != existing_parent.parent:
            existing_parent = existing_parent.parent
        free = shutil.disk_usage(existing_parent).free
        required = counts["largest_pending_file"] + 512 * 1024 * 1024
        if free < required:
            errors.append(
                f"Insufficient free space for largest file plus 512 MB buffer: "
                f"need {human_bytes(required)}, have {human_bytes(free)}"
            )
    return errors, counts

def copy_verify_one(
    row: dict[str, str],
    progress_path: Path,
    errors_path: Path,
) -> tuple[str, int]:
    source = Path(row["source_path"])
    destination = Path(row["archive_path"])
    expected_size = int(row["size_bytes"])
    known_hash = row["sha256_if_known"].strip().lower()
    temp_destination = destination.with_name(destination.name + ".vrp_archive_partial")

    try:
        destination.parent.mkdir(parents=True, exist_ok=True)

        # Resume after a verified archive copy.
        states = load_events(progress_path)
        state = states.get(str(source), {})
        verified_event = state.get("archived_verified")

        if verified_event and destination.exists():
            expected_hash = verified_event["sha256"]
            archive_hash = hash_file(destination)
            if archive_hash != expected_hash:
                raise RuntimeError(f"Previously verified archive copy failed re-verification: {destination}")
            if source.exists():
                source_hash = hash_file(source)
                if source_hash != expected_hash:
                    raise RuntimeError(f"Source changed after archive copy was verified: {source}")
                source.unlink()
            append_event(progress_path, {
                "event": "source_removed",
                "timestamp_utc": now_utc(),
                "source_path": str(source),
                "archive_path": str(destination),
                "size_bytes": expected_size,
                "sha256": expected_hash,
                "resumed": True,
            })
            return expected_hash, expected_size

        if not source.exists():
            raise FileNotFoundError(f"Source disappeared before copy: {source}")
        if destination.exists():
            raise FileExistsError(f"Destination unexpectedly exists: {destination}")
        if temp_destination.exists():
            temp_destination.unlink()

        source_stat = source.stat()
        if source_stat.st_size != expected_size:
            raise RuntimeError(
                f"Source size changed before copy: expected {expected_size}, found {source_stat.st_size}"
            )

        digest = hashlib.sha256()
        copied = 0
        next_progress = PROGRESS_EVERY_BYTES

        with source.open("rb") as src, temp_destination.open("xb") as dst:
            while True:
                chunk = src.read(CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
                digest.update(chunk)
                copied += len(chunk)
                if copied >= next_progress and expected_size >= PROGRESS_EVERY_BYTES:
                    print(
                        f"      {human_bytes(copied)} / {human_bytes(expected_size)} copied",
                        flush=True,
                    )
                    next_progress += PROGRESS_EVERY_BYTES
            dst.flush()
            os.fsync(dst.fileno())

        if copied != expected_size:
            raise RuntimeError(f"Copied byte count mismatch: expected {expected_size}, copied {copied}")

        source_hash = digest.hexdigest()
        if known_hash and source_hash != known_hash:
            raise RuntimeError(
                f"Source SHA-256 changed from reviewed manifest: expected {known_hash}, found {source_hash}"
            )

        archive_hash = hash_file(temp_destination)
        if archive_hash != source_hash:
            raise RuntimeError(
                f"Archive SHA-256 mismatch: source {source_hash}, archive {archive_hash}"
            )

        shutil.copystat(source, temp_destination)
        os.replace(temp_destination, destination)

        append_event(progress_path, {
            "event": "archived_verified",
            "timestamp_utc": now_utc(),
            "source_path": str(source),
            "relative_path": row["relative_path"],
            "archive_path": str(destination),
            "size_bytes": expected_size,
            "sha256": source_hash,
            "category": row["category"],
            "reason": row["reason"],
        })

        # Re-validate the source immediately before removal.
        if source.stat().st_size != expected_size:
            raise RuntimeError(f"Source size changed after copy verification: {source}")
        source_hash_after = hash_file(source)
        if source_hash_after != source_hash:
            raise RuntimeError(f"Source changed after copy verification: {source}")

        source.unlink()

        append_event(progress_path, {
            "event": "source_removed",
            "timestamp_utc": now_utc(),
            "source_path": str(source),
            "archive_path": str(destination),
            "size_bytes": expected_size,
            "sha256": source_hash,
            "resumed": False,
        })
        return source_hash, expected_size

    except Exception as exc:
        if temp_destination.exists():
            try:
                temp_destination.unlink()
            except Exception:
                pass
        append_error(errors_path, {
            "timestamp_utc": now_utc(),
            "source_path": str(source),
            "archive_path": str(destination),
            "error": repr(exc),
        })
        raise

def prune_empty_directories(rows: list[dict[str, str]], source_root: Path) -> int:
    candidates: set[Path] = set()
    for row in rows:
        parent = Path(row["source_path"]).parent
        while parent != source_root and is_within(parent, source_root):
            candidates.add(parent)
            parent = parent.parent

    removed = 0
    for directory in sorted(candidates, key=lambda p: len(p.parts), reverse=True):
        try:
            directory.rmdir()
            removed += 1
        except OSError:
            pass
    return removed

def write_completed_manifest(
    rows: list[dict[str, str]],
    progress_path: Path,
    completed_path: Path,
) -> int:
    states = load_events(progress_path)
    completed_rows = []
    for row in rows:
        state = states.get(row["source_path"], {})
        removed = state.get("source_removed")
        verified = state.get("archived_verified")
        if not (removed and verified):
            continue
        completed_rows.append({
            "source_path": row["source_path"],
            "relative_path": row["relative_path"],
            "archive_path": row["archive_path"],
            "size_bytes": row["size_bytes"],
            "sha256": verified["sha256"],
            "category": row["category"],
            "reason": row["reason"],
            "archived_verified_utc": verified["timestamp_utc"],
            "source_removed_utc": removed["timestamp_utc"],
        })

    completed_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "source_path", "relative_path", "archive_path", "size_bytes", "sha256",
        "category", "reason", "archived_verified_utc", "source_removed_utc",
    ]
    with completed_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(completed_rows)
    return len(completed_rows)

def main() -> int:
    args = parse_args()
    execute = bool(args.execute)
    source_root = args.source_root.resolve()
    archive_root = args.archive_root.resolve()
    manifest_path = args.manifest.resolve()
    meta = metadata_paths(archive_root)

    print("=" * 100)
    print("VRP ARCHIVE BATCH 1 MOVER v1")
    print("=" * 100)
    print(f"Mode:          {'EXECUTE' if execute else 'DRY RUN — NO FILES WILL MOVE'}")
    print(f"Source root:   {source_root}")
    print(f"Archive root:  {archive_root}")
    print(f"Manifest:      {manifest_path}")
    print("=" * 100)

    rows = read_manifest(manifest_path)
    states = load_events(meta["progress"])
    errors, counts = preflight(rows, source_root, archive_root, states)

    total_bytes = sum(int(r["size_bytes"]) for r in rows)
    print(f"Approved files:                  {len(rows):,}")
    print(f"Approved size:                   {human_bytes(total_bytes)}")
    print(f"Pending files:                   {counts['pending']:,}")
    print(f"Pending bytes:                   {human_bytes(counts['bytes_pending'])}")
    print(f"Resumable verified files:        {counts['resumable_archived_verified']:,}")
    print(f"Already complete files:          {counts['complete']:,}")
    print(f"Largest pending file:            {human_bytes(counts['largest_pending_file'])}")

    if errors:
        print("\nPREFLIGHT FAILED")
        for error in errors[:100]:
            print(f"  - {error}")
        if len(errors) > 100:
            print(f"  ... plus {len(errors) - 100:,} additional errors")
        return 2

    print("\nPREFLIGHT PASSED")
    if not execute:
        print("No files were moved. Run 02_EXECUTE_archive_batch_1.bat after reviewing this result.")
        return 0

    if not args.yes:
        print("\nThis will archive the approved 2,099-file Batch 1 manifest.")
        print("The archive remains on C:, so this organizes the project but does not free C: capacity.")
        typed = input(f'Type exactly "{CONFIRM_PHRASE}" to continue: ').strip()
        if typed != CONFIRM_PHRASE:
            print("Confirmation did not match. No files were moved.")
            return 3

    meta["root"].mkdir(parents=True, exist_ok=True)
    if not meta["approved_manifest"].exists():
        shutil.copy2(manifest_path, meta["approved_manifest"])
    else:
        existing_hash = hash_file(meta["approved_manifest"])
        current_hash = hash_file(manifest_path)
        if existing_hash != current_hash:
            raise RuntimeError("Archive metadata contains a different approved manifest.")

    start = time.time()
    processed = 0
    bytes_processed = 0

    for idx, row in enumerate(rows, start=1):
        source = Path(row["source_path"])
        destination = Path(row["archive_path"])
        states = load_events(meta["progress"])
        state = states.get(str(source), {})
        if state.get("source_removed") and destination.exists() and not source.exists():
            processed += 1
            bytes_processed += int(row["size_bytes"])
            continue

        print(
            f"[{idx:,}/{len(rows):,}] {row['relative_path']} "
            f"({human_bytes(int(row['size_bytes']))})",
            flush=True,
        )
        try:
            _sha, moved_bytes = copy_verify_one(row, meta["progress"], meta["errors"])
        except Exception as exc:
            print(f"\nFAILED on: {source}")
            print(f"Reason: {exc}")
            print("The source was retained unless a verified archive event had already been recorded.")
            print("Re-run the same execution BAT after resolving the issue; the mover is resumable.")
            write_completed_manifest(rows, meta["progress"], meta["completed"])
            return 4
        processed += 1
        bytes_processed += moved_bytes

    removed_dirs = 0
    if not args.no_prune_empty_dirs:
        removed_dirs = prune_empty_directories(rows, source_root)

    completed_count = write_completed_manifest(rows, meta["progress"], meta["completed"])
    elapsed = time.time() - start
    summary = {
        "status": "COMPLETE" if completed_count == len(rows) else "INCOMPLETE",
        "timestamp_utc": now_utc(),
        "source_root": str(source_root),
        "archive_root": str(archive_root),
        "approved_file_count": len(rows),
        "completed_file_count": completed_count,
        "approved_bytes": total_bytes,
        "approved_human": human_bytes(total_bytes),
        "empty_directories_removed": removed_dirs,
        "elapsed_seconds": round(elapsed, 3),
        "completed_manifest": str(meta["completed"]),
        "progress_log": str(meta["progress"]),
        "restore_tool": "restore_vrp_archive_batch_1_v1.py",
    }
    meta["summary"].write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n" + "=" * 100)
    print("ARCHIVE BATCH 1 COMPLETE")
    print("=" * 100)
    print(f"Files archived and verified: {completed_count:,}")
    print(f"Data archived:               {human_bytes(total_bytes)}")
    print(f"Empty folders removed:       {removed_dirs:,}")
    print(f"Completed manifest:          {meta['completed']}")
    print(f"Archive summary:             {meta['summary']}")
    print("No production-canonical Hybrid v2 path was included in the approved manifest.")
    print("The archive is still on C: and therefore does not free C: drive capacity.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
