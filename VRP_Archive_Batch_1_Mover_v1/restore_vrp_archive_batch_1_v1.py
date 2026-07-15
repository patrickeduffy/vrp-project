from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_ARCHIVE_ROOT = Path(r"C:\Users\patri\VRP_Archive\batch_1_historical_research_20260712")
DEFAULT_COMPLETED = DEFAULT_ARCHIVE_ROOT / "_archive_metadata" / "completed_manifest.csv"
CONFIRM_PHRASE = "RESTORE BATCH 1"
CHUNK_SIZE = 16 * 1024 * 1024

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore files from VRP Archive Batch 1.")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--completed-manifest", type=Path, default=DEFAULT_COMPLETED)
    parser.add_argument("--yes", action="store_true")
    return parser.parse_args()

def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Completed archive manifest not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise RuntimeError("Completed archive manifest is empty.")
    return rows

def copy_verify(archive_path: Path, source_path: Path, expected_hash: str, expected_size: int) -> None:
    if source_path.exists():
        raise FileExistsError(f"Restore destination already exists: {source_path}")
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive file is missing: {archive_path}")
    if archive_path.stat().st_size != expected_size:
        raise RuntimeError(f"Archive size mismatch: {archive_path}")
    archive_hash = hash_file(archive_path)
    if archive_hash != expected_hash:
        raise RuntimeError(f"Archive SHA-256 mismatch: {archive_path}")

    source_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = source_path.with_name(source_path.name + ".vrp_restore_partial")
    if temp_path.exists():
        temp_path.unlink()
    shutil.copyfile(archive_path, temp_path)
    with temp_path.open("rb") as f:
        os.fsync(f.fileno())
    restored_hash = hash_file(temp_path)
    if restored_hash != expected_hash:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Restored SHA-256 mismatch: {source_path}")
    shutil.copystat(archive_path, temp_path)
    os.replace(temp_path, source_path)
    archive_path.unlink()

def main() -> int:
    args = parse_args()
    rows = read_rows(args.completed_manifest.resolve())
    execute = args.execute

    print("=" * 92)
    print("VRP ARCHIVE BATCH 1 RESTORE v1")
    print("=" * 92)
    print(f"Mode: {'EXECUTE' if execute else 'DRY RUN — NO FILES WILL MOVE'}")
    print(f"Completed manifest: {args.completed_manifest.resolve()}")
    print(f"Files in manifest: {len(rows):,}")

    errors = []
    restorable = 0
    already_restored = 0
    for row in rows:
        source = Path(row["source_path"])
        archive = Path(row["archive_path"])
        expected_size = int(row["size_bytes"])
        if source.exists() and not archive.exists():
            if source.stat().st_size == expected_size and hash_file(source) == row["sha256"]:
                already_restored += 1
            else:
                errors.append(f"Existing source does not match archived manifest: {source}")
        elif not source.exists() and archive.exists():
            if archive.stat().st_size != expected_size:
                errors.append(f"Archive size mismatch: {archive}")
            else:
                restorable += 1
        elif source.exists() and archive.exists():
            errors.append(f"Both source and archive exist; manual review required: {source}")
        else:
            errors.append(f"Neither source nor archive exists: {source}")

    if errors:
        print("\nPREFLIGHT FAILED")
        for error in errors[:100]:
            print(f"  - {error}")
        return 2

    print(f"Restorable files:       {restorable:,}")
    print(f"Already restored files: {already_restored:,}")
    print("PREFLIGHT PASSED")

    if not execute:
        print("No files were restored.")
        return 0

    if not args.yes:
        typed = input(f'Type exactly "{CONFIRM_PHRASE}" to continue: ').strip()
        if typed != CONFIRM_PHRASE:
            print("Confirmation did not match. No files were restored.")
            return 3

    restored = 0
    for idx, row in enumerate(rows, start=1):
        source = Path(row["source_path"])
        archive = Path(row["archive_path"])
        if source.exists() and not archive.exists():
            continue
        print(f"[{idx:,}/{len(rows):,}] {row['relative_path']}", flush=True)
        copy_verify(archive, source, row["sha256"], int(row["size_bytes"]))
        restored += 1

    restore_log = args.completed_manifest.parent / "restore_summary.json"
    restore_log.write_text(json.dumps({
        "status": "COMPLETE",
        "timestamp_utc": now_utc(),
        "restored_file_count": restored,
        "completed_manifest": str(args.completed_manifest.resolve()),
    }, indent=2), encoding="utf-8")

    print(f"Restore complete. Files restored: {restored:,}")
    print(f"Restore summary: {restore_log}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
