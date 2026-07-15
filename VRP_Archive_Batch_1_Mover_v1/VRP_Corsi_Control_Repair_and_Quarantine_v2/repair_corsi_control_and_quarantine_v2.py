from __future__ import annotations

import argparse
import hashlib
import os
import shutil
from pathlib import Path

PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
QUARANTINE_ROOT = Path(
    r"C:\Users\patri\VRP_Archive\batch_1_historical_research_20260712"
    r"\_quarantine\failed_corsi_panels"
)

KEEPER_PARQUET = (
    PROJECT_ROOT / r"data\processed"
    / "production_feature_panel_v0_1.parquet"
)
KEEPER_CSV = (
    PROJECT_ROOT / r"data\processed"
    / "production_feature_panel_v0_1.csv"
)

STAGING_PARQUET = (
    PROJECT_ROOT / r"data\processed\staging"
    / "production_feature_panel_v0_1_candidate_20180625_20260702.parquet"
)
STAGING_CSV = (
    PROJECT_ROOT / r"data\processed\staging"
    / "production_feature_panel_v0_1_candidate_20180625_20260702.csv"
)

EXPECTED = {
    KEEPER_PARQUET: {
        "size_bytes": 3359807,
        "sha256": "70ec429d304a4964bfda12e8e62be17d28a43ca8657fff3871005f33dd788123",
    },
    KEEPER_CSV: {
        "size_bytes": 14313700,
        "sha256": "df75e5eec2c7ecec38f9d62cc8e7bf4b16c96de2d96decfa68c0648546c5cb72",
    },
}

VALID_FALLBACK_PANEL = (
    PROJECT_ROOT / r"data\processed\forecast_model_corsi_v1"
    / "corsi_model_feature_panel_v1_20180625_20260710_utp_cta_20260711_223214.parquet"
)

FAILED_PANEL_DIR = PROJECT_ROOT / r"data\processed\forecast_model_corsi_v1"
FAILED_PANEL_GLOB = (
    "corsi_model_feature_panel_v1_20180625_20260710_utp_cta_20260712_*.parquet"
)

REQUIRED_COLUMNS = {"spx_close", "spx_log_return"}
EXPECTED_TENORS = {9, 12, 15, 18, 21, 24, 27, 30, 33}
CHUNK_SIZE = 8 * 1024 * 1024


def import_pandas():
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas and a Parquet engine are required.") from exc
    return pd


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_exact(path: Path, expected_size: int, expected_hash: str, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} is missing: {path}")
    if not path.is_file():
        raise RuntimeError(f"{label} is not a regular file: {path}")
    actual_size = path.stat().st_size
    if actual_size != expected_size:
        raise RuntimeError(
            f"{label} size mismatch: expected {expected_size}, found {actual_size}: {path}"
        )
    actual_hash = sha256_file(path)
    if actual_hash != expected_hash:
        raise RuntimeError(
            f"{label} SHA-256 mismatch: expected {expected_hash}, found {actual_hash}: {path}"
        )


def read_parquet(path: Path):
    pd = import_pandas()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Could not read Parquet file {path}: {exc}") from exc


def validate_good_panel(path: Path, label: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{label} is missing: {path}")
    df = read_parquet(path)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise RuntimeError(
            f"{label} is missing required columns {sorted(missing)}: {path}"
        )
    if "tenor" in df.columns:
        tenors = set(int(x) for x in df["tenor"].dropna().unique())
        if not EXPECTED_TENORS.issubset(tenors):
            raise RuntimeError(
                f"{label} is missing expected tenors. Found: {sorted(tenors)}"
            )
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "required_columns_present": True,
    }


def copy_exact(source: Path, destination: Path, execute: bool) -> str:
    expected = EXPECTED[source]
    verify_exact(
        source,
        expected["size_bytes"],
        expected["sha256"],
        f"Keeper {source.name}",
    )

    if destination.exists():
        try:
            verify_exact(
                destination,
                expected["size_bytes"],
                expected["sha256"],
                f"Existing staging {destination.name}",
            )
            return "STAGING_ALREADY_PRESENT_AND_VERIFIED"
        except Exception:
            raise RuntimeError(
                f"Staging destination exists but does not match the verified keeper: {destination}"
            )

    if not execute:
        return "READY_TO_CREATE_STAGING_COPY"

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(destination.name + ".vrp_repair_partial")
    if temp.exists():
        temp.unlink()

    shutil.copy2(source, temp)
    verify_exact(
        temp,
        expected["size_bytes"],
        expected["sha256"],
        f"Temporary staging copy {temp.name}",
    )
    os.replace(temp, destination)
    verify_exact(
        destination,
        expected["size_bytes"],
        expected["sha256"],
        f"Final staging copy {destination.name}",
    )
    return "STAGING_CREATED_AND_VERIFIED"


def find_poisoned_panels() -> list[Path]:
    poisoned: list[Path] = []
    for path in sorted(FAILED_PANEL_DIR.glob(FAILED_PANEL_GLOB)):
        df = read_parquet(path)
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing == REQUIRED_COLUMNS:
            poisoned.append(path)
        elif missing:
            raise RuntimeError(
                f"Refusing automatic quarantine because panel has a partial protected schema "
                f"(missing {sorted(missing)}): {path}"
            )
        else:
            print(f"KEEP VALID 2026-07-12 PANEL: {path.name}")
    return poisoned


def quarantine_panel(source: Path, execute: bool) -> str:
    destination = QUARANTINE_ROOT / source.name
    source_hash = sha256_file(source)
    source_size = source.stat().st_size

    if destination.exists():
        if destination.stat().st_size != source_size:
            raise RuntimeError(f"Quarantine destination has different size: {destination}")
        if sha256_file(destination) != source_hash:
            raise RuntimeError(f"Quarantine destination has different hash: {destination}")
        if execute:
            source.unlink()
            return "SOURCE_REMOVED_AFTER_ARCHIVE_REVERIFY"
        return "READY_SOURCE_MATCHES_EXISTING_QUARANTINE"

    if not execute:
        return "READY_TO_QUARANTINE"

    QUARANTINE_ROOT.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(destination.name + ".partial")
    if temp.exists():
        temp.unlink()

    shutil.copy2(source, temp)
    if temp.stat().st_size != source_size or sha256_file(temp) != source_hash:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f"Quarantine copy verification failed: {source}")

    os.replace(temp, destination)
    if destination.stat().st_size != source_size or sha256_file(destination) != source_hash:
        raise RuntimeError(f"Final quarantine verification failed: {destination}")

    source.unlink()
    return "QUARANTINED_AND_VERIFIED"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair the Corsi staging control panel and quarantine poisoned panels."
    )
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print("=" * 104)
    print("VRP CORSI CONTROL REPAIR + FAILED PANEL QUARANTINE v2")
    print("=" * 104)
    print(f"Mode:             {mode}")
    print(f"Project root:     {PROJECT_ROOT}")
    print(f"Quarantine root:  {QUARANTINE_ROOT}")
    print("=" * 104)

    try:
        # Validate the exact duplicate keepers preserved by the archive manifest.
        verify_exact(
            KEEPER_PARQUET,
            EXPECTED[KEEPER_PARQUET]["size_bytes"],
            EXPECTED[KEEPER_PARQUET]["sha256"],
            "Keeper production feature panel Parquet",
        )
        verify_exact(
            KEEPER_CSV,
            EXPECTED[KEEPER_CSV]["size_bytes"],
            EXPECTED[KEEPER_CSV]["sha256"],
            "Keeper production feature panel CSV",
        )
        keeper_info = validate_good_panel(
            KEEPER_PARQUET, "Keeper production feature panel"
        )
        print(
            f"PASS keeper panel: rows={keeper_info['rows']:,}; "
            f"columns={keeper_info['columns']:,}; "
            f"required SPX columns present"
        )

        fallback_info = validate_good_panel(
            VALID_FALLBACK_PANEL, "Valid pre-failure Corsi panel"
        )
        print(
            f"PASS fallback panel: rows={fallback_info['rows']:,}; "
            f"columns={fallback_info['columns']:,}; "
            f"required SPX columns present"
        )

        parquet_status = copy_exact(
            KEEPER_PARQUET, STAGING_PARQUET, args.execute
        )
        csv_status = copy_exact(
            KEEPER_CSV, STAGING_CSV, args.execute
        )
        print(f"{parquet_status}: {STAGING_PARQUET}")
        print(f"{csv_status}: {STAGING_CSV}")

        # In execute mode, validate the newly created staging Parquet before quarantine.
        if args.execute:
            staging_info = validate_good_panel(
                STAGING_PARQUET, "Repaired staging control panel"
            )
            print(
                f"PASS repaired staging panel: rows={staging_info['rows']:,}; "
                f"columns={staging_info['columns']:,}"
            )

        poisoned = find_poisoned_panels()
        print(f"Poisoned 2026-07-12 panels found: {len(poisoned)}")
        for path in poisoned:
            df = read_parquet(path)
            print(
                f"  {path.name}: rows={len(df):,}; columns={len(df.columns):,}; "
                f"missing={sorted(REQUIRED_COLUMNS - set(df.columns))}"
            )

        if not poisoned:
            print("No poisoned panels remain in the active directory.")
        else:
            for path in poisoned:
                status = quarantine_panel(path, args.execute)
                print(f"{status}: {path.name}")

    except Exception as exc:
        print("\nFAILED")
        print(repr(exc))
        return 2

    if args.execute:
        print("\nREPAIR COMPLETE")
        print("The staging control panel was recreated from its verified in-project keeper.")
        print("Only 2026-07-12 Corsi panels missing both protected SPX columns were quarantined.")
    else:
        print("\nDRY RUN PASSED")
        print("No files were copied or moved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
