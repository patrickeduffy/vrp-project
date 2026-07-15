# VRP Production v1 — Step 02 EOD Staging Updater

This step wraps the existing VIX-style builder notebook without running the backfill/update usage cells.
It builds missing EOD term-structure rows into staging files only. It does not overwrite the canonical repaired history.

## Main command

Run from this folder:

```powershell
py run_step02_eod_staging.py --project-root "C:\Users\patri\vrp_project" --start-date 20260626 --end-date 20260702 --refresh-sofr
```

## Outputs

Staging outputs are written to:

```text
data/processed/staging/
data/audit/production_v1/
```

The script writes:

- `vix_term_structure_eod_update_<start>_<end>.parquet`
- `vix_term_structure_eod_update_<start>_<end>.csv`
- `vix_term_structure_eod_update_<start>_<end>_errors.csv`
- `step02_eod_staging_<timestamp>.md`
- `step02_eod_staging_<timestamp>.json`

## Safety

This script intentionally calls `run_vix_term_structure_batch_v7(...)`, not the notebook's history upsert functions.
It should only create raw chain cache files and staging/audit outputs.

It should not overwrite:

```text
data/processed/vix_term_structure_history_v0_7_1_repaired_total_variance.parquet
```
