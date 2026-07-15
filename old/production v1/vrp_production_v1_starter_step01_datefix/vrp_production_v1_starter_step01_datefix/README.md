# VRP Production v1 Starter — Step 01 Inventory

This starter package is designed to be copied into the root of the VRP project.
It does **not** modify production data. Its first job is to inventory the current
state of the project and identify what needs to be updated next.

## What Step 01 does

1. Finds the latest date in the repaired VIX-style term-structure history.
2. Finds the latest date in the raw ThetaData chain cache filenames.
3. Finds latest dates in SPX, SOFR, realized variance, VRP, and feature files when present.
4. Checks the canonical term-structure schema.
5. Checks whether there are 9 tenors per trade date.
6. Computes completed trading dates missing from the EOD term-structure history.
7. Writes a JSON and Markdown inventory report under `data/audit/production_v1/`.

## Install / run

From your VRP project root:

```bash
python -m pip install -r requirements.txt
python run_step01_inventory.py --project-root . --as-of 2026-07-03
```

You can omit `--as-of` to use today's local date.

## Expected note for July 3, 2026

July 3, 2026 is an observed Independence Day market holiday, so the latest completed
EOD trading date should be July 2, 2026 if you run the script on July 3 after the holiday close.
If your history is only through June 26, 2026, the likely missing EOD dates are:

- 2026-06-29
- 2026-06-30
- 2026-07-01
- 2026-07-02

The script should verify this using the XNYS calendar if `pandas_market_calendars` is installed.

## Files

```text
run_step01_inventory.py              # main entry point
vrp_prod/src/vrp_prod/production/    # reusable inventory/calendar logic
vrp_prod/src/vrp_prod/qa/            # schema and data checks
vrp_prod/config/paths.yaml           # canonical file locations
requirements.txt
```

## Next after Step 01

Once the inventory report confirms the exact missing EOD dates, Step 02 is the append-only
EOD updater. That should call the existing v0.7 market-close ThetaData chain builder for
missing dates, run curve QA, and append only accepted rows to the official EOD panel.
