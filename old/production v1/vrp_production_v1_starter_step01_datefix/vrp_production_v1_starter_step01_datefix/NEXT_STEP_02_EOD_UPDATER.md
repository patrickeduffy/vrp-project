# Step 02 — Append-only EOD updater design

After Step 01 identifies missing completed EOD dates, Step 02 should build the official
EOD update path. This should initially wrap your existing v0.7 market-close notebook logic
rather than rewrite the whole variance engine at once.

## Inputs

- Missing EOD dates from Step 01 inventory.
- Existing repaired EOD term-structure file.
- SOFR history.
- SPX daily close history.
- ThetaData local terminal/API access.
- Existing v0.7 VIX-style builder functions or notebook code.

## Process contract

For each missing EOD date:

1. Confirm date is an XNYS trading day.
2. Get actual market close time from XNYS calendar.
3. Pull/cache required SPX/SPXW chains at market-close quote time.
4. Build single-expiration variances.
5. Interpolate to 9, 12, 15, 18, 21, 24, 27, 30, and 33 days in total-variance space.
6. Run curve QA.
7. If GREEN or allowed YELLOW, append to staging file.
8. Never overwrite the repaired historical seed.
9. Save audit rows for every date, including failures.

## Acceptance checks before appending to official EOD file

- 9 rows per trade date.
- Target tenors exactly match the locked universe.
- No duplicate trade_date / target_days rows.
- No negative or zero implied variance.
- No materially negative adjacent forward variance.
- Raw values preserved if repaired.
- Methodology version explicitly written.

## Output files

- `data/processed/vix_term_structure_eod_staging.parquet`
- `data/audit/production_v1/eod_update_run_log_*.csv`
- `data/audit/production_v1/eod_curve_qa_*.csv`

Only after visual/audit review should the staging output be merged into the canonical EOD file.
