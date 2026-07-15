# VRP Production v1 — Step 07 Signal Snapshot

Builds the official EOD locked 2621 signal snapshot from the current production feature panel.

Inputs:
- `data/processed/production_feature_panel_v0_1.parquet`

Outputs:
- `data/processed/signals/locked_2621_eod_signal_snapshot_<YYYYMMDD>.csv`
- `data/processed/signals/locked_2621_eod_signal_snapshot_<YYYYMMDD>.parquet`
- `data/processed/signals/locked_2621_eod_signal_summary_<YYYYMMDD>.csv`
- `data/processed/signals/locked_2621_eod_signal_summary_<YYYYMMDD>.json`
- `data/audit/production_v1/step07_signal_snapshot_<run_id>.md`
- `data/audit/production_v1/step07_signal_snapshot_<run_id>.json`

Run:

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step07_signal_snapshot"
py -m pip install -r requirements.txt
py run_step07_signal_snapshot.py --project-root "C:\Users\patri\vrp_project" --signal-date 20260702 --nav 1000000
```

If `--signal-date` is omitted, the script uses the latest trade_date in the feature panel.

Notes:
- This is a signal/sizing snapshot only, not an order-entry script.
- Sizing is max-risk at inception, not premium, margin, expected loss, or portfolio-level risk.
- Portfolio approval remains manual.
