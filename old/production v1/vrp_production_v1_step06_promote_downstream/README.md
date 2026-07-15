# VRP Production v1 - Step 06 Promote Downstream Candidate

Promotes the QA'd Step 05 candidate downstream panels to official files, with backups and audit report.

Run from PowerShell:

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step06_promote_downstream"
py -m pip install -r requirements.txt
py run_step06_promote_downstream.py --project-root "C:\Users\patri\vrp_project" --expected-end-date 20260702 --confirm-promote
```

Without `--confirm-promote`, the script performs a dry run only.

Official files promoted:

- `data/processed/realized_variance_panel_v0_1.parquet/.csv`
- `data/processed/vrp_panel_v0_1.parquet/.csv`
- `data/processed/production_feature_panel_v0_1.parquet/.csv`
- `data/processed/production_feature_panel_latest_snapshot_v0_1.csv`

Backups are written to:

- `data/processed/backups/production_v1/`

Audit report is written to:

- `data/audit/production_v1/`
