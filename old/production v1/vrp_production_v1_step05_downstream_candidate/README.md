# VRP Production v1 — Step 05 Downstream Candidate Builder

Builds candidate downstream panels after the official repaired VIX-style term-structure history has been updated.

This script:

1. Refreshes FRED SPX daily closes when requested.
2. Saves normalized SPX close/log-return history.
3. Rebuilds realized variance by trade_date × tenor.
4. Rebuilds the VRP panel.
5. Rebuilds the clean production feature panel.
6. Writes candidate files under `data/processed/staging/`.
7. Writes QA reports under `data/audit/production_v1/`.

It does **not** overwrite the official realized/VRP/feature panels. Promotion should be done in a later step after QA review.

## Run

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step05_downstream_candidate"
py -m pip install -r requirements.txt
py run_step05_downstream_candidate.py --project-root "C:\Users\patri\vrp_project" --refresh-spx
```
