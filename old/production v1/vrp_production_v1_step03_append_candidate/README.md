# VRP Production v1 - Step 03 Append Candidate

Creates a candidate appended repaired term-structure history from the Step 02 staging rows.

Safety rules:
- Does not overwrite the canonical repaired history.
- Requires Step 02 QA to be all GREEN by default.
- Fails on duplicate date/tenor overlap with the current canonical file.
- Writes candidate files under `data/processed/staging/`.
- Writes audit files under `data/audit/production_v1/`.

Example:

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step03_append_candidate"
py -m pip install -r requirements.txt
py run_step03_append_candidate.py --project-root "C:\Users\patri\vrp_project" --start-date 20260626 --end-date 20260702
```

Expected result for the current update:
- Old rows: 18,099
- New rows: 45
- Candidate rows: 18,144
- Candidate date range: 2018-06-25 to 2026-07-02
- Candidate dates not having 9 tenors: 0
- Duplicate date/tenor rows: 0
