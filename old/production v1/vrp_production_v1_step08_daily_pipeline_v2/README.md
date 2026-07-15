# VRP Production v1 - Step 08 Daily Pipeline Wrapper v2

Patch note: v2 fixes the stale-check logic when Step 01 prints a compact stdout JSON but writes the full inventory to the `json_report` file. The prior wrapper could incorrectly treat downstream panels as stale and fail with `'NoneType' object has no attribute 'replace'`.

## Run

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step08_daily_pipeline_v2"

py run_step08_daily_pipeline.py --project-root "C:\Users\patri\vrp_project" --as-of 2026-07-03 --nav 1000000
```

Expected when everything is current:

```text
No missing EOD term-structure dates. Skipping Steps 02-04.
Downstream panels already match term-structure latest date. Skipping Steps 05-06.
Step 07 signal snapshot complete.
Step 08 daily pipeline complete.
All green: True
```
