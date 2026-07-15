# VRP Production v1 - Step 08 Daily Pipeline Wrapper

This wrapper calls the already-tested Steps 01-07 from one command.

It searches under your project folder for the existing step scripts, then:

1. Runs Step 01 inventory.
2. If missing EOD term-structure dates exist, runs Steps 02-04 to stage, append-candidate, and promote the term-structure update.
3. Runs Step 01 again.
4. If downstream panels are stale, runs Steps 05-06 to rebuild and promote realized variance, VRP, and feature panels.
5. Runs Step 01 a final time.
6. Runs Step 07 signal snapshot for the final latest term-structure date.
7. Writes a master audit report/log under `data/audit/production_v1`.

## Install/run

Unzip this folder into:

```text
C:\Users\patri\vrp_project\production v1
```

Then run:

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step08_daily_pipeline"

py run_step08_daily_pipeline.py --project-root "C:\Users\patri\vrp_project" --as-of 2026-07-03 --nav 1000000
```

If the feature panel is already current, Steps 05-06 are skipped. To rebuild downstream files anyway:

```powershell
py run_step08_daily_pipeline.py --project-root "C:\Users\patri\vrp_project" --as-of 2026-07-03 --nav 1000000 --force-downstream
```

To run the signal for a specific date:

```powershell
py run_step08_daily_pipeline.py --project-root "C:\Users\patri\vrp_project" --as-of 2026-07-03 --nav 1000000 --force-signal-date 20260330
```
