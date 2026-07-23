# Production operations

## Operating boundary

The active system is a **completed-EOD decision pipeline** for the locked Hybrid v2 SPY put-spread model.

It does not:

- place orders;
- choose live option legs or perform whole-contract rounding;
- approve portfolio overlap or stress exposure;
- produce a live intraday trading signal;
- modify locked signal parameters from the dashboard.

A displayed `TRADE` is a model decision that still requires separate execution and portfolio-risk approval.

## Current production contract

- Release ID: `vrp_corsi_intraday_hybrid_v2`
- Target tenors: 9, 12, 15, 18, 21, 24, 27, 30, and 33 DTE
- Forecast return source: canonical SPY adjusted close and SPY log returns
- Return-source fallback: prohibited
- Implied-variance source: SPX/SPXW option chains
- SOFR rule: latest observation strictly before the trade date
- Expiration clock: actual XNYS session close, including early closes
- RSI formula: `wilder_rsi14_spy_close_v3_clean_session_rebuild`
- Historical repair baseline: accepted and published through 2026-07-16

The operational source of truth is `config/vrp_hybrid_v2_eod_runtime_config.json`.

## Prerequisites

- Python environment installed from root `requirements.txt`
- ThetaData Terminal running at `127.0.0.1:25503` for upstream refreshes
- Local canonical data and locked research artifacts available at the paths in the runtime config
- Clean and synchronized `main` branch for production code
- PostgreSQL migrations `0001` and `0002` applied
- `VRP_DATABASE_URL` set in the invoking process environment for a normal
  published run, without placing the password in a command or file

Install direct dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Standard EOD run

From `C:\Users\patri\vrp_project`:

```powershell
python scripts\run_eod.py `
  --project-root C:\Users\patri\vrp_project `
  --approved-nav 1000000
```

The stable command delegates to the accepted EOD orchestrator. After a normal
published file run passes, it records the exact result in PostgreSQL as a
non-authoritative shadow. The source checkout and data-bearing checkout must be
clean and on the same Git commit. Use `--dry-run` to inspect the delegated file
command and post-pass policy without executing either step.

Each EOD process atomically claims its timestamped audit directory. If another
process has already claimed the same directory, the later process fails instead
of sharing or overwriting run evidence.

The wrapper also holds a project-wide operation lock across both the file run
and PostgreSQL post-pass. The legacy child independently holds a canonical-file
writer lock while it can change file outputs, so it remains protected even if
its wrapper exits unexpectedly. A standalone finalizer takes those same locks
plus an exact-run lock. PostgreSQL reference/shadow writers are additionally
serialized by a database-global advisory lock.

The default target is the latest completed XNYS session after the configured close buffer.

For a same-day refresh, wait until ThetaData has published the stock EOD record (normally after approximately 5:15 p.m. ET). If the requested SPY EOD row is unavailable, retain the prior accepted production output and retry later; do not manufacture a same-day close.

### Diagnostic modes

```text
--target-date YYYYMMDD   Run through an explicit completed session.
--force-recalculate      Rebuild from the earliest detected gap.
--skip-upstream          Use existing upstream inputs; requires --no-publish.
--no-publish             Build and validate staged outputs without replacing canonical outputs.
--no-postgres-shadow     Explicitly run file EOD without the database post-pass.
```

Use `--no-publish` for investigation. `--skip-upstream` is rejected unless
`--no-publish` is also supplied; do not use it to conceal missing or stale
source data.

`--no-publish` skips PostgreSQL finalization. It still respects older
finalization debt when it refreshes upstream data; only the combined
`--skip-upstream --no-publish` diagnostic can avoid that gate.

`--no-postgres-shadow` is a deliberate break-glass bypass, not a retry mode. If
`--no-publish` is absent, canonical file publication still occurs, but the run
does not require `VRP_DATABASE_URL`, is not marked as owing a later post-pass,
and does not create finalization status. The bypass still stops if an older
obligated run is unresolved. Use it only when a file-only production result has
been explicitly accepted. Its manifest records
`postgres_postpass_bypass_reason: explicit-no-postgres-shadow`.

A direct legacy publication is rejected before the model pipeline starts unless
it declares either the normal post-pass obligation or that audited bypass. This
prevents an unmarked direct command from silently creating a file-only result.

A normal published run records its database obligation in the manifest. If
finalization fails, the process returns nonzero while retaining the healthy
file result. Read the exact audit directory's status and retry only that
post-pass with `scripts/finalize_eod_postgres.py`, using the recorded
`code_version`, `run_manifest_sha256`, and `source_bundle_sha256`; do not
discover or guess the latest run. The retry reads the frozen SOFR snapshot named
by that exact source bundle, even if the canonical SOFR history has advanced.

`postgres_finalization_status.json` is the primary attempt. Once it records
matching `COMPLETED` evidence, it is preserved. Every later full reconciliation
is recorded in `postgres_finalization_last_attempt.json`. The latest retry must
also be matching `COMPLETED`; a failed or mismatched latest retry reopens the
gate even when the primary file still shows the older success.

Treat a failed required post-pass as a stop gate for later production EOD
runs. Resolve exact run directories oldest-first before allowing a newer
PostgreSQL reference release to advance. Direct mutating reference and shadow
loaders enforce the same ordering and database-global advisory lock; they do
not bypass debt and do not replace the exact-run finalizer.

The finalizer also verifies, under that lock, that every earlier obligated
sidecar still maps to its exact completed full projection, assets, stage/QA
evidence, and no-publication contract in the connected database. It recomputes
the current read-back fingerprint, so missing rows and same-count value changes
both block the new run.

The production obligation queue is fixed at
`data/audit/vrp_hybrid_v2_eod`; changing the runtime configuration cannot move
the queue and hide unresolved history.

The PostgreSQL post-pass remains non-authoritative: it never inserts into
`vrp.signal_publications`. The accepted file decision remains the official EOD
result until a separately approved database-publication cutover.

## PostgreSQL restore or target replacement

Finalization sidecars describe the database projection observed when they were
written; they do not continuously monitor the current database. A new
finalization actively checks all earlier obligated identities against its
connected target, but switching `VRP_DATABASE_URL`, restoring a backup, or
replacing the database still requires a controlled full replay even if every
sidecar currently says `COMPLETED`.

1. Stop all EOD and standalone loader processes.
2. Apply migrations `0001` and `0002` to the active target.
3. Find all timestamped published `PASS` audits marked
   `postgres_postpass_required: true` for the environment.
4. Replay them oldest-first with `scripts/finalize_eod_postgres.py`, checking
   out each exact producing commit and supplying that audit's exact manifest
   and source-bundle hashes.
5. Require the fresh attempt for every audit to reconcile as `COMPLETED`.
   Previously completed runs write this fresh evidence to
   `postgres_finalization_last_attempt.json`.
6. Resume EOD only after the full obligated history reconciles against the
   active database.

Do not trust filesystem sidecars alone, delete them to clear the gate, or run a
newer reference load while this recovery is incomplete.

## Health check

Run after a production refresh or when diagnosing the dashboard:

```powershell
python notebooks\vrp_hybrid_v2_health_check.py `
  --project-root C:\Users\patri\vrp_project `
  --runtime-config C:\Users\patri\vrp_project\config\vrp_hybrid_v2_eod_runtime_config.json
```

For a local check that intentionally skips the ThetaData connectivity probe:

```powershell
python notebooks\vrp_hybrid_v2_health_check.py `
  --project-root C:\Users\patri\vrp_project `
  --runtime-config C:\Users\patri\vrp_project\config\vrp_hybrid_v2_eod_runtime_config.json `
  --no-thetadata-probe
```

A production-ready result requires `OVERALL_STATUS: PASS` and zero hard failures.

## Golden reconciliation

After structural calculation or storage changes, reconcile the accepted historical examples:

```powershell
python scripts\golden_eod.py verify `
  --source-root C:\Users\patri\vrp_project
```

A production-ready result requires `GOLDEN_STATUS: PASS`. Routine daily history extensions do not require recapturing the fixture.

## Dashboard

Launch from the repository root:

```powershell
python -m streamlit run notebooks\streamlit_vrp_hybrid_v2_eod.py
```

The dashboard launches the standalone pipeline and reads canonical outputs. It contains no independent model mathematics.
Its normal refresh button invokes `scripts/run_eod.py`, so the Streamlit process
must inherit `VRP_DATABASE_URL`. The dashboard's skip-upstream diagnostic uses
`--skip-upstream --no-publish` and does not publish files or run the database
post-pass.

## Canonical output set

The active processed directory is:

```text
data/processed/vrp_hybrid_v2_eod/
```

Primary outputs include:

- `vrp_hybrid_v2_forecast_history.parquet`
- `vrp_hybrid_v2_signal_history.parquet`
- `vrp_hybrid_v2_latest_snapshot.parquet`
- `vrp_hybrid_v2_selected_decisions.parquet`
- `vrp_hybrid_v2_static_tiebreaks.csv`
- `vrp_hybrid_v2_latest_execution_handoff.csv`
- `vrp_hybrid_v2_data_status.json`

Repair lineage and acceptance records are also stored there and referenced by the runtime config.

## Failure handling

1. Do not manually overwrite a canonical output.
2. Read the pipeline error and per-step stdout/stderr in the latest audit directory.
3. Run the health check separately to isolate the failing component.
4. Confirm ThetaData and SOFR availability before changing code.
5. Use `--no-publish` for a repair trial.
6. Require tests and health to pass before publishing or merging a production fix.

The current file pipeline stages outputs and retains a backup for rollback, but it replaces the canonical files sequentially and performs final health checks afterward. It is recoverable, not transactionally atomic to concurrent readers. Do not treat the dashboard as stable while publication is in progress. PostgreSQL is currently a reconciled shadow, not the authoritative publication boundary. A file-pipeline failure restores the last healthy canonical set; a later PostgreSQL post-pass failure retains the newly validated healthy file set and is retried by exact run directory.

## Change control

Changes to thresholds, sizing, or selection require a new model-lock version. Operational source repairs require regression tests, explicit lineage in the runtime config, and a clean health check.
