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

The stable command delegates to the accepted EOD orchestrator. Use `--dry-run` to inspect the delegated command without executing it.

The default target is the latest completed XNYS session after the configured close buffer.

For a same-day refresh, wait until ThetaData has published the stock EOD record (normally after approximately 5:15 p.m. ET). If the requested SPY EOD row is unavailable, retain the prior accepted production output and retry later; do not manufacture a same-day close.

### Diagnostic modes

```text
--target-date YYYYMMDD   Run through an explicit completed session.
--force-recalculate      Rebuild from the earliest detected gap.
--skip-upstream          Use existing upstream inputs; publisher checks still apply.
--no-publish             Build and validate staged outputs without replacing canonical outputs.
--shadow-write           After publication, sync references and record the exact PostgreSQL shadow.
--shadow-timeout-seconds Bound each post-publication loader (default: 300 seconds).
```

Use `--no-publish` for investigation. Do not use `--skip-upstream` to conceal missing or stale source data.

### Optional PostgreSQL shadow record

PostgreSQL remains a non-authoritative reconciliation copy. Shadow writing is
disabled by default. Enable it only after the two least-privilege runtime
accounts in `docs/EOD_POSTGRES_SHADOW.md` have been configured:

```powershell
$env:VRP_REFERENCE_DATABASE_URL = "host=127.0.0.1 port=5432 dbname=vrp_shadow user=vrp_reference_loader_local"
$env:VRP_EOD_DATABASE_URL = "host=127.0.0.1 port=5432 dbname=vrp_shadow user=vrp_eod_shadow_local"

python scripts\run_eod.py `
  --project-root C:\Users\patri\vrp_project `
  --approved-nav 1000000 `
  --shadow-write
```

The connection strings should omit passwords and use an owner-restricted
PostgreSQL password file or another local secret mechanism. Never put a
password in a BAT file, Git, or command argument.

Before canonical work begins, the wrapper connects read-only to both targets
and proves that they are distinct non-superuser accounts with exactly the
intended reference-loader/EOD-writer separation. A superuser, database owner,
shared account, unexpected role membership, object owner, or account with any
privilege outside the reviewed table/column/function allowlist is rejected.

The wrapper pins one clean full Git commit before execution. After the file
pipeline publishes and passes final health, it captures the exact manifest
path printed by that child process, loads revision-safe SOFR/SPY history, and
revalidates the clean HEAD and tracked runtime/loader paths before either
credentialed loader starts. The wrapper
then records that exact run. It never selects the “latest” audit directory.

Shadow failure has deliberately different semantics from file-pipeline
failure:

- ordinary file-pipeline failure retains/restores the prior canonical output;
- exit code `3` means canonical publication succeeded, but PostgreSQL shadow
  recording failed;
- canonical files are never rolled back because a non-authoritative shadow
  write failed; and
- `post_publish_shadow_status.json` in the exact run directory records the
  sanitized outcome without database credentials.

Stopping a dashboard refresh first sends a graceful cancellation request so
the legacy pipeline can restore its canonical backup. A forced process-tree
termination is used only after a 30-second grace period. Post-publication
database loaders are bounded by connection, lock, statement, and process
timeouts so a dead database cannot leave the dashboard waiting indefinitely.

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

The dashboard launches the stable `scripts/run_eod.py` interface and reads
canonical outputs. It contains no independent model mathematics. The advanced
controls expose opt-in shadow recording. If the file pipeline publishes but
the shadow step fails, the dashboard reports the canonical publication as
healthy and the PostgreSQL copy as needing attention.

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

For an exit-code-`3` shadow failure, do not rerun or roll back a healthy
canonical publication. Inspect `post_publish_shadow_status.json`, repair the
database/credential issue, and rerun the idempotent reference and snapshot
loaders against the exact completed run.

The current file pipeline stages outputs and retains a backup for rollback, but it replaces the canonical files sequentially and performs final health checks afterward. It is recoverable, not transactionally atomic to concurrent readers. Do not treat the dashboard as stable while publication is in progress. The planned PostgreSQL publication record will become the true atomic visibility boundary; until then, a failed run should restore the last healthy canonical set from the run backup.

## Change control

Changes to thresholds, sizing, or selection require a new model-lock version. Operational source repairs require regression tests, explicit lineage in the runtime config, and a clean health check.
