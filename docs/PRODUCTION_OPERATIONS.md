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
```

Use `--no-publish` for investigation. Do not use `--skip-upstream` to conceal missing or stale source data.

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

The current file pipeline stages outputs and retains a backup for rollback, but it replaces the canonical files sequentially and performs final health checks afterward. It is recoverable, not transactionally atomic to concurrent readers. Do not treat the dashboard as stable while publication is in progress. The planned PostgreSQL publication record will become the true atomic visibility boundary; until then, a failed run should restore the last healthy canonical set from the run backup.

## Change control

Changes to thresholds, sizing, or selection require a new model-lock version. Operational source repairs require regression tests, explicit lineage in the runtime config, and a clean health check.
