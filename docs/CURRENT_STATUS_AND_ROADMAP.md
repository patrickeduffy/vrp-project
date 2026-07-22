# Current status and roadmap

## Current status

The locked Hybrid v2 put-sleeve methodology and repaired completed-EOD calculation path are the accepted production baseline.

- Canonical history is current through 2026-07-21.
- The July 2026 source, SOFR, expiration-clock, and Wilder-RSI repairs are accepted.
- The existing EOD regression suite passes.
- Baseline commit: `c3857984def9d295bd49dc7eab7c5a8421b0ed5b`.
- Baseline tag: `eod-v2-production-baseline-2026-07-21`.
- Golden EOD examples protect representative trade and no-trade decisions.

The current objective is to complete the production pipeline, intraday signal capability, and deployment before resuming other research.

## 1. Production foundation and deterministic EOD pipeline

This is the active workstream.

1. Preserve accepted production outputs as golden cases.
2. Introduce stable `src/vrp/` package boundaries around the validated calculations.
3. Keep one production entry point at `scripts/run_eod.py`.
4. Add versioned PostgreSQL migrations for operational and signal data.
5. Retain raw and standardized large market data as partitioned Parquet.
6. Record every run, stage, data asset, model version, configuration version, QA result, and selected signal.
7. Make reruns idempotent and failed stages restartable.
8. Publish the latest signal only after every required stage passes.
9. Remove runtime dependencies on archived notebooks and ignored generated source dumps one validated component at a time.

Completion requires:

- one command rebuilds a complete EOD signal;
- the golden dates reconcile within their locked tolerances;
- stale or incomplete inputs cannot publish a valid signal;
- every output carries data, code, model, and configuration lineage;
- PostgreSQL holds operational outputs while Parquet retains large source datasets;
- the latest result states either the selected trade or an explicit no-trade reason.

## 2. Intraday signal engine

Begin only after the EOD path is deterministic through the new production interfaces.

Version 1 will run approximately every 15 minutes during market hours and operate in shadow mode.

Updated intraday:

- SPX/SPXW option quotes;
- SPX spot;
- implied-variance term structure;
- VRP numerator;
- preview qualification and tenor ranking;
- an explicitly defined intraday RSI estimate.

Fixed from the prior official close:

- Corsi forecast denominator;
- historical three-month and one-year z-score distributions;
- RV21D;
- model parameters and signal thresholds.

Snapshots will distinguish `NO_SIGNAL`, `PREVIEW_SIGNAL`, `PREVIEW_SIGNAL_CHANGED`, `DATA_DEGRADED`, and `EOD_OFFICIAL`. No automatic order placement is in scope.

Completion requires retained snapshots, quote-freshness and chain-quality assessments, one-query latest-signal retrieval, traceable signal changes, shared calculation code with EOD, and automatic last-snapshot-versus-official reconciliation.

## 3. Deployment and remote signal visibility

Deploy only after intraday shadow mode is reliable locally.

- Run collection, EOD, intraday scheduling, and QA without an open desktop session.
- Use managed PostgreSQL for operational data and object storage for Parquet.
- Keep credentials in a managed secret store, never in Git.
- Record the deployed Git commit and configuration version.
- Provide a secure phone-accessible latest-signal page.
- Alert on new or materially changed signals, stale data, and failed runs.
- Support manual reruns, backfills, backups, and rollback.

The ThetaData access arrangement is the deployment gate: either collect directly on the deployment host or run a local collector that publishes standardized snapshots to cloud storage.

## 4. Dashboard

The completed-EOD Streamlit dashboard remains useful but is below the signal engine in priority. Calculation logic stays outside Streamlit.

After deployment, extend it with intraday-versus-official status, data freshness, QA state, threshold distances, signal history, and calculation traceability.

## 5. Deferred research

The following remain intentionally on the back burner until sections 1 through 3 are complete:

- 30D Excel short-call replication;
- forecast-VRP call research;
- call-sleeve term-structure expansion;
- combined put-and-call portfolio research;
- portfolio overlap and sizing layers;
- multi-ticker extensions.

## Required order

```text
Golden EOD contract
    -> deterministic and traceable EOD production
    -> 15-minute intraday shadow engine
    -> remote deployment and alerts
    -> dashboard expansion
    -> deferred research
```
