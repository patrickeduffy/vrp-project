# Database architecture

## Decision

The production data layer uses three complementary components:

1. **Parquet** stores immutable raw and standardized market data, including full SPX/SPXW option chains and large historical panels.
2. **PostgreSQL** stores operational state, lineage, compact model outputs, QA results, and published signal snapshots.
3. **DuckDB** is the local research and reconciliation engine over Parquet. It is not a production system of record.

This split keeps high-volume quote data inexpensive and portable while giving the production pipeline transactional publication, constraints, and fast latest-signal queries. TimescaleDB is intentionally deferred until measured intraday volume demonstrates a need for it.

The operational schema begins in [`migrations/0001_operational_schema.sql`](../migrations/0001_operational_schema.sql). Compact revision-safe SOFR and SPY daily history is added by [`migrations/0002_reference_data.sql`](../migrations/0002_reference_data.sql). Both use only built-in PostgreSQL types and features. The application must supply UUID values; no UUID extension is required.

## Data flow

```text
ThetaData / market references
          |
          v
immutable raw Parquet -----> standardized Parquet
          |                          |
          +----------+---------------+
                     v
             version-pinned run
                     |
          +----------+-----------+
          |                      |
          v                      v
  derived Parquet        PostgreSQL outputs
  and manifests          stages, terms, signals, QA
                                 |
                                 v
                    atomic publication record
                                 |
                                 v
                latest signal / dashboard / alerts
```

Raw quotes are not duplicated into PostgreSQL. PostgreSQL records each file's location, SHA-256 digest, time coverage, row count, schema version, and use in a run. This is sufficient to reproduce an output from immutable Parquet inputs while keeping the operational database small.

## Schema map

All objects live in the `vrp` schema.

| Object | Purpose |
|---|---|
| `model_versions` | Immutable model identity, content digest, and model-lock manifest. |
| `configuration_versions` | Immutable runtime and signal configuration payloads. |
| `pipeline_runs` | One version-pinned calculation for one valuation date and snapshot time. |
| `pipeline_run_stages` | Restart checkpoints, attempt counts, fingerprints, errors, and stage metrics. |
| `data_assets` | Content-addressed manifests for Parquet and other persisted artifacts. |
| `pipeline_run_data_assets` | Input, output, intermediate, manifest, and QA-evidence lineage. |
| `reference_data_releases` | Immutable accepted versions of normalized compact historical datasets. |
| `reference_rate_observations` | Append-only SOFR observations with explicit percentage and decimal units. |
| `daily_market_feature_definitions` | Immutable SPY close, return, RSI14, and signal-RV21D formula contracts. |
| `daily_market_features` | Append-only compact SPY daily values and recursive RSI state. |
| `market_snapshots` | Compact market context and freshness status for a run. |
| `implied_variance_term_structure` | VIX-style implied variance results by target tenor. |
| `forecast_variance_term_structure` | Locked Corsi forecast variance results by target tenor. |
| `signal_features` | VRP, prior-window means and sample deviations, z-scores, RSI, and realized-volatility inputs by tenor. |
| `signal_evaluations` | Layer-specific threshold outcomes and ranking details. |
| `selected_signals` | Exactly one trade, no-trade, or withheld decision for each snapshot. |
| `qa_results` | Machine-readable hard gates, warnings, evidence, and expected values. |
| `signal_publications` | Append-only visibility boundary for fully accepted snapshots. |
| `latest_published_snapshot` | One-query latest decision in each publication scope. |

The reference-data tables retain only new or corrected rows for each accepted
release. Corrections point to their predecessors; current views select the
unsuperseded row. See [`REFERENCE_DATA_STORAGE.md`](REFERENCE_DATA_STORAGE.md)
for the exact source units, formulas, warm-up behavior, and migration contract.

Nine rows are expected in each term-structure and feature set for the current 9, 12, 15, 18, 21, 24, 27, 30, and 33-day contract. A missing tenor should still receive a row with `MISSING` or `FAILED` status so absence is explicit and can fail the applicable QA gate.

## Run identity and idempotency

The application creates a stable `idempotency_key` for a logical run. The key is unique within an environment. It should be derived from, or encode, at least:

- run kind;
- valuation date and snapshot timestamp;
- data cutoff;
- model version;
- configuration version;
- code version; and
- an explicit force-recalculation generation, when applicable.

Submitting the same logical job again must look up and resume the existing `pipeline_runs` row rather than insert another row. A deliberate replacement uses a new key and records the prior run in `supersedes_run_id`.

The application supplies every UUID. UUID generation must occur before database writes so the same identifiers can be used in logs and staged artifacts.

## Restart behavior

Each pipeline stage has one row per run and a stable order. Before executing a stage, the runner records:

- `RUNNING` status;
- the incremented attempt count;
- the start time; and
- a SHA-256 fingerprint of the stage's effective inputs.

After success, it records the output fingerprint, metrics, finish time, and `COMPLETED` status in the same transaction as the stage outputs and asset lineage. On restart, a completed stage may be reused only when its input fingerprint still matches and all referenced output assets still match their recorded digests. Otherwise that stage and its downstream stages must be recalculated.

The stage table is a checkpoint ledger, not a task queue. Only one orchestrator should own a given run at a time; deployment should use an application-level lease or PostgreSQL advisory lock around run execution if overlapping schedulers become possible.

## Versioning and traceability

Every run points to one immutable model version, one immutable configuration version, and one code version. The model and configuration rows include SHA-256 digests so a familiar label cannot silently acquire different contents.

Every material file used or produced by a run is registered in `data_assets` and attached through `pipeline_run_data_assets`. The link records its role, logical name, stage, and optional lineage metadata. A signal can therefore be traced through:

```text
published decision
  -> selected evaluation
  -> tenor feature row
  -> implied and forecast variance rows
  -> market snapshot and pipeline run
  -> model, configuration, code, stages, QA, and immutable data assets
```

Operational output rows are treated as immutable after publication. Corrections create a superseding run and a new publication; they do not rewrite the prior accepted record.

## Atomic publication

Calculations are private until the final `signal_publications` insert. The database enforces that a publication references:

- one internally consistent run, market snapshot, and selected decision;
- a run whose status is `COMPLETED`; and
- a run whose aggregate QA status is `PASS`.

The runner performs finalization in one database transaction:

```sql
BEGIN;

-- Insert or update all final QA rows first.
-- Verify in application code that every required stage completed and every
-- hard gate passed.

UPDATE vrp.pipeline_runs
SET status = 'COMPLETED',
    qa_status = 'PASS',
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE pipeline_run_id = :pipeline_run_id
  AND status = 'RUNNING';

INSERT INTO vrp.signal_publications (
    signal_publication_id,
    publication_scope,
    pipeline_run_id,
    market_snapshot_id,
    selected_signal_id,
    published_by
) VALUES (
    :signal_publication_id,
    'production/eod',
    :pipeline_run_id,
    :market_snapshot_id,
    :selected_signal_id,
    :published_by
);

COMMIT;
```

If any statement or the commit fails, the publication is absent and consumers continue seeing the preceding healthy snapshot. There is no mutable `is_latest` flag. The latest view orders the append-only publications, avoiding a two-row current-pointer race.

Before finalization, register the golden verification manifest as a `QA_EVIDENCE` data asset and attach it to the run. Use the publisher-side validator to require `PASS`/`STAGED`, validate the manifest schema and content ID, pin the accepted baseline and fixture digest, enforce allowed staging paths, and rehash the fixture plus both artifacts. Store the manifest's verification ID in publication metadata so the visible snapshot is bound to the exact artifacts that passed reconciliation. The database publisher must consume those validated paths in the same controlled orchestration step; the current legacy file publisher does not yet enforce this boundary.

Hard-failed input must set the run to `FAILED`, set aggregate QA to `FAIL`, and omit the publication. A deliberately withheld decision caused by recognized degraded data can be represented as a `WITHHELD` selected signal with `DATA_DEGRADED` state only when the pipeline's validation of that state itself passes. It cannot appear as a valid trade.

A `NO_TRADE` or `WITHHELD` row must include an explicit `no_trade_reason`. During file-to-database dual writes, derive that reason deterministically from the per-tenor evaluation failures and retain the complete breakdown in `selection_trace`; do not copy the current file output's null summary reason into PostgreSQL.

## Reading the latest signal

The dashboard, notification service, and lightweight phone view read the publication view rather than uncommitted calculation tables:

```sql
SELECT *
FROM vrp.latest_published_snapshot
WHERE publication_scope = 'production/eod';
```

The row contains the data timestamp, model/config/code versions, market freshness, selected layer and tenor, key signal features, sizing, and the exact no-trade reason. The full term structure for that same snapshot is retrieved with its `market_snapshot_id`:

```sql
SELECT
    feature.tenor_days,
    implied.annualized_variance AS implied_variance,
    forecast.annualized_variance AS forecast_variance,
    feature.vrp_log,
    feature.zscore_3m,
    feature.zscore_1y,
    feature.rsi14,
    feature.rv21d_volatility_pct
FROM vrp.signal_features AS feature
JOIN vrp.implied_variance_term_structure AS implied
  ON implied.implied_variance_id = feature.implied_variance_id
JOIN vrp.forecast_variance_term_structure AS forecast
  ON forecast.forecast_variance_id = feature.forecast_variance_id
WHERE feature.market_snapshot_id = :market_snapshot_id
ORDER BY feature.tenor_days;
```

Use separate scopes for independently supported consumers or decision types. The initial official scope is `production/eod`. A future shadow engine should use `shadow/intraday`; it must never overwrite or masquerade as the official EOD scope.

## EOD operating convention

For an EOD run:

- `valuation_date` is the completed XNYS trading session.
- `snapshot_at` is the official session close, including early closes.
- `data_cutoff_at` is the latest timestamp the calculation is permitted to consume.
- `snapshot_kind` is `EOD_OFFICIAL`.
- SOFR remains the latest published observation strictly before the valuation date.
- the selected decision uses `EOD_OFFICIAL` signal state whether it is a trade or no-trade result.

All `TIMESTAMPTZ` values should be written in UTC by the application. The original source timezone may be recorded in asset or snapshot metadata. `DATE` columns retain trading-session semantics and must not be inferred by casting a UTC timestamp without the XNYS calendar.

Variance columns contain annualized variance in decimal units. Volatility columns ending in `_pct` contain percentage points, consistent with the locked model artifacts. Rates must have an explicit unit in the configuration version and should not be inferred from column scale alone.

## Intraday extension

No schema migration is required to begin 15-minute shadow snapshots. Each scheduled observation becomes its own `INTRADAY` run and market snapshot:

- `snapshot_kind = 'INTRADAY_PREVIEW'`;
- `snapshot_at` is the intended scheduled observation time;
- `source_latest_at` records the newest quote actually used;
- freshness and incomplete-chain outcomes are explicit;
- `forecast_as_of_date` identifies the prior official close supplying the fixed Corsi forecast; and
- each preview publishes only to `shadow/intraday` after its hard validation gates pass.

The selected-signal states support no signal, a new preview, a changed preview, degraded data, and an official EOD result. `first_observed_at` and `consecutive_snapshots` allow a UI to show persistence. EOD/intraday reconciliation should run as a separate `RECONCILIATION` run with both compared snapshots registered as input lineage.

If intraday history later becomes large, first measure PostgreSQL table size, write rate, retention, and query latency. Add native partitioning or TimescaleDB only in a later reviewed migration; neither is justified by the initial EOD workload.

## Parquet layout

Raw and standardized files should be immutable and partitioned by stable query keys. A recommended convention is:

```text
data/
  raw/source=thetadata/symbol=SPX/trade_date=YYYY-MM-DD/snapshot_time=HHMMSS/*.parquet
  standardized/dataset=option_chain/symbol=SPX/trade_date=YYYY-MM-DD/snapshot_time=HHMMSS/*.parquet
  derived/dataset=feature_panel/valuation_date=YYYY-MM-DD/*.parquet
```

Do not put credentials, signed URLs, or access tokens in `storage_uri` or JSON metadata. Local paths may be used during development; deployment should use durable object-storage URIs. Each registered asset is immutable. If bytes change, create a new asset and digest even when the logical dataset name is unchanged.

## JSON usage

JSONB fields hold manifests, diagnostic evidence, calculation traces, and forward-compatible details. Values used for identity, filtering, joins, constraints, or the latest-signal screen have dedicated typed columns. Do not hide core outputs or status fields in JSON.

## Migration and deployment

Apply migrations in numeric order inside transactions using a dedicated database role. `0001` creates the `vrp` schema and records itself in `vrp.schema_migrations`. A migration is applied once; `IF NOT EXISTS` is not used for tables because silently accepting a partially different production schema is unsafe.

Recommended role boundaries are:

- a migration owner that can create and alter schema objects;
- a reference-history loader that can append validated SOFR/SPY revisions;
- an EOD shadow writer that can insert operational snapshots but cannot mutate
  reference history or write `signal_publications`;
- a later publication writer, introduced only at an approved cutover;
- a dashboard reader with `SELECT` on the latest view and required history tables; and
- a backup operator managed by the hosting platform.

The reviewed capability-role grants are in
`ops/postgres/provision_shadow_runtime_roles.sql`. The current opt-in EOD
integration stops before `signal_publications`: PostgreSQL remains a
reconciliation shadow while the file pipeline is authoritative.

Back up PostgreSQL and object storage independently. A recovery test must verify both the operational database and every Parquet object referenced by a published run. PostgreSQL alone is not a complete production backup.

## Deferred work

The first migration intentionally does not include:

- raw quote rows in PostgreSQL;
- TimescaleDB or any other extension;
- table partitioning before actual volume is known;
- an execution/order-management schema;
- portfolio overlap, hedging, or aggregate risk tables;
- automated order placement; or
- a bulk migration of all historical research artifacts.

The current stage dual-writes new EOD operational outputs without creating a
database publication record. Reconcile repeated daily results first. A later
cutover may publish only after the database path reproduces the locked
file-based result and receives separate approval. Backfill compact derived
history only when it serves a concrete operational or dashboard query.
