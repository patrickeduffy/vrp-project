# EOD PostgreSQL shadow recorder

The shadow recorder copies one already-completed Hybrid v2 EOD result into the
operational PostgreSQL schema. It does **not** recalculate the locked model,
replace canonical Parquet/CSV files, or publish a trading signal. The existing
file pipeline remains authoritative while the database projection is compared
against it.

## Preconditions

- The supplied project root is the data-bearing VRP checkout.
- The supplied run directory is one completed EOD audit directory whose
  `run_manifest.json` has `status: PASS`.
- The PostgreSQL migrations and the historical SOFR/SPY reference-data loads
  have completed.
- `VRP_DATABASE_URL` identifies the target database for a load. Validation-only
  mode neither requires nor opens a database connection.
- `environment`, `code-version`, and `requested-by` are provided explicitly for
  every database load. Do not put a password in the command line or commit it to
  a file.

The SOFR updater evidence may have status `PUBLISHED` or `NO_CHANGE`. A
`NO_CHANGE` snapshot is accepted only when its hard checks passed, it was not
published, `changes_detected` is false, and both added and revised row counts
are zero. `CHECK_ONLY`, failed, or internally inconsistent updater evidence is
rejected.

The updater snapshot may already contain an observation dated on or after the
EOD valuation date, especially when a completed run is recorded the following
morning. The recorder hashes the complete updater snapshot, then pins the
latest observation strictly before the valuation date. Later rows are evidence
in the frozen source snapshot but are never selected as the rate for that EOD
valuation.

The recorder reads only the staged files named by the completed run. It hashes
them again and validates their row-level projection before any database work.
It never repairs or rewrites a staged or canonical file.

## Validate without PostgreSQL

Run this first:

```powershell
$env:PYTHONPATH = "src"
& "C:\Users\patri\AppData\Local\Programs\Python\Python313\python.exe" `
    scripts\load_eod_snapshot.py `
    --project-root "C:\Users\patri\vrp_project" `
    --run-dir "C:\path\to\completed\eod\run" `
    --validate-only
```

A successful result is concise JSON with `status` equal to `VALID`. It includes
the valuation date, official XNYS close timestamp, artifact/model/configuration
fingerprints, the exact SOFR history digest and selected prior observation, and
the expected row counts. No artifact directory is created.

## Record the shadow snapshot

Set `VRP_DATABASE_URL` only for the process, then run:

```powershell
$env:PYTHONPATH = "src"
& "C:\Users\patri\AppData\Local\Programs\Python\Python313\python.exe" `
    scripts\load_eod_snapshot.py `
    --project-root "C:\Users\patri\vrp_project" `
    --run-dir "C:\path\to\completed\eod\run" `
    --environment local `
    --code-version "<full Git commit SHA>" `
    --requested-by "patrickeduffy"
```

The load uses one PostgreSQL transaction. Within it, the recorder:

1. resolves the immutable SOFR/SPY revision pins used in the run identity;
2. takes a transaction-scoped advisory lock for that logical snapshot;
3. registers immutable model, configuration, and staged-asset identities;
4. links every required input and QA asset to the run;
5. pins the latest accepted SOFR observation strictly before the valuation
   date and the accepted SPY feature row on the exact valuation date;
6. inserts the market snapshot, nine implied rows, nine forecast rows, nine
   feature rows, all explicit signal-layer evaluations, and one decision;
7. records the staged golden-contract result;
8. reads the database projection back and compares categorical/identity fields
   exactly and numerical fields with the locked tolerances; and
9. marks the stage and run `COMPLETED/PASS` only after every gate passes.

Any mismatch raises an error and rolls the entire transaction back. In
particular, a failed run cannot leave a partial snapshot or an apparently valid
run ledger behind.

## Idempotency and reruns

The run identity is a deterministic UUID derived from the environment, run
kind, valuation date, official session-close timestamp, data cutoff, full code
version, model/configuration digests, and the ordered staged-artifact digests.
The immutable SOFR/SPY row, release, definition, and row-digest pins are also
included, so an accepted correction creates a new revision-aware run identity.
Staged run-directory paths and generation timestamps are not direct identity
inputs; the exact model and configuration content remains part of the identity.

Re-running the same command returns `no_op: true`. It does not trust the prior
success blindly: it rehashes the staged files, revalidates the golden evidence,
checks the stored version and asset lineage, and reconciles the complete
database projection again. A changed code version or changed content produces a
different logical shadow run.

## Deliberate boundaries

- No row is inserted into `vrp.signal_publications`.
- No canonical or staged Parquet, CSV, JSON, or configuration file is changed.
- The shadow decision is evidence for reconciliation, not an execution
  instruction.
- The official EOD signal continues to come from the accepted file pipeline
  until a later, separately approved cutover.

## Initial local acceptance

The initial durable local acceptance completed on 2026-07-23 against
PostgreSQL 17 using the successful EOD audit run
`data/audit/vrp_hybrid_v2_eod/20260723_142305` for valuation date 2026-07-22.

- migrations `0001` and `0002` were registered successfully;
- the reference loader inserted 2,073 SOFR observations and 2,149 SPY daily
  feature rows with zero corrections;
- the EOD recorder inserted one market snapshot, nine implied-variance rows,
  nine forecast rows, nine signal-feature rows, 18 rule evaluations, and one
  `NO_TRADE` decision;
- the selected SOFR observation was 2026-07-21 at 3.61%, strictly before the
  valuation date;
- identical reference and EOD reruns returned `no_op: true` with the same
  release and run identities;
- direct database counts reconciled exactly; and
- `vrp.signal_publications` remained empty.

This acceptance proves the recorder and schema for one completed run. It does
not authorize a database-backed signal publication or make PostgreSQL the
calculation source of record. Automated daily shadow writing requires a
separate least-privilege runtime role, orchestration integration, and repeated
file-versus-database reconciliation.
