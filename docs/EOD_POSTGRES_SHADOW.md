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
- PostgreSQL migrations `0001` and `0002` are applied. The automatic post-pass
  synchronizes the required compact SOFR/SPY reference history before it loads
  the EOD shadow.
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

The recorder reads only the staged files named by the completed run. It hashes
them again and validates their row-level projection before any database work.
It never repairs or rewrites a staged or canonical file.

## Automatic EOD post-pass

Normal published runs through `scripts/run_eod.py` require
`VRP_DATABASE_URL` before the file pipeline begins and perform the PostgreSQL
shadow step after that file pipeline passes. The legacy calculation child never
receives `VRP_DATABASE_URL`, `PGPASSWORD`, or related database credential
variables. It writes a caller-selected, atomic JSON handoff that identifies the
exact completed audit directory and the SHA-256 digest of its final
`run_manifest.json`. It also pins a path-independent source-bundle digest over
the fixed run status, staged shadow inputs, and exact SOFR updater evidence. The
stable wrapper validates that handoff before starting database work. The
validation pass then pins the semantic snapshot digest into the later
credentialed shadow load, so source content cannot change between phases or
retries.

The post-pass then:

1. validates the exact completed and published EOD run without a database;
2. synchronizes the compact SOFR and SPY daily reference histories;
3. requires the synchronized SOFR digest to equal the run's frozen SOFR
   evidence;
4. records and reconciles the exact EOD shadow projection; and
5. atomically writes `postgres_finalization_status.json` inside that audit
   directory.

The published manifest records `postgres_postpass_required: true` and the
target PostgreSQL environment. That obligation is what makes a missing, failed,
or mismatched finalization a stop gate for later production work. Older audit
runs created before this marker was introduced are not retroactively treated as
unresolved debt.

The diagnostic and bypass flags have deliberately different meanings:

- `--no-publish` does not replace canonical files and does not start the
  database post-pass. When it still refreshes upstream data, it must respect
  any older unresolved finalization.
- `--skip-upstream` is rejected unless `--no-publish` is also present. The
  combined `--skip-upstream --no-publish` mode is the only narrow diagnostic
  path that may run without the oldest-unresolved gate because it neither
  refreshes sources nor publishes files.
- `--no-postgres-shadow` is an explicit break-glass bypass. Without
  `--no-publish`, it can still publish the canonical file result, but it does
  not require `VRP_DATABASE_URL`, does not set the post-pass obligation marker,
  and does not create PostgreSQL finalization status. It still refuses to
  leapfrog an older obligated run. Because the resulting run is intentionally
  exempt, its manifest records
  `postgres_postpass_bypass_reason: explicit-no-postgres-shadow`. Use this only
  when a file-only production run is explicitly accepted; do not use it to
  retry a failed post-pass.

Every published legacy EOD invocation must therefore declare either the normal
post-pass obligation or that one audited bypass. An unmarked direct invocation
is rejected before the model pipeline starts, so it cannot silently create a
third publication mode outside the stable wrapper.

If the file run passes but PostgreSQL finalization fails, the command returns a
nonzero post-pass exit code and reports `FILE_EOD=PASS SHADOW=FAILED`. Healthy
canonical file outputs are retained and are not relabeled or rolled back.

## Serialization and oldest-first order

The file and database phases share fail-closed coordination:

- `.eod_writer.lock` is the project-wide operation gate. The stable wrapper
  holds it across the complete file run and PostgreSQL post-pass. Direct legacy
  EOD, standalone finalizer, and standalone mutating loaders also claim it.
- `.eod_canonical_writer.lock` protects the interval in which canonical files
  or their exact evidence could be read or changed. The legacy child holds it
  independently while it runs, so a child that survives a wrapper crash cannot
  overlap a replacement writer. The finalizer also holds it while validating
  and loading the exact run.
- `.postgres_finalization.lock` permits only one finalizer for one exact audit
  directory.
- A database-global PostgreSQL advisory lock serializes compact reference and
  EOD shadow writers even when they originate from different processes or
  hosts. The finalizer also holds a random token-specific advisory lock; its
  two mutating children must prove that both parent locks are already held.
  Each child also holds a fixed child-active advisory lock throughout its
  mutation, so it remains visible and blocks a replacement coordinator if the
  parent process or session dies first.

Before a publish-capable EOD run, before a bypassed file-only publication, and
before a standalone mutation can advance state, the oldest-unresolved gate
checks the relevant timestamped audit history. It refuses to advance past the
earliest published, healthy run whose manifest requires the post-pass but lacks
matching completed evidence. The finalizer checks the older-run order once
before database work and again while holding the database-global lock, closing
the race with another writer. While that database lock is held, the finalizer
also queries the connected target for every earlier obligated run's exact
pipeline run, full term/feature/evaluation grids, selected signal, assets,
stage/QA evidence, code version, content digests, completion state, and
no-publication contract. It recomputes an independently pinned fingerprint from
the current PostgreSQL read-back, so same-count value changes fail as well as
missing rows. A restored or redirected target that lacks or changes prior rows
fails before the new projection begins.

The production obligation queue is permanently rooted at
`data/audit/vrp_hybrid_v2_eod`. A configuration change cannot redirect that
queue and thereby hide older debt; any future migration needs an explicit,
separately reviewed queue-migration mechanism.

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

This is a low-level administrative interface. Routine published EOD work and
retries must use `scripts/run_eod.py` or `scripts/finalize_eod_postgres.py`,
because a direct snapshot load does not synchronize reference history or create
the finalization sidecars that resolve an obligated run.

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

A standalone mutating invocation claims the project operation lock, rejects
older unresolved obligated EOD runs, and claims the database-global advisory
lock before writing. When the automatic finalizer invokes this loader, the
child instead verifies the finalizer's delegated global and token-specific
database lease. Supplying a token-shaped environment value alone is not enough
to authorize a write.

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

## Standalone finalizer for a published EOD run

`finalize_eod_postgres.py` is the operational post-pass around the two loaders.
It never runs the EOD calculation and never searches for the latest audit
directory. The caller must supply the exact completed run directory. The
directory must be a timestamped child of the fixed canonical production audit
queue. The finalizer rejects failed, no-publish, `--skip-upstream`,
file-only bypass, or otherwise non-obligated runs before opening a database
connection.

After preflight validation it loads the run-frozen SOFR snapshot and the
reconciled SPY daily reference history, verifies that the loaded SOFR digest is
the digest pinned by the EOD evidence, and then records the shadow snapshot for
that same valuation date. A later retry therefore does not substitute a newer
canonical SOFR file for the completed run's evidence:

```powershell
python scripts\finalize_eod_postgres.py `
  --project-root "C:\Users\patri\vrp_project" `
  --run-dir "C:\exact\completed\eod\audit-directory" `
  --artifact-root "C:\Users\patri\vrp_project\data\reference_history" `
  --environment local `
  --code-version "<full 40-character Git commit SHA>" `
  --run-manifest-sha256 "<lowercase SHA-256 from the failed run status>" `
  --source-bundle-sha256 "<lowercase source-bundle SHA-256 from the failed run status>" `
  --requested-by "patrickeduffy"
```

Set `VRP_DATABASE_URL` only in the calling process. No password or database URL
is accepted on the command line or written to the audit status. The validation
subprocess receives an environment with database credential variables removed;
only the reference and shadow subprocesses inherit database configuration.
The executing source checkout must still be clean and on the exact supplied
commit, and the data-bearing checkout must match it. The manifest digest is
recorded as `run_manifest_sha256` in the status sidecar created by the automatic
post-pass. The exact source-bundle pin is recorded as `source_bundle_sha256`.

The finalizer uses two atomic status sidecars in the supplied audit directory:

- `postgres_finalization_status.json` is the primary attempt and becomes
  immutable terminal evidence once it records matching `COMPLETED` with exit
  code `0`.
- `postgres_finalization_last_attempt.json` records the latest full
  reconciliation retry after terminal completion. A failed or mismatched latest
  retry reopens the oldest-unresolved gate even though the earlier successful
  primary record is preserved. A later successful retry closes it again.

The file result remains authoritative and unchanged for every finalizer
outcome. A retry never returns cached success: it rehashes and revalidates the
source bundle, synchronizes reference evidence, and reconciles the current
database projection. Only one finalizer may operate on a run directory at a
time. A second process returns preflight code `10` without overwriting the
active attempt's status. Return codes are deliberately distinct:

- `0`: reference history and EOD shadow completed (including an idempotent no-op);
- `10`: exact-run or staged-output preflight failed;
- `20`: reference-history synchronization failed;
- `30`: EOD shadow load or reconciliation failed;
- `40`: the atomic status sidecar could not be written.

Retry the same command with the same run directory, environment, code SHA,
run-manifest SHA, and source-bundle SHA.
Do not rerun or republish EOD merely because this non-authoritative post-pass
failed.

Process unresolved finalizations oldest-first. Do not advance a newer EOD
PostgreSQL post-pass past an older failed one: the compact reference loader is
revision-safe and will not rewind current SOFR leaves to register an older
snapshot after a newer release has already advanced them.

## Database restore or target switch

The JSON sidecars prove what a prior finalizer observed; they are not a
continuous database monitor. Every new finalization now recomputes each exact
prior PostgreSQL read-back fingerprint and will stop if rows are missing,
partial, or changed. After restoring PostgreSQL, replacing the
database, or changing `VRP_DATABASE_URL` to a different target, do **not** wait
for that automatic failure or trust existing `COMPLETED` sidecars alone.

Apply this hard recovery rule:

1. Stop all EOD, finalizer, reference-loader, and shadow-loader processes.
2. Apply migrations `0001` and `0002` to the restored or replacement target.
3. Enumerate every timestamped audit run whose published `PASS` manifest has
   `postgres_postpass_required: true` for that environment.
4. Starting with the oldest, check out the exact producing Git commit, keep both
   source and data-bearing checkouts clean, set `VRP_DATABASE_URL` to the new
   target, and run `scripts/finalize_eod_postgres.py` with the exact identity
   values recorded for that audit.
5. Require each full replay/reconciliation to finish `COMPLETED` before moving
   to the next run. A pre-existing primary `COMPLETED` record causes the fresh
   result to be written to `postgres_finalization_last_attempt.json`; inspect
   that latest-attempt file.
6. Resume normal EOD only after every obligated audit has reconciled
   oldest-first against the active target.

Do not delete sidecars, skip an obligated audit, advance reference data
manually, or resume production merely because the filesystem gate appears
clear. The replay is the database recovery gate.

## Deliberate boundaries

- No row is inserted into `vrp.signal_publications`.
- No canonical or staged Parquet, CSV, JSON, or configuration file is changed.
- The shadow decision is evidence for reconciliation, not an execution
  instruction.
- The official EOD signal continues to come from the accepted file pipeline
  until a later, separately approved cutover.
