# Code Validator: validating incoming codes against versioned reference data

A small, tested data-engineering pipeline. It ingests event files, validates every `code` against
**versioned reference datasets** (effective dates, deprecation, unknown values, refreshes),
publishes accepted events, quarantines everything else with a traceable reason, and proves
correctness with reconciliation checks.

* Python 3.9+ expected (developed and tested on Python <FILL: output of `py --version`>), **standard library only** (SQLite). No installs, no credentials, no network.
* Safe to re-run: same input + same reference versions => identical target state.
* On Windows use `py` instead of `python`.

## Quick start

```powershell
# from the project root
py -m unittest discover -s tests -t . -v      # 29 tests
py -m codevalidator demo                      # end-to-end on the committed sample + writes a report
py bench.py --rows 100000                     # performance measurement
```

Step by step (what `demo` does):

```powershell
py -m codevalidator init        --db out\codes.db
py -m codevalidator load-ref    --db out\codes.db --file data\sample\reference\reference_v1.csv
py -m codevalidator load-events --db out\codes.db --file data\sample\events_sample.csv
py -m codevalidator report      --db out\codes.db --input data\sample\events_sample.csv --out out\report.md

# larger, reproducible run (fixed seed) incl. independent expected-answer labels
py -m codevalidator generate    --out-dir out\data --rows 100000 --seed 42
py -m codevalidator load-ref    --db out\big.db --file out\data\reference_v1.csv
py -m codevalidator load-events --db out\big.db --file out\data\events.csv
py -m codevalidator report      --db out\big.db --input out\data\events.csv --out output\validation_report_100k.md
```

Exit code is `0` on success, `2` when a run fails (the failure is also stored in `pipeline_run`).

## Evidence included in `output/`

| File | What it shows |
|---|---|
| `test_results.txt` | verbose run of all 29 tests (all pass) |
| `validation_report_sample.md` | sample run after a reference refresh: counters, 10 reconciliation checks, rejects by reason, event history |
| `validation_report_100k.md` | same report for 100,000 generated rows |
| `benchmark_results.txt` | throughput, per-stage timings, optimisation comparison |

Sample result: 24 input rows -> reference v1: 8 valid / 2 valid_deprecated / 14 rejected. After refreshing to reference v2 and re-running the same file: 5 / 4 / 15. Three events change outcome because of the refresh. 100k run (seed 42): 61,903 valid / 9,973 valid_deprecated / 28,124 rejected.

## Architecture / flow

```
 reference_vN.csv --load-ref--> [validate snapshot: dates, overlaps, dangling replaced_by]
   (full snapshot)                  | same content hash as latest? -> no-op (idempotent)
                                    +-> new immutable ref_version + ref_code rows (old versions kept)

 events.csv --load-events-->  1 read + schema check (missing column => fail file; extra column => warn)
                              2 field validation   (arity, required, ISO-8601 tz-aware ts, amount)  --+
                              3 in-file dedupe     (exact dup / newest updated_at wins)             --+
                              4 cross-file stale check vs event_validated (late/out-of-order)       --+ rejects
                              5 reference check    (in-memory index: effective window, deprecated)  --+
                              6 ONE TRANSACTION: replace this file's rejects, upsert accepted,
                                append history, write run summary
                              7 reconciliation assertions inside the transaction (mismatch => ROLLBACK)
                                         |
        pipeline_run (metrics) <---------+-->  event_validated | event_reject | event_history
                                                         |
                                               report (SQL re-computation, 10 checks)
```

Modules: `rules.py` (constants) · `validate.py` (pure row parsing) · `reference.py` (load + versioned lookup) ·
`pipeline.py` (orchestration) · `report.py` (reconciliation/report) · `datagen.py` (generator) · `cli.py` · `db.py` (DDL).

## Business rules

**Reference snapshot** (`dataset_name, code, description, status, effective_from, effective_to, replaced_by_code`)
* One row = one code in one validity window. `effective_from` **inclusive**, `effective_to` **exclusive**, blank = open-ended.
* `status` is `active` or `deprecated`. Windows of one code may not overlap; `replaced_by_code` must exist in the same dataset.
* Any problem in a snapshot rejects the **whole** refresh (nothing is published): a half-loaded reference is worse than none.

**Event decision** for `(dataset_name, code)` on the **UTC date of `event_ts`** against the chosen reference version:

| Situation | Result |
|---|---|
| inside an `active` window | `valid` |
| inside a `deprecated` window | `valid_deprecated` (accepted, warning `CODE_DEPRECATED`, suggested `replaced_by_code`) |
| before the first window | reject `CODE_NOT_YET_EFFECTIVE` |
| after / between windows | reject `CODE_EXPIRED` |
| code not in snapshot | reject `UNKNOWN_CODE` |
| dataset not loaded | reject `UNKNOWN_REFERENCE_DATASET` |

Row-level rules run in this order and the first failure is the single recorded reason:
`MALFORMED_RECORD` (field count) -> `MISSING_REQUIRED_FIELD` -> `INVALID_TIMESTAMP` (must be ISO-8601 **with** offset/`Z`) ->
`INVALID_AMOUNT` (non-negative, max 2 decimals, stored as integer cents) -> in-file `DUPLICATE_EVENT` / `SUPERSEDED_VERSION` ->
cross-file `STALE_VERSION` -> reference reasons above. Normalisation: values trimmed, `code` upper-cased, `dataset_name` lower-cased.

**Versions of the same event** (`event_id` is the primary key; `updated_at` is the source's version clock):
newest `updated_at` wins; ties keep the first occurrence. An older/equal-but-different version arriving in a later file is `STALE_VERSION`.

## Idempotency, retry, restart, backfill

* **Atomic per file.** Everything for one input file is one SQLite transaction. A crash/exception rolls back; the run row is then marked `failed` with `failure_reason`. **Retry = run the same command again.** Tested by injecting a mid-load failure.
* **Rerun-safe.** Accepted rows are upserted by `event_id`; rejects are keyed by `(source_file, source_line)` and replaced per file; history uses a natural primary key with `INSERT OR IGNORE`. Reruns add a new `pipeline_run` row but never duplicate data (tested, including a 20k-row rerun).
* **Reference refresh.** `load-ref` of identical content is a no-op; changed content appends a new version (v1 stays queryable).
* **Backfill.** After a refresh, re-run the old event file(s): they are re-validated against the latest versions. Outcome changes are kept in `event_history`. Use `--pin <dataset>=<version>` to re-validate against an older reference version.
* **Restart.** There is no hidden state; files are idempotent, so restart means "re-run the files not yet successful in `pipeline_run`".

## Observability

Each run writes `pipeline_run`: `run_id`, `started_at`/`ended_at` (ISO-8601 UTC), `duration_seconds`, `rows_read`, `rows_valid`,
`rows_valid_deprecated`, `rows_rejected`, amounts (cents), `input_sha256`, reference versions used, `notes` (e.g. schema drift),
`failure_reason`, per-stage timings, and `reconciled`. `run_reject_summary` keeps the per-run reject breakdown. Logs are one line per event with the `run_id` (timestamps in UTC).

## Naming conventions

snake_case tables/columns; ISO-8601 UTC timestamps (`YYYY-MM-DDTHH:MM:SSZ`), dates `YYYY-MM-DD`; `*_cents` integers for money;
`run_id = <evt|ref>_<UTC time>_<8 hex>`; `reject_id` and `record_hash` are deterministic hashes;
`source_file` is the file's base name, `source_line` is the line in that file. Schema: [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md). Trade-offs: [`docs/DESIGN_NOTES.md`](docs/DESIGN_NOTES.md).

## Test coverage (29 tests)

`py -m unittest discover -s tests -t . -v`

| File | Tests | Covers |
|---|---|---|
| `test_validators.py` | 10 | code normalisation, timezone required, UTC conversion (+05:30), amount to cents, invalid amount, missing-field reason; reference windows: start date inclusive, end date exclusive, deprecated code with replacement, unknown code |
| `test_event_pipeline.py` | 11 | happy path (valid + deprecated), header-only batch succeeds, zero-byte file fails, missing column fails, malformed row and missing required field rejected with reason, duplicate event, stale version arriving in a later file, reference refresh re-validates an old file (backfill), rerun does not duplicate, injected mid-load failure then retry, sample file matches hand-derived expected results (reference v1) and all reconciliation checks pass |
| `test_reference_load.py` | 5 | reference load, same content twice creates no new version, changed content creates a new version and keeps the old one, 7 kinds of invalid reference rejected as a whole, missing columns / empty file / header-only reference fail |
| `test_scale_and_cli.py` | 3 | generator is reproducible with a fixed seed; 20,000 generated rows match the independent label file row by row (unknown dataset, bad timestamps, duplicates, out-of-order versions included) and a rerun leaves the tables unchanged; CLI commands and exit codes |

Expected answers live in separate files (`data/sample/expected/...`, `events_expected_outcomes.csv`) and are never read by the pipeline.
The reference v2 result of the sample (5 / 4 / 15) is checked by running `py -m codevalidator demo` and comparing with `data/sample/expected/events_sample_expected_ref_v2.csv`.

## Performance

`py bench.py --rows 100000` (my machine, Windows; results in `output/benchmark_results.txt`): about **10.5k rows/s** end to end (100k rows in roughly 9-10 s).
Slowest stages: field validation (`datetime`/`Decimal` parsing) and publish (SQLite upserts).

**Optimisation implemented:** the reference is loaded once into an in-memory `{(dataset, code): windows}` index, so validation does no SQL per row. Measured **about 4.3x faster** than one indexed SELECT per row on my machine.
Rows are also written with `executemany` in chunks, and existing events are fetched in batches.
Speed depends on the machine; the numbers above come from my own benchmark run.
Next candidates: cache parsed timestamps, bulk-insert into a staging table, stream in chunks.

## Assumptions & known limitations

* Reference snapshots are **full** per dataset: a code missing from a newer snapshot becomes `UNKNOWN_CODE` (expire it with `effective_to` instead of deleting).
* The stale check compares against **accepted** stored events only; a newer version that was rejected is not remembered, so an older valid version may stay as current.
* `event_reject` mirrors the *latest* run per `source_file`; earlier rejects survive only as counts (`run_reject_summary`) and in `event_history`.
* An identical payload re-delivered in a *different* file is treated as a re-validation (not a duplicate).
* The input file is held in memory (fine for millions of rows, not billions). SQLite allows a single writer.
* `source_file` identity is the base name; two different files with the same name share reject/rerun semantics.
* Automated tests cover effective-date boundaries at day level (not to the second), and `--pin` is only tested for bad input.

## Possible next steps

Chunked/streaming ingestion · Postgres/warehouse target with `MERGE` · scheduler + alerting on `failed` runs and reject-rate thresholds ·
`as_of` reference selection by event time (bi-temporal) · reject-rate and deprecated-usage dashboards · more tests (extra column, gaps/reactivation, pinned versions) · property-based tests.