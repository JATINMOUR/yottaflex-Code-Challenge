# Design and trade-off note

## Goals (from the brief)
Correctness and data quality first, then clear design, tests, idempotency/recovery/observability, performance reasoning. A complete vertical slice beats breadth.

## Key decisions

| Decision | Why | Trade-off |
|---|---|---|
| **SQLite + stdlib only** | Runs anywhere with `python`; transactional DDL/DML gives real atomicity; SQL reconciliation is natural. | Single writer; not a distributed warehouse. The SQL is portable (upserts → `MERGE`). |
| **Immutable reference versions** (new row set per changed snapshot, content-hash dedupe) | Refresh is idempotent, old results are reproducible (`--pin`), and nothing is silently overwritten. | Storage grows per change (small for reference data). |
| **Effective window = `[from, to)` on the UTC date of `event_ts`** | One unambiguous boundary rule, tested to the second; offsets can't shift the business date unpredictably. | A source that thinks in local dates may need a configurable timezone (next step). |
| **Deprecated ≠ invalid**: accept with warning and replacement hint | Matches how code sets are really retired: use of a deprecated code is a quality signal, not data loss. | Consumers must read `validation_status`. Switchable to reject by a one-line change in `RefIndex.classify`. |
| **One transaction per file** | Partial failure leaves no half-loaded state; retry is just a rerun. | A single bad *run-level* problem (e.g. schema change) blocks the whole file, by design; bad *rows* are quarantined individually. |
| **Reject path = table with raw record + line + deterministic id** | Every rejected row is traceable and reprocessable after a reference fix (backfill). | `event_reject` mirrors the latest run per file; history is in counts + `event_history`. |
| **`updated_at` as the version clock** | Gives a deterministic answer for duplicates and out-of-order/late data without relying on arrival order. | Needs the source to supply it (it's a required column). |
| **Money as integer cents, exact `Decimal` parsing; >2 decimals rejected** | No float drift; aggregates reconcile to the cent. | Stricter than rounding; surfaces upstream issues instead of hiding them. |
| **Reconciliation inside the transaction** | A mismatch rolls the load back, so a published run is always self-consistent. | Slight overhead (two aggregate queries). |
| **Generated labels kept in separate files, built by construction** | Tests compare the pipeline to an oracle that doesn't share its code path. | The generator encodes the same *rules* (as specification), so a wrong spec would be wrong in both; the hand-written sample guards that. |

## Validation strategy
1. Unit tests for pure functions (timestamps, amounts, window classification).
2. Scenario tests for each required edge case (see README).
3. Hand-written sample with hand-derived expected outcomes for **two** reference versions (the refresh changes 3 outcomes).
4. 20k-row generated run compared row-by-row with independent labels, rerun for idempotency.
5. In-pipeline and report-level reconciliation (counts, amounts, FK integrity, traceability, independent input recount).

## Performance reasoning
Measured on 100k rows (~22k rows/s). Time is dominated by per-row parsing (`datetime`, `Decimal`) and the publish step. The implemented optimisation (in-memory reference index) removes per-row SQL from validation (≈2.2–2.5× on that step in isolation); the measured gain is modest because SQLite point lookups are already fast, which is itself a useful finding - the next bottlenecks are parsing and writes, not lookups.
Memory is O(rows in file); for very large files the same stages can be run per chunk, keeping the dedupe map on `event_id` (or deduping in SQL via a staging table).

## Failure handling matrix
| Situation | Behaviour |
|---|---|
| Bad row (malformed, null, invalid value, unknown/expired code) | quarantined with reason + line; file continues |
| Missing required column / zero-byte / unreadable / unknown pin / no reference | run `failed`, nothing published, reason stored |
| Crash mid-write | rollback; run stays `failed`; rerun |
| Reconciliation mismatch | rollback, `RECONCILIATION_FAILED` |
| Invalid reference snapshot | whole refresh rejected; previous version stays current |
| Same file again | identical state; new run row |
| Reference refreshed, same file again | re-validated; changed outcomes moved between tables, history kept |

## Known limitations
See README. The most important: stale detection only knows accepted versions; reject mirror is per latest run; single-process SQLite.
