# Testing

What the test suite contains, how each part decides pass/fail (its **oracle**, AGENTS.md §7),
and how to run it. Counts below come from `#[test]` / `#[tokio::test]` attributes and
`cargo test … -- --list`, measured on 2026-09-27; re-count after changes rather than trusting
the numbers.

**Rule of the suite: nothing skips.** Unit tests never touch a database. Every integration test
needs the live databases from `tests/docker/compose.yaml` and **panics** (fails) with a message
pointing at the stack when they are unreachable. A broken harness is a failure, never a silent
"skipped" or green.

## 0. Running the tests

```bash
# 1. Databases: Postgres 17 + MySQL 8, both seeded with the dvdrental dataset from tests/data/.
#    The same stack is the demo database and the CI database.
docker compose -f tests/docker/compose.yaml up -d --wait

# 2. Unit tests (no database, seconds once built).
cargo test --lib --bins

# 3. Doc tests (the `# Examples` blocks; most are `no_run`, i.e. compile-checked).
cargo test --doc

# 4. Every integration file, serially (the fixtures share one server per engine).
cargo test --tests -- --test-threads=1

# One file at a time:
cargo test --test pg_pushdown -- --test-threads=1

# Tear down (data lives on tmpfs; the next `up` re-seeds from scratch).
docker compose -f tests/docker/compose.yaml down -v
```

- `scripts/e2e.sh` does steps 1 + 4 + teardown in one go (`cargo test --all -- --test-threads=1`).
- `scripts/examples.sh` runs every example against the compose Postgres (plus a local scheduler
  and two workers for the distributed ones), checks each one's output, and prints a PASS/FAIL
  summary. Each run works in a fresh `target/examples-run/<timestamp>/`, so checkpointed
  examples always start from zero.
- Endpoints default to the compose services: Postgres
  `postgres://postgres:postgres@127.0.0.1:5432/test`, MySQL
  `mysql://root:password@127.0.0.1:3306/test`. `DATABASE_URL` / `MYSQL_URL` point the suites
  at another server (it still has to be seeded like the compose stack for the dvdrental suites).

## 1. Counts (measured 2026-09-27)

| Target | Tests | How counted / run |
|---|---:|---|
| Unit (`src/**`, lib target) | 252 | `cargo test --lib -- --list` |
| Binary target (`src/main.rs`) | 0 | `cargo test --bins -- --list` (the bin is a thin wrapper over the lib) |
| Doc tests | 134 | `cargo test --doc`: 133 run, 1 `ignore`d (`Predicate::render_to`'s sketch) |
| Integration (`tests/*.rs`, 16 files) | 98 | `cargo test --tests -- --list`; run with `--test-threads=1` |

Run times are not re-measured for these counts; `cargo test --tests` prints each file's time.

## 2. Unit tests — no database

Pure logic, in `#[cfg(test)] mod tests` next to the code. Per module:

| Module | Tests | What it covers (oracle: hand-written expected values unless noted) |
|---|---:|---|
| `config` | 22 | Strict job-spec parsing (`deny_unknown_fields`: a `sink` / `watermark` block is a load error), lowercase `strategy` / `policy` enums, validation, password resolution, every checked-in example config, `benchmark/rust/bench-config.json` and the job spec `benchmark/run.sh` generates |
| `pushdown::cost_model` | 20 | Push/keep decisions through `decide_push`: index shortcut, selectivity and cost gates; histogram / most-common-value range estimates; sibling filters joined into one window; no window discount without the column distribution; window cost ignores a one-sided EXPLAIN |
| `connector::postgres::parallel` | 16 | Keyset / ctid partition math in `i128`: even splits, degenerate ranges, first partition takes `IS NULL`, last is open-ended, `i64::MAX` keys, full `i64` span without overflow |
| `connector::postgres::pipeline::filters` | 14 | Shorthand vs structured filter lowering equivalence, value typing, schema-coerced timestamp/date literals |
| `connector::postgres::copy` | 11 | Binary COPY decoding: bad signature / truncation fail loudly, typed values and NULLs, Postgres-epoch timestamps, `±infinity` and NUMERIC `NaN` / overscale as typed errors, empty results keep the schema, zero-column projections |
| `connector::postgres::row_adapter` | 11 | Per-column binary decoders, `±infinity` / NUMERIC overflow as typed errors, builder capacity from the byte cap vs `batch_size`, bad fixed-length values as errors |
| `pushdown::translate` | 10 | `Expr` → `Predicate` fidelity: `COLLATE "C"` text (Exact), float `=` Inexact / ranges not pushed, `NOT` only over Exact, uuid/json `=`/`<>` via `::text`, date literals Exact (cast columns and out-of-range years stay in Arrow) |
| `checkpoint::json_store` | 10 | Split state machine, plan fingerprint / `PlanMismatch`, stored bounds, atomic writes, collision-free file names, orphan tmp cleanup |
| `connector::postgres::distributed::watchdog` | 9 | Hang detection from scheduler listings (removed executor, silent executor after the timeout, REST outage falls back to the job timeout), executor failures recognised in job errors, a hung job retried then aborted, a successful retry delivers its rows once, query errors not retried |
| `connector::postgres::explain` | 8 | `EXPLAIN (FORMAT JSON)` parsing and its failure paths |
| `connector::postgres::execution_plan` | 8 | Bind order, COPY vs cursor SQL shapes, partition count, debug tags |
| `connector::postgres::distributed::pool_registry` | 8 | Budgeted pool per descriptor (`pool_max / workers`), key covers budget + session settings, shared scan slots, acquire timeout ≥ statement timeout, `close_all`, missing password env fails cleanly |
| `connector::postgres::query_builder` | 7 | Full / keyset SQL shapes, `::text` casts for json/jsonb/uuid/enum |
| `pushdown::policy` | 6 | Policy matrix (`always`/`never`/`cost_based`/`strict`/`hinted`) |
| `connector::postgres::table_provider` | 6 | Scan pushes what `supports_filters_pushdown` accepted and rejects an untranslatable filter; a decoded (scheduler-side) provider decides identically; `COUNT(*)` empty projection |
| `connector::postgres::arrow_type_mapper` | 6 | Postgres type → Arrow type |
| `logging` | 5 | CLI/env log option resolution |
| `connector::query_tag` | 5 | SQL comment tag rendering, `*/` injection defence |
| `connector::mysql::type_mapper` | 5 | MySQL type → Arrow (unsigned, `tinyint(1)`, `YEAR`, `TIME`) |
| `checkpoint::lock` | 5 | Per-job `O_EXCL` lock, heartbeat, TTL takeover, RAII release |
| `connector::postgres::distributed::executors` | 4 | Executor-count check against the connection budget (scheduler REST API): outcomes, identity / chunked HTTP responses, non-200 and malformed bodies as errors, a live HTTP probe |
| `connector::postgres::distributed::context` | 1 | A remote session targets one partition per worker |
| `telemetry` | 1 | Recording metrics without an installed recorder is a no-op |
| `run_report` | 4 | Totals and status from split outcomes; snake_case JSON round trip; run ids validated as file names; atomic write, read and oldest-first listing (junk files skipped, no temp files left) |
| others (21 modules) | 50 | `pipeline::splits` 4, `inline_sql` 4, `postgres::dialect` 4, `mysql::row_adapter` 4, `types::table_metadata` 3, `pushdown::ir` 3, `postgres::stats` 3, `postgres::api` 3, `checkpoint::fingerprint` 3, `pushdown::stats` 2, `errors` 3, `table_codec` 2, `mysql::query_builder` 2, `mysql::dialect` 2, `checkpoint::progress` 2, and one each in `types::job_id`, `pushdown`, `pushdown::explain`, `postgres::engine`, `plan_codec`, `checkpoint` |

## 3. Integration harness and fixtures

- **Postgres — `tests/common/postgres.rs` (`TestDb`).** Connects to `DATABASE_URL` or the
  compose default, creates a private schema `test_<pid>_<n>`, and loads the **hostile fixture**
  from `tests/data/hostile.sql` (the single source of truth; its `__SCHEMA__` token is replaced
  with the schema name). Teardown drops the schema over a fresh connection with a 10 s bound
  (`Drop`), or explicitly with `TestDb::cleanup().await`.
- **Hostile fixture** (`hostile`, 13 rows, ids 1..13): NULL in every nullable column, `''` vs
  NULL, mixed-case / accented text, `varchar` / `bpchar`, `numeric(12,2)` and `numeric(30,15)`,
  `INT4`/`INT8` MIN/MAX, `NaN` / `±Infinity` floats, `text[]` with NULL and `''` elements, `jsonb`,
  `uuid`, a real enum, `bytea` with `0x00` / `0xFF` bytes, an empty `bytea` and NULLs, dates and
  timestamps at 1970-01-01, epoch − 1 µs, a leap day, 2038-01-19 03:14:07/08 and
  9999-12-31 23:59:59.999999, and a **5-row tie** on `updated_at` (ids 9..13). Rows 1..8 have
  `updated_at` 2024-01-01..08, one per day. `±infinity` lives in a separate `hostile_infinity`
  table because extracting it must fail by design.
- **MySQL — `tests/common/mysql.rs` (`MySqlTestDb`).** Same rules; a private database per test
  with its own 8-row hostile table.
- **Cluster — `tests/common/cluster.rs` (`TestCluster`).** Distributed tests start a real
  `el-ballista scheduler` + `el-ballista worker` cluster as child processes and can kill a worker
  mid-job.
- **dvdrental.** Both compose engines are seeded from the same `.dat` files
  (`tests/data/dvdrental`; MySQL through `tests/data/dvdrental_mysql.sql`), so the matrix and
  cross-engine suites compare real, identical data.
- Self-contained suites (`pg_decode.rs`, `regressions.rs`) create their own schemas/tables and
  clean up explicitly instead of using `TestDb`.

## 4. Integration suites — `tests/*.rs`

Some test names start with a short code (`b2_`, `r1_`, `c9_`, …): a stable regression ID for a
correctness bug fixed earlier; the rest of the name describes the behaviour under test.

| File | Tests | What it proves | Oracle |
|---|---:|---|---|
| `pg_edge.rs` | 7 | 5-row timestamp tie all extracted (filter on the tied `updated_at`); empty filter still carries the schema; cursor batch boundaries (13 rows at 3 → 3,3,3,3,1); projection; date/timestamp fidelity for every row incl. 2038/9999/epoch − 1 µs; bytea round trip; `hostile_infinity` fails with a typed error naming the column on cursor and COPY | reference (direct SQL, `extract(epoch …)`, `encode(bin,'hex')`) + trivial |
| `pg_matrix.rs` | 5 | 1 vs 3 Ballista worker processes (real cluster) return the same rows; multi-batch (`batch_size` 7) through the provider, cursor and COPY; `collect()` vs `stream()` (standalone and distributed); full empty-result schema (names, types, timezones, precision, nullability); `copy_statement_timeout_ms` applies to each COPY scan and never leaks into the pooled session | metamorphic + reference (direct SQL ids) + trivial (hand-written schema, `build_arrow_schema`) + the server's timeout error |
| `pg_decode.rs` | 8 | `±infinity` typed errors, extreme finite timestamps, NUMERIC exact-or-error, json/jsonb/uuid text, keyset partitions cover NULL / i64 MIN/MAX keys exactly once, strict inputs, dropping a stream stops the source query | reference (Postgres `::text`, `extract`), differential (cursor vs COPY) |
| `pg_copy.rs` | 3 | Binary COPY returns the same whole rows as the cursor path (full + keyset ranges); unsupported types fail loudly | differential (whole-row, not per-column) |
| `pg_numeric.rs` | 1 | Exact decimal magnitudes (`123.45` scale 2 → `12345`) | trivial |
| `pg_paths.rs` | 1 | Full scan, keyset partitions, cursor batching and filtered extraction agree | metamorphic + reference |
| `pg_pushdown.rs` | 4 | `always` vs `never` return identical rows over a hazard table (ICU collation ranges, `NOT` under a nondeterministic collation, `-0.0`/`NaN`, `(NOT flag) IS NULL`, enum `OR`, uuid/jsonb, backslashes, date operators / window / cast) and each case asserts whether it was pushed; statistics refresh after TTL; one-day windows on unindexed timestamptz / timestamp / date columns keep alone but push together under `cost_based` | differential + non-vacuity check on the plan + provider decisions |
| `pg_pushdown_prop.rs` | 1 | 96 `proptest`-generated predicate trees (fixed seed; `NOT`, `IS NULL`, `AND`, `OR` up to depth 3) over ICU / case-insensitive text, `-0.0`/`NaN`/NULL floats, integer extremes, booleans and an enum return the same rows with `always` and `never` | differential + non-vacuity (at least a quarter of the cases must push a filter) |
| `pg_catalog.rs` | 8 | `table_statistics` / `table_indexes` / `table_enum_columns`, `ExplainEstimator` (real plan, TTL cache, missing table) | reference (the fixture's known shape) |
| `pg_distributed.rs` | 5 | A real 2-worker cluster over the hostile table (codecs, keyset partitions, budgeted pools, exact decimals); distributed `run_with` checkpoints one split and skips it on retry; a worker killed mid-job (SIGKILL) does not hang the job: the watchdog re-runs it on the remaining worker and every row arrives exactly once; with no worker left the job aborts with `DistributedJobAborted`; single-process DataFusion uses the whole `pool_max` and never more | trivial + reference row count + checkpoint state + typed error |
| `pg_checkpoint.rs` | 8 | Stored bounds reused after the table grew; the first split still reads keys inserted below the stored minimum; changed filter → `PlanMismatch`; a failed split does not block others; concurrent run → `LockHeld`; an undrained stream does not complete a split; one run report per attempt (failed run, retry with skipped splits, plan-mismatch run) and the `run_reports` / `diagnostic_run_reports` switches | reference (direct SQL) + typed errors + checkpoint state + run reports |
| `e2e.rs` | 7 | Full / filtered / selective / distributed extraction on a real 2-worker cluster; retry skips completed splits (counted via consumer calls); crash recovery after a consumer failure and after a cancelled run mid-split — no gap, no duplicate | reference (direct SQL) + checkpoint state |
| `mysql.rs` | 14 | MySQL prototype: schema, type mapping, typed extraction, NULL vs `''`, projection, empty tables, streamed batches union to the full scan, typed errors, lossless unsigned / bool / YEAR / TIME / BIT, zero dates as a typed error and ENUM/SET as text, URL-metacharacter passwords, DataFusion over the batches | reference (direct SQL) + trivial |
| `extraction_matrix.rs` | 3 | Every dvdrental table and column through both connectors vs direct `CAST(… AS text)`, and Postgres vs MySQL; Postgres filtered + keyset paths | reference + cross-engine differential |
| `dvdrental_cross_engine.rs` | 4 | Row counts vs the known dvdrental counts on both engines, directly and through the connectors; identical category names and staff picture bytes | reference (published counts) + cross-engine |
| `regressions.rs` | 19 | Regression tests for correctness bugs fixed earlier (collation-sensitive text ranges, `NOT` over an inexact filter, `-0.0` float ranges, `NOT` / `IS NULL` precedence, enum `OR` and uuid `=` filters, reruns with a changed filter, `±infinity` timestamps, NULL / `i64::MAX` keyset keys, unconstrained NUMERIC and big jsonb numbers, projection typos, `batch_size` 0, unsigned / boolean / YEAR / TIME MySQL columns); each asserts the correct behaviour, so a failure is a regression | differential / reference / trivial, named per test |

## 5. CI — `.github/workflows/ci.yml`

Runs on pull requests and on pushes to `master`, `ubuntu-latest`, 75-minute job timeout:

1. `cargo fmt --all -- --check` — blocking.
2. `cargo clippy --all-targets --all-features -- -D warnings` — blocking.
3. `cargo build --workspace --all-targets` (lib, bin, all examples).
4. `cargo test --lib --bins` — unit tests.
5. `cargo test --doc` — doc tests.
6. `docker compose -f tests/docker/compose.yaml up -d --wait` — the same stack as local runs.
7. `cargo test --tests -- --test-threads=1` — every integration file.
8. On failure, the database logs; always `docker compose … down -v`.

## 6. Debug SQL comment tags

`connector::query_tag::QuerySession` prepends a comment such as

```
/* el-ballista query_id=q_1a2b3c4d pipeline=payment_extract run_id=r_9f8e7d6c strategy=full partition=7/23 */
```

to every data-scan query from `PostgresExecutionPlan` and `PostgresExtractor`, so a statement is
identifiable in `pg_stat_activity` / server logs during a test run. The same tag is how the COPY
cancel-on-drop path finds its backend (covered by `pg_decode.rs`
`dropping_a_scan_stream_stops_the_source`).

## 7. Notes and open items

- `main.rs` does not re-declare the library's modules (it `use`s
  `el_ballista::…`), so unit tests compile and run once, under the lib target;
  the bin target has none.
- Open: a live check that `PushdownPolicy::Strict` returns the same rows as `always`
  (`pg_pushdown.rs` and `pg_pushdown_prop.rs` compare `always` vs `never` only).
- Open: shrinking for `pg_pushdown_prop.rs` failures (a mismatch is reported as generated,
  already small at depth ≤ 3).
- Open: a coverage ratchet.
