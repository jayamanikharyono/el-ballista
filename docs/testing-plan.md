# Testing

What the test suite contains today, how each part decides pass/fail (its **oracle**, AGENTS.md
§7), and how to run it. Counts below come from `cargo test … -- --list` and a full run on
2026-09-24 (against Postgres 16 and MySQL 8.0 seeded exactly like the compose stack, whose
Postgres image is 17); re-run those commands rather than trusting the numbers after changes.

**Rule of the suite: nothing skips.** Unit tests never touch a database. Every integration test
needs the live databases from `tests/docker/compose.yaml` and **panics** (fails) with a message
pointing at the stack when they are unreachable — a broken harness is a failure, never a silent
"skipped" or green.

## 0. Running the tests

```bash
# 1. Databases: Postgres + MySQL, both seeded with the dvdrental dataset from tests/data/.
docker compose -f tests/docker/compose.yaml up -d --wait

# 2. Unit tests (no database, ~seconds once built).
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
- `scripts/verify-review.sh [--full] [--up]` runs `tests/review_repro.rs` and prints
  `FIXED (passes)` / `REGRESSED (fails)` per review finding; `--full` also runs every other
  integration file under a 600 s watchdog and reports leftover `test_*` schemas.
- Endpoints default to the compose services: Postgres
  `postgres://postgres:postgres@127.0.0.1:5432/test`, MySQL
  `mysql://root:password@127.0.0.1:3306/test`. `DATABASE_URL` / `MYSQL_URL` point the suites
  at another server (it still has to be seeded like the compose stack for the dvdrental suites).

## 1. Counts (measured)

| Target | Tests | How counted / run |
|---|---:|---|
| Unit (`src/**`, lib target) | 215 | `cargo test --lib -- --list`; all pass |
| Binary target (`src/main.rs`) | 0 | `cargo test --bins -- --list` (the bin is a thin wrapper over the lib) |
| Doc tests | 48 | `cargo test --doc`: 47 pass, 1 `ignore`d (`Predicate::render_to`'s sketch) |
| Integration (`tests/*.rs`, 15 files) | 88 | `cargo test --tests -- --list`; all pass with `--test-threads=1` |

The full `cargo test --tests -- --test-threads=1` run (which also re-runs the 215 unit tests)
took 87 s wall clock on a 2-CPU dev machine with the test binaries already built — about 82 s of
it in the test binaries themselves (per-file times in §4, as reported by libtest).

## 2. Unit tests — no database

Pure logic, in `#[cfg(test)] mod tests` next to the code. Per module (from `--list`):

| Module | Tests | What it covers (oracle: hand-written expected values unless noted) |
|---|---:|---|
| `config` | 22 | Strict job-spec parsing (`deny_unknown_fields`: a `sink` / `watermark` block is a load error), lowercase `strategy` / `policy` enums, validation, password resolution, every checked-in example config, `benchmark/rust/bench-config.json` and the job spec `benchmark/run.sh` generates |
| `connector::postgres::parallel` | 15 | Keyset / ctid partition math in `i128`: even splits, degenerate ranges, first partition takes `IS NULL`, last is open-ended |
| `connector::postgres::pipeline::filters` | 14 | Shorthand vs structured filter lowering equivalence, value typing, schema-coerced timestamp/date literals |
| `pushdown::cost_model` | 12 | Push/keep decisions through `decide_push`: index shortcut, selectivity and cost gates |
| `connector::postgres::copy` | 11 | Binary COPY decoding: bad signature / truncation fail loudly, typed values and NULLs, Postgres-epoch timestamps, `±infinity` and NUMERIC `NaN` / overscale as typed errors, empty results keep the schema, zero-column projections |
| `checkpoint::json_store` | 10 | Split state machine, plan fingerprint / `PlanMismatch`, stored bounds, atomic writes, collision-free file names, orphan tmp cleanup |
| `pushdown::translate` | 9 | `Expr` → `Predicate` fidelity: `COLLATE "C"` text (Exact), float `=` Inexact / ranges not pushed, `NOT` only over Exact, uuid/json `=`/`<>` via `::text` |
| `connector::postgres::row_adapter` | 9 | Per-column binary decoders, `±infinity` / NUMERIC overflow as typed errors |
| `connector::postgres::explain` | 8 | `EXPLAIN (FORMAT JSON)` parsing and its failure paths |
| `connector::postgres::execution_plan` | 8 | Bind order, COPY vs cursor SQL shapes, partition count, debug tags |
| `connector::postgres::query_builder` | 7 | Full / keyset SQL shapes, `::text` casts for json/jsonb/uuid/enum |
| `connector::postgres::distributed::pool_registry` | 7 | Budgeted pool per descriptor (`pool_max / workers`), key covers budget + session settings, acquire timeout ≥ statement timeout, `close_all` |
| `pushdown::policy` | 6 | Policy matrix (`always`/`never`/`cost_based`/`strict`/`hinted`) |
| `connector::postgres::table_provider` | 6 | Scan pushes what `supports_filters_pushdown` accepted and rejects an untranslatable filter; a decoded (scheduler-side) provider decides identically; `COUNT(*)` empty projection |
| `connector::postgres::arrow_type_mapper` | 6 | Postgres type → Arrow type |
| `logging` | 5 | CLI/env log option resolution |
| `connector::query_tag` | 5 | SQL comment tag rendering, `*/` injection defence |
| `connector::mysql::type_mapper` | 5 | MySQL type → Arrow (unsigned, `tinyint(1)`, `YEAR`, `TIME`) |
| `checkpoint::lock` | 5 | Per-job `O_EXCL` lock, heartbeat, TTL takeover, RAII release |
| others (20 modules) | 45 | `pipeline::splits` 4, `inline_sql` 4, `mysql::row_adapter` 4, `types::table_metadata` 3, `pushdown::ir` 3, `postgres::dialect` 3, `postgres::api` 3, `checkpoint::fingerprint` 3, `pushdown::stats` 2, `errors` 2, `table_codec` 2, `mysql::dialect` 2, `checkpoint::progress` 2, and one each in `types::job_id`, `pushdown::explain`, `pushdown`, `postgres::stats`, `postgres::engine`, `plan_codec`, `mysql::query_builder`, `checkpoint` |

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
- **dvdrental.** Both compose engines are seeded from the same `.dat` files
  (`tests/data/dvdrental`; MySQL through `tests/data/dvdrental_mysql.sql`), so the matrix and
  cross-engine suites compare real, identical data.
- Self-contained suites (`pg_decode.rs`, `review_repro.rs`) create their own schemas/tables and
  clean up explicitly instead of using `TestDb`.

## 4. Integration suites — `tests/*.rs`

| File | Tests | Time | What it proves | Oracle |
|---|---:|---:|---|---|
| `pg_edge.rs` | 7 | 5.4 s | 5-row timestamp tie all extracted (filter on the tied `updated_at`); empty filter still carries the schema; cursor batch boundaries (13 rows at 3 → 3,3,3,3,1); projection; date/timestamp fidelity for every row incl. 2038/9999/epoch − 1 µs; bytea round trip; `hostile_infinity` fails with a typed error naming the column on cursor and COPY | reference (direct SQL, `extract(epoch …)`, `encode(bin,'hex')`) + trivial |
| `pg_matrix.rs` | 4 | 5.6 s | 1 vs 3 in-process Ballista workers return the same rows; multi-batch (`batch_size` 7) through the provider, cursor and COPY; `collect()` vs `stream()` (standalone and distributed); full empty-result schema (names, types, timezones, precision, nullability) | metamorphic + reference (direct SQL ids) + trivial (hand-written schema, `build_arrow_schema`) |
| `pg_decode.rs` | 8 | 21.0 s | `±infinity` typed errors, extreme finite timestamps, NUMERIC exact-or-error, json/jsonb/uuid text, keyset partitions cover NULL / i64 MIN/MAX keys exactly once, strict inputs, dropping a stream stops the source query | reference (Postgres `::text`, `extract`), differential (cursor vs COPY) |
| `pg_copy.rs` | 3 | 2.0 s | Binary COPY returns the same whole rows as the cursor path (full + keyset ranges); unsupported types fail loudly | differential (whole-row, not per-column) |
| `pg_numeric.rs` | 1 | 0.7 s | Exact decimal magnitudes (`123.45` scale 2 → `12345`) | trivial |
| `pg_paths.rs` | 1 | 1.2 s | Full scan, keyset partitions, cursor batching and filtered extraction agree | metamorphic + reference |
| `pg_pushdown.rs` | 3 | 7.4 s | `always` vs `never` return identical rows over a hazard table (ICU collation ranges, `NOT` under a nondeterministic collation, `-0.0`/`NaN`, `(NOT flag) IS NULL`, enum `OR`, uuid/jsonb, backslashes) and each case asserts whether it was pushed; statistics refresh after TTL | differential + non-vacuity check on the plan |
| `pg_catalog.rs` | 8 | 3.3 s | `table_statistics` / `table_indexes` / `table_enum_columns`, `ExplainEstimator` (real plan, TTL cache, missing table) | reference (the fixture's known shape) |
| `pg_distributed.rs` | 2 | 2.4 s | In-process Ballista over the hostile table (codecs, keyset partitions, budgeted pools, exact decimals); distributed `run_with` checkpoints one split and skips it on retry | trivial + checkpoint state |
| `pg_checkpoint.rs` | 5 | 3.2 s | Stored bounds reused after the table grew; changed filter → `PlanMismatch`; a failed split does not block others; concurrent run → `LockHeld`; an undrained stream does not complete a split | reference (direct SQL) + typed errors + checkpoint state |
| `e2e.rs` | 7 | 6.6 s | Full / filtered / selective / distributed extraction in-process; retry skips completed splits (counted via consumer calls); crash recovery after a consumer failure and after a cancelled run mid-split — no gap, no duplicate | reference (direct SQL) + checkpoint state |
| `mysql.rs` | 13 | 3.3 s | MySQL prototype: schema, type mapping, typed extraction, NULL vs `''`, projection, empty tables, streamed batches union to the full scan, typed errors, lossless unsigned / bool / YEAR / TIME / BIT, URL-metacharacter passwords, DataFusion over the batches | reference (direct SQL) + trivial |
| `extraction_matrix.rs` | 3 | 5.0 s | Every dvdrental table and column through both connectors vs direct `CAST(… AS text)`, and Postgres vs MySQL; Postgres filtered + keyset paths | reference + cross-engine differential |
| `dvdrental_cross_engine.rs` | 4 | 1.5 s | Row counts vs the known dvdrental counts on both engines, directly and through the connectors; identical category names and staff picture bytes | reference (published counts) + cross-engine |
| `review_repro.rs` | 19 | 11.9 s | One reproduction per review finding (`b1_…`, `c9_…`); each asserts the correct behaviour, so all pass now and a failure is a regression | differential / reference / trivial, named per test |

## 5. CI — `.github/workflows/ci.yml`

Runs on pull requests and on pushes to `master`, `ubuntu-latest`, 75-minute job timeout:

1. `cargo fmt --all -- --check` — blocking.
2. `cargo clippy --all-targets --all-features -- -D warnings` — blocking.
3. `cargo build --workspace --all-targets` (lib, bin, all examples).
4. `cargo test --lib --bins` — unit tests.
5. `docker compose -f tests/docker/compose.yaml up -d --wait` — the same stack as local runs.
6. `cargo test --tests -- --test-threads=1` — every integration file.
7. On failure, the database logs; always `docker compose … down -v`.

Doc tests (`cargo test --doc`) run in CI as a separate step before the integration tests.

## 6. Debug SQL comment tags

`connector::query_tag::QuerySession` prepends a comment such as

```
/* rust-extract query_id=q_1a2b3c4d pipeline=orders_extract run_id=r_9f8e7d6c strategy=full partition=7/23 */
```

to every data-scan query from `PostgresExecutionPlan` and `PostgresExtractor`, so a statement is
identifiable in `pg_stat_activity` / server logs during a test run. The same tag is how the COPY
cancel-on-drop path finds its backend (covered by `pg_decode.rs`
`dropping_a_scan_stream_stops_the_source`).

## 7. Notes and open items

- `main.rs` no longer re-declares the library's modules (it `use`s
  `rust_ballista_extraction_layer::…`), so unit tests compile and run once, under the lib target;
  the bin target has none.
- Open: a live check that `PushdownPolicy::Strict` returns the same rows as `always`
  (`pg_pushdown.rs` differentials `always` vs `never` only).
- Open: `proptest`-generated predicate trees for the pushdown differential (the hazard table is
  hand-written); a coverage ratchet; multi-process scheduler/worker runs (the wire format is
  exercised in-process and by the codec unit tests).
