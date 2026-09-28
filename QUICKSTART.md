# Quickstart Guide

Run the examples first: each one is a runnable demo of one capability of the layer.
Setup takes a few minutes; every example below assumes it.

Two datasets, kept apart on purpose:

- **Demo and tests:** the dvdrental dataset from `tests/docker/compose.yaml` (database
  `test`). Every example, demo config and integration test reads it; the examples use
  `public.payment` (14,596 rows).
- **Benchmark only:** the `orders` table, loaded into its own container by
  `benchmark/run.sh` and scaled to millions of rows. It is not a demo database; see
  [`benchmark/README.md`](benchmark/README.md).

`el-ballista` below is the built binary; from a checkout, use
`cargo run --release --bin el-ballista --`.

---

## Prerequisites

- **Rust stable** with edition 2024 support (the benchmark numbers were measured with Rust
  1.98.1; there is no `rust-toolchain.toml` pin)
- **Docker** with `docker compose`, for the seeded databases below

### 1. Start the demo databases

```bash
docker compose -f tests/docker/compose.yaml up -d --wait
# Postgres 17 at localhost:5432, database `test`, user/password postgres/postgres,
# seeded from tests/data/dvdrental and ANALYZEd (plus MySQL 8 on 3306 with the same data)
export PGPASSWORD=postgres
```

Every demo config, every example and `el-ballista demo` read the password from `$PGPASSWORD`; no
example hard-codes one. The integration tests and CI use the same stack.

Check it end to end with a diagnostic run (counts rows, delivers nothing, writes no checkpoint):

```bash
cargo run --bin el-ballista -- run --config examples/configs/extract.example.json
# job 'payment_extract' (filtered, diagnostic): counted 422 row(s) in 1 split(s); nothing delivered, no checkpoint written
```

### 2. The demo configs

All of them read `public.payment` on `localhost:5432/test`:

| Config | What it is |
|---|---|
| `examples/configs/extract.example.json` | Job `payment_extract`: a one-week `payment_date` window, `customer_id >= 300`, `amount > 5` (422 rows) |
| `examples/configs/full_extract.example.json` | Job `payment_full`: no filters (14,596 rows) |
| `examples/configs/pushdown_showcase.json` | One filter per pushdown outcome, for `el-ballista plan` |
| `examples/configs/*.dvd_rental.json` | Extra full / selective / date-range examples |

`full_extraction` and `parallel_extraction` take no config: they connect to
`localhost:5432 / postgres / test` with the password from `$PGPASSWORD`.

---

## Examples: run order and what each shows

Suggested path: `full` → `filtered` → `pipeline` → `parquet_export` → `dataframe` → `parallel` →
`distributed`. `bench_full_load` is the benchmark harness, not a demo.

### 1. `full_extraction`: the basic contract, DB → Arrow

```bash
cargo run --example full_extraction
```

- **Inside:** `PostgresExtractor::connect` → `extract_full_table("public.payment", columns)` → one Arrow `RecordBatch` → prints schema + row/column counts.
- **Showcases:** whole-table extraction, schema discovery and type mapping, row → Arrow conversion. No filters, no DataFusion, no checkpointing: the minimal end-to-end data contract.

### 2. `filtered_extraction`: caller-provided predicates + pushdown preview

```bash
cargo run --example filtered_extraction
```

- **Inside:** loads `examples/configs/extract.example.json` → `explain_filters()` prints per-filter push/keep decisions → `connector.extract().standalone().collect()` → `.run_with(consumer)` for the checkpointed operational run.
- **Showcases:** filters as orchestrator input (an ANDed list of structured predicates: a one-week `payment_date` window, a customer range, an amount predicate), previewing *where* each predicate runs (Postgres or DataFusion) before extracting, and `collect()` (data, no side effects) vs `run_with()` (split checkpointing: a split is recorded completed only after the consumer returned `Ok`).

### 3. `pipeline_extraction`: the one builder every path uses

```bash
cargo run --example pipeline_extraction -- [config.json]   # default: full_extract.example.json
```

- **Inside:** `PostgresConnector::from_config_file` → `extract().standalone().collect()` → `extract().standalone().run_with(consumer)` → `extract().distributed().run()` (diagnostic count on a running cluster; skipped with a note when none is reachable).
- **Showcases:** the fluent entry point that the CLI (`el-ballista run` / `el-ballista distribute`) also goes through, and its four terminals: `collect()` / `stream()` (data, no checkpoint), `run_with(consumer)` (operational, checkpointed), `run()` (diagnostic count, no checkpoint). To learn the API from one example, read this one.

### 3b. `parquet_export`: a real checkpointed job, with a run report

```bash
cargo run --release --example parquet_export -- [config.json] [output_dir]
# defaults: examples/configs/full_extract.example.json, output
```

- **Inside:** `extract().standalone().run_with(consumer)` where the consumer writes each split's
  stream to `<output_dir>/<job_id>/<split_id>.parquet` (temp file, then rename).
- **Showcases:** the operational path end to end. A split is recorded completed only after its
  file is in place, and the run leaves a report in `<checkpoint.dir>/runs/<job_id>/<run_id>.json`
  (`el-ballista runs list` / `el-ballista runs show`). Run it twice: the second run skips the completed split
  and its report shows it as `skipped`. For a fresh export use a new `job_id` or
  `el-ballista checkpoint reset`. `bench_full_load` also writes Parquet but through `stream()`, which
  has no checkpoint and no run report.

### 4. `dataframe_extraction`: DataFusion-native querying over the source

```bash
cargo run --example dataframe_extraction -- [config.json]  # default: extract.example.json
```

- **Inside:** `ExtractContext::from_config` → builder chain on `public.payment` (`filter(customer_id >= 300)` → `select` → `with_column` → `limit` → `collect`) → then the SQL entry point (`SELECT COUNT(*) AS n FROM public.payment`) against the same registered table.
- **Showcases:** filtering, projection and derived columns as DataFusion's job (not reimplemented), pushdown through the builder API, and that the builder and SQL hit the same source (including the `COUNT(*)` empty-projection path).

### 5. `parallel_extraction`: keyset partitioning mechanics

```bash
cargo run --example parallel_extraction
```

- **Inside:** `compute_keyset_partitions` (4 ranges over `payment_id`) → one `tokio::spawn` per partition, each with its own extractor and connection → `extract_keyset_partition(lo, hi)` → combined row counts.
- **Showcases:** non-overlapping `WHERE payment_id >= lo AND payment_id < hi` ranges (keyset, never `LIMIT/OFFSET`), per-partition connections, and why the ranges leave no gaps and no duplicates. Single-node parallelism; distribution is the next example.

### 6. `distributed_extraction`: the same job spec on Ballista

```bash
cargo run --example distributed_extraction -- [config.json] [workers] [output.parquet]  # defaults: extract.example.json, 2
```

- **Needs:** a running cluster: `el-ballista scheduler` plus `workers` `el-ballista worker` processes (give each worker on one host its own `--port` / `--grpc-port`).
- **Inside:** `PostgresConnector::from_config_file` → `extract().distributed().workers(n).stream()` (the config's filters, coerced to the table schema exactly as in standalone, plus the column projection) → streams into one Parquet file (default `output/distributed_extraction.parquet`).
- **Showcases:** scaling out without scaling source load (each worker process opens only `pool_max / workers` connections), and that distributed extraction applies the same filter semantics as standalone. `el-ballista plan` on the same config shows which predicates run in Postgres; the rest are filtered in Ballista.

### 7. `bench_full_load`: the benchmark harness (not a demo)

```bash
cargo run --release --example bench_full_load -- \
  [config.json] [workers] [output.parquet] [scenario_label]
# defaults: benchmark/rust/bench-config.json, 2, output/bench_full_load.parquet, full
# Remote cluster instead of standalone:
BENCH_SCHEDULER_URL=http://host:port cargo run --release --example bench_full_load -- ...
```

- **Data:** the default config reads the benchmark's `orders` table on `bench-pg` (password
  from `BENCH_PG_PASSWORD`), which only `benchmark/run.sh` creates. Pass a demo config
  (e.g. `examples/configs/full_extract.example.json`) to try it against dvdrental.
- **Inside:** `PostgresConnector::from_config` → `extract().standalone().stream()` (or `.distributed().scheduler(url).workers(n).stream()` when `BENCH_SCHEDULER_URL` is set) → straight into a Snappy Parquet writer → prints a JSON summary (`scan_ms` / `write_ms` / rows / batches / the config's `filters` and projection) for `benchmark/run.sh`.
- **Scenario = config:** what is extracted comes only from the job config (`table`, structured `filters`, `columns`); the harness takes no filter or column arguments and builds no SQL. `run.sh` writes one config per scenario and renders Spark's SQL predicate from the same structured definition.
- **Showcases:** benchmark comparability, and streaming that keeps memory bounded by scan concurrency at any row or partition count. Normally invoked through `benchmark/run.sh`.

---

## The job spec in 60 seconds

`examples/configs/extract.example.json` is what the config-driven examples load:

- `table` / `columns`: what to read (omitted `columns` = all).
- `filters`: an ANDed list; an inner array is an OR-group:
  ```json
  "filters": [
    [{ "column": "staff_id", "op": "=", "value": 1 },
     { "column": "amount",   "op": ">", "value": 5 }],
    { "column": "payment_date", "op": ">=", "value": "2007-04-06" }
  ]
  ```
  means `(staff_id = 1 OR amount > 5) AND payment_date >= '2007-04-06'`. Empty = full extraction.
- Accepted `op` values: `=` `!=` `>` `>=` `<` `<=` `is_null` `is_not_null` (the last two take
  no `value`). Shorthand strings such as `"customer_id>=300"` cover the first six and lower
  to the same predicate.
- Dates: an RFC3339 or `YYYY-MM-DD` string becomes a timestamp literal on a timestamp column,
  and a `YYYY-MM-DD` string becomes a date literal on a date column, so both can push to
  Postgres.
- `source.password_env`: the name of the env var holding the password (never inline one).
- `pushdown.policy`: `always` / `never` / `cost_based` (default) / `strict` / `hinted`.
- `parallel_scan` (`strategy`: `none` / `keyset` / `ctid`) / `execution.batch_size` / `distributed.workers`: splitting, fetch size, worker count.
- `checkpoint.dir` / `checkpoint.lock_ttl_secs` (default 1800): where `run_with` keeps split state and its per-job lock, and when a crashed run's lock may be taken over.
- `checkpoint.run_reports` (default `true`) / `checkpoint.diagnostic_run_reports` (default `false`): write a run report per `run_with` run (and per diagnostic run) under `<checkpoint.dir>/runs/<job>/`.
- There is no `sink` block (this layer is not a sink): unknown fields anywhere are a load error, and enum values are exact lowercase names.

`full_extract.example.json` is the same shape with empty `filters` (full load). The full
spec with every block is in [`docs/running.md`](docs/running.md#job-spec-full-example).

---

## CLI essentials (same paths as the examples)

```bash
# DIAGNOSTIC single-node run: scans (full, or config filters ANDed with --filter flags),
# counts rows, discards them. Delivers no data, writes no checkpoint.
cargo run --bin el-ballista -- run --config my-job.json [--filter "customer_id>=300"]

# What will push to the source under a policy, plus a preview of at most --limit rows (default 20)
cargo run --bin el-ballista -- plan --config examples/configs/pushdown_showcase.json --policy cost_based

# DIAGNOSTIC distributed run on a running cluster (--scheduler-url, else the config's, else localhost:50050)
cargo run --bin el-ballista -- distribute --config my-job.json --workers 4

# Split checkpoints of `run_with` jobs (execution progress only, never watermarks)
cargo run --bin el-ballista -- checkpoint show --config my-job.json
cargo run --bin el-ballista -- checkpoint reset --config my-job.json

# Run reports: one JSON record per `run_with` run under <checkpoint.dir>/runs/<job>/
cargo run --bin el-ballista -- runs list --config my-job.json
cargo run --bin el-ballista -- runs show --config my-job.json [--run r_1a2b3c4d]
```

The operational, checkpointed job is the library call
`PostgresConnector::from_config(cfg)?.extract().standalone().run_with(consumer)` (see
`examples/pipeline_extraction.rs`). `el-ballista` with no arguments prints usage, and `el-ballista demo`
runs the demo pipeline.

---

## Tests and benchmarks (pointers)

```bash
cargo test --lib                                           # unit tests, no database
docker compose -f tests/docker/compose.yaml up -d --wait   # the same stack as the demo
cargo test --tests -- --test-threads=1                     # every integration file
cargo test --test pg_pushdown -- --test-threads=1          # or one file
cd benchmark && ./run.sh --skip-scale --repeat 1           # smoke benchmark (needs a >= 8-core Docker host)
```

Current test counts are in the README's [Testing](README.md#testing) section. Details live
in [`docs/testing-plan.md`](docs/testing-plan.md) and
[`benchmark/README.md`](benchmark/README.md).

---

## Common issues

| Problem | Solution |
|---------|----------|
| `password_env` / `PGPASSWORD` not set | `export PGPASSWORD=postgres` |
| `batch_size` validation error | Must be ≥ 1 in config |
| `unknown field \`sink\`` (or another field) | Remove it: the job spec is strict and has no sink block |
| `the stored checkpoint belongs to a different extraction plan` | The job's filters/table/partitioning changed: use a new `job_id` or `el-ballista checkpoint reset --config …` |
| `job '…' is already running` | Another `run_with` of the same job holds its lock; a crashed run's lock is taken over after `checkpoint.lock_ttl_secs` |
| `connection refused` on 5432 | Start the demo stack: `docker compose -f tests/docker/compose.yaml up -d --wait` |
| Linker `__eh_frame` warning on macOS | Toolchain noise, harmless |
