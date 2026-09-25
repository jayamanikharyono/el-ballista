# Quickstart Guide

Run the examples first — each one is a runnable demo of one layer capability.
Setup takes a few minutes; every example below assumes it.

---

## Prerequisites

- **Rust stable** with edition 2024 support (the benchmark image builds with 1.98.1; there is no
  `rust-toolchain.toml` pin)
- **Docker** with `docker compose`, for the seeded Postgres below
- Password exported: `export ORDERS_PG_PASSWORD=postgres`

### 1. Start Postgres with seed data

```bash
docker compose -f benchmark/PostgresDB/compose.yaml up -d
# Postgres 17 at localhost:5432, database `app`, user/password postgres/postgres,
# seeded once from benchmark/PostgresDB/initdb (500 users, 20k orders)
```

Check it end to end with a diagnostic run (counts rows, delivers nothing, writes no checkpoint):

```bash
export ORDERS_PG_PASSWORD=postgres
cargo run --bin rust-ballista-extraction-layer -- run --config examples/configs/extract.example.json
# job 'orders_extract' (filtered, diagnostic): counted 28 row(s) in 1 split(s); nothing delivered, no checkpoint written
```

The test stack (`tests/docker/compose.yaml`: Postgres + MySQL with the dvdrental dataset,
database `test`) also publishes port 5432 — stop one before starting the other. The
`examples/configs/*.dvd_rental.json` configs target it, e.g.
`… run --config examples/configs/full_extract.dvd_rental.json` counts 14596 `payment` rows.

### 2. Point the examples at it

Config-driven examples read connection + credentials from a job spec
(`examples/configs/extract.example.json` → database `app`, password from
`$ORDERS_PG_PASSWORD`):

```bash
export ORDERS_PG_PASSWORD=postgres
export PGPASSWORD=postgres   # full_extraction, parallel_extraction and `rel demo`
```

`full_extraction` and `parallel_extraction` take no config: they connect to
`localhost:5432 / postgres / app` with the password from `$PGPASSWORD` (no example
hard-codes a password).

---

## Examples: run order and what each shows

Suggested path: `full` → `filtered` → `pipeline` → `dataframe` → `parallel` → `distributed` → `bench`.

### 1. `full_extraction` — the basic contract: DB → Arrow

```bash
PGPASSWORD=postgres cargo run --example full_extraction
```

- **Inside:** `PostgresExtractor::connect` → `extract_full_table("public.orders", columns)` → one Arrow `RecordBatch` → prints schema + row/column counts.
- **Showcases:** whole-table extraction, schema discovery/type mapping, row→Arrow conversion. No filters, no DataFusion, no checkpointing — the minimal end-to-end data contract.

### 2. `filtered_extraction` — caller-provided predicates + pushdown preview

```bash
cargo run --example filtered_extraction
```

- **Inside:** loads `examples/configs/extract.example.json` → `explain_filters()` prints per-filter push/keep decisions → `connector.extract().standalone().collect()` → `.run_with(consumer)` for the checkpointed operational run.
- **Showcases:** filters as orchestrator input (an ANDed list of structured predicates: status, amount, a time range, a user range), previewing *where* each predicate executes (source vs Arrow) before extracting, and `collect()` (data, no side effects) vs `run_with()` (split checkpointing: a split is recorded completed only after the consumer returned `Ok`).

### 3. `pipeline_extraction` — the one builder every path uses

```bash
cargo run --example pipeline_extraction -- [config.json]   # default: full_extract.example.json
```

- **Inside:** `PostgresConnector::from_config_file` → `extract().standalone().collect()` → `extract().standalone().run_with(consumer)` → `extract().distributed().in_process().run()` (diagnostic count).
- **Showcases:** the fluent entry point the CLI (`rel run` / `rel distribute`) also funnels through, and its four terminals: `collect()` / `stream()` (data, no checkpoint), `run_with(consumer)` (operational, checkpointed), `run()` (diagnostic count, no checkpoint). If you only read one example to learn the API surface, read this one.

### 4. `dataframe_extraction` — DataFusion-native querying over the source

```bash
cargo run --example dataframe_extraction -- [config.json]  # default: extract.example.json
```

- **Inside:** `ExtractContext::from_config` → builder chain (`filter` → `select` → `with_column` → `limit` → `collect`) → then the SQL entry point (`SELECT COUNT(*) AS n FROM public.orders`) against the same registered table.
- **Showcases:** filtering/projection/derived columns as DataFusion's job (not reimplemented), pushdown through the builder API, and that builder and SQL hit the same source (including the `COUNT(*)` empty-projection path).

### 5. `parallel_extraction` — keyset partitioning mechanics

```bash
PGPASSWORD=postgres cargo run --example parallel_extraction
```

- **Inside:** `compute_keyset_partitions` (4 ranges over `order_id`) → one `tokio::spawn` per partition, each with its own extractor/connection → `extract_keyset_partition(lo, hi)` → combined row counts.
- **Showcases:** non-overlapping `WHERE order_id >= lo AND order_id < hi` ranges (keyset, never `LIMIT/OFFSET`), per-partition connections, and the no-gaps/no-duplicates argument. Single-node parallelism; distribution is the next example.

### 6. `distributed_extraction` — the same job spec on Ballista

```bash
cargo run --example distributed_extraction -- <config.json> [workers] [output.parquet]  # default workers: 2
```

- **Inside:** `PostgresConnector::from_config_file` → `extract().distributed().in_process().workers(n).stream()` (the config's filters through `filter_exprs_with_schema`, the shared choke point, + column projection) → streams into one Parquet file (default `output/distributed_extraction.parquet`).
- **Showcases:** scaling out without scaling source load (each process opens only `pool_max / workers` connections), and that distributed extraction applies the *identical* filter semantics as standalone. Check the log line `pushed_filters=N`: pushed predicates run in Postgres, the rest filter in Ballista — both AND-correct.

### 7. `bench_full_load` — the fair-benchmark harness (not a demo)

```bash
cargo run --release --example bench_full_load -- \
  <config.json> <workers> <output.parquet> [filter_sql] [columns_csv] [scenario]
# Remote cluster instead of standalone:
BENCH_SCHEDULER_URL=http://host:port cargo run --release --example bench_full_load -- ...
```

- **Inside:** `DistributedContext` (standalone or remote, same code path) → raw `SELECT ... WHERE <filter_sql>` → `execute_stream()` straight into a Snappy Parquet writer → prints a JSON summary (`scan_ms` / `write_ms` / rows / batches) for `benchmark/run.sh`.
- **Showcases:** benchmark comparability, not extraction features — raw SQL passthrough is deliberate so Spark runs the byte-identical predicate, and streaming keeps memory O(batch) at any row count. Normally invoked via `cd benchmark && ./run.sh --skip-scale --repeat 1`.

---

## The job spec in 60 seconds

`examples/configs/extract.example.json` is what the config-driven examples load:

- `table` / `columns` — what to read (omitted `columns` = all).
- `filters` — ANDed list; an inner array is an OR-group:
  ```json
  "filters": [
    [{ "column": "status", "op": "=", "value": "PAID" },
     { "column": "amount", "op": ">", "value": 100 }],
    { "column": "updated_at", "op": ">=", "value": "2026-01-01T00:00:00Z" }
  ]
  ```
  means `(status='PAID' OR amount>100) AND updated_at>=...`. Shorthand strings (`"status=PAID"`) work too and lower identically. Empty = full extraction.
- `source.password_env` — env var name holding the password (never inline one).
- `pushdown.policy` — `always` / `never` / `cost_based` (default) / `strict` / `hinted`.
- `parallel_scan` (`strategy`: `none` / `keyset` / `ctid`) / `execution.batch_size` / `distributed.workers` — splitting, FETCH size, executor count.
- `checkpoint.dir` / `checkpoint.lock_ttl_secs` (default 1800) — where `run_with` keeps split state and its per-job lock, and when a crashed run's lock may be taken over.
- There is no `sink` block (this layer is not a sink): unknown fields anywhere are a load error, and enum values are exact lowercase names.

`full_extract.example.json` is the same shape with empty `filters` (full load).

---

## CLI essentials (same paths as the examples)

```bash
# DIAGNOSTIC single-node run: scans (full, or config filters ANDed with --filter flags),
# counts rows, discards them. Delivers no data, writes no checkpoint.
cargo run --bin rust-ballista-extraction-layer -- run --config my-job.json [--filter "status=PAID"]

# What will push to the source under a policy, plus a preview of at most --limit rows (default 20)
cargo run --bin rust-ballista-extraction-layer -- plan --config my-job.json --policy cost_based

# DIAGNOSTIC distributed run: in-process by default, or point at a live scheduler
cargo run --bin rust-ballista-extraction-layer -- distribute --config my-job.json --workers 4

# Split checkpoints of `run_with` jobs (execution progress only, never watermarks)
cargo run --bin rust-ballista-extraction-layer -- checkpoint show --config my-job.json
cargo run --bin rust-ballista-extraction-layer -- checkpoint reset --config my-job.json
```

The operational, checkpointed job is the library call
`PostgresConnector::from_config(cfg)?.extract().standalone().run_with(consumer)` (see
`examples/pipeline_extraction.rs`); `rel` with no arguments prints usage, and `rel demo`
runs the demo pipeline.

---

## Tests and benchmarks (pointers)

```bash
cargo test --lib                                           # 215 unit tests, no DB
docker compose -f tests/docker/compose.yaml up -d --wait   # test databases (not the app DB above)
cargo test --tests -- --test-threads=1                     # all 15 integration files (88 tests)
cargo test --test pg_pushdown -- --test-threads=1          # or one file
cd benchmark && ./run.sh --skip-scale --repeat 1           # smoke benchmark (needs a >= 6-core Docker host)
```

Details live in [`docs/testing-plan.md`](docs/testing-plan.md) and
[`benchmark/README.md`](benchmark/README.md).

---

## Common issues

| Problem | Solution |
|---------|----------|
| `password_env` / `ORDERS_PG_PASSWORD` not set | `export ORDERS_PG_PASSWORD=postgres` |
| `batch_size` validation error | Must be ≥ 1 in config |
| `unknown field \`sink\`` (or another field) | Remove it: the job spec is strict and has no sink block |
| `the stored checkpoint belongs to a different extraction plan` | The job's filters/table/partitioning changed: use a new `job_id` or `rel checkpoint reset --config …` |
| `job '…' is already running` | Another `run_with` of the same job holds its lock; a crashed run's lock is taken over after `checkpoint.lock_ttl_secs` |
| `connection refused` on 5432 | Start the compose stack for the config you run (`app` DB: `benchmark/PostgresDB/compose.yaml`; `test` DB: `tests/docker/compose.yaml`) |
| Linker `__eh_frame` warning on macOS | Toolchain noise, harmless |
