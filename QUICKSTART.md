# Quickstart Guide

Run the examples first — each one is a runnable demo of one layer capability.
Setup takes a few minutes; every example below assumes it.

---

## Prerequisites

- **Rust 1.98+** (toolchain managed via `rust-toolchain.toml`)
- **PostgreSQL 17+** with an `orders` table (seed via Docker, below)
- Password exported: `export ORDERS_PG_PASSWORD=postgres`

### 1. Start Postgres with seed data

```bash
cd benchmark
docker compose -f PostgresDB/compose.yaml up -d
# Postgres at localhost:5432, password: postgres (500 users, 20k orders)
```

### 2. Point the examples at it

Config-driven examples read connection + credentials from a job spec
(`examples/configs/extract.example.json` → database `app`, password from
`$ORDERS_PG_PASSWORD`):

```bash
export ORDERS_PG_PASSWORD=postgres
```

`full_extraction` and `parallel_extraction` instead hardcode
`localhost:5432 / postgres / postgres / app` — no env needed, but no config either.

---

## Examples: run order and what each shows

Suggested path: `full` → `filtered` → `pipeline` → `dataframe` → `parallel` → `distributed` → `bench`.

### 1. `full_extraction` — the basic contract: DB → Arrow

```bash
cargo run --example full_extraction
```

- **Inside:** `PostgresExtractor::connect` → `extract_full_table("public.orders", columns)` → one Arrow `RecordBatch` → prints schema + row/column counts.
- **Showcases:** whole-table extraction, schema discovery/type mapping, row→Arrow conversion. No filters, no DataFusion, no checkpointing — the minimal end-to-end data contract.

### 2. `filtered_extraction` — caller-provided predicates + pushdown preview

```bash
cargo run --example filtered_extraction
```

- **Inside:** loads `examples/configs/extract.example.json` → `explain_filters()` prints per-filter push/keep decisions → `connector.extract().standalone().collect()` → `.run()` for the checkpointed operational run.
- **Showcases:** filters as orchestrator input (an OR-group plus time/user ranges), previewing *where* each predicate executes (source vs Arrow) before extracting, and `collect()` (data, no side effects) vs `run()` (split checkpointing).

### 3. `pipeline_extraction` — the one builder every path uses

```bash
cargo run --example pipeline_extraction -- [config.json]   # default: full_extract.example.json
```

- **Inside:** `PostgresConnector::from_config_file` → `extract().standalone().collect()` → `extract().standalone().run()` → `extract().distributed().in_process().run()`.
- **Showcases:** the fluent entry point the CLI (`rel run` / `rel distribute`) also funnels through. If you only read one example to learn the API surface, read this one.

### 4. `dataframe_extraction` — DataFusion-native querying over the source

```bash
cargo run --example dataframe_extraction -- [config.json]  # default: extract.example.json
```

- **Inside:** `ExtractContext::from_config` → builder chain (`filter` → `select` → `with_column` → `limit` → `collect`) → then the SQL entry point (`SELECT COUNT(*) AS n FROM public.orders`) against the same registered table.
- **Showcases:** filtering/projection/derived columns as DataFusion's job (not reimplemented), pushdown through the builder API, and that builder and SQL hit the same source (including the `COUNT(*)` empty-projection path).

### 5. `parallel_extraction` — keyset partitioning mechanics

```bash
cargo run --example parallel_extraction
```

- **Inside:** `compute_keyset_partitions` (4 ranges over `order_id`) → one `tokio::spawn` per partition, each with its own extractor/connection → `extract_keyset_partition(lo, hi)` → combined row counts.
- **Showcases:** non-overlapping `WHERE order_id >= lo AND order_id < hi` ranges (keyset, never `LIMIT/OFFSET`), per-partition connections, and the no-gaps/no-duplicates argument. Single-node parallelism; distribution is the next example.

### 6. `distributed_extraction` — the same job spec on Ballista

```bash
cargo run --example distributed_extraction -- <config.json> [workers]  # default workers: 2
```

- **Inside:** `Pipeline::from_config` → in-process Ballista scheduler + executors → applies the config's filters through `filter_exprs_with_schema` (the shared choke point) + column projection → collects → writes `<sink.path>.parquet`.
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
- `parallel_scan` / `execution.batch_size` / `distributed.workers` — splitting, FETCH size, executor count.

`full_extract.example.json` is the same shape with empty `filters` (full load).

---

## CLI essentials (same paths as the examples)

```bash
# Full or filtered single-node run (+ optional CLI filters, ANDed with config filters)
cargo run --bin rust-ballista-extraction-layer -- run --config my-job.json [--filter "status=PAID"]

# What will push to the source under a policy
cargo run --bin rust-ballista-extraction-layer -- plan --config my-job.json --policy cost_based

# Distributed: in-process by default, or point at a live scheduler
cargo run --bin rust-ballista-extraction-layer -- distribute --config my-job.json --workers 4

# Split checkpoints (execution progress only, never watermarks)
cargo run --bin rust-ballista-extraction-layer -- checkpoint show --config my-job.json
cargo run --bin rust-ballista-extraction-layer -- checkpoint reset --config my-job.json
```

---

## Tests and benchmarks (pointers)

```bash
cargo test --lib                                  # unit, no DB
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/app cargo test --test pg_
cd benchmark && ./run.sh --skip-scale --repeat 1  # smoke benchmark
```

Details live in `docs/testing-plan.md` and `benchmark/README.md`.

---

## Common issues

| Problem | Solution |
|---------|----------|
| `password_env` / `ORDERS_PG_PASSWORD` not set | `export ORDERS_PG_PASSWORD=postgres` |
| `batch_size` validation error | Must be ≥ 1 in config |
| `SELECT COUNT(*)`-style queries fail | Fixed — empty-projection scans select a constant; update past this doc |
| Linker `__eh_frame` warning on macOS | Toolchain noise, harmless |
