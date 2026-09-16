# Quickstart Guide

Complete setup and usage guide for the Rust Extract Layer.

---

## Prerequisites

- **Rust 1.98+** (toolchain managed via `rust-toolchain.toml`)
- **Docker or Podman** (for benchmark/integration tests)
- **PostgreSQL 17+** (for live testing)

---

## Quick Start (TL;DR)

```bash
# 1. Start a Postgres instance (Docker)
docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:17

# 2. Set password env var
export ORDERS_PG_PASSWORD=postgres

# 3. Run single-node extraction
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/extract.example.json
```

---

## Detailed Setup

### 1. PostgreSQL

**Option A: Docker (recommended for development)**

```bash
# Start with benchmark seed data (500 users, 20k orders)
cd benchmark
docker compose -f PostgresDB/compose.yaml up -d
# Postgres available at localhost:5432, password: postgres
```

**Option B: Local installation**

```bash
# Create database and user
psql -c "CREATE DATABASE app;"
psql -c "CREATE ROLE rel_extract LOGIN PASSWORD 'your_password';"
psql -c "GRANT CONNECT ON DATABASE app TO rel_extract;"
psql -d app -c "GRANT USAGE ON SCHEMA public TO rel_extract;"
psql -d app -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO rel_extract;"
psql -d app -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rel_extract;"
psql -c "GRANT pg_read_all_stats TO rel_extract;"  # for exact high watermark
```

### 2. Build the Project

```bash
# Full release build (first time takes ~5-10 min; cargo-chef caches deps)
cargo build --release --all-targets

# Or just the binary
cargo build --release --bin rust-ballista-extraction-layer
```

### 3. Configure Extraction

Copy and edit the example config:

```bash
cp examples/configs/extract.example.json my-job.json
# Edit host, password_env, table, incremental.column, etc.
```

**Key config fields:**

| Field | Purpose |
|-------|---------|
| `source.host` / `port` | Postgres connection |
| `source.password_env` | Env var name holding password (never inline passwords) |
| `incremental.column` | Watermark column (must be `timestamptz`) |
| `incremental.safety_lag_secs` | Seconds to subtract from `now()` for safety |
| `incremental.max_window_secs` | Max window per incremental run |
| `parallel_scan.strategy` | `keyset` (default) or `ctid` |
| `parallel_scan.partitions` | Number of parallel partitions |
| `pushdown.policy` | `always` / `never` / `cost_based` / `strict` / `hinted` |

---

## Running Extractions

### Single-Node (in-process Ballista)

```bash
# Full table load
cargo run --bin rust-ballista-extraction-layer -- run \
  --config my-job.json

# Incremental window
cargo run --bin rust-ballista-extraction-layer -- run \
  --config my-job.json \
  --filter "status = 'PAID'" \
  --columns "order_id,amount,status"
```

### Distributed (Scheduler + Workers)

**Terminal 1 - Scheduler:**
```bash
cargo run --bin rust-ballista-extraction-layer -- scheduler \
  --scheduler-url http://localhost:50050 --bind-host 0.0.0.0
```

**Terminal 2..N - Workers:**
```bash
# Each worker on a separate machine or container
cargo run --bin rust-ballista-extraction-layer -- worker \
  --scheduler-url http://<scheduler-host>:50050 \
  --bind-host 0.0.0.0 --external-host <worker-hostname>
```

**Terminal N+1 - Client:**
```bash
cargo run --bin rust-ballista-extraction-layer -- distribute \
  --config my-job.json --workers 4 \
  --scheduler-url http://localhost:50050
```

### Pushdown Plan Explanation

```bash
cargo run --bin rust-ballista-extraction-layer -- plan \
  --config my-job.json \
  --policy cost_based \
  --filter "status = 'PAID'"
```

Output shows per-operator push/keep decisions with cost estimates.

---

## Backfill (Historical Load)

```bash
cargo run --bin rust-ballista-extraction-layer -- backfill \
  --config my-job.json \
  --namespace historical_load \
  --from "2023-01-01T00:00:00Z" \
  --to "2024-01-01T00:00:00Z"
```

- Walks `[from, to]` in `max_window_secs`-bounded chunks
- Each chunk: acquire lease → extract → commit watermark
- Crash-safe: resumes from last committed chunk

---

## Benchmarks

```bash
cd benchmark

# Full run (builds images, scales to 5M rows, 3 repeats)
./run.sh --repeat 3

# Smoke test on 20k seed (seconds)
./run.sh --skip-scale --repeat 1

# Skip builds, use local images
./run.sh --no-build --repeat 3

# Cleanup everything
./run.sh --clean
```

**Key flags:**

| Flag | Purpose |
|------|---------|
| `--repeat N` | Best-of-N runs (smooths JVM warmup) |
| `--mode standalone\|distributed\|both` | Which Rust deployments to run |
| `--batch-size N` | Spark JDBC fetchsize (Rust uses DataFusion streaming) |
| `--spark-cores N` | Cap Spark parallelism (e.g., `--spark-cores 2` on small VMs) |
| `--skip-correctness` | Skip DuckDB row-by-row check |

Outputs in `benchmark/results/`:
- `summary.md` — comparison table
- `*.json` — per-run stats (CPU, RSS, elapsed, rows)
- `correctness.json` — DuckDB row-by-row verification

---

## Testing

```bash
# Unit tests only (no DB, milliseconds)
cargo test --lib

# Integration tests (needs live Postgres via DATABASE_URL)
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/app cargo test --test pg_

# E2E tests (needs live Postgres + running scheduler/workers)
E2E_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/app \
E2E_SCHEDULER_URL=http://localhost:50050 \
cargo test --test e2e

# All tests
cargo test
```

**Integration test suites (skip without DB):**

| Test file | What it covers |
|-----------|----------------|
| `pg_numeric.rs` | Exact decimal decoding (123.45 → 12345) |
| `pg_paths.rs` | Full ≡ incremental ≡ cursor path equality |
| `pg_pushdown.rs` | `always` vs `never` differential |
| `pg_distributed.rs` | Standalone collect over partitions |
| `pg_edge.rs` | Dup timestamps, empty windows, batch boundaries |
| `pg_catalog.rs` | Statistics/indexes/enums from live catalog |
| `e2e.rs` | Full/incremental/selective/distributed over remote cluster |

---

## Common Issues

| Problem | Solution |
|---------|----------|
| `password_env` not set | `export ORDERS_PG_PASSWORD=your_password` |
| `max_window_secs` validation error | Must be ≥ 1 in config |
| Backfill infinite loop | Config validation now rejects `max_window_secs=0` |
| Spark OOM on small VM | Use `SPARK_DRIVER_MEM=1g ./run.sh --spark-cores 2` |
| `pg_read_all_stats` missing | `GRANT pg_read_all_stats TO rel_extract;` or set `safety_lag_secs` higher |
| Workers can't connect to scheduler | Ensure `--external-host` is reachable from workers |
| Linker OOM during build | Docker Desktop → Settings → Resources → Memory: 4GB+ |

---

## Architecture Overview

```
PostgreSQL ──► Cursor-based portal scans (sqlx)
       │
       ▼
   Watermark resolution (pg_stat_activity.xact_start)
       │
       ▼
   Pushdown decision (Exact/Inexact/Keep via cost model)
       │
       ▼
   DataFusion LogicalPlan → PhysicalPlan (PostgresExecutionPlan)
       │
       ▼
   Streaming RecordBatch (sqlx::query().fetch(), RowBatchBuilder)
       │
       ▼
   Arrow IPC (standalone) or Ballista shuffle (distributed)
       │
       ▼
   Sink (Parquet, DataFusion writers, or orchestrator)
       │
       ▼
   Checkpoint commit (atomic JSON rename)
```

---

## Next Steps

- Read the [Roadmap](docs/roadmap.md) for phased delivery status
- Review [Pushdown](docs/pushdown.md) for fidelity/cost model details
- See [PostgreSQL Connector](docs/connectors/postgres.md) for type mapping, partitioning, watermarks
- Check [Testing Plan](docs/testing-plan.md) for test strategy