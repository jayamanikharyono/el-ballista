# Benchmark: full initial load, Rust vs PySpark

Same Postgres, same query, both containerized, profiled the same way.

## Versions

| Component | Version |
|---|---|
| Rust | 1.98.1 |
| DataFusion | 54.1.0 |
| Ballista | 54.1.0 |
| PySpark | 3.5.4 |
| PostgreSQL | 17.11 |

## Workload

Two scenarios, same filter/projection strings on both engines:

| Scenario | SQL shape | What it exercises |
|---|---|---|
| `full` | `SELECT *` (all 12 columns) → Parquet | Pure extraction throughput; pushdown can't confound it |
| `selective` | `SELECT order_id,amount,status ... WHERE status = 'REFUNDED'` (~3% of rows, indexed) → Parquet | Filter + projection pushdown end to end on both sides |

Two Rust deployments (`--mode standalone|distributed|both`, default `both`):

| Mode | How the scan runs |
|---|---|
| `standalone` | In-process scheduler + executor, one container. Same code path minus network. |
| `distributed` | Real `bench-scheduler` + `bench-worker-N` containers (our scheduler/worker with Postgres codecs — stock Ballista binaries cannot decode our plans), remote client container. Workers advertise container names so the scheduler dials them back; every process resolves the source password independently. |

| | Rust | Spark |
|---|---|---|
| Engine | Standalone Ballista or scheduler+workers (see below) | PySpark 3.5.4, `local[*]` |
| Read fan-out | `--rust-partitions` keyset partitions, derived by default as ceil(table_rows / `--batch-size`) (`parallel_scan.partitions`; `workers` only divides the pool budget and fills in when `partitions <= 1`) | `--spark-partitions` JDBC partitions on `order_id`, same derivation by default |
| Fetch | Streaming portal, `--batch-size` rows per batch when set, else code default 8192 | JDBC `fetchsize` = `--batch-size` when set, else omitted (Spark default 0 = driver default, which buffers each partition fully) |
| Sink | `parquet::arrow::ArrowWriter`, Snappy, single file | `df.write.parquet`, Snappy |
| Timed | Schema discovery + scan + collect + encode (`scan_ms` + `write_ms` split in JSON) | Bounds query excluded; read + write + read-back count |

Batch size (`--batch-size`) is optional. **Auto run** (flag absent): each tool uses its
own default — Rust 8192 rows/batch, Spark no `fetchsize` (driver default). **Manual
run** (`--batch-size 64000`): both engines stream that many rows per batch. Either
way, partition counts derive from the 64000 reference as ceil(table_rows / 64000) —
~95 partitions at 6M rows — so an auto run and a manual run differ only in per-batch
streaming, never in fan-out. Override counts independently (`--rust-partitions`,
`--spark-partitions`) to test sensitivity, but keep them equal for the headline
number.

CPU/MEM for distributed runs aggregates the whole cluster (scheduler + workers + client
summed per tick), comparable to single-container runs. Everything runs sequentially —
nothing contends for CPU. Best-of-`REPEAT` smooths JVM warmup and page-cache effects.

## Layout

```
benchmark/
├── PostgresDB/        # postgres:17 + seed (500 users, 20k orders) — the shared source
├── rust/
│   ├── Dockerfile     # multi-stage release build of bench_full_load
│   └── bench-config.json  # job spec: host bench-pg, keyset on order_id
├── spark/
│   ├── Dockerfile     # apache/spark:3.5.4 + PostgreSQL JDBC 42.7.13 baked in
│   └── load.py        # the equivalent workload, JSON summary to stdout
├── scale.sql          # grow toward SCALE_ROWS (~5M default); run.sh computes the
#                       # factor from the live count and skips when already there
├── run.sh             # orchestrator: build → postgres → scale → run → profile → gate → report
├── results/           # *.json, *_cluster.csv, *_c_*.csv, summary.md (gitignored)
└── output/            # parquet outputs (gitignored)
```

## Quickstart

Needs `docker` or `podman` CLI. ~5M-row default takes a few minutes plus a one-time Rust
release build (Ballista + DataFusion — go make coffee).

```bash
# Full benchmark, best of 3:
benchmark/run.sh --repeat 3

# Smoke test on the 20k seed (seconds):
benchmark/run.sh --skip-scale --repeat 1

# Skip builds entirely (images already present):
benchmark/run.sh --no-build --repeat 3

# Cleanup everything (containers, network, volume, results):
benchmark/run.sh --clean
```

## Prebuilt images (skip the build)

The Rust release build is the slow part (Ballista + DataFusion, ~400 crates). Two ways
around it:

1. **Rebuilds are cheap already**: `cargo-chef` layers split dependency compilation (cached
   unless `Cargo.toml`/`Cargo.lock` change) from our crates, so editing sources and
   re-running costs minutes, not a full rebuild.
2. **Pull instead of build**: `.github/workflows/bench-images.yml` builds multi-arch
   images on demand (`Actions → bench-images → Run workflow`, or push a `bench-*` tag)
   and pushes to GHCR. Then:
   ```bash
   BENCH_RUST_IMAGE=ghcr.io/<owner>/<repo>/rel-bench-rust:latest \
   BENCH_SPARK_IMAGE=ghcr.io/<owner>/<repo>/rel-bench-spark:latest \
   benchmark/run.sh --pull --repeat 3
   ```
   (The workflow needs no configuration — image names derive from the repository. First
   enable GitHub Packages for the repo; `GITHUB_TOKEN` already has push rights via the
   workflow's `packages: write` permission.)

`PG_PORT` defaults to 5433 so it never clashes with a dev DB on 5432. `BENCH_PG_PASSWORD`
defaults to `postgres` (matches the seed compose). `BENCH_CONCURRENT_TASKS`, when set,
pins each worker's task slots (default: the worker's visible CPU count) — the closest
thing Ballista has to CPUs-per-executor; there is no CPU pinning, tasks share one Tokio
runtime over all visible cores.

## What you get

`results/summary.md` (one row per engine × scenario × mode) plus per-run JSONs with peaks
baked in (`avg_cpu_pct`, `peak_cpu_pct`, `peak_rss_mib`), raw 100ms CPU/MEM series
(`*_cluster.csv` totals plus `*_c_<container>.csv` per container), and `correctness.json`.

### Monitoring: totals + per-container breakdown

Single-container runs (standalone, Spark) record one series. Distributed runs record one
series **per container** (`*_c_bench-scheduler.csv`, `*_c_bench-worker-N.csv`,
`*_c_bench-rust-dist.csv`) plus a summed cluster series (`*_cluster.csv`), so you see
both the whole-cluster cost and who spent it — e.g. whether both workers actually shared
the scan or one idled. The JSON carries both levels:

```json
{
  "avg_cpu_pct": 124.2, "peak_cpu_pct": 137.1, "peak_rss_mib": 1390,
  "containers": {
    "bench-scheduler":   { "avg_cpu_pct": 3.1,  "peak_cpu_pct": 5.0,   "peak_rss_mib": 62 },
    "bench-worker-1":    { "avg_cpu_pct": 60.5, "peak_cpu_pct": 70.2,  "peak_rss_mib": 664 },
    "bench-worker-2":    { "avg_cpu_pct": 60.6, "peak_cpu_pct": 69.8,  "peak_rss_mib": 664 },
    "bench-rust-dist":   { "avg_cpu_pct": 0.0,  "peak_cpu_pct": 0.4,   "peak_rss_mib": 12 }
  }
}
```

and the summary prints per-container sub-rows under each distributed row. CPU% sums across
cores *and* containers (200% on 2 cores fully lit is normal); RSS sums the high-water marks
(processes don't share Arrow buffers, so the sum is the true cluster footprint). Series are
100ms samples — spikes inside a tick are still invisible; size conclusions on
averages and peaks, not single samples.

## Correctness check (independent, row-by-row)

After the gate, `run.sh` runs `benchmark/correctness.py` — a DuckDB read of the actual
Parquet files, sharing no code with either engine:

- every file re-counted from disk,
- all engines' outputs row-identical per scenario (order-insensitive set equality),
- selective files satisfy the predicate and carry exactly the projected columns,
- selective ⊆ full per engine,
- `metadata` jsonb compared semantically (key order differs: our side sorts keys,
  pgjdbc preserves storage order — raw string compare would false-fail),
- timestamps compared as instants (UTC-annotated vs naive can't false-fail).

Needs the `duckdb` python package (`pip install duckdb`) or `--skip-correctness`.
Any failure fails the run. The check is order-insensitive by design: parallel partitions
make row order nondeterministic, so file hashes would false-fail on every run.

## Results

Toolchain: see [Versions](#versions). Source database is the PostgreSQL above.
Spark heap is sized as machine RAM minus 2 GB (`SPARK_DRIVER_MEM`).

### 8 GB machine — tool-default batching (auto)

| engine                   | scenario  | rows              | elapsed                        | avg CPU% | max CPU% | max RSS MiB |
|--------------------------|-----------|-------------------|--------------------------------|----------|----------|-------------|
| pyspark-3.5.4            | full      | 6060000           | 19690ms                        | 221.5    | 245.7    | 3767        |
| rust-ballista-standalone | full      | 6060000           | 18944ms                        | 122.9    | 168.4    | 127         |
| rust-ballista-remote     | full      | 6060000           | 15508ms                        | 139.6    | 150.2    | 294         |
|                          |           | `bench-rust-dist` | avg 6.9% / peak 19.5% / 86MiB  |          |          |             |
|                          |           | `bench-scheduler` | avg 0.3% / peak 0.3% / 8MiB    |          |          |             |
|                          |           | `bench-worker-1`  | avg 33.6% / peak 37.0% / 66MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 33.1% / peak 36.5% / 60MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 32.6% / peak 35.8% / 65MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 33.0% / peak 36.1% / 88MiB |          |          |             |
| pyspark-3.5.4            | selective | 194223            | 4374ms                         | 192.0    | 212.1    | 734         |
| rust-ballista-standalone | selective | 194223            | 6174ms                         | 5.6      | 7.9      | 16          |
| rust-ballista-remote     | selective | 194223            | 602ms                          | 118.1    | 124.2    | 219         |
|                          |           | `bench-rust-dist` | avg 1.1% / peak 4.8% / 7MiB    |          |          |             |
|                          |           | `bench-scheduler` | avg 0.3% / peak 0.4% / 9MiB    |          |          |             |
|                          |           | `bench-worker-1`  | avg 29.7% / peak 30.3% / 50MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 29.4% / peak 30.0% / 51MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 28.7% / peak 29.3% / 51MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 28.9% / peak 29.5% / 52MiB |          |          |             |

### 8 GB machine — batch size 64000 (manual)

| engine                   | scenario  | rows              | elapsed                         | avg CPU% | max CPU% | max RSS MiB |
|--------------------------|-----------|-------------------|---------------------------------|----------|----------|-------------|
| pyspark-3.5.4            | full      | 6060000           | 18690ms                         | 207.2    | 236.8    | 2884        |
| rust-ballista-standalone | full      | 6060000           | 21269ms                         | 122.9    | 169.1    | 303         |
| rust-ballista-remote     | full      | 6060000           | 19317ms                         | 134.5    | 143.5    | 584         |
|                          |           | `bench-rust-dist` | avg 7.1% / peak 18.3% / 87MiB   |          |          |             |
|                          |           | `bench-scheduler` | avg 0.3% / peak 0.3% / 8MiB     |          |          |             |
|                          |           | `bench-worker-1`  | avg 33.4% / peak 36.5% / 130MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 31.5% / peak 34.3% / 146MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 30.4% / peak 33.2% / 130MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 31.8% / peak 34.8% / 141MiB |          |          |             |
| pyspark-3.5.4            | selective | 194223            | 4337ms                          | 187.3    | 212.5    | 767         |
| rust-ballista-standalone | selective | 194223            | 6280ms                          | 16.1     | 30.5     | 16          |
| rust-ballista-remote     | selective | 194223            | 613ms                           | 114.4    | 119.5    | 491         |
|                          |           | `bench-rust-dist` | avg 1.1% / peak 4.4% / 6MiB     |          |          |             |
|                          |           | `bench-scheduler` | avg 0.3% / peak 0.4% / 9MiB     |          |          |             |
|                          |           | `bench-worker-1`  | avg 29.7% / peak 30.2% / 113MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 27.9% / peak 28.4% / 117MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 27.0% / peak 27.4% / 111MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 28.3% / peak 28.7% / 136MiB |          |          |             |


### 4 GB machine — tool-default batching (auto)

| engine                   | scenario  | rows              | elapsed                        | avg CPU% | max CPU% | max RSS MiB |
|--------------------------|-----------|-------------------|--------------------------------|----------|----------|-------------|
| pyspark-3.5.4            | full      | 6060000           | 18364ms                        | 202.3    | 228.2    | 1933        |
| rust-ballista-standalone | full      | 6060000           | 18841ms                        | 122.0    | 167.1    | 129         |
| rust-ballista-remote     | full      | 6060000           | 16104ms                        | 220.3    | 271.8    | 367         |
|                          |           | `bench-rust-dist` | avg 8.4% / peak 21.1% / 140MiB |          |          |             |
|                          |           | `bench-scheduler` | avg 0.8% / peak 2.9% / 56MiB   |          |          |             |
|                          |           | `bench-worker-1`  | avg 54.6% / peak 71.6% / 59MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 48.6% / peak 63.5% / 52MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 54.0% / peak 68.1% / 55MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 54.0% / peak 69.5% / 59MiB |          |          |             |
| pyspark-3.5.4            | selective | 194223            | 4158ms                         | 192.0    | 217.2    | 593         |
| rust-ballista-standalone | selective | 194223            | 6158ms                         | 14.1     | 23.7     | 17          |
| rust-ballista-remote     | selective | 194223            | 633ms                          | 119.8    | 124.5    | 265         |
|                          |           | `bench-rust-dist` | avg 1.1% / peak 3.3% / 5MiB    |          |          |             |
|                          |           | `bench-scheduler` | avg 0.3% / peak 0.4% / 43MiB   |          |          |             |
|                          |           | `bench-worker-1`  | avg 30.5% / peak 31.1% / 56MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 29.1% / peak 29.7% / 52MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 28.8% / peak 29.4% / 51MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 30.0% / peak 30.6% / 58MiB |          |          |             |

### 4 GB machine — batch size 64000 (manual)

| engine                   | scenario  | rows              | elapsed                         | avg CPU% | max CPU% | max RSS MiB |
|--------------------------|-----------|-------------------|---------------------------------|----------|----------|-------------|
| pyspark-3.5.4            | full      | 6060000           | 17268ms                         | 208.3    | 232.0    | 2209        |
| rust-ballista-remote     | full      | 6060000           | 16677ms                         | 129.3    | 139.3    | 567         |
| rust-ballista-standalone | full      | 6060000           | 20098ms                         | 119.4    | 166.8    | 214         |
|                          |           | `bench-rust-dist` | avg 6.9% / peak 18.4% / 83MiB   |          |          |             |
|                          |           | `bench-scheduler` | avg 0.3% / peak 0.3% / 11MiB    |          |          |             |
|                          |           | `bench-worker-1`  | avg 30.5% / peak 33.5% / 145MiB |          |          |             |
|                          |           | `bench-worker-2`  | avg 30.4% / peak 33.8% / 157MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 30.4% / peak 33.6% / 149MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 30.8% / peak 34.0% / 139MiB |          |          |             |
| pyspark-3.5.4            | selective | 194223            | 4435ms                          | 186.6    | 214.6    | 594         |
| rust-ballista-standalone | selective | 194223            | 6216ms                          | 13.5     | 22.4     | 16          |
| rust-ballista-remote     | selective | 194223            | 704ms                           | 114.8    | 118.8    | 472         |
|                          |           | `bench-rust-dist` | avg 4.8% / peak 8.9% / 9MiB     |          |          |             |
|                          |           | `bench-scheduler` | avg 0.4% / peak 0.4% / 30MiB    |          |          |             |
|                          |           | `bench-worker-1`  | avg 27.3% / peak 27.3% / 85MiB  |          |          |             |
|                          |           | `bench-worker-2`  | avg 27.6% / peak 27.6% / 134MiB |          |          |             |
|                          |           | `bench-worker-3`  | avg 27.3% / peak 27.4% / 100MiB |          |          |             |
|                          |           | `bench-worker-4`  | avg 27.4% / peak 27.5% / 117MiB |          |          |             |


### Reading the numbers (read before quoting them)

Do not compare `output_bytes` across engines for equality — only for magnitude. Same rows,
different bytes, always:

- **Different writers make different files.** Spark uses parquet-mr, Rust uses parquet-rs:
  different page/row-group chunking, dictionary-fallback thresholds, statistics blocks, and
  footer metadata. Expect ~single-digit % divergence on large files, more on small ones
  where fixed overheads (footers, dictionaries for a 1-value column like selective
  `status`) dominate. Your selective numbers show this exactly: ~30% apart at 194k rows,
  ~3% apart at 6M rows — the gap amortizes with size.
- **Same writer, different run, slightly different bytes** (your remote vs standalone
  selective: 1,239,491 vs 1,241,870, ~0.2%). Parallel scans deliver batches in
  nondeterministic groupings, so row-group boundaries land in different places. Content
  is identical — which is why the gate compares row *sets*, never bytes.
- The one byte comparison that matters: an engine disagreeing with *itself* across runs
  beyond low-single-digit % on the same data would indicate nondeterministic encoding,
  worth investigating.

- 20k seeded rows complete in seconds on either engine (noise dominates) — that's why
  scaling exists (`SCALE_ROWS`, default 5M; `--skip-scale` stays on the raw seed).
  Always report the row count next to the time.
- JVM boot is excluded from Spark's time; process init is negligible for Rust. What's timed
  is scan + write on both sides.
- Spark reads back its own output to count rows (inside its timed section); Rust counts
  collected batches (free). The read-back is honest work but not identical work — the JSON
  splits don't separate it. Compare `elapsed_ms` as system-vs-system, not kernel-vs-kernel.
- Both containers are unconstrained (no `--memory`/`--cpus` limits) on the same host,
  run sequentially. Numbers are host-specific; the *ratio* is the portable part.
- Memory shape: both sides stream to disk, so memory stays O(batch) at any row count
  on the Rust side (batches flow straight into the writer) and bounded on Spark's.
- Apple Silicon works unmodified: every image is multi-arch (amd64 + arm64).

## Troubleshooting

- `Cannot connect to Podman ... dial tcp 127.0.0.1:53801: connect: connection refused`:
  the CLI is pointed at a machine SSH endpoint that isn't up, while the engine may be
  reachable on the unix socket. Either start the machine (`podman machine start`) or
  bypass it: `export CONTAINER_HOST=unix:///var/run/docker.sock`. (`run.sh` preflights
  the connection and prints this itself.)
- `Address already in use` on 5433: `PG_PORT=5434 benchmark/run.sh ...`.
- Slow first Rust build: expected (cold cargo registry + release Ballista). Later builds reuse
  the cached dependency layers via cargo-chef — only our crates recompile.
- Host sizing: the benchmark host needs headroom for Postgres + one engine at a time plus
  build layers. Rule of thumb: 4GB+ RAM. On tiny hosts, lower `SPARK_DRIVER_MEM=1g` (JVM
  OOM shows as `Py4JNetworkError: Answer from Java side is empty` — the JVM died, Python
  survived to report it) and/or lower `SCALE_ROWS`. The Rust side streams with O(batch)
  memory and is far less sensitive to host RAM.
- Matching fan-out to CPUs is optional: `--workers 4 --spark-partitions 4` on a 4-CPU host
  (keep both at 2 for the headline apples-to-apples number; DataFusion/Spark compute threads
  already scale with core count either way).
- `collect2: ld terminated with signal 9` during the Rust build: the final link OOM'd (GNU ld
  holding hundreds of rlibs). The Dockerfile already switches to `lld` for this; if you still
  see it, give the builder more RAM (Docker Desktop → Settings → Resources) — 4GB+ recommended.
- `rustc ... (signal: 9, SIGKILL)` mid-build: the *compiler* OOM'd on a big crate (release
  codegen parallelizes per crate). The Dockerfile already pins
  `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1` for this; same remedy — more builder RAM.
- Before blaming the build, check what else is eating the host: `docker ps` /
  `podman ps` — a running Postgres plus scheduler plus workers can leave under 1GB free,
  and then even serialized `rustc` dies. Stop everything non-essential before building,
  or build where RAM is plentiful and `--pull` the result. The GHCR workflow
  (`.github/workflows/bench-images.yml`) exists precisely so the small host never has
  to link or codegen this tree at all.
- Spark OOM in container: raise `--shm-size` in `run.sh` (`run_spark`), or give Docker Desktop
  more memory.
- `row mismatch` gate: check the engine JSONs — usually a failed write or an
  incompletely-scaled table (re-run without `--skip-scale`).
- Output files may be root-owned (both images run as root for bind-mount simplicity);
  `chown` them if your umask complains.
- Two `rel worker`s on one host collide on ports 50051/50052 (hardcoded) — run co-located
  workers in containers (separate net namespaces, no clash) or give each its own host.
  This only affects manual multi-worker setups; the benchmark always uses containers.
- A root `.dockerignore` keeps `target/` (tens of GB) out of the build context — do not
  delete it or `docker build` will try to send the entire workspace.
