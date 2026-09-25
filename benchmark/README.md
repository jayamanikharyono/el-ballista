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
| Fetch | Binary `COPY … TO STDOUT` by default (`--no-use-copy`: cursor `FETCH`), `execution.batch_size` rows per Arrow batch / `FETCH` window — `run.sh` writes `--batch-size` into the generated job spec when set, else the code default 8192 applies | JDBC `fetchsize` = `--batch-size` when set, else omitted (Spark default 0 = driver default, which buffers each partition fully) |
| Output writer | `parquet::arrow::ArrowWriter` in the bench binary, Snappy, single file | `df.write.parquet`, Snappy |
| Timed | Schema discovery + scan + collect + encode (`scan_ms` + `write_ms` split in JSON) | Bounds query excluded; read + write + read-back count |

Batch size (`--batch-size`) is optional and applies to **both** engines: Spark's JDBC `fetchsize` and the Rust job spec's `execution.batch_size` (rows per Arrow batch and per cursor `FETCH`; `run.sh` writes it into the generated `bench-config.json`). **Auto run** (flag absent): Spark uses the driver default (buffers each partition fully); Rust uses its code default of 8192 rows per batch. **Manual run** (`--batch-size 64000`): both engines use that many rows per batch. Partition counts derive from the 64000 reference as ceil(table_rows / 64000) — 157 partitions at 10M rows — so an auto run and a manual run differ only in per-batch size, never in fan-out. Override counts independently (`--rust-partitions`, `--spark-partitions`) to test sensitivity, but keep them equal for the headline number.

CPU/MEM for distributed runs aggregates the whole cluster (scheduler + workers + client
summed per tick), comparable to single-container runs. bench-pg is profiled on the same
ticks into a separate series so every run shows source-database cost without polluting
engine totals. Everything runs sequentially —
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
├── scale.sql          # grow toward SCALE_ROWS (~10M default); run.sh computes the
#                       # factor from the live count and skips when already there
├── run.sh             # orchestrator: build → postgres → scale → run → profile → gate → report
├── results/           # *.json, *_cluster.csv, *_c_*.csv, summary.md (gitignored)
└── output/            # parquet outputs (gitignored)
```

## Quickstart

Needs the `docker` CLI. ~10M-row default takes a few minutes plus a one-time Rust
release build (Ballista + DataFusion — go make coffee).

```bash
# Full benchmark, best of 3:
benchmark/run.sh --repeat 3

# Smoke test on the 20k seed (seconds):
benchmark/run.sh --skip-scale --repeat 1

# Tiny host (~1GB RAM) smoke test: shrink everything to fit, stay on the 20k seed.
# Adjust --cpuset-cpus to the cores you actually have (the default 0-3 fails fast
# otherwise). bench-pg's hardcoded 4-5 pin also needs >= 6 cores: on a smaller host edit
# PG_CPUSET in run.sh for the smoke run (the script refuses to start otherwise).
# Explicit small slices bypass the floors — your risk. Smoke ONLY: different data
# scale means different spec, never quote these numbers.
benchmark/run.sh --skip-scale --repeat 1 --mode standalone \
  --cpuset-cpus 0-1 --memory 1g --workers 1 \
  --scheduler-memory 128m --client-memory 128m --worker-memory 256m

# Skip builds entirely (images already present):
benchmark/run.sh --no-build --repeat 3

# Apples-to-apples on 4 cores (default: every container pinned to cores 0-3 and
# running its runtime unrestricted — Spark local[*], DataFusion defaults):
benchmark/run.sh --repeat 3

# Cleanup everything (containers, network, volume, results):
benchmark/run.sh --clean
```

Every container the script starts defaults to a hard pin on cores 0-3
(`--cpuset-cpus 0-3`, overridable; `run.sh` checks the Docker host's core count against
every pin before starting anything, so a host with fewer cores than the pins name — 6 in
the default layout — fails fast instead of benchmarking the wrong budget) and `--memory 4g`. Memory is a per-deployment
budget: Spark and the standalone Rust client each get the full 4g, while
the distributed deployment shares it — 512m scheduler + 512m client + the rest split
evenly across workers (768m each at `--workers 4`). Postgres is fixed outside this
budget: `run.sh` pins bench-pg to cores 4-5 + 2g RAM (`PG_CPUSET` / `PG_MEM`, hardcoded,
clear of the engine pin, and checked with `docker inspect` on every run) so the shared
fixture never changes shape. (The compose files — `benchmark/PostgresDB/compose.yaml` and
`tests/docker/compose.yaml` — are not the benchmark fixture: they use a `cpus: 2` quota +
2g, no pin.) Tune with `--cpus N` (replaces
the pin with a quota), `--cpuset-cpus RANGE`, `--memory SIZE`, `--scheduler-memory`
/ `--client-memory` / `--worker-memory` (or `CPUS` / `MEMORY` / `CPUSET_CPUS` /
`SCHEDULER_MEMORY` / `CLIENT_MEMORY` / `WORKER_MEMORY` env; empty lifts the default).
Prefer the pin over the quota: the JVM honors a CFS quota, but Rust's
`available_parallelism` (which sizes DataFusion partitions and Ballista task slots)
only sees a cpuset pin — under plain `--cpus` the Rust side still sizes its pools
from all host CPUs.

## Prebuilt images (skip the build)

The Rust release build is the slow part (Ballista + DataFusion, ~400 crates). Two ways
around it:

1. **Rebuilds are cheap already**: `cargo-chef` layers split dependency compilation (cached
   unless `Cargo.toml`/`Cargo.lock` change) from our crates, so editing sources and
   re-running costs minutes, not a full rebuild.

`PG_PORT` defaults to 5433 so it never clashes with a dev DB on 5432. `BENCH_PG_PASSWORD`
defaults to `postgres` (matches the seed compose). `BENCH_CONCURRENT_TASKS` pins each worker's task slots (default: all visible CPUs,
i.e. the pinned budget) — the closest thing Ballista has to CPUs-per-executor; there
is no CPU pinning, tasks share one Tokio runtime over all visible cores. Leave it
unset for headline runs; it is a diagnostic knob, not a spec knob.

## What you get

`results/summary.md` (one row per engine × scenario × mode) plus per-run JSONs with peaks
baked in (`cpu_seconds`, `avg_cpu_pct`, `peak_cpu_pct`, `peak_rss_mib`, `peak_mem_mib`,
`mem_peak_exact_mib`), raw 100ms CPU/MEM series
(`*_cluster.csv` totals plus `*_c_<container>.csv` per container), and `correctness.json`.

### Monitoring: totals + per-container breakdown

Single-container runs (standalone, Spark) record one series. Distributed runs record one
series **per container** (`*_c_bench-scheduler.csv`, `*_c_bench-worker-N.csv`,
`*_c_bench-rust-dist.csv`) plus a summed cluster series (`*_cluster.csv`), so you see
both the whole-cluster cost and who spent it — e.g. whether both workers actually shared
the scan or one idled. The JSON carries both levels:

```json
{
  "elapsed_ms": 9120, "t_start_epoch_ms": 1790000000000, "t_end_epoch_ms": 1790000009120,
  "stats_window": "timed section",
  "cpu_seconds": 11.33, "avg_cpu_pct": 124.2, "peak_cpu_pct": 181.0,
  "peak_rss_mib": 1102, "peak_mem_mib": 1390, "mem_peak_exact_mib": 2950,
  "containers": {
    "bench-scheduler": { "cpu_seconds": 0.28, "avg_cpu_pct": 3.1,  "peak_cpu_pct": 6.0,  "peak_rss_mib": 48,  "peak_mem_mib": 62,  "mem_peak_exact_mib": 70 },
    "bench-worker-1":  { "cpu_seconds": 5.52, "avg_cpu_pct": 60.5, "peak_cpu_pct": 92.3, "peak_rss_mib": 527, "peak_mem_mib": 664, "mem_peak_exact_mib": 1420 },
    "bench-worker-2":  { "cpu_seconds": 5.53, "avg_cpu_pct": 60.6, "peak_cpu_pct": 90.8, "peak_rss_mib": 527, "peak_mem_mib": 664, "mem_peak_exact_mib": 1440 },
    "bench-rust-dist": { "cpu_seconds": 0.00, "avg_cpu_pct": 0.0,  "peak_cpu_pct": 0.4,  "peak_rss_mib": 9,   "peak_mem_mib": 12,  "mem_peak_exact_mib": 20 }
  },
  "pg": { "cpu_seconds": 7.9, "avg_cpu_pct": 86.6, "peak_cpu_pct": 140.2, "peak_rss_mib": 180, "peak_mem_mib": 610 }
}
```
(Illustrative values.) `mem_peak_exact_mib` is absent on cgroup v1 hosts (no `memory.peak`).

and the summary prints per-container sub-rows under each distributed row, plus a
`postgres` sub-row per engine run: bench-pg is sampled on the same ticks into
`<prefix>_pg.csv` (never summed into engine totals) so every run also shows what the
source database cost — the way to tell a pushdown win (low PG CPU) from a fast scan.

**How CPU and memory are measured.** Series are 100 ms Engine-API polls
(`BENCH_SAMPLE_INTERVAL` to change). Every statistic in the JSON and the tables is
restricted to the engine's **timed section** — each engine prints `t_start_epoch_ms` /
`t_end_epoch_ms`, the same window as `elapsed_ms` — so container start-up (JVM boot,
Ballista start, source registration) is excluded; `stats_window` in the JSON says which
window was used (older engine JSON without the bounds falls back to the container lifetime).

- **CPU.** Each interval's CPU% is Δ(container CPU ns) / Δ(daemon read time ns) × 100
  (100% = one full core; sums across cores *and* containers, so 200% on 2 fully lit cores
  is normal). Both counters are nanosecond-precise, so there are no granularity artifacts —
  the old denominator (`system_cpu_usage`, 10 ms jiffies) produced impossible one-tick
  spikes above the pin and needed smoothing and capping; peaks are now raw per-interval
  maxima. `cpu_seconds` is the exact CPU work in the window, read from the
  cumulative counter (independent of sampling), and `avg_cpu_pct` = `cpu_seconds` ÷
  window.
- **Memory.** `peak_rss_mib` is process memory proper: the cgroup's anonymous pages
  (`anon`, cgroup v1 `total_rss`). `peak_mem_mib` is the working set, `usage −
  inactive_file`, i.e. what `docker stats` shows. Neither counts inactive page cache — the
  old figure (raw `usage`) did, so writing a 2.4 GB Parquet inflated it toward the
  container limit. Both are 100 ms samples; distributed totals are the peak of the per-sample
  sum across containers (not a sum of per-container peaks). `mem_peak_exact_mib` is the
  exact cgroup high-water mark (`memory.peak`, cgroup v2), **including** page cache — the
  figure a `--memory` limit / OOM kill applies to, immune to sampling gaps. Engine
  containers read their own at the end of the run; for the long-lived scheduler/workers
  `run.sh` reads it after each client run, so those are lifetime peaks and the distributed
  total is a sum of per-container peaks (an upper bound).
- **Postgres** is sampled on the same ticks into `<prefix>_pg.csv` and reported as a
  `postgres` sub-row, never summed into engine totals.

A tail sample after client exit catches compute that finished on the last tick. Containers
are polled one after another within a tick, so summed samples are not exactly simultaneous
(harmless for `cpu_seconds`/averages; slightly blurs summed peaks). Timed sections are not
identical across engines: Spark's includes a read-back `count()` of its output; Rust's
excludes Ballista start-up and source registration/schema discovery (done before the timer).

`run.sh` also verifies the pin itself
(`docker inspect` on every started container, `pin: <name> -> [0-3]` per run, WARN
otherwise — a stale `bench-pg` reused from an unpinned run would otherwise silently
benchmark a different spec). Sampled memory (`peak_rss_mib`, `peak_mem_mib`) can still
miss sub-100ms spikes; `mem_peak_exact_mib` cannot, so use it for "does it fit in the
limit" questions and the sampled peaks for "how much does the process itself hold". One lifecycle asymmetry to read correctly: the distributed
cluster (scheduler + workers) starts once per mode and serves `full` then `selective`,
while the standalone/Spark clients start fresh per scenario — so selective worker RSS
carries retained memory from the earlier full scan (allocator arenas and pools don't
return RSS; Ballista installs no memory pool by default, so this is residue, not
reservation). That is the honest cost of a warm long-lived cluster, but it means
selective worker RSS is not comparable to the cold-start standalone/Spark numbers.

## Correctness check (independent, row-by-row)

After the gate, `run.sh` runs `benchmark/correctness.py` — a DuckDB read of the actual
Parquet files, sharing no code with either engine:

- every file re-counted from disk and compared against the source's `count(*)` for its
  scenario (full / selective),
- all engines' outputs row-identical per scenario: order-insensitive **multiset**
  equality (`EXCEPT ALL` in both directions plus equal counts), so a duplicated row
  (e.g. overlapping partitions) fails,
- selective files satisfy the predicate and carry exactly the projected columns,
- selective ⊆ full per engine,
- `metadata` jsonb compared semantically, by extracted fields (the two engines' text
  renderings need not be byte-identical — raw string compare could false-fail),
- timestamps compared as instants (UTC-annotated vs naive can't false-fail).

Needs the `duckdb` python package (`pip install duckdb`) or `--skip-correctness`.
Any failure fails the run. The check is order-insensitive by design: parallel partitions
make row order nondeterministic, so file hashes would false-fail on every run.

## Results

Toolchain: see [Versions](#versions). Source database is the PostgreSQL above.
Spark heap follows the formula (`overhead = max(384m, 10% of heap)`) inside whatever
engine budget the run uses — 1664m at 2g, 3712m at 4g, 7447m at 8g; explicit
`SPARK_DRIVER_MEM` always wins, and the effective heap prints in every run's budget
line, so heap is never a hidden variable.

All runs below: 10M rows, best of 3, tool-default batching, pin 0-3, 157/157 fan-out.
**Caveat:** these tables were recorded while `run.sh` started bench-pg under a
`--cpus=4` CFS quota instead of the documented 4-5 pin (the pin line was commented out;
restored since). The source database was therefore not confined to cores 4-5 and could
share cores with the pinned engines, so the numbers may include source/engine
contention. Re-run `benchmark/run.sh --repeat 3` before quoting them. The only variable across the three tables is the engine memory
budget (2g/4g/8g). `postgres` sub-rows show source-database cost during that engine's
run (same ticks, never summed into engine totals).

### Engine budget 2g (workers 256m each, spark heap 1664m)

| engine                   | scenario  | component         | elapsed_ms | avg_cpu_pct | peak_cpu_pct | peak_rss_mib |
|--------------------------|-----------|-------------------|------------|-------------|--------------|--------------|
| rust-ballista-remote     | full      |                   | 19650      | 266.3       | 400.0        | 1531         |
|                          |           | `bench-rust-dist` |            | 32.2        | 95.7         | 512          |
|                          |           | `bench-scheduler` |            | 0.3         | 0.9          | 8            |
|                          |           | `bench-worker-1`  |            | 60.6        | 154.3        | 256          |
|                          |           | `bench-worker-2`  |            | 56.4        | 121.3        | 256          |
|                          |           | `bench-worker-3`  |            | 60.1        | 146.9        | 257          |
|                          |           | `bench-worker-4`  |            | 56.7        | 126.5        | 256          |
|                          |           | `postgres`        |            | 53.2        | 127.8        | 2048         |
| rust-ballista-standalone | full      |                   | 27368      | 174.5       | 221.8        | 1486         |
|                          |           | `postgres`        |            | 39.4        | 68.8         | 2048         |
| pyspark-3.5.4            | full      |                   | 20741      | 250.2       | 392.3        | 2049         |
|                          |           | `postgres`        |            | 60.4        | 106.7        | 2048         |
| rust-ballista-remote     | selective |                   | 1424       | 57.5        | 72.8         | 1045         |
|                          |           | `bench-rust-dist` |            | 6.1         | 33.6         | 10           |
|                          |           | `bench-scheduler` |            | 4.5         | 6.2          | 12           |
|                          |           | `bench-worker-1`  |            | 12.2        | 15.5         | 256          |
|                          |           | `bench-worker-2`  |            | 11.4        | 13.9         | 256          |
|                          |           | `bench-worker-3`  |            | 11.9        | 15.6         | 256          |
|                          |           | `bench-worker-4`  |            | 11.2        | 13.6         | 256          |
|                          |           | `postgres`        |            | 148.6       | 196.1        | 2048         |
| rust-ballista-standalone | selective |                   | 10273      | 12.7        | 24.4         | 22           |
|                          |           | `postgres`        |            | 23.7        | 78.1         | 2048         |
| pyspark-3.5.4            | selective |                   | 5030       | 220.5       | 344.5        | 637          |
|                          |           | `postgres`        |            | 34.6        | 130.3        | 2048         |

### Engine budget 4g (workers 768m each, spark heap 3712m) — engine totals

| engine                   | scenario  | component         | elapsed_ms | avg_cpu_pct | peak_cpu_pct | peak_rss_mib |
|--------------------------|-----------|-------------------|------------|-------------|--------------|--------------|
| rust-ballista-remote     | full      |                   | 19461      | 269.1       | 400.0        | 1646         |
|                          |           | `bench-rust-dist` |            | 32.6        | 97.4         | 512          |
|                          |           | `bench-scheduler` |            | 0.3         | 0.8          | 8            |
|                          |           | `bench-worker-1`  |            | 57.8        | 104.0        | 292          |
|                          |           | `bench-worker-2`  |            | 59.4        | 120.0        | 272          |
|                          |           | `bench-worker-3`  |            | 59.7        | 140.9        | 283          |
|                          |           | `bench-worker-4`  |            | 59.3        | 125.8        | 295          |
|                          |           | `postgres`        |            | 52.7        | 124.0        | 2048         |
| rust-ballista-standalone | full      |                   | 27296      | 185.1       | 221.3        | 1477         |
|                          |           | `postgres`        |            | 40.1        | 71.5         | 2048         |
| pyspark-3.5.4            | full      |                   | 21704      | 246.8       | 391.1        | 3154         |
|                          |           | `postgres`        |            | 62.5        | 138.0        | 2048         |
| rust-ballista-remote     | selective |                   | 1573       | 62.0        | 80.5         | 2987         |
|                          |           | `bench-rust-dist` |            | 6.0         | 23.8         | 9            |
|                          |           | `bench-scheduler` |            | 5.0         | 7.1          | 12           |
|                          |           | `bench-worker-1`  |            | 12.6        | 15.8         | 766          |
|                          |           | `bench-worker-2`  |            | 13.0        | 15.9         | 719          |
|                          |           | `bench-worker-3`  |            | 13.2        | 16.9         | 729          |
|                          |           | `bench-worker-4`  |            | 12.3        | 15.6         | 752          |
|                          |           | `postgres`        |            | 137.9       | 195.1        | 2048         |
| rust-ballista-standalone | selective |                   | 10249      | 12.3        | 20.6         | 22           |
|                          |           | `postgres`        |            | 21.6        | 72.6         | 2048         |
| pyspark-3.5.4            | selective |                   | 5990       | 216.2       | 330.7        | 641          |
|                          |           | `postgres`        |            | 33.2        | 125.2        | 2048         |

### Engine budget 8g (workers 1792m each, spark heap 7447m)

| engine                   | scenario  | component         | elapsed_ms | avg_cpu_pct | peak_cpu_pct | peak_rss_mib |
|--------------------------|-----------|-------------------|------------|-------------|--------------|--------------|
| rust-ballista-remote     | full      |                   | 19440      | 269.5       | 400.0        | 1635         |
|                          |           | `bench-rust-dist` |            | 35.1        | 99.7         | 512          |
|                          |           | `bench-scheduler` |            | 0.3         | 0.7          | 8            |
|                          |           | `bench-worker-1`  |            | 55.9        | 101.7        | 283          |
|                          |           | `bench-worker-2`  |            | 59.5        | 123.5        | 285          |
|                          |           | `bench-worker-3`  |            | 59.6        | 143.0        | 288          |
|                          |           | `bench-worker-4`  |            | 59.1        | 114.6        | 283          |
|                          |           | `postgres`        |            | 50.9        | 106.3        | 2048         |
| rust-ballista-standalone | full      |                   | 27275      | 183.5       | 218.8        | 1485         |
|                          |           | `postgres`        |            | 38.4        | 64.7         | 2048         |
| pyspark-3.5.4            | full      |                   | 22018      | 251.7       | 393.7        | 4323         |
|                          |           | `postgres`        |            | 64.8        | 129.7        | 2048         |
| rust-ballista-remote     | selective |                   | 1429       | 57.9        | 73.7         | 3006         |
|                          |           | `bench-rust-dist` |            | 6.3         | 34.9         | 10           |
|                          |           | `bench-scheduler` |            | 4.5         | 6.0          | 11           |
|                          |           | `bench-worker-1`  |            | 11.4        | 13.9         | 762          |
|                          |           | `bench-worker-2`  |            | 12.1        | 14.6         | 738          |
|                          |           | `bench-worker-3`  |            | 11.8        | 14.0         | 765          |
|                          |           | `bench-worker-4`  |            | 11.7        | 14.5         | 719          |
|                          |           | `postgres`        |            | 148.4       | 195.8        | 2047         |
| rust-ballista-standalone | selective |                   | 10154      | 12.0        | 20.8         | 22           |
|                          |           | `postgres`        |            | 23.6        | 77.6         | 2048         |
| pyspark-3.5.4            | selective |                   | 5602       | 214.8       | 340.0        | 861          |
|                          |           | `postgres`        |            | 37.9        | 136.8        | 2048         |

### Takeaways (10M rows, best of 3, equal-spec per the rule above)


Across three runs at each engine budget (2g, 4g, and 8g), the distributed Rust/DataFusion/Ballista implementation consistently delivered the best overall performance for the tested PostgreSQL extraction workloads.

Full extraction: distributed Rust averaged 19.52s, compared with 21.49s for PySpark and 27.31s for standalone Rust.
Selective extraction: distributed Rust averaged 1.48s, compared with 5.54s for PySpark and 10.23s for standalone Rust.
Memory: distributed Rust used approximately 1.5–1.65 GiB during full extraction, while PySpark reached 2.0–4.3 GiB depending on the configured heap.
Memory scaling: increasing the engine budget from 2g → 4g → 8g produced little change in execution time for either workload, indicating that the tested workloads were not significantly memory-bound.
Distributed execution: the remote scheduler/worker deployment substantially outperformed standalone execution, particularly for selective extraction.

Overall, these results show that the distributed Rust implementation can provide comparable or better performance than PySpark for this workload, with lower memory requirements for full extraction, while retaining a distributed execution model through Ballista.

### Equal-spec rule (read before quoting a number)

A benchmark number is only quotable when every engine ran the same spec:

- same data (`SCALE_ROWS`, same seed/scale path),
- same container budget (default `--cpuset-cpus 0-3` + `--memory 4g`, shared across
  the distributed deployment — not per container),
- same source fixture (bench-pg pinned to cores 4-5 + 2g, shared by all runs),
- same scan fan-out derivation (default `ceil(rows/64000)` both sides),
- same batch knob (explicit `--batch-size` both sides, or label the run "tool defaults"),
- sequential runs, best-of-`REPEAT`.

`run.sh` prints this spec card on every run and bakes the spec into
`results/summary.md`. A missing item means the numbers are smoke, not headlines.
Equality lives ONLY at the container boundary: inside, every runtime runs
unrestricted (Spark `local[*]`, DataFusion defaults, Ballista visible-CPU slots).
Never cap one side from the inside (`--spark-cores`, `BENCH_CONCURRENT_TASKS` are
diagnostics only) — that is precisely benchmarking with a different spec.

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
  scaling exists (`SCALE_ROWS`, default 10M; `--skip-scale` stays on the raw seed).
  Always report the row count next to the time.
- JVM boot is excluded from Spark's time; process init is negligible for Rust. What's timed
  is scan + write on both sides.
- Spark reads back its own output to count rows (inside its timed section); Rust counts
  collected batches (free). The read-back is honest work but not identical work — the JSON
  splits don't separate it. Compare `elapsed_ms` as system-vs-system, not kernel-vs-kernel.
- Every container runs pinned to cores 0-3 with `--memory 4g` unless overridden
  (distributed shares the 4g across scheduler/client/workers — see Quickstart);
  everything runs sequentially on the same host. Numbers are host-specific; the
  *ratio* is the portable part. For cross-host comparisons keep the pin — runtimes
  adapt from the inside, so the same script flag means the same budget everywhere.
  bench-pg (cores 4-5 + 2g, fixed) is the same fixture for every engine and scenario.
- Memory shape: both sides stream to disk, so memory stays O(batch) at any row count
  on the Rust side (batches flow straight into the writer) and bounded on Spark's.
- Apple Silicon works unmodified: every image is multi-arch (amd64 + arm64).

## Troubleshooting

- `Cannot connect to the Docker daemon ... connect: connection refused`:
  the Docker engine isn't reachable. Either start Docker Desktop or check
  `docker info`. (`run.sh` preflights
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
- Before blaming the build, check what else is eating the host: `docker ps` —
  a running Postgres plus scheduler plus workers can leave under 1GB free,
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
