# Benchmark: full initial load, Rust vs PySpark

Same Postgres, same query, both containerized, profiled the same way.

## Data

The benchmark uses its own `orders` table (plus `users`), not the demo data:

- Seed SQL: `benchmark/PostgresDB/initdb/*.sql` (500 users, 20k orders).
- Loaded only into the `bench-pg` container that `run.sh` starts with the plain `docker`
  CLI, then grown toward `SCALE_ROWS` rows by `scale.sql` (50M for the published numbers).
- Password: `BENCH_PG_PASSWORD` (default `postgres`).

The demo and test database is dvdrental (`tests/docker/compose.yaml`, database `test`); it
is unrelated to the benchmark.

## Results summary

50M `orders` rows, 4g engine budget, engine containers pinned to cores 0-3 and bench-pg to
cores 4-7 with 2g, best of 3 ([full spec and the 2g / 8g tables](#results)):

| engine | scenario | elapsed_ms | cpu_seconds | peak_mem_mib |
|---|---|---|---|---|
| el-ballista-standalone | full | 33702 | 52.17 | 190 |
| el-ballista-distributed | full | 66904 | 105.17 | 288 |
| pyspark-3.5.4 | full | 150437 | 212.2 | 3768 |
| el-ballista-standalone | selective | 8326 | 16.61 | 31 |
| el-ballista-distributed | selective | 9088 | 17.13 | 70 |
| pyspark-3.5.4 | selective | 19501 | 13.04 | 541 |

These runs used `--skip-correctness`: the gate checked row counts, not row multisets (see
[Correctness check](#correctness-check-independent-row-by-row)).

Reproduce the headline (needs a Docker host with >= 8 cores):

```bash
SCALE_ROWS=50000000 benchmark/run.sh --repeat 3 --memory 4g --skip-correctness
```

`--memory 4g` is the default and is spelled out for clarity. Drop `--skip-correctness` to
also run the DuckDB row-by-row check (needs `pip install duckdb`). Change `--memory` to
`2g` or `8g` for the other two tables.

## Versions

| Component | Version |
|---|---|
| Rust | 1.98.1 |
| DataFusion | 54.1.0 |
| Ballista | 54.1.0 |
| PySpark | 3.5.4 |
| PostgreSQL | 17.11 |

Versions as measured: the Rust and PostgreSQL images use floating tags (`rust:1-slim-bookworm`,
`postgres:17`).

Versions as measured: the Rust and PostgreSQL images use floating tags (`rust:1-slim-bookworm`, `postgres:17`).

## Workload

Two scenarios, one definition in `run.sh` for both engines: Rust gets it as job config (`bench-config-<scenario>.json`: structured `filters` + `columns`); Spark gets the same predicate rendered as SQL for its JDBC read.

| Scenario | Definition | What it exercises |
|---|---|---|
| `full` | all 12 columns, every row → Parquet | Pure extraction throughput; pushdown can't confound it |
| `selective` | filter `status = 'REFUNDED'` (~3% of rows) + columns `order_id, amount, status` → Parquet | Projection pushdown on both sides. Spark pushes the filter through JDBC. Rust keeps it in DataFusion on cost grounds, so Postgres sends every row's 3 columns (see below) |

**Why Rust keeps `status = 'REFUNDED'` in DataFusion.** `status` is an enum, so to compare
exactly like Arrow the connector renders the filter as `CAST(status AS text) COLLATE "C"`.
That expression cannot use `orders_status_idx`. Without an index the cost model prices the
filter as a full scan of the 50M-row table, far over `max_source_cost` (default 50,000), so
it stays in DataFusion. See the README's
[pushdown section](../README.md#pushdown-push-keep-or-never) for the same rule pushing on a
small table.

Two Rust deployments (`--mode standalone|distributed|both`, default `both`):

| Mode | How the scan runs |
|---|---|
| `standalone` | Plain DataFusion in one container — no Ballista scheduler or executor. The harness runs the connector's `.extract().standalone()`, which registers the provider in a plain `SessionContext`; DataFusion runs the keyset partitions concurrently on one Tokio runtime (one thread per visible CPU), and the process uses the whole `pool_max`. Engine label `el-ballista-standalone`. |
| `distributed` | Real `bench-scheduler` + `bench-worker-N` containers (our scheduler/worker with Postgres codecs — stock Ballista binaries cannot decode our plans), remote client container. The engine cores are **split**, not shared: the first core runs the scheduler + client, each of the 3 workers (default) gets its own core from the rest (0 / 1 / 2 / 3 on the default pin). Each worker gets as many task slots as source connections (`pool_max / workers` = 4). A **fresh cluster is started for every attempt**, like every other engine container. Workers advertise container names so the scheduler dials them back. The client and every worker resolve the source password independently; the scheduler never connects to the source. The client checks the executors through the scheduler REST API, which is required: the generated config sets no `distributed.job_timeout_secs`. |

| | Rust | Spark |
|---|---|---|
| Engine | Plain DataFusion (standalone) or scheduler+workers (see below), always through the connector API | PySpark 3.5.4, `local[*]` |
| Scenario definition | The job config file only: `run.sh` writes `bench-config-full.json` and `bench-config-selective.json` with structured `filters` / `columns`; the harness builds no SQL | SQL predicate + column list, rendered by `run.sh` from the same structured definition (JDBC needs SQL) |
| Read fan-out | `--rust-partitions` keyset partitions; default = engine cores (4 on the 0-3 pin) | `--spark-partitions` JDBC partitions on `order_id`; default = engine cores, Spark's own default parallelism for `local[*]` |
| Source connections | `pool_max` 12: standalone 12, distributed 3 workers × 4 | JDBC: one connection per running task |
| Fetch | Binary `COPY … TO STDOUT` by default (`--no-use-copy`: cursor `FETCH`), `execution.batch_size` rows per Arrow batch / `FETCH` window — `run.sh` writes `--batch-size` into the generated job spec when set, else the code default 8192 applies | JDBC `fetchsize` = `--batch-size` when set, else 8192 (matching Rust's default batch; Spark's own default 0 makes the driver buffer each partition fully) |
| Output writer | `parquet::arrow::ArrowWriter` in the bench binary, Snappy, **one file** | `df.repartition(1).write.parquet`, Snappy, **one file** (the read stays partitioned; one task writes) |
| Timed | **End to end:** program entry point (before the config is read) to the last Parquet byte written — config load, schema discovery, split planning, scan, encode (`scan_ms` + `write_ms` split in JSON) | **End to end:** program entry point (before the SparkSession / JVM starts) to the last Parquet byte written — session start-up and the partition-bounds query included; the row-count read-back runs after the timer |
| Other settings | Tool defaults | Tool defaults (no shuffle or other tuning); only the driver heap (solved from the container budget) and the JDBC `fetchsize` are set |

Batch size (`--batch-size`) is optional and applies to **both** engines: Spark's JDBC `fetchsize` and the Rust job spec's `execution.batch_size` (rows per Arrow batch and per cursor `FETCH`; `run.sh` writes it into the generated `bench-config-<scenario>.json`). **Auto run** (flag absent): Rust uses its code default of 8192 rows per batch, and Spark's JDBC `fetchsize` is set to the same 8192. With Spark's own default (`fetchsize` 0) the Postgres JDBC driver buffers each partition's whole result in the heap, 12.5M rows per task at 50M rows over 4 partitions, so the published runs always set a fetch size (see *Why Spark gets a fetch size* under Results). **Manual run** (`--batch-size 64000`): both engines use that many rows per batch. The batch size never changes the partition count (engine cores by default), so a batch experiment changes one thing. Override counts independently (`--rust-partitions`, `--spark-partitions`) to test sensitivity, but keep them equal for the headline number.

CPU/MEM for distributed runs aggregates the whole cluster (scheduler + workers + client
summed per tick), comparable to single-container runs. bench-pg is profiled on the same
ticks into a separate series so every run shows source-database cost without polluting
engine totals. Everything runs sequentially —
nothing contends for CPU. Best-of-`REPEAT` smooths page-cache effects.

Failed and hung attempts never stall the run:

- Every attempt runs under a watchdog. It kills the attempt when a container it depends on
  stops (for example an OOM-killed worker; the reason names the container, its exit code and
  `OOMKilled`), or when the attempt has no result after `--attempt-timeout` seconds (default
  1800, `0` = no limit).
- A killed or failed attempt is discarded and run again from scratch, on a fresh cluster for
  distributed, up to `--max-retries` times per case (default 2). After that the case is marked
  FAILED and the run moves on. Every discarded attempt is listed at the end of
  `results/summary.md`; none of them reaches a number.
- The client's own distributed job retry is off in the generated config
  (`distributed.max_retries: 0`): a re-run inside the client would finish on fewer workers, and
  that time is not comparable. A deterministic failure (e.g. Spark's full load running out of
  memory) repeats on every retry; `--max-retries 0` skips them.

## Layout

```
benchmark/
├── PostgresDB/initdb/ # seed SQL for bench-pg (500 users, 20k orders), mounted by run.sh
├── rust/
│   ├── Dockerfile     # multi-stage release build of bench_full_load (examples/bench_full_load.rs)
│   └── bench-config.json  # template job spec (image default); run.sh mounts a generated
│                          # bench-config-<scenario>.json per run instead
├── spark/
│   ├── Dockerfile     # apache/spark:3.5.4 + PostgreSQL JDBC 42.7.13 baked in
│   └── load.py        # the equivalent workload, JSON summary to stdout
├── scale.sql          # grow toward SCALE_ROWS (~10M default); run.sh computes the
│                      # factor from the live count and skips when already there
├── correctness.py     # DuckDB row-by-row check of the Parquet outputs
├── run.sh             # orchestrator: build → postgres → scale → run → profile → gate → report
├── results/           # *.json, *_cluster.csv, *_c_*.csv, summary.md (gitignored)
└── output/            # parquet outputs (gitignored)
```

## Quickstart

Needs the `docker` CLI (no compose) and a Docker host with >= 8 cores for the default pins.
The ~10M-row default (`SCALE_ROWS`) takes a few minutes plus a one-time Rust release build
(Ballista + DataFusion). The published numbers use 50M rows; see
[Results summary](#results-summary) for that command.

```bash
# Full benchmark at the default 10M rows, best of 3. Every container is pinned to
# cores 0-3 and runs its runtime unrestricted (Spark local[*], DataFusion defaults):
benchmark/run.sh --repeat 3

# Smoke test on the 20k seed (seconds):
benchmark/run.sh --skip-scale --repeat 1

# Tiny host (~1GB RAM) smoke test: shrink everything to fit, stay on the 20k seed.
# Adjust --cpuset-cpus to the cores you actually have (the default 0-3 fails fast
# otherwise). bench-pg's hardcoded 4-7 pin also needs >= 8 cores: on a smaller host edit
# PG_CPUSET in run.sh for the smoke run (the script refuses to start otherwise).
# Explicit small slices bypass the memory floors. Smoke ONLY: different data
# scale means different spec, never quote these numbers.
benchmark/run.sh --skip-scale --repeat 1 --mode standalone \
  --cpuset-cpus 0-1 --memory 1g --workers 1 \
  --scheduler-memory 128m --client-memory 128m --worker-memory 256m

# Skip builds entirely (images already present):
benchmark/run.sh --no-build --repeat 3

# Cleanup everything (containers, network, volume, results):
benchmark/run.sh --clean
```

Every container the script starts defaults to a hard pin on cores 0-3
(`--cpuset-cpus 0-3`, overridable; `run.sh` checks the Docker host's core count against
every pin before starting anything, so a host with fewer cores than the pins name — 8 in
the default layout — fails fast instead of benchmarking the wrong budget) and `--memory 4g`. Memory is a per-deployment
budget: Spark and the standalone Rust client each get the full 4g, while
the distributed deployment shares it — 512m scheduler + 512m client + the rest split
evenly across workers (1024m each at the default 3 workers). The distributed deployment also
splits the pinned cores instead of sharing them (core 0: scheduler + client; cores 1-3: one
worker each), so its total is still exactly the engine pin. Postgres is fixed outside this
budget: `run.sh` pins bench-pg to cores 4-7 + 2g RAM (`PG_CPUSET` / `PG_MEM`, hardcoded,
clear of the engine pin, and checked with `docker inspect` on every run) so the shared
fixture never changes shape. (The test stack in `tests/docker/compose.yaml` is not the
benchmark fixture: it uses a `cpus: 2` quota + 2g, no pin.) Tune with `--cpus N` (replaces
the pin with a quota), `--cpuset-cpus RANGE`, `--memory SIZE`, `--scheduler-memory`
/ `--client-memory` / `--worker-memory` (or `CPUS` / `MEMORY` / `CPUSET_CPUS` /
`SCHEDULER_MEMORY` / `CLIENT_MEMORY` / `WORKER_MEMORY` env; empty lifts the default).
Prefer the pin over the quota: the JVM honors a CFS quota, but Rust's
`available_parallelism` (which sizes DataFusion partitions and Ballista task slots)
only sees a cpuset pin — under plain `--cpus` the Rust side still sizes its pools
from all host CPUs.

## Build time and prebuilt images

The Rust release build is the slow part (Ballista + DataFusion, ~400 crates):

- Rebuilds are cheap: `cargo-chef` layers keep dependency compilation cached unless
  `Cargo.toml` / `Cargo.lock` change, so editing the crate's sources and re-running costs
  minutes, not a full rebuild.
- `--no-build` reuses images already present locally.
- `--pull` pulls the images named by `BENCH_RUST_IMAGE` / `BENCH_SPARK_IMAGE` (defaults
  `el-ballista-bench-rust:latest` / `el-ballista-bench-spark:latest`) instead of building. The repository
  publishes no images, so point these at a registry where the images were pushed after a
  build elsewhere.

## Settings

`PG_PORT` defaults to 5433 so it never clashes with a dev DB on 5432. `BENCH_PG_PASSWORD`
defaults to `postgres`; `run.sh` uses it both when creating bench-pg and in the generated
job configs. `BENCH_CONCURRENT_TASKS` sets each worker's task slots (default: the worker's source-connection
share, `POOL_MAX / WORKERS` = 4, so every budgeted connection can be busy — the same rule
standalone follows; scans are bounded by connections, not cores). `POOL_MAX` (default 12) is the
source connection budget written into the Rust job config; 12 divides by 3 and 4, so standalone and
the distributed deployment open the same number of connections.

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
`t_end_epoch_ms`, the same window as `elapsed_ms`: end to end, from the program's entry point to
the last byte written (Spark's session / JVM start-up is inside it; the distributed cluster's
start-up is not, since the cluster is infrastructure the client connects to). `stats_window`
in the JSON says which window was used (older engine JSON without the bounds falls back to the
container lifetime).

- **CPU.** Each interval's CPU% is Δ(container CPU ns) / Δ(daemon read time ns) × 100
  (100% = one full core; sums across cores *and* containers, so 200% on 2 fully lit cores
  is normal). Both counters are nanosecond-precise, so there are no granularity artifacts,
  and peaks are raw per-interval maxima with no smoothing or capping. `cpu_seconds` is the exact CPU work in the window, read from the
  cumulative counter (independent of sampling), and `avg_cpu_pct` = `cpu_seconds` ÷
  window.
- **Memory is compared at the container level** — the whole engine deployment, not one
  process. The summary shows two container figures:
  - `peak_mem_mib`: the container working set, `usage − inactive_file`, i.e. what
    `docker stats` shows; 100 ms samples, and for distributed the peak of the per-sample sum
    across the deployment's containers.
  - `mem_peak_exact_mib`: the exact container high-water mark (cgroup v2 `memory.peak`) —
    everything the containers were charged, **page cache included**. This is the figure a
    `--memory` limit / OOM kill applies to, immune to sampling gaps. A load that writes a
    multi-GB Parquet file climbs toward the limit here through page cache, whatever the engine.
    Engine containers read their own at the end of the timed section; for distributed,
    `run.sh` reads the scheduler's and workers' right after the client finishes. The cluster
    is fresh for every attempt, so these cover exactly one run; the total is a sum of
    per-container peaks (an upper bound).

  Process memory (`peak_rss_mib`: the cgroup's anonymous pages) stays in the detail table.
- **Postgres** is sampled on the same ticks into `<prefix>_pg.csv` and reported as a
  `postgres` sub-row, never summed into engine totals.

A tail sample after client exit catches compute that finished on the last tick. Containers
are polled one after another within a tick, so summed samples are not exactly simultaneous
(harmless for `cpu_seconds`/averages; slightly blurs summed peaks). Timed sections are the same
span on both engines: program entry point to the last Parquet byte written.

`run.sh` also verifies the pin itself
(`docker inspect` on every started container, `pin: <name> -> [0-3]` per run, WARN
otherwise — a stale `bench-pg` reused from an unpinned run would otherwise silently
benchmark a different spec). Sampled memory (`peak_rss_mib`, `peak_mem_mib`) can still
miss sub-100ms spikes; `mem_peak_exact_mib` cannot, so use it for "does it fit in the
limit" questions and the sampled peaks for the working set. Every engine container, the
distributed cluster included, starts fresh for every attempt, so no scenario inherits memory
retained by an earlier one.

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

Toolchain: see [Versions](#versions). Source database is the PostgreSQL above. These numbers were measured before commit 0327f07 (client-computed partition bounds, the scheduler REST-or-timeout requirement) and later changes; they have not been re-measured since.

Spec (all three tables):

- **Data:** 50,000,000 `orders` rows; selective returns 1,602,500.
- **Parallelism:** 4 keyset / JDBC partitions on both sides (= engine cores); `pool_max` 12.
- **Output:** one Snappy Parquet file per engine and scenario (Spark: `repartition(1)` before the write).
- **Timing:** end to end, from the program entry point to the last byte written. Spark's session start-up is included; its read-back count is not.
- **Pinning:** engine containers on cores 0-3; bench-pg on cores 4-7 with 2g.
- **Rust modes:**
  - standalone: plain DataFusion (`el-ballista-standalone`);
  - distributed: scheduler + client on core 0, 3 workers on cores 1, 2 and 3 with 4 task slots each, a fresh cluster per attempt (`el-ballista-distributed`).
- **Batch / fetch size:** 8192 rows on both sides (Rust's default batch; Spark's JDBC `fetchsize` set to match, because the driver default buffers a whole partition in the heap, see below).
- **Runs:** sequential, best of 3, with `--skip-correctness`: the gate checked row counts, not row multisets. Each run's `results/summary.md` holds its spec card.
- **What varies:** only the engine memory budget, which the distributed deployment splits across its containers:

| Budget | Distributed: scheduler / client / each worker | Spark heap |
|---|---|---|
| 8g | 512m / 512m / 2389m | 7447m |
| 4g | 512m / 512m / 1024m | 3712m |
| 2g | 512m / 512m / 341m | 1664m |

Columns (container level, summed over a deployment's containers):

- **`cpu_seconds`:** exact CPU time over the timed section.
- **`peak_mem_mib`:** peak container working set (usage minus inactive page cache, as `docker stats` reports it; 100 ms samples). Use this one to compare engines.
- **`mem_peak_exact_mib`:** the cgroup high-water mark **including page cache**, which is what a `--memory` limit applies to:
  - a full load writes a ~2.4 GB Parquet file, so this column climbs toward the limit for any engine. Rust standalone and Spark reach the 2g limit and still finish, because page cache is reclaimable;
  - for distributed it is a sum of per-container peaks, including the workers' shuffle-file cache (at 2g every worker sits at its 341m limit).

Process memory (`peak_rss_mib`) and the per-container breakdown, bench-pg included, are in each run's detail table.

### Engine budget 8g

| engine                     | scenario  | elapsed_ms | cpu_seconds | peak_mem_mib | mem_peak_exact_mib |
|----------------------------|-----------|------------|-------------|--------------|--------------------|
| el-ballista-distributed    | full      | 65696      | 104.76      | 357          | 5172               |
| el-ballista-standalone     | full      | 35143      | 50.97       | 183          | 2398               |
| pyspark-3.5.4              | full      | 154468     | 230.02      | 4912         | 8192               |
| el-ballista-distributed | selective | 9573       | 17.72       | 69           | 113                |
| el-ballista-standalone | selective | 8394       | 17.55       | 32           | 39                 |
| pyspark-3.5.4              | selective | 19958      | 13.62       | 651          | 676                |

One Spark full attempt at 8g was discarded and re-run: a `bench-spark` container left over from an earlier, interrupted run blocked it. `run.sh` removes leftover containers before every run.

### Engine budget 4g (default)

| engine                     | scenario  | elapsed_ms | cpu_seconds | peak_mem_mib | mem_peak_exact_mib |
|----------------------------|-----------|------------|-------------|--------------|--------------------|
| el-ballista-distributed    | full      | 66904      | 105.17      | 288          | 3599               |
| el-ballista-standalone     | full      | 33702      | 52.17       | 190          | 2435               |
| pyspark-3.5.4              | full      | 150437     | 212.2       | 3768         | 4099               |
| el-ballista-distributed | selective | 9088       | 17.13       | 70           | 114                |
| el-ballista-standalone | selective | 8326       | 16.61       | 31           | 38                 |
| pyspark-3.5.4              | selective | 19501      | 13.04       | 541          | 559                |

### Engine budget 2g

| engine                     | scenario  | elapsed_ms | cpu_seconds | peak_mem_mib | mem_peak_exact_mib |
|----------------------------|-----------|------------|-------------|--------------|--------------------|
| el-ballista-distributed    | full      | 65785      | 103.41      | 250          | 1550               |
| el-ballista-standalone     | full      | 34455      | 52.74       | 180          | 2049               |
| pyspark-3.5.4              | full      | 137234     | 193.62      | 1844         | 2052               |
| el-ballista-distributed | selective | 9120       | 16.89       | 70           | 116                |
| el-ballista-standalone | selective | 8103       | 16.84       | 34           | 39                 |
| pyspark-3.5.4              | selective | 19317      | 12.94       | 532          | 552                |

**Why Spark gets a fetch size.** Spark's JDBC reader leaves `fetchsize` unset by default, and the Postgres driver then buffers each partition's entire result in the heap: 12.5M rows × 12 columns per task at 4 partitions. The published runs therefore set `fetchsize` 8192, the same chunk size Rust reads in, and with it Spark completed the full load at all three budgets: 137 s at 2g, 150 s at 4g and 154 s at 8g, peaking at 1,844 / 3,768 / 4,912 MiB. Runs with the driver default are not part of these results.

### Takeaways (50M rows, equal spec, the three budgets above)

- **Standalone Rust (plain DataFusion) is the fastest in both scenarios at every budget.**
  - Full load: ~33.7–35.1 s, vs ~65.7–66.9 s for distributed Rust (1.9–2.0×) and ~137–154 s for PySpark (4.0–4.5×).
  - Selective: ~8.1–8.4 s, vs ~9.1–9.6 s distributed (1.09–1.14×) and ~19.3–20.0 s PySpark (2.3–2.4×).
- **On the full load, standalone does the least work:** ~51–53 CPU-seconds, vs ~103–105 for distributed (2×) and ~194–230 for PySpark (~4×).
- **On selective, PySpark uses the least CPU but is the slowest.**
  - CPU: ~12.9–13.6 CPU-seconds, vs Rust's ~16.6–17.7. PySpark pushes the filter through JDBC, so Postgres sends only the ~3% of rows that match; Rust receives every row's 3 columns and filters them in DataFusion.
  - Time: PySpark mostly waits. It averages ~67% of one core, and Postgres serving it averages ~65%: each 8192-row fetch is a synchronous round trip to Postgres.
- **PySpark keeps the source idle on the full load, too.** Postgres averages ~53–59% of a core serving Spark, vs ~185–195% serving Rust standalone. Rust's full scan uses binary `COPY`, one continuous stream per partition; Spark's JDBC read fetches 8192 rows per round trip.
- **Standalone selective runs close to the source limit.** Postgres averages ~296–313% of its 4 cores and peaks near 400%, because Rust keeps `status = 'REFUNDED'` in DataFusion: the enum comparison cannot use the index, and a full scan of 50M rows is over `max_source_cost` ([why](#workload)). It is still the fastest end to end.
- **Rust memory is small and flat across budgets.**
  - Standalone: 180–190 MiB for the full load, 31–34 MiB selective.
  - Distributed: 250–357 MiB full and 69–70 MiB selective, summed over its 5 containers.
  - PySpark takes what the budget offers: 1.8 / 3.7 / 4.8 GiB for the full load at 2g / 4g / 8g, and 0.52–0.64 GiB selective.
- **Budget doesn't move Rust times.** From 2g to 8g every Rust case stays within ~5%, with no trend by budget. PySpark's full load is fastest at 2g (137 s vs 150–154 s): more heap is not faster for it here.
- **Why distributed is slower on one host:**
  - 4 partitions over 3 workers split 2:1:1 (round-robin placement). The worker with two partitions does ~32–33 CPU-seconds on its one core, vs ~20 for each of the others.
  - Every row is written to shuffle files and sent over Arrow Flight before it reaches the client, which is where the extra CPU goes.
  - The client decodes all 50M rows and writes the one Parquet file (~31 CPU-seconds, about one core), sharing core 0 with the scheduler.

  Distributed pays off when it adds machines, not on one pinned host.

These are measurements of this implementation on this workload and host, not a general Rust-vs-Spark claim.

### Equal-spec rule (read before quoting a number)

A benchmark number is only quotable when every engine ran the same spec:

- same data (`SCALE_ROWS`, same seed/scale path),
- same container budget (default `--cpuset-cpus 0-3` + `--memory 4g`, shared across
  the distributed deployment — not per container),
- same source fixture (bench-pg pinned to cores 4-7 + 2g, shared by all runs),
- same scan fan-out (default: engine cores on both sides),
- same output shape (one Parquet file per engine and scenario),
- same timed span (program entry point to the last byte written),
- same source connection budget for both Rust modes (`pool_max` 12),
- same batch size on both sides (auto: Rust's default 8192 rows, Spark `fetchsize` 8192; or an explicit `--batch-size`),
- sequential runs, best-of-`REPEAT`.

`run.sh` prints this spec card on every run and bakes the spec into
`results/summary.md`. A missing item means the numbers are smoke, not headlines.
Equality lives ONLY at the container boundary: inside, every runtime runs
unrestricted (Spark `local[*]`, DataFusion defaults). The distributed deployment splits the
engine pin across its containers (core 0: scheduler + client, one core per worker) and gives
each worker one task slot per source connection; its total is still the engine pin.
Never cap one side from the inside (`--spark-cores`, a lower `BENCH_CONCURRENT_TASKS` are
diagnostics only) — that is precisely benchmarking with a different spec.

### Reading the numbers (read before quoting them)

Do not compare `output_bytes` across engines for equality — only for magnitude. Same rows,
different bytes, always:

- **Different writers make different files.** Spark uses parquet-mr, Rust uses parquet-rs:
  different page/row-group chunking, dictionary-fallback thresholds, statistics blocks, and
  footer metadata. Expect ~single-digit % divergence on large files, more on small ones
  where fixed overheads (footers, dictionaries for a 1-value column like selective
  `status`) dominate. Measured selective outputs: ~30% apart at 194k rows, ~3% apart at
  6M rows; the gap amortizes with size.
- **Same writer, different run, slightly different bytes** (e.g. distributed vs standalone
  selective files: 1,239,491 vs 1,241,870 bytes, ~0.2%). Parallel scans deliver batches in
  nondeterministic groupings, so row-group boundaries land in different places. Content
  is identical — which is why the gate compares row *sets*, never bytes.
- The one byte comparison that matters: an engine disagreeing with *itself* across runs
  beyond low-single-digit % on the same data would indicate nondeterministic encoding,
  worth investigating.

- 20k seeded rows complete in seconds on either engine (noise dominates), which is why
  scaling exists (`SCALE_ROWS`, default 10M, 50M for the published numbers; `--skip-scale`
  stays on the raw seed).
  Always report the row count next to the time.
- Both engines are timed end to end: Spark's JVM / session start-up is inside its time,
  as are Rust's config load and schema discovery. Compare `elapsed_ms` as
  system-vs-system, not kernel-vs-kernel.
- Spark reads back its own output to count rows **after** its timer stops; Rust counts
  collected batches (free).
- Every container runs pinned to cores 0-3 with `--memory 4g` unless overridden
  (distributed shares the 4g across scheduler/client/workers — see Quickstart);
  everything runs sequentially on the same host. Numbers are host-specific; the
  *ratio* is the portable part. For cross-host comparisons keep the pin — runtimes
  adapt from the inside, so the same script flag means the same budget everywhere.
  bench-pg (cores 4-7 + 2g, fixed) is the same fixture for every engine and scenario.
- Memory shape: Rust streams to disk, so its memory stays O(batch) at any row count
  (batches flow straight into the writer). Spark's JDBC read is bounded by its fetch size, which
  `run.sh` always sets (8192, or `--batch-size`); with Spark's own default (0) the Postgres
  driver would buffer each whole partition in the heap, so the partition size, not the batch,
  would set Spark's memory.
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
- A worker container that dies mid-run (`bench-worker-N stopped mid-run (exit 137,
  OOMKilled=true)`) usually ran out of its share of `--memory`: at the 2g budget each worker
  gets 341m, which large `--batch-size` values exceed. The attempt is killed and retried
  (see above); raise `--memory` or lower `--batch-size`.
- Fan-out already matches the engine cores by default (4 partitions on the 0-3 pin, both
  engines). With the default 3 workers, 4 partitions do not divide evenly (round-robin
  placement gives one worker two); `run.sh` prints a note when that happens.
- `collect2: ld terminated with signal 9` during the Rust build: the final link OOM'd (GNU ld
  holding hundreds of rlibs). The Dockerfile already switches to `lld` for this; if you still
  see it, give the builder more RAM (Docker Desktop → Settings → Resources) — 4GB+ recommended.
- `rustc ... (signal: 9, SIGKILL)` mid-build: the *compiler* OOM'd on a big crate (release
  codegen parallelizes per crate). The Dockerfile already pins
  `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1` for this; same remedy — more builder RAM.
- Before blaming the build, check what else is eating the host: `docker ps` —
  a running Postgres plus scheduler plus workers can leave under 1GB free,
  and then even serialized `rustc` dies. Stop everything non-essential before building,
  or build on a machine with more RAM, push the images to a registry, and run with
  `--pull` (see [Build time and prebuilt images](#build-time-and-prebuilt-images)).
- Spark OOM in container: raise `--shm-size` in `run.sh` (`run_spark`), or give Docker Desktop
  more memory.
- `row mismatch` gate: check the engine JSONs — usually a failed write or an
  incompletely-scaled table (re-run without `--skip-scale`).
- Output files may be root-owned (both images run as root for bind-mount simplicity);
  `chown` them if your umask complains.
- Two `el-ballista worker`s on one host need their own ports: the defaults are 50051 / 50052, so
  give each extra worker `--port` / `--grpc-port`. This only affects manual multi-worker
  setups; the benchmark runs each worker in its own container.
- A root `.dockerignore` keeps `target/` (tens of GB) out of the build context — do not
  delete it or `docker build` will try to send the entire workspace.
