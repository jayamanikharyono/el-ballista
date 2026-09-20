#!/bin/bash
# Full-load benchmark: Rust extraction layer vs PySpark, same Postgres, same query.
#
# Modes (`--mode standalone|distributed|both`, default both):
# - standalone:  Rust scans through in-process Ballista (one container, no cluster).
# - distributed: Rust scans through a real scheduler + worker deployment
#                (bench-scheduler + bench-worker-N containers, remote client).
# Spark runs local[*] unless --spark-cores caps it (its own cluster modes are out of
# scope here).
#
# Workload (identical everywhere): scenarios `full` (SELECT * -> Parquet) and `selective`
# (indexed ~3% slice + narrow projection -> Parquet).
# Rust side fans out over RUST_PARTITIONS keyset ranges; Spark over SPARK_PARTITIONS
# JDBC partitions on order_id. Both default to ceil(table_rows / 64000) unless
# --batch-size overrides the reference. Everything runs sequentially (never parallel)
# so nothing contends for CPU.
#
# Usage:
#   benchmark/run.sh [--clean] [--skip-scale] [--repeat N] [--workers N] [--spark-partitions N]
#                    [--spark-cores N] [--rust-partitions N] [--batch-size N] [--pull] [--no-build]
#                    [--mode standalone|distributed|both] [--parallel-strategy keyset|ctid]
#
# Images: built locally by default. `--pull` pulls prebuilt images instead (see
# benchmark/README.md "prebuilt images"), `--no-build` reuses whatever is already
# present locally without building or pulling. Override the refs with
# BENCH_RUST_IMAGE / BENCH_SPARK_IMAGE.
#
# Env knobs: PG_PORT (default 5433, avoids clashing with a dev DB on 5432),
#            BENCH_PG_PASSWORD (default postgres), REPEAT (default 1; 3+ recommended,
#            best-of kept to smooth JVM warmup and page-cache effects),
#            SCHED_API_PORT (default 50550, host port publishing the scheduler REST API),
#            SCALE_ROWS (default 5000000: grow orders toward ~5M rows; --skip-scale bypasses),
#            SPARK_DRIVER_MEM (default 2g: JVM heap for the Spark container; lower to 1g
#            on tiny hosts where the JVM gets OOM-killed mid-write),
#            BENCH_CONCURRENT_TASKS (default empty: each worker uses its visible CPU count
#            as task slots; set to pin it, e.g. 4 on a 4-CPU host).
#            PARALLEL_STRATEGY (default keyset): keyset or ctid - partitioning strategy.
#
# Outputs: benchmark/results/*.json (best runs, peaks + per-container breakdown baked in),
#          *_cluster.csv (summed CPU/MEM series), *_c_*.csv (per-container series),
#          correctness.json (row-by-row file check), summary.md (comparison table).
#          Row counts must match or the run FAILS.
#
# Prereqs: docker CLI. No compose needed; plain CLI commands only.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$BENCH_DIR")"
RESULTS="$BENCH_DIR/results"
OUTPUT="$BENCH_DIR/output"

IMG_RUST="${BENCH_RUST_IMAGE:-rel-bench-rust:latest}"
IMG_SPARK="${BENCH_SPARK_IMAGE:-rel-bench-spark:latest}"
NET="rel-bench-net"
PG="bench-pg"
PGDATA="bench-pgdata"
PG_PORT="${PG_PORT:-5433}"
PG_PASSWORD="${BENCH_PG_PASSWORD:-postgres}"
WORKERS=4
# Batch size is the input knob (empty = auto, each tool's own default); partition
# counts DERIVE from a reference batch size (64000 unless --batch-size overrides it)
# as ceil(table_rows / ref) so each partition holds roughly one batch worth of rows.
# Both engines use the same derived count — neither side wins on granularity.
# Explicit --rust-partitions / --spark-partitions flags override the derivation
# per engine.
SPARK_PARTITIONS=""
RUST_PARTITIONS=""
# BATCH_SIZE empty = auto: batch_size is omitted from the Rust config (code default
# 8192 applies) and BENCH_FETCHSIZE is left unset for Spark (JDBC default applies).
# Set via --batch-size to force the same rows-per-batch on both engines.
BATCH_SIZE=""
REPEAT=1
SCALE=1
CLEAN=0
PULL=0
NO_BUILD=0
SKIP_CORRECTNESS=0
MODE=both
PARALLEL_STRATEGY=keyset
SCHED_API_PORT="${SCHED_API_PORT:-50550}"
SCALE_ROWS="${SCALE_ROWS:-5000000}"
SPARK_DRIVER_MEM="${SPARK_DRIVER_MEM:-2g}"
# Spark concurrency: local[N] caps simultaneous tasks (each buffers a partition).
# Empty = all host CPUs. Set to fit the box, e.g. --spark-cores 2 on a 2GB machine.
SPARK_CORES=""
SCHEDULER_URL="http://bench-scheduler:50050"

while [ $# -gt 0 ]; do
  case "$1" in
    --clean) CLEAN=1; shift ;;
    --skip-scale) SCALE=0; shift ;;
    --repeat) REPEAT="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --spark-partitions) SPARK_PARTITIONS="$2"; shift 2 ;;
    --spark-cores) SPARK_CORES="$2"; shift 2 ;;
    --rust-partitions) RUST_PARTITIONS="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --pull) PULL=1; shift ;;
    --no-build) NO_BUILD=1; shift ;;
    --skip-correctness) SKIP_CORRECTNESS=1; shift ;;
    --mode) MODE="$2"; shift 2 ;;
    --parallel-strategy) PARALLEL_STRATEGY="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

case "$MODE" in
  standalone|distributed|both) ;;
  *) echo "--mode must be standalone, distributed, or both" >&2; exit 2 ;;
esac

if ! command -v docker >/dev/null 2>&1; then echo "need docker CLI" >&2; exit 1; fi
CLI=docker

# Fail fast with a useful message instead of dying mid-run on the first CLI call.
if ! $CLI info >/dev/null 2>&1; then
  cat >&2 <<'EOF'
cannot reach Docker through the docker CLI.
- Docker Desktop not running? Start Docker Desktop (then retry)
- socket live but CLI pointed elsewhere?
  export DOCKER_HOST=unix:///var/run/docker.sock  (then retry)
- no engine at all? Install Docker Desktop.
EOF
  exit 1
fi

cleanup_containers() {
  local c
  for c in bench-rust bench-rust-dist bench-spark bench-scheduler; do
    $CLI rm -f "$c" >/dev/null 2>&1 || true
  done
  for c in $($CLI ps -a --format '{{.Names}}' 2>/dev/null | grep -E '^bench-worker-[0-9]+$' || true); do
    $CLI rm -f "$c" >/dev/null 2>&1 || true
  done
}

if [ "$CLEAN" = "1" ]; then
  echo "cleaning benchmark containers, network, volume, results..."
  cleanup_containers
  $CLI rm -f "$PG" >/dev/null 2>&1 || true
  $CLI network rm "$NET" >/dev/null 2>&1 || true
  $CLI volume rm "$PGDATA" >/dev/null 2>&1 || true
  rm -rf "$RESULTS" "$OUTPUT"
  echo "clean."
  exit 0
fi

# --- distributed cluster (scheduler + workers) -------------------------------
# Started once per run when MODE includes distributed; torn down afterwards (and by
# the EXIT trap). Workers advertise --hostname-matching names so the scheduler can
# dial them back; all processes resolve the source password independently.

start_cluster() {
  echo "== start scheduler + $WORKERS worker(s)"
  $CLI run -d --name bench-scheduler --hostname bench-scheduler --network "$NET" \
    -e BENCH_ROLE=scheduler -e BENCH_SCHEDULER_URL="$SCHEDULER_URL" \
    -e BENCH_PG_PASSWORD="$PG_PASSWORD" \
    -p "127.0.0.1:${SCHED_API_PORT}:50050" \
    "$IMG_RUST" >/dev/null

  echo -n "waiting for scheduler REST API"
  for _ in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:${SCHED_API_PORT}/api/version" >/dev/null 2>&1; then break; fi
    echo -n "."; sleep 2
  done
  echo
  curl -sf "http://127.0.0.1:${SCHED_API_PORT}/api/version" >/dev/null \
    || { echo "scheduler REST API never came up" >&2; return 1; }

  local i
  for i in $(seq 1 "$WORKERS"); do
    $CLI run -d --name "bench-worker-$i" --hostname "bench-worker-$i" --network "$NET" \
      -e BENCH_ROLE=worker -e BENCH_SCHEDULER_URL="$SCHEDULER_URL" \
      -e BENCH_PG_PASSWORD="$PG_PASSWORD" \
      -e BENCH_CONCURRENT_TASKS="${BENCH_CONCURRENT_TASKS:-}" \
      "$IMG_RUST" >/dev/null
  done

  echo -n "waiting for $WORKERS executor(s) to register"
  for _ in $(seq 1 60); do
    local n
    n=$(curl -s "http://127.0.0.1:${SCHED_API_PORT}/api/executors" \
      | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0)
    if [ "$n" = "$WORKERS" ]; then break; fi
    echo -n "."; sleep 2
  done
  echo
  local n
  n=$(curl -s "http://127.0.0.1:${SCHED_API_PORT}/api/executors" \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
  [ "$n" = "$WORKERS" ] || { echo "only $n/$WORKERS executors registered" >&2; return 1; }
  echo "cluster ready: 1 scheduler + $n workers"
}

stop_cluster() {
  local i
  for i in $(seq 1 "$WORKERS"); do
    $CLI rm -f "bench-worker-$i" >/dev/null 2>&1 || true
  done
  $CLI rm -f bench-scheduler >/dev/null 2>&1 || true
}

# Aggregate sampler: sums CPU% and RSS across every cluster container each tick, so the
# distributed series represents whole-cluster cost, comparable to single-container runs.
sample_cluster() { # $1=file prefix $2=client-container, rest...=all containers
  # Writes <prefix>_cluster.csv (summed CPU/RSS per tick) plus <prefix>_c_<name>.csv
  # per container, so both the whole-cluster cost and who spent it survive.
  local prefix=$1 client=$2; shift 2
  # shellcheck disable=SC2068
  CLI_BIN=$CLI python3 - "$prefix" "$client" "$@" <<'EOF'
import csv, os, subprocess, sys, time
prefix, client, containers = sys.argv[1], sys.argv[2], sys.argv[3:]
cli = os.environ["CLI_BIN"]
def ps_names():
    out = subprocess.run([cli, "ps", "--format", "{{.Names}}"],
                         capture_output=True, text=True).stdout
    return set(out.split())
def stats_all(names):
    # One stats invocation for the whole set: at 10Hz, a subprocess per container per
    # tick would cost more than the sampling itself.
    try:
        out = subprocess.run(
            [cli, "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}} {{.MemUsage}}"]
            + names,
            capture_output=True, text=True).stdout.strip().split("\n")
    except Exception:
        return {}
    rows = {}
    for line in out:
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        try:
            rows[parts[0]] = (float(parts[1].rstrip("%")), to_mib(parts[2]))
        except ValueError:
            continue
    return rows

def to_mib(raw):
    # docker stats reports IEC (MiB/GiB).
    try:
        if raw.endswith("TiB"):
            return float(raw[:-3]) * 1048576
        if raw.endswith("TB"):
            return float(raw[:-2]) * 1000000000000 / 1048576
        if raw.endswith("GiB"):
            return float(raw[:-3]) * 1024
        if raw.endswith("GB"):
            return float(raw[:-2]) * 1000000000 / 1048576
        if raw.endswith("MiB"):
            return float(raw[:-3])
        if raw.endswith("MB"):
            return float(raw[:-2]) * 1000000 / 1048576
        if raw.endswith("KiB"):
            return float(raw[:-3]) / 1024
        if raw.endswith("KB"):
            return float(raw[:-2]) * 1000 / 1048576
        if raw.endswith("B"):
            return float(raw[:-1]) / 1048576
    except ValueError:
        pass
    return 0.0
t0 = time.time()
cls = {c: open("{}_c_{}.csv".format(prefix, c), "w", newline="") for c in containers}
writers = {}
try:
    agg = open(prefix + "_cluster.csv", "w", newline="")
    aw = csv.writer(agg)
    aw.writerow(["t_ms", "cpu_pct", "mem_mib"])
    for c, fh in cls.items():
        w = csv.writer(fh)
        w.writerow(["t_ms", "cpu_pct", "mem_mib"])
        writers[c] = w
    while client in ps_names():
        rows = stats_all(containers)
        t = int((time.time() - t0) * 1000)
        cpu = mem = 0.0
        for c in containers:
            a, b = rows.get(c, (0.0, 0.0))
            cpu += a
            mem += b
            writers[c].writerow([t, round(a, 1), round(b)])
            cls[c].flush()
        aw.writerow([t, round(cpu, 1), round(mem)])
        agg.flush()
        time.sleep(0.1)
finally:
    agg.close()
    for fh in cls.values():
        fh.close()
EOF
}

mkdir -p "$RESULTS" "$OUTPUT"
trap cleanup_containers EXIT
# Drop previous result files: a skipped mode (--mode standalone) must not report
# another invocation's distributed numbers as its own.
shopt -s nullglob
rm -f "$RESULTS"/rust_*.json "$RESULTS"/spark_*.json \
  "$RESULTS"/rust_*.csv "$RESULTS"/spark_*.csv
shopt -u nullglob

if [ "$PULL" = "1" ]; then
  echo "== pull prebuilt images"
  $CLI pull "$IMG_RUST"
  $CLI pull "$IMG_SPARK"
elif [ "$NO_BUILD" = "0" ]; then
  echo "== build images (Rust release build takes a while on first run;"
  echo "   rebuilds reuse cached dependency layers - or use --pull / --no-build)"
  $CLI build -f "$BENCH_DIR/rust/Dockerfile" -t "$IMG_RUST" "$ROOT"
  $CLI build -f "$BENCH_DIR/spark/Dockerfile" -t "$IMG_SPARK" "$BENCH_DIR/spark"
else
  echo "== --no-build: reusing local images as-is"
fi

echo "== start postgres"
$CLI network create "$NET" >/dev/null 2>&1 || true
$CLI volume create "$PGDATA" >/dev/null 2>&1 || true
if $CLI ps --format '{{.Names}}' | grep -qx "$PG"; then
  echo "postgres already running"
elif $CLI ps -a --format '{{.Names}}' | grep -qx "$PG"; then
  echo "postgres exists but stopped - starting it"
  $CLI start "$PG" >/dev/null
else
  $CLI run -d --name "$PG" --network "$NET" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="$PG_PASSWORD" -e POSTGRES_DB=app \
    -e TZ=UTC -e PGTZ=UTC \
    -v "$PGDATA:/var/lib/postgresql/data" \
    -v "$BENCH_DIR/PostgresDB/initdb:/docker-entrypoint-initdb.d:ro" \
    -p "127.0.0.1:${PG_PORT}:5432" \
    docker.io/library/postgres:17 \
    postgres -c track_activities=on -c autovacuum=on \
             -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4 >/dev/null
fi
echo -n "waiting for postgres"
for _ in $(seq 1 60); do
  if $CLI exec "$PG" pg_isready -U postgres -d app >/dev/null 2>&1; then break; fi
  echo -n "."; sleep 2
done
echo; $CLI exec "$PG" pg_isready -U postgres -d app

if [ "$SCALE" = "1" ]; then
  echo "== scale dataset toward $SCALE_ROWS rows"
  COUNT=$($CLI exec "$PG" psql -U postgres -d app -tAc 'select count(*) from public.orders')
  if [ "$COUNT" -eq 0 ]; then
    echo "orders table is empty - seed did not run?" >&2
    exit 1
  fi
  if [ "$COUNT" -ge "$SCALE_ROWS" ]; then
    echo "already at $COUNT rows, skipping scale"
  else
    # Ceiling division: how many copies of the current table reach the target.
    FACTOR=$(( (SCALE_ROWS - COUNT + COUNT - 1) / COUNT ))
    [ "$FACTOR" -ge 1 ] || FACTOR=1
    echo "growing $COUNT rows x$FACTOR toward ~$SCALE_ROWS"
    $CLI exec -i "$PG" psql -U postgres -d app -v ON_ERROR_STOP=1 \
      -v SCALE_FACTOR="$FACTOR" < "$BENCH_DIR/scale.sql"
  fi
fi
EXPECTED=$($CLI exec "$PG" psql -U postgres -d app -tAc 'select count(*) from public.orders')
echo "orders in db: $EXPECTED"

# Sample `stats` (CPU %, MEM) every 100ms until the container exits. 10Hz (not 1Hz)
# because benchmark runs are short and single-second ticks miss sub-second phases.
sample_stats() { # $1=container $2=csv
  echo "t_ms,cpu_pct,mem_mib" > "$2"
  local t0; t0=$(python3 -c 'import time; print(int(time.time()*1000))')
  while $CLI ps --format '{{.Names}}' | grep -qx "$1"; do
    line=$($CLI stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$1" 2>/dev/null || true)
    if [ -n "$line" ]; then
      # line looks like: 12.3% 1.5GiB / 15.6GiB (docker, IEC).
      cpu=$(echo "$line" | awk '{gsub(/%/,"",$1); print $1}')
      mem=$(echo "$line" | awk '{print to_mib($2)}
        function to_mib(s) {
          if (s ~ /TiB$/) { gsub(/TiB$/,"",s); return s*1048576 }
          if (s ~ /TB$/)  { gsub(/TB$/,"",s);  return s*1000000000000/1048576 }
          if (s ~ /GiB$/) { gsub(/GiB$/,"",s); return s*1024 }
          if (s ~ /GB$/)  { gsub(/GB$/,"",s);  return s*1000000000/1048576 }
          if (s ~ /MiB$/) { gsub(/MiB$/,"",s); return s+0 }
          if (s ~ /MB$/)  { gsub(/MB$/,"",s);  return s*1000000/1048576 }
          if (s ~ /KiB$/) { gsub(/KiB$/,"",s); return s/1024 }
          if (s ~ /KB$/)  { gsub(/KB$/,"",s);  return s*1000/1048576 }
          if (s ~ /B$/)   { gsub(/B$/,"",s);   return s/1048576 }
          return 0
        }')
      now_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
      echo "$((now_ms - t0)),$cpu,$mem" >> "$2"
    fi
    sleep 0.1
  done
}

run_rust() { # $1=tag $2=scenario $3=filter $4=columns $5=mode $6=stats prefix
  local tag=$1 scenario=$2 filter=$3 columns=$4 mode=$5 sprefix=$6
  local name=bench-rust sched_url=""
  if [ "$mode" = "distributed" ]; then
    name=bench-rust-dist
    sched_url="$SCHEDULER_URL"
  fi
  $CLI run -d --name "$name" --network "$NET" \
    -e BENCH_PG_PASSWORD="$PG_PASSWORD" -e BENCH_WORKERS="$WORKERS" \
    -e BENCH_OUTPUT=/output/orders_rust_${mode}_${scenario}.parquet \
    -e BENCH_FILTER="$filter" -e BENCH_COLUMNS="$columns" -e BENCH_SCENARIO="$scenario" \
    -e BENCH_SCHEDULER_URL="$sched_url" \
    -v "$OUTPUT:/output" \
    -v "$BENCH_DIR/bench-config.json:/etc/bench/config.json:ro" \
    "$IMG_RUST" >/dev/null
  # Mount sanity: a stale bind shows up as ENOENT minutes later otherwise.
  if $CLI ps --format '{{.Names}}' | grep -qx "$name" \
    && ! $CLI exec "$name" sh -c 'touch /output/.writetest && rm /output/.writetest' >/dev/null 2>&1; then
    echo "WARN: /output not writable inside $name (stale bind mount?)" >&2
  fi
  if [ "$mode" = "distributed" ]; then
    # Aggregate scheduler + workers + client: whole-cluster cost per tick, plus one
    # series per container so the breakdown survives alongside the totals.
    sample_cluster "$sprefix" "$name" \
      bench-scheduler $(seq -f "bench-worker-%g" 1 "$WORKERS") "$name" &
  else
    sample_stats "$name" "${sprefix}_cluster.csv" &
  fi
  local code; code=$($CLI wait "$name")
  wait
  local json; json=$($CLI logs "$name" 2>/dev/null | grep '^{' | tail -1)
  if [ "$code" != "0" ]; then
    echo "--- $name logs (tail) ---" >&2
    $CLI logs "$name" 2>&1 | tail -30 >&2
    echo "--- host output dir ---" >&2
    ls -la "$OUTPUT" >&2 || true
  fi
  $CLI rm -f "$name" >/dev/null
  [ "$code" = "0" ] || { echo "rust container failed (exit $code)" >&2; return 1; }
  [ -n "$json" ] || { echo "rust container produced no JSON summary" >&2; return 1; }
  echo "$json"
}

run_spark() { # $1=tag $2=scenario $3=filter $4=columns $5=stats prefix
  local tag=$1 scenario=$2 filter=$3 columns=$4 sprefix=$5
  local master="local[*]"
  [ -n "$SPARK_CORES" ] && master="local[$SPARK_CORES]"
  $CLI run -d --name bench-spark --network "$NET" --shm-size=1g \
    -e BENCH_PG_HOST="$PG" -e BENCH_PG_PORT=5432 -e BENCH_PG_DB=app \
    -e BENCH_PG_USER=postgres -e BENCH_PG_PASSWORD="$PG_PASSWORD" \
    -e BENCH_PARTITIONS="$SPARK_PARTITIONS" -e SPARK_DRIVER_MEM="$SPARK_DRIVER_MEM" \
    -e BENCH_SPARK_MASTER="$master" \
    -e BENCH_FETCHSIZE="$BATCH_SIZE" \
    -e BENCH_OUTPUT=/output/orders_spark_${scenario}.parquet \
    -e BENCH_FILTER="$filter" -e BENCH_COLUMNS="$columns" -e BENCH_SCENARIO="$scenario" \
    -v "$OUTPUT:/output" "$IMG_SPARK" >/dev/null
  # Mount sanity: a stale bind shows up as ENOENT minutes later otherwise.
  if $CLI ps --format '{{.Names}}' | grep -qx bench-spark \
    && ! $CLI exec bench-spark sh -c 'touch /output/.writetest && rm /output/.writetest' >/dev/null 2>&1; then
    echo "WARN: /output not writable inside bench-spark (stale bind mount?)" >&2
  fi
  sample_stats bench-spark "${sprefix}_cluster.csv" &
  local code; code=$($CLI wait bench-spark)
  wait
  local json; json=$($CLI logs bench-spark 2>/dev/null | grep '^{' | tail -1)
  if [ "$code" != "0" ]; then
    echo "--- bench-spark logs (tail) ---" >&2
    $CLI logs bench-spark 2>&1 | tail -30 >&2
    echo "--- host output dir ---" >&2
    ls -la "$OUTPUT" >&2 || true
  fi
  $CLI rm -f bench-spark >/dev/null
  [ "$code" = "0" ] || { echo "spark container failed (exit $code)" >&2; return 1; }
  [ -n "$json" ] || { echo "spark container produced no JSON summary" >&2; return 1; }
  echo "$json"
}

# Merge CPU/MEM peaks into the JSON summary (self-contained artifact): cluster totals
# from <prefix>_cluster.csv plus a per-container breakdown from <prefix>_c_*.csv.
finalize_json() { # $1=json $2=file prefix (dir + stem, no suffix)
  python3 - "$1" "$2" <<'EOF'
import csv, glob, json, os, sys
jpath, prefix = sys.argv[1], sys.argv[2]
doc = json.load(open(jpath))

def peaks(path):
    cpu, mem = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            try:
                cpu.append(float(row["cpu_pct"])); mem.append(float(row["mem_mib"]))
            except ValueError:
                pass
    if not cpu:
        return None
    return {"avg_cpu_pct": round(sum(cpu) / len(cpu), 1),
            "peak_cpu_pct": round(max(cpu), 1),
            "peak_rss_mib": round(max(mem))}

cluster = peaks(prefix + "_cluster.csv")
doc["avg_cpu_pct"] = cluster["avg_cpu_pct"]
doc["peak_cpu_pct"] = cluster["peak_cpu_pct"]
doc["peak_rss_mib"] = cluster["peak_rss_mib"]
doc["containers"] = {}
for path in sorted(glob.glob(prefix + "_c_*.csv")):
    name = os.path.basename(path)[len(os.path.basename(prefix)) + 3:-4]
    p = peaks(path)
    if p is not None:
        doc["containers"][name] = p
json.dump(doc, open(jpath, "w"), indent=2)
print("{} avg_cpu={} peak_cpu={} peak_rss={}MiB containers={}".format(
    doc.get("engine"), doc["avg_cpu_pct"], doc["peak_cpu_pct"],
    doc["peak_rss_mib"], sorted(doc["containers"])))
EOF
}

best_of() { # $1=tag $2=engine $3=mode $4=scenario $5=filter $6=columns
  # tag namespaces files, e.g. rust_standalone / rust_distributed / spark.
  # Each attempt writes to its own stats prefix; only the winner is promoted.
  local tag=$1 engine=$2 mode=$3 scenario=$4 filter=$5 columns=$6
  local ms best_ms="" best_i=0 i json sprefix
  for i in $(seq 1 "$REPEAT"); do
    echo "-- $tag/$scenario attempt $i/$REPEAT"
    # Clear BEFORE the attempt, not after: Spark refuses to write over an existing
    # path, and deleting after would remove the final attempt's output that the
    # correctness check compares.
    rm -rf "$OUTPUT"/orders_*_${scenario}.parquet
    sprefix="$RESULTS/.${tag}_${scenario}_try${i}"
    if [ "$engine" = "rust" ]; then json=$(run_rust "$tag" "$scenario" "$filter" "$columns" "$mode" "$sprefix");
    else json=$(run_spark "$tag" "$scenario" "$filter" "$columns" "$sprefix"); fi
    echo "$json" > "$RESULTS/.${tag}_${scenario}_try${i}.json"
    ms=$(echo "$json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["elapsed_ms"])')
    if [ -z "$best_ms" ] || [ "$ms" -lt "$best_ms" ]; then
      best_ms=$ms
      best_i=$i
    fi
  done
  cp "$RESULTS/.${tag}_${scenario}_try${best_i}.json" "$RESULTS/${tag}_${scenario}.json"
  mv "$RESULTS/.${tag}_${scenario}_try${best_i}_cluster.csv" "$RESULTS/${tag}_${scenario}_cluster.csv"
  shopt -s nullglob
  for f in "$RESULTS"/."${tag}_${scenario}"_try"${best_i}"_c_*.csv; do
    name=${f##*_c_}; name=${name%.csv}
    mv "$f" "$RESULTS/${tag}_${scenario}_c_${name}.csv"
  done
  shopt -u nullglob
  rm -f "$RESULTS"/."${tag}_${scenario}"_try*.json "$RESULTS"/."${tag}_${scenario}"_try*.csv
  finalize_json "$RESULTS/${tag}_${scenario}.json" "$RESULTS/${tag}_${scenario}"
  echo "$tag/$scenario best: ${best_ms}ms"
}

# Scenarios: full load (everything) + selective (indexed ~3% slice + narrow projection).
# Same filter/projection strings go to both engines verbatim.
FULL_FILTER=""; FULL_COLUMNS=""
SELECTIVE_FILTER="status = 'REFUNDED'"; SELECTIVE_COLUMNS="order_id,amount,status"

EXPECTED_FULL=$($CLI exec "$PG" psql -U postgres -d app -tAc 'select count(*) from public.orders')
EXPECTED_SELECTIVE=$($CLI exec "$PG" psql -U postgres -d app -tAc "select count(*) from public.orders where $SELECTIVE_FILTER")

echo "expected rows: full=$EXPECTED_FULL selective=$EXPECTED_SELECTIVE"
# Batch mode: empty BATCH_SIZE = auto (each tool's own default); set = manual override
# applied to both engines. Partition counts always derive from a reference batch size
# (64000 unless overridden) so an auto run and a manual run differ ONLY in per-batch
# streaming, never in fan-out.
if [ -n "$BATCH_SIZE" ]; then
  [ "$BATCH_SIZE" -ge 1 ] 2>/dev/null || { echo "BATCH_SIZE must be >= 1 (got '$BATCH_SIZE')" >&2; exit 1; }
  echo "batch size: manual $BATCH_SIZE rows/batch on both engines"
else
  echo "batch size: auto (rust default 8192/batch, spark JDBC default = driver default)"
fi
REF_BATCH=${BATCH_SIZE:-64000}
if [ -z "$RUST_PARTITIONS" ]; then
  RUST_PARTITIONS=$(( (EXPECTED_FULL + REF_BATCH - 1) / REF_BATCH ))
  [ "$RUST_PARTITIONS" -ge 1 ] || RUST_PARTITIONS=1
fi
if [ -z "$SPARK_PARTITIONS" ]; then
  SPARK_PARTITIONS=$(( (EXPECTED_FULL + REF_BATCH - 1) / REF_BATCH ))
  [ "$SPARK_PARTITIONS" -ge 1 ] || SPARK_PARTITIONS=1
fi
echo "partitions: rust=$RUST_PARTITIONS spark=$SPARK_PARTITIONS (ceil($EXPECTED_FULL / $REF_BATCH); override with --rust-partitions / --spark-partitions)"

# Omit batch_size in auto mode so the Rust code default (8192) applies.
BATCH_JSON=""; [ -n "$BATCH_SIZE" ] && BATCH_JSON="\"batch_size\": $BATCH_SIZE"
# Generate bench-config.json with the specified parallel strategy
cat > "$BENCH_DIR/bench-config.json" <<EOF
{
  "job_id": "orders_bench",
  "table": "orders",
  "source": {
    "host": "bench-pg",
    "port": 5432,
    "user": "postgres",
    "password_env": "BENCH_PG_PASSWORD",
    "database": "app",
    "pool_max": 8,
    "statement_timeout_ms": 300000,
    "application_name": "rel-bench-rust",
    "schema": "public"
  },
  "incremental": {
    "column": "updated_at",
    "safety_lag_secs": 300,
    "max_window_secs": 21600
  },
  "checkpoint": {
    "dir": "/tmp/.checkpoints"
  },
  "sink": {
    "path": "/tmp/bench_sink"
  },
  "pushdown": {
    "policy": "cost_based",
    "deny": [],
    "push": []
  },
  "parallel_scan": {
    "strategy": "$PARALLEL_STRATEGY",
    "partitions": $RUST_PARTITIONS,
    "partition_column": "order_id"
  },
  "execution": { ${BATCH_JSON} },
  "distributed": {
    "scheduler_url": "",
    "workers": $WORKERS
  }
}
EOF

run_mode() { # $1=mode(standalone|distributed)
  local mode=$1 tag
  if [ "$mode" = "distributed" ]; then
    start_cluster
    tag=rust_distributed
  else
    tag=rust_standalone
  fi
  echo "== rust/$mode scenario: full ($REPEAT attempt(s), $WORKERS workers)"
  best_of "$tag" rust "$mode" full "$FULL_FILTER" "$FULL_COLUMNS"
  echo "== rust/$mode scenario: selective ($REPEAT attempt(s))"
  best_of "$tag" rust "$mode" selective "$SELECTIVE_FILTER" "$SELECTIVE_COLUMNS"
  if [ "$mode" = "distributed" ]; then
    stop_cluster
  fi
}

case "$MODE" in
  standalone)
    run_mode standalone
    ;;
  distributed)
    run_mode distributed
    ;;
  both)
    run_mode standalone
    run_mode distributed
    ;;
esac

echo "== spark scenarios ($REPEAT attempt(s), $SPARK_PARTITIONS partitions)"
best_of spark spark local full "$FULL_FILTER" "$FULL_COLUMNS"
best_of spark spark local selective "$SELECTIVE_FILTER" "$SELECTIVE_COLUMNS"

report_table() { # $1=results dir -> padded markdown comparison table on stdout
  python3 - "$1" <<'EOF'
import glob, json, os, sys
results = sys.argv[1]
header = ["engine", "scenario", "rows", "elapsed", "avg CPU%", "max CPU%", "max RSS MiB"]
rows = []
for pat in ("rust_*_full.json", "spark_full.json",
            "rust_*_selective.json", "spark_selective.json"):
    for path in sorted(glob.glob(os.path.join(results, pat))):
        d = json.load(open(path))
        rows.append([d["engine"], d["scenario"], str(d["rows"]),
                     "{}ms".format(d["elapsed_ms"]),
                     str(d["avg_cpu_pct"]), str(d["peak_cpu_pct"]),
                     str(d["peak_rss_mib"])])
        for name in sorted(d.get("containers", {})):
            c = d["containers"][name]
            rows.append(["", "", "`{}`".format(name),
                         "avg {}% / peak {}% / {}MiB".format(
                             c["avg_cpu_pct"], c["peak_cpu_pct"], c["peak_rss_mib"]),
                         "", "", ""])
widths = [len(h) for h in header]
for r in rows:
    widths = [max(w, len(c)) for w, c in zip(widths, r)]
def line(cells):
    return "| " + " | ".join(c.ljust(w) for c, w in zip(cells, widths)) + " |"
print(line(header))
print("|-" + "-|-".join("-" * w for w in widths) + "-|")
for r in rows:
    print(line(r))
EOF
}

{
  echo "# Benchmark: initial loads, public.orders"
  echo
  echo "Full: SELECT * over all columns -> Snappy Parquet. Selective: WHERE $SELECTIVE_FILTER + projection ($SELECTIVE_COLUMNS). Rust standalone: in-process Ballista, $RUST_PARTITIONS keyset partitions. Rust distributed: scheduler + $WORKERS workers over the network (CPU/RSS summed across the cluster; per-container peaks below). Spark 3.5.4 local ($([ -n "$SPARK_CORES" ] && echo "$SPARK_CORES cores" || echo "all cores")): $SPARK_PARTITIONS JDBC partitions on order_id. Batch: $([ -n "$BATCH_SIZE" ] && echo "$BATCH_SIZE rows/batch both engines" || echo "tool defaults (rust 8192, spark driver default)"). Sequential runs, best of $REPEAT."
  echo
  report_table "$RESULTS"
  echo
} | tee "$RESULTS/summary.md"

GATE="PASS"
check_gate() { # $1=json $2=expected $3=label
  local rows; rows=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["rows"])' "$1")
  [ "$rows" = "$2" ] || GATE="FAIL ($3: got $rows, want $2)"
}
for f in "$RESULTS"/rust_*_full.json "$RESULTS"/spark_full.json; do
  [ -f "$f" ] || continue
  check_gate "$f" "$EXPECTED_FULL" "$(basename "$f" .json)/full"
done
for f in "$RESULTS"/rust_*_selective.json "$RESULTS"/spark_selective.json; do
  [ -f "$f" ] || continue
  check_gate "$f" "$EXPECTED_SELECTIVE" "$(basename "$f" .json)"
done
echo "Correctness gate: **$GATE**" | tee -a "$RESULTS/summary.md"

[ "$GATE" = "PASS" ] || exit 1

if [ "$SKIP_CORRECTNESS" = "1" ]; then
  echo "correctness check skipped (--skip-correctness)" | tee -a "$RESULTS/summary.md"
  exit 0
fi

if ! python3 -c "import duckdb" 2>/dev/null; then
  echo "correctness check needs the duckdb python package for $(command -v python3):" >&2
  echo "  python3 -m pip install duckdb   (or rerun with --skip-correctness)" >&2
  echo "NOTE: plain 'pip install' may target a different interpreter; use 'python3 -m pip'." >&2
  exit 1
fi

echo "== independent row-by-row correctness check (benchmark/correctness.py)"
if python3 "$BENCH_DIR/correctness.py" "$OUTPUT" "$RESULTS/correctness.json"; then
  {
    echo
    echo "Row-by-row file check (independent DuckDB read of every output): **PASS**"
  } | tee -a "$RESULTS/summary.md"
else
  {
    echo
    echo "Row-by-row file check: **FAIL** — see $RESULTS/correctness.json"
  } | tee -a "$RESULTS/summary.md"
  exit 1
fi
