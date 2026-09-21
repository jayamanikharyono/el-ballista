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
#                    [--cpus N] [--cpuset-cpus RANGE] [--memory SIZE]
#                    [--scheduler-memory SIZE] [--client-memory SIZE] [--worker-memory SIZE]
#
# CPU + memory budget (every container the script starts):
#   CPU defaults to a hard pin on cores 0-3 (--cpuset-cpus 0-3). Equality is enforced
#   ONLY at this container boundary: inside, every runtime runs unrestricted (Spark
#   local[*], DataFusion defaults, Ballista visible-CPU slots) and takes all the CPUs
#   the container offers. A host with fewer than 4 cores fails fast here instead of
#   silently benchmarking the wrong budget. --cpuset-cpus RANGE pins a different set
#   ("" lifts the pin); --cpus N replaces the pin with a softer CFS quota.
#   Never cap a runtime from the inside (--spark-cores, BENCH_CONCURRENT_TASKS) for
#   headline numbers — those knobs are diagnostics only; capping one side while the
#   other runs free is exactly the "different spec" this harness exists to prevent.
#   Memory (--memory, default 4g) is the per-deployment budget: Spark and the
#   standalone Rust client each get it in full; the distributed deployment (scheduler +
#   client + workers) SHARES it — scheduler/client take --scheduler-memory/--client-memory
#   (512m each) and each worker gets --worker-memory (default: the remainder split evenly).
#   Postgres is NOT on this budget: bench-pg is hardcoded to --cpuset-cpus=4-5 --memory=512m
#   (PG_CPU_FLAGS/PG_MEM_FLAGS) so the shared fixture never changes shape between runs.
#   Pass an empty value to lift any default (e.g. --memory "" = unconstrained).
#   Also settable via CPUSET_CPUS / CPUS / MEMORY / SCHEDULER_MEMORY / CLIENT_MEMORY /
#   WORKER_MEMORY env.
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
#            SCALE_ROWS (default 10000000: grow orders toward ~10M rows; --skip-scale bypasses),
#            SPARK_DRIVER_MEM (default: solved from the container budget via Spark's
#            own overhead formula, max(384m, 10% of heap); explicit value always wins;
#            on tiny hosts the computed heap can OOM mid-write — set it explicitly),
#            BENCH_CONCURRENT_TASKS (default empty: each worker uses all its visible
#            CPUs as task slots — the pinned budget, never capped for headline runs).
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
# Postgres test budget — HARDCODED, not a flag: every benchmark run faces the same
# source database (cores 4-5, 512m RAM). A hard pin, not a quota: like the engines, PG
# gets named cores or the run fails fast on a smaller host instead of silently
# benchmarking a different fixture. Cores sit clear of the engine pin (0-3) so the
# fixture never contends with the engines. A shared fixture must never change shape
# between runs; override by editing here, never per invocation, so results stay
# comparable.
PG_CPU_FLAGS="--cpuset-cpus=4-5"
PG_MEM_FLAGS="--memory=2g"
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
SCALE_ROWS="${SCALE_ROWS:-10000000}"
SPARK_DRIVER_MEM="${SPARK_DRIVER_MEM:-}"
# Explicit heap wins; otherwise it is solved from the container budget below (after
# MEMORY resolves) via Spark's overhead formula — see the derivation block.
# Spark concurrency: local[N] caps simultaneous tasks (each buffers a partition).
# Empty = all host CPUs. Set to fit the box, e.g. --spark-cores 2 on a 2GB machine.
SPARK_CORES=""
# Container CPU/memory budget (see header). Env values count as explicit, same as flags.
_CPUSET_ENV="${CPUSET_CPUS:-}"
_CPUS_ENV="${CPUS:-}"
_MEMORY_ENV="${MEMORY:-}"
_SCHED_ENV="${SCHEDULER_MEMORY:-}"
_CLIENT_ENV="${CLIENT_MEMORY:-}"
_WORKER_ENV="${WORKER_MEMORY:-}"
CPUSET_CPUS="${_CPUSET_ENV:-0-3}"
CPUS="$_CPUS_ENV"
MEMORY="${_MEMORY_ENV:-4g}"
SCHEDULER_MEMORY="$_SCHED_ENV"
CLIENT_MEMORY="$_CLIENT_ENV"
WORKER_MEMORY="$_WORKER_ENV"
CPUSET_GIVEN=0; [ -n "$_CPUSET_ENV" ] && CPUSET_GIVEN=1
CPUS_GIVEN=0; [ -n "$_CPUS_ENV" ] && CPUS_GIVEN=1
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
    --cpuset-cpus) CPUSET_CPUS="$2"; CPUSET_GIVEN=1; shift 2 ;;
    --cpus) CPUS="$2"; CPUS_GIVEN=1; shift 2 ;;
    --memory) MEMORY="$2"; shift 2 ;;
    --scheduler-memory) SCHEDULER_MEMORY="$2"; shift 2 ;;
    --client-memory) CLIENT_MEMORY="$2"; shift 2 ;;
    --worker-memory) WORKER_MEMORY="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

case "$MODE" in
  standalone|distributed|both) ;;
  *) echo "--mode must be standalone, distributed, or both" >&2; exit 2 ;;
esac

if ! command -v docker >/dev/null 2>&1; then echo "need docker CLI" >&2; exit 1; fi
CLI=docker

# Container CPU/memory budget for every `docker run` below (postgres, scheduler,
# workers, both engines). Unquoted `$CPU_FLAGS`/`$MEM_*` at the call sites is
# intentional (word splitting).
# An explicit quota replaces the default pin (not an addition): keeping the 0-3 pin
# alongside an explicitly requested quota would benchmark a different budget than asked.
if [ "$CPUS_GIVEN" = 1 ] && [ "$CPUSET_GIVEN" = 0 ]; then CPUSET_CPUS=""; fi
CPU_FLAGS=""
[ -n "$CPUSET_CPUS" ] && CPU_FLAGS="$CPU_FLAGS --cpuset-cpus=$CPUSET_CPUS"
[ -n "$CPUS" ] && CPU_FLAGS="$CPU_FLAGS --cpus=$CPUS"

# Peak correction: a cpuset pin is kernel-enforced, so no true peak can ever exceed
# pin_cpus x 100%. Measured per-tick sums still overshoot it (misaligned per-container
# windows + counter granularity), so finalize_json caps peaks at this ceiling. The cap
# only ever removes artifact — real signal is always below it by physics. Quota
# (--cpus) caps the same way; unset = no cap. Averages are never capped: their errors
# cancel out, and an average above the ceiling would be a real red flag worth seeing.
cpuset_count() { # 0-3 -> 4, 0,2 -> 2, 0-1,3 -> 3
  local spec=$1 count=0 part lo hi oldifs=$IFS
  IFS=','
  for part in $spec; do
    IFS=$oldifs
    case "$part" in
      *-*) lo=${part%-*}; hi=${part#*-}
        [ "$lo" -eq "$lo" ] 2>/dev/null && [ "$hi" -eq "$hi" ] 2>/dev/null \
          || { echo "bad --cpuset-cpus range '$spec'" >&2; exit 2; }
        count=$(( count + hi - lo + 1 )) ;;
      *) [ "$part" -eq "$part" ] 2>/dev/null \
          || { echo "bad --cpuset-cpus range '$spec'" >&2; exit 2; }
        count=$(( count + 1 )) ;;
    esac
    IFS=','
  done
  IFS=$oldifs
  [ "$count" -ge 1 ] || { echo "bad --cpuset-cpus range '$spec'" >&2; exit 2; }
  echo "$count"
}
PIN_MAX_PCT=""
if [ -n "$CPUSET_CPUS" ]; then PIN_MAX_PCT=$(( $(cpuset_count "$CPUSET_CPUS") * 100 ));
elif [ -n "$CPUS" ]; then PIN_MAX_PCT=$(python3 -c "import sys
try: print(int(float(sys.argv[1]) * 100))
except ValueError: sys.exit('bad --cpus value %r' % sys.argv[1])" "$CPUS"); fi
export PIN_MAX_PCT
# Per-role memory. `--memory` is the engine-deployment budget: postgres, Spark, and the
# standalone Rust client each get it in full, while the distributed deployment SHARES it
# (scheduler + dist-client take fixed slices, workers split the rest). Explicit role
# flags override their slice; empty `--memory` lifts every default at once.
_mib() { # 4g->4096, 512m->512, plain number passes through; fails otherwise
  case "$1" in
    *[gG]) n=${1%[gG]}; [ "$n" -eq "$n" ] 2>/dev/null || return 1; echo $(( n * 1024 )) ;;
    *[mM]) n=${1%[mM]}; [ "$n" -eq "$n" ] 2>/dev/null || return 1; echo "$n" ;;
    *) [ "$1" -eq "$1" ] 2>/dev/null || return 1; echo "$1" ;;
  esac
}
[ "$WORKERS" -eq "$WORKERS" ] 2>/dev/null && [ "$WORKERS" -ge 1 ] \
  || { echo "--workers must be an integer >= 1 (got '$WORKERS')" >&2; exit 2; }
SCHED_MEM="$SCHEDULER_MEMORY"; CLIENT_MEM="$CLIENT_MEMORY"; WORKER_MEM="$WORKER_MEMORY"
if [ -n "$MEMORY" ]; then
  ENGINE_MIB=$(_mib "$MEMORY") \
    || { echo "--memory must look like 4g or 512m (got '$MEMORY')" >&2; exit 2; }
  [ -z "$SCHED_MEM" ] && SCHED_MEM="512m"
  [ -z "$CLIENT_MEM" ] && CLIENT_MEM="512m"
  # No worker share in standalone mode (no cluster is started); the floor below only
  # guards real distributed layouts, so `--memory 1g --mode standalone` just works.
  if [ -z "$WORKER_MEM" ] && [ "$MODE" != "standalone" ]; then
    # Floor applies to the DERIVED share only: an explicit --worker-memory bypasses
    # it (your risk — a too-small worker OOMs mid-run instead of failing here).
    WORKER_MIB=$(( (ENGINE_MIB - 1024) / WORKERS ))
    [ "$WORKER_MIB" -ge 256 ] || {
      echo "engine budget $MEMORY is too small for $WORKERS worker(s): each needs >= 256m after the 512m scheduler + 512m client slices (or set --worker-memory explicitly)" >&2
      exit 2
    }
    WORKER_MEM="${WORKER_MIB}m"
  fi
fi
MEM_SPARK=""; MEM_RUST=""; MEM_SCHED=""; MEM_CLIENT=""; MEM_WORKER=""
if [ -n "$MEMORY" ]; then MEM_SPARK="--memory=$MEMORY"; MEM_RUST="--memory=$MEMORY"; fi
[ -n "$SCHED_MEM" ] && MEM_SCHED="--memory=$SCHED_MEM"
[ -n "$CLIENT_MEM" ] && MEM_CLIENT="--memory=$CLIENT_MEM"
[ -n "$WORKER_MEM" ] && MEM_WORKER="--memory=$WORKER_MEM"
# Spark heap tracks the Spark container budget using Spark's own overhead formula
# (overhead = max(384m, 10% of heap)): heap is solved so heap + overhead fits MEMORY
# exactly — heap = MEM - 384 below the 3840m-heap crossover, heap = floor(MEM / 1.1)
# above it. An explicit SPARK_DRIVER_MEM always wins. A budget that leaves < 1g heap
# fails fast — an oversized heap OOMs mid-write as a cryptic Py4JNetworkError, which
# is worse than an upfront error.
if [ -z "$SPARK_DRIVER_MEM" ]; then
  if [ -n "$MEMORY" ]; then
    if [ "$ENGINE_MIB" -ge 4224 ]; then
      DRIVER_MIB=$(( ENGINE_MIB * 10 / 11 ))
    else
      # Small budgets land here: Spark's minimum overhead (384m), heap takes the rest.
      DRIVER_MIB=$(( ENGINE_MIB - 384 ))
    fi
    if [ "$DRIVER_MIB" -le 0 ]; then
      echo "engine budget $MEMORY leaves no room for any Spark heap: raise --memory or set SPARK_DRIVER_MEM explicitly" >&2
      exit 2
    fi
    if [ "$DRIVER_MIB" -lt 1024 ]; then
      echo "WARN: derived Spark heap is only ${DRIVER_MIB}m (minimum-overhead fallback) — smoke on the seed only, it will OOM on large data" >&2
    fi
    SPARK_DRIVER_MEM="${DRIVER_MIB}m"
  else
    SPARK_DRIVER_MEM="2g"
  fi
fi
# Unquoted `$CPU_FLAGS`/`$MEM_*` at the call sites is intentional (word splitting).
if [ -n "$CPU_FLAGS$MEM_SPARK$MEM_SCHED$MEM_CLIENT$MEM_WORKER" ]; then
  echo "container budget:$CPU_FLAGS mem pg=[$PG_CPU_FLAGS $PG_MEM_FLAGS] spark/standalone=[${MEM_SPARK:-none}] sched=[${MEM_SCHED:-none}] dist-client=[${MEM_CLIENT:-none}] worker(each)=[${MEM_WORKER:-none}] spark-heap=[$SPARK_DRIVER_MEM]"
else echo "container budget: unconstrained (all host CPUs/memory)"; fi

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
  # shellcheck disable=SC2086
  $CLI run -d $CPU_FLAGS $MEM_SCHED --name bench-scheduler --hostname bench-scheduler --network "$NET" \
    -e BENCH_ROLE=scheduler -e BENCH_SCHEDULER_URL="$SCHEDULER_URL" \
    -e BENCH_PG_PASSWORD="$PG_PASSWORD" \
    -p "127.0.0.1:${SCHED_API_PORT}:50050" \
    "$IMG_RUST" >/dev/null
  verify_pin bench-scheduler

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
    # shellcheck disable=SC2086
    $CLI run -d $CPU_FLAGS $MEM_WORKER --name "bench-worker-$i" --hostname "bench-worker-$i" --network "$NET" \
      -e BENCH_ROLE=worker -e BENCH_SCHEDULER_URL="$SCHEDULER_URL" \
      -e BENCH_PG_PASSWORD="$PG_PASSWORD" \
      -e BENCH_CONCURRENT_TASKS="${BENCH_CONCURRENT_TASKS:-}" \
      "$IMG_RUST" >/dev/null
    verify_pin "bench-worker-$i"
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

# Evidence, not trust: confirm the daemon actually applied the requested cpuset to
# a started container. Catches the silent killers — a stale bench-pg reused from an
# earlier unpinned run, or a daemon ignoring the flag. Called after every `docker run`.
verify_pin() { # $1=container [$2=expected cpuset; default $CPUSET_CPUS; "" = expect unpinned] [$3=expected mem like 2g; empty = print only]
  # All output goes to stderr: this runs inside run_rust/run_spark, whose stdout is
  # captured as the engine JSON summary — anything on stdout corrupts best_of's parse.
  local want got rawmem mem wantmem
  if [ $# -ge 2 ]; then want=$2; else want=$CPUSET_CPUS; fi
  if [ $# -ge 3 ]; then wantmem=$3; else wantmem=""; fi
  got=$($CLI inspect -f '{{.HostConfig.CpusetCpus}}' "$1" 2>/dev/null || true)
  rawmem=$($CLI inspect -f '{{.HostConfig.Memory}}' "$1" 2>/dev/null || true)
  if [ "$rawmem" = "0" ] || [ -z "$rawmem" ]; then mem="unlimited"; else mem="$(( rawmem / 1048576 ))m"; fi
  if [ "$got" != "$want" ]; then
    echo "WARN: container $1 cpuset is [${got:-unpinned}], expected [${want:-unpinned}] — CPU budget NOT enforced; numbers are not comparable" >&2
  else
    echo "pin: $1 -> [${got:-unpinned}] mem=[$mem]" >&2
  fi
  if [ -n "$wantmem" ]; then
    local wantbytes
    wantbytes=$(_mib "$wantmem") || wantbytes=""
    if [ -n "$wantbytes" ] && [ "$rawmem" != "$(( wantbytes * 1048576 ))" ]; then
      echo "WARN: container $1 memory limit is [$mem], expected [$wantmem] — recreate it (docker rm -f $1); numbers are not comparable" >&2
    fi
  fi
}

# Aggregate sampler: sums CPU% and RSS across every cluster container each tick, so the
# distributed series represents whole-cluster cost, comparable to single-container runs.
sample_cluster() { # $1=file prefix $2=client-container, rest...=all containers
  # Writes <prefix>_cluster.csv (summed CPU/RSS per tick) plus <prefix>_c_<name>.csv
  # per container, so both the whole-cluster cost and who spent it survive.
  # Engine-API poller (no `docker stats` CLI per tick): one-shot stats carries
  # precpu_stats, so every CPU% is an exact daemon-side delta. `--single` skips the
  # per-container files (used by sample_stats); the tail sample catches bursts that
  # finished between the last tick and client exit. Interval via BENCH_SAMPLE_INTERVAL.
  local prefix=$1 client=$2; shift 2
  # shellcheck disable=SC2068
  python3 - "$prefix" "$client" "$@" <<'EOF'
import csv, http.client, json, os, socket, sys, time, urllib.parse
prefix, client = sys.argv[1], sys.argv[2]
args = sys.argv[3:]
# Optional `--pg NAME`: the source database, sampled on the same ticks into
# <prefix>_pg.csv but NEVER summed into the engine totals — it answers "was Postgres
# the bottleneck?" per run without polluting cross-engine comparability.
pg = None
if "--pg" in args:
    i = args.index("--pg")
    pg = args[i + 1]
    args = args[:i] + args[i + 2:]
single = len(args) > 0 and args[0] == "--single"
containers = args[1:] if single else args
INTERVAL = float(os.environ.get("BENCH_SAMPLE_INTERVAL", "0.1"))

def find_socket():
    host = os.environ.get("DOCKER_HOST", "")
    if host.startswith("unix://"):
        return host[len("unix://"):]
    if host.startswith("tcp://") or host.startswith("ssh://"):
        raise SystemExit("sample: DOCKER_HOST is remote; this sampler needs a local unix socket")
    for p in ("/var/run/docker.sock",
              os.path.expanduser("~/.docker/run/docker.sock")):
        if os.path.exists(p):
            return p
    raise SystemExit("sample: no docker socket found")

class UnixConn(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost")
        self._sock_path = path
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect(self._sock_path)

SOCK = find_socket()

def api(method, target):
    c = UnixConn(SOCK)
    c.request(method, target)
    r = c.getresponse()
    body = r.read()
    c.close()
    return r.status, body

status, body = api("GET", "/version")
ver = json.loads(body).get("ApiVersion", "1.41") if status == 200 else "1.41"
BASE = "/v%s" % ver

def api_json(method, target):
    status, body = api(method, target)
    if status == 404:
        return None
    if status != 200:
        raise RuntimeError("docker API %s %s -> %s" % (method, target, status))
    return json.loads(body)

def raw_stats(name):
    # Cumulative counters (never a point-in-time guess): the caller differences
    # consecutive reads, so every interval after the first is exact even when a burst
    # falls between polls. (The old `docker stats --no-stream` CLI loop reported 0 for
    # exactly those.) Previous counters are tracked here, not taken from precpu_stats:
    # the daemon omits zero-valued previous fields, which would corrupt the delta.
    doc = api_json("GET", BASE + "/containers/%s/stats?stream=false&one-shot=true"
                   % urllib.parse.quote(name))
    if doc is None:
        return None
    try:
        total = doc["cpu_stats"]["cpu_usage"]["total_usage"]
        sysc = doc["cpu_stats"]["system_cpu_usage"]
        # Normalization base: host-wide online_cpus is the CORRECT base for the
        # standard convention (100% = one full core) — verified live: a pinned
        # single-core busy loop reads 100%. (An earlier revision normalized by the
        # pin size and read 50% for the same load — wrong units.) Single-tick spikes
        # from counter-granularity mismatch are handled downstream: finalize_json
        # takes peaks over a 500ms rolling window, not raw ticks.
        ncpu = (doc["cpu_stats"].get("online_cpus")
                or len(doc["cpu_stats"]["cpu_usage"].get("percpu_usage") or [1]))
        mem = doc["memory_stats"].get("usage", 0) / 1048576.0
    except (KeyError, TypeError):
        return None
    return (total, sysc, ncpu, mem)

def running(name):
    doc = api_json("GET", BASE + "/containers/%s/json" % urllib.parse.quote(name))
    return bool(doc) and doc.get("State", {}).get("Running", False)

prev = {}

def sample_one(name):
    try:
        r = raw_stats(name)
    except Exception:
        r = None
    if r is None:
        return (0.0, 0.0)
    total, sysc, ncpu, mem = r
    p = prev.get(name)
    if p is None or sysc <= p[1]:
        pct = 0.0
    else:
        pct = (total - p[0]) / (sysc - p[1]) * ncpu * 100.0
        if pct < 0.0:
            pct = 0.0
    prev[name] = (total, sysc)
    return (pct, mem)

def sample_all():
    return {c: sample_one(c) for c in containers}

def write_row(writers, aw, rows, t):
    cpu = mem = 0.0
    for c in containers:
        a, b = rows[c]
        cpu += a
        mem += b
        if c in writers:
            writers[c].writerow([t, round(a, 1), round(b)])
    aw.writerow([t, round(cpu, 1), round(mem)])

t0 = time.time()
cls = {} if single else {c: open("{}_c_{}.csv".format(prefix, c), "w", newline="")
                         for c in containers}
writers = {}
pgf = open(prefix + "_pg.csv", "w", newline="") if pg else None
pgw = None
try:
    agg = open(prefix + "_cluster.csv", "w", newline="")
    aw = csv.writer(agg)
    aw.writerow(["t_ms", "cpu_pct", "mem_mib"])
    if pgf is not None:
        pgw = csv.writer(pgf)
        pgw.writerow(["t_ms", "cpu_pct", "mem_mib"])
    for c, fh in cls.items():
        w = csv.writer(fh)
        w.writerow(["t_ms", "cpu_pct", "mem_mib"])
        writers[c] = w
    n = 0
    while True:
        try:
            alive = running(client)
        except Exception:
            break
        if not alive:
            break
        try:
            rows = sample_all()
            pgrow = sample_one(pg) if pg else None
        except Exception:
            rows = {c: (0.0, 0.0) for c in containers}
            pgrow = None
        t = int((time.time() - t0) * 1000)
        write_row(writers, aw, rows, t)
        if pgw is not None and pgrow is not None:
            pgw.writerow([t, round(pgrow[0], 1), round(pgrow[1])])
        agg.flush()
        for fh in cls.values():
            fh.flush()
        if pgf is not None:
            pgf.flush()
        n += 1
        time.sleep(max(0.0, t0 + n * INTERVAL - time.time()))
    # Tail sample: catches compute that finished between the last tick and client exit.
    try:
        rows = sample_all()
        pgrow = sample_one(pg) if pg else None
    except Exception:
        rows = {c: (0.0, 0.0) for c in containers}
        pgrow = None
    t = int((time.time() - t0) * 1000)
    write_row(writers, aw, rows, t)
    if pgw is not None and pgrow is not None:
        pgw.writerow([t, round(pgrow[0], 1), round(pgrow[1])])
    agg.flush()
finally:
    agg.close()
    if pgf is not None:
        pgf.close()
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
  $CLI run -d $PG_CPU_FLAGS $PG_MEM_FLAGS --name "$PG" --network "$NET" \
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
# Verify even when reusing an existing container: a stale bench-pg from an earlier
# run would otherwise silently benchmark a different spec.
verify_pin "$PG" "4-5" "2g"

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

# Sample CPU/MEM over the Engine API every 100ms until the container exits. 10Hz
# (not 1Hz) because benchmark runs are short and single-second ticks miss sub-second
# phases; cumulative-counter deltas (not `docker stats` one-shots) so short bursts
# are measured exactly. See benchmark/README.md "What you get".
sample_stats() { # $1=container $2=csv
  # Single-container profiling through sample_cluster's Engine-API poller.
  # Contract unchanged: writes one t_ms,cpu_pct,mem_mib series to $2. Callers pass
  # "${sprefix}_cluster.csv" as $2, so strip the suffix back to the prefix the
  # poller appends "_cluster.csv" to itself.
  sample_cluster "${2%_cluster.csv}" "$1" --single "$1" --pg "$PG"
}

run_rust() { # $1=tag $2=scenario $3=filter $4=columns $5=mode $6=stats prefix
  local tag=$1 scenario=$2 filter=$3 columns=$4 mode=$5 sprefix=$6
  local name=bench-rust sched_url="" mem=$MEM_RUST
  if [ "$mode" = "distributed" ]; then
    name=bench-rust-dist
    sched_url="$SCHEDULER_URL"
    mem=$MEM_CLIENT
  fi
  # shellcheck disable=SC2086
  $CLI run -d $CPU_FLAGS $mem --name "$name" --network "$NET" \
    -e BENCH_PG_PASSWORD="$PG_PASSWORD" -e BENCH_WORKERS="$WORKERS" \
    -e BENCH_OUTPUT=/output/orders_rust_${mode}_${scenario}.parquet \
    -e BENCH_FILTER="$filter" -e BENCH_COLUMNS="$columns" -e BENCH_SCENARIO="$scenario" \
    -e BENCH_SCHEDULER_URL="$sched_url" \
    -v "$OUTPUT:/output" \
    -v "$BENCH_DIR/bench-config.json:/etc/bench/config.json:ro" \
    "$IMG_RUST" >/dev/null
  verify_pin "$name"
  # Mount sanity: a stale bind shows up as ENOENT minutes later otherwise.
  if $CLI ps --format '{{.Names}}' | grep -qx "$name" \
    && ! $CLI exec "$name" sh -c 'touch /output/.writetest && rm /output/.writetest' >/dev/null 2>&1; then
    echo "WARN: /output not writable inside $name (stale bind mount?)" >&2
  fi
  if [ "$mode" = "distributed" ]; then
    # Aggregate scheduler + workers + client: whole-cluster cost per tick, plus one
    # series per container so the breakdown survives alongside the totals.
    sample_cluster "$sprefix" "$name" \
      bench-scheduler $(seq -f "bench-worker-%g" 1 "$WORKERS") "$name" --pg "$PG" &
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
  # shellcheck disable=SC2086
  $CLI run -d $CPU_FLAGS $MEM_SPARK --name bench-spark --network "$NET" --shm-size=1g \
    -e BENCH_PG_HOST="$PG" -e BENCH_PG_PORT=5432 -e BENCH_PG_DB=app \
    -e BENCH_PG_USER=postgres -e BENCH_PG_PASSWORD="$PG_PASSWORD" \
    -e BENCH_PARTITIONS="$SPARK_PARTITIONS" -e SPARK_DRIVER_MEM="$SPARK_DRIVER_MEM" \
    -e BENCH_SPARK_MASTER="$master" \
    -e BENCH_FETCHSIZE="$BATCH_SIZE" \
    -e BENCH_OUTPUT=/output/orders_spark_${scenario}.parquet \
    -e BENCH_FILTER="$filter" -e BENCH_COLUMNS="$columns" -e BENCH_SCENARIO="$scenario" \
    -v "$OUTPUT:/output" "$IMG_SPARK" >/dev/null
  verify_pin bench-spark
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
    # CPU peak is the max over a 5-tick (~500ms) rolling mean, not the raw single-tick
    # max: counter-granularity mismatch between the container and system counters can
    # spike one 100ms tick far above anything physical (observed: 643% on a 4-core
    # pin, where 400% is the ceiling). A real sustained burst spans many ticks and
    # survives the smoothing; a one-tick artifact does not. The smoothed peak is then
    # capped at PIN_MAX_PCT (the run's CPU budget x 100, exported by run.sh): the pin
    # is kernel-enforced, so no true peak can exceed it — the cap only ever removes
    # artifact, never real signal. Memory stays a raw max — it is an instantaneous
    # gauge, not a ratio, so its spikes are real.
    w = 5
    smooth = [sum(cpu[i:i + w]) / len(cpu[i:i + w]) for i in range(len(cpu))]
    peak = max(smooth)
    try:
        cap = float(os.environ.get("PIN_MAX_PCT", "") or 0.0)
    except ValueError:
        cap = 0.0
    if cap and peak > cap:
        peak = cap
    return {"avg_cpu_pct": round(sum(cpu) / len(cpu), 1),
            "peak_cpu_pct": round(peak, 1),
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
# Source-database series (same ticks, never mixed into engine totals): answers whether
# Postgres was the bottleneck on this run. Missing file (older runs) simply omits it.
if os.path.exists(prefix + "_pg.csv"):
    pgp = peaks(prefix + "_pg.csv")
    if pgp is not None:
        doc["pg"] = {"avg_cpu_pct": pgp["avg_cpu_pct"],
                     "peak_cpu_pct": pgp["peak_cpu_pct"],
                     "peak_rss_mib": pgp["peak_rss_mib"]}
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
    # correctness check compares. Scoped to this engine's own file (tag): a broad
    # orders_*_ glob would wipe the OTHER engines' outputs of the same scenario and
    # silently gut the cross-engine file check down to one file.
    rm -rf "$OUTPUT"/orders_${tag}_${scenario}.parquet
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
  mv "$RESULTS/.${tag}_${scenario}_try${best_i}_pg.csv" "$RESULTS/${tag}_${scenario}_pg.csv"
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
# Equal-spec card (the comparability contract — see the rule in benchmark/README.md):
# equality is enforced ONLY at the container boundary (pin + memory). Inside, every
# runtime runs unrestricted — Spark local[*], DataFusion defaults, Ballista visible-CPU
# slots — and sizes itself from the pinned budget. Never cap runtimes from the inside
# (--spark-cores, BENCH_CONCURRENT_TASKS) for headline numbers; those knobs are for
# diagnostics only.
echo "compute: spark=local[${SPARK_CORES:-*}] rust-standalone=defaults rust-distributed=$WORKERS workers x ${BENCH_CONCURRENT_TASKS:-all-visible} slot(s)"

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
header = ["engine", "scenario", "rows", "component",
          "elapsed_ms", "avg_cpu_pct", "peak_cpu_pct", "peak_rss_mib"]
rows = []
for pat in ("rust_*_full.json", "spark_full.json",
            "rust_*_selective.json", "spark_selective.json"):
    for path in sorted(glob.glob(os.path.join(results, pat))):
        d = json.load(open(path))
        rows.append([d["engine"], d["scenario"], str(d["rows"]), "",
                     str(d["elapsed_ms"]),
                     str(d["avg_cpu_pct"]), str(d["peak_cpu_pct"]),
                     str(d["peak_rss_mib"])])
        for name in sorted(d.get("containers", {})):
            c = d["containers"][name]
            rows.append(["", "", "", "`{}`".format(name), "",
                         str(c["avg_cpu_pct"]), str(c["peak_cpu_pct"]),
                         str(c["peak_rss_mib"])])
        if "pg" in d:
            p = d["pg"]
            rows.append(["", "", "", "`postgres`", "",
                         str(p["avg_cpu_pct"]), str(p["peak_cpu_pct"]),
                         str(p["peak_rss_mib"])])
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
  echo "Spec: containers [$CPU_FLAGS ${MEM_SPARK:-unconstrained}] pg=[$PG_CPU_FLAGS $PG_MEM_FLAGS] mem sched/workers/client [$MEM_SCHED/${MEM_WORKER:-none}/${MEM_CLIENT:-none}] compute spark=local[${SPARK_CORES:-*}] rust-distributed=$WORKERS x ${BENCH_CONCURRENT_TASKS:-all-visible} slots."
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
