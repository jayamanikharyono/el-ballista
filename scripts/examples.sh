#!/usr/bin/env bash
# Run every example in examples/ against the dvdrental demo database and report PASS/FAIL.
#
#   scripts/examples.sh                    # start the compose Postgres, run all, tear it down
#   scripts/examples.sh --no-db            # use a Postgres already on localhost:5432 (db `test`)
#   scripts/examples.sh --keep-db          # leave the compose Postgres running afterwards
#   scripts/examples.sh --no-distributed   # skip the three examples that need a Ballista cluster
#   scripts/examples.sh --release          # build and run in release mode (default: debug)
#   scripts/examples.sh full_extraction parquet_export   # run only the named examples
#
# Needs: cargo, python3, curl; Docker (`docker compose`) unless --no-db. The distributed
# examples start a local `el-ballista scheduler` on :50050 plus two workers
# (:50051/:50052 and :50061/:50062), so those ports must be free.
#
# Nothing is written into the repository: every run gets a fresh work directory under
# target/examples-run/ (configs copied with their own checkpoint dir, Parquet output, logs),
# so checkpointed examples start from zero every time.
set -euo pipefail
cd "$(dirname "$0")/.."

USE_DB=1 KEEP_DB=0 DISTRIBUTED=1 PROFILE=debug ONLY=()
while [ $# -gt 0 ]; do
  case "$1" in
    --no-db) USE_DB=0 ;;
    --keep-db) KEEP_DB=1 ;;
    --no-distributed) DISTRIBUTED=0 ;;
    --release) PROFILE=release ;;
    -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
    *) ONLY+=("$1") ;;
  esac
  shift
done

export PGPASSWORD="${PGPASSWORD:-postgres}"   # compose default (tests/docker/compose.yaml)
export RUST_LOG="${RUST_LOG:-warn}"
COMPOSE="docker compose -f tests/docker/compose.yaml"
SCHEDULER_URL="http://localhost:50050"

WORK="target/examples-run/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$WORK/configs" "$WORK/logs" "$WORK/out"
WORK="$(cd "$WORK" && pwd)"

PIDS=()
cleanup() {
  # Workers first, then the scheduler.
  local i
  for (( i = ${#PIDS[@]} - 1; i >= 0; i-- )); do kill "${PIDS[$i]}" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ "$USE_DB" = 1 ] && [ "$KEEP_DB" = 0 ]; then $COMPOSE down -v >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

if [ "$USE_DB" = 1 ]; then
  echo "== starting Postgres (tests/docker/compose.yaml, dvdrental)"
  $COMPOSE up -d --wait postgres
fi

echo "== building el-ballista + examples ($PROFILE)"
BUILD_FLAGS=(); [ "$PROFILE" = release ] && BUILD_FLAGS=(--release)
cargo build ${BUILD_FLAGS[@]+"${BUILD_FLAGS[@]}"} --bin el-ballista --examples
BIN="${CARGO_TARGET_DIR:-target}/$PROFILE"

# Copy each demo config with checkpoint.dir moved into the work directory.
for cfg in examples/configs/*.json; do
  python3 - "$cfg" "$WORK/configs/$(basename "$cfg")" "$WORK/checkpoints" <<'EOF'
import json, sys
src, dst, ckpt = sys.argv[1:4]
job = json.load(open(src))
job.setdefault("checkpoint", {})["dir"] = ckpt
json.dump(job, open(dst, "w"), indent=2)
EOF
done
CFG="$WORK/configs"

# name | needs cluster (1/0) | expected output (regex) | command...
declare -a CASES=(
  "full_extraction|0|Total 14596 rows|$BIN/examples/full_extraction $CFG/full_extract.dvd_rental.json"
  "parallel_extraction|0|14596 rows extracted across all partitions|$BIN/examples/parallel_extraction $CFG/full_extract.dvd_rental.json"
  "filtered_extraction|0|Run outcome: 422 row\\(s\\) delivered|$BIN/examples/filtered_extraction $CFG/extract.example.json"
  "dataframe_extraction|0|dataframe extraction: 1000 row\\(s\\)|$BIN/examples/dataframe_extraction $CFG/extract.example.json"
  "pipeline_extraction|0|run_with\\(\\): 14596 row|$BIN/examples/pipeline_extraction $CFG/full_extract.example.json"
  "parquet_export|0|14596 row\\(s\\) written|$BIN/examples/parquet_export $CFG/full_extract.dvd_rental.json $WORK/out/parquet_export"
  "bench_full_load|0|\"rows\": ?14596|$BIN/examples/bench_full_load $CFG/full_extract.example.json 1 $WORK/out/bench_standalone.parquet full"
  "distributed_extraction|1|422 row\\(s\\) written|$BIN/examples/distributed_extraction $CFG/extract.example.json 2 $WORK/out/distributed.parquet"
  "pipeline_extraction (distributed)|1|distributed run\\(\\) \\[diagnostic\\]: 14596 row|$BIN/examples/pipeline_extraction $CFG/full_extract.dvd_rental.json"
  "bench_full_load (distributed)|1|\"rows\": ?14596|env BENCH_SCHEDULER_URL=$SCHEDULER_URL $BIN/examples/bench_full_load $CFG/full_extract.example.json 2 $WORK/out/bench_distributed.parquet full"
)

selected() {
  [ ${#ONLY[@]} -eq 0 ] && return 0
  local base="${1%% (*}"
  for o in "${ONLY[@]}"; do [ "$o" = "$base" ] && return 0; done
  return 1
}

cluster_up() {
  [ "${#PIDS[@]}" -gt 0 ] && return 0
  echo "== starting Ballista cluster: scheduler + 2 workers"
  "$BIN/el-ballista" scheduler --scheduler-url "$SCHEDULER_URL" >"$WORK/logs/scheduler.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 100); do curl -fs "$SCHEDULER_URL/api/executors" >/dev/null 2>&1 && break; sleep 0.2; done
  "$BIN/el-ballista" worker --scheduler-url "$SCHEDULER_URL" >"$WORK/logs/worker1.log" 2>&1 &
  PIDS+=($!)
  "$BIN/el-ballista" worker --scheduler-url "$SCHEDULER_URL" --port 50061 --grpc-port 50062 \
    >"$WORK/logs/worker2.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 150); do
    n=$(curl -fs "$SCHEDULER_URL/api/executors" 2>/dev/null | grep -o '"task_slots"' | wc -l | tr -d ' ')
    [ "${n:-0}" -ge 2 ] && return 0
    sleep 0.2
  done
  echo "cluster did not register 2 workers in 30 s; see $WORK/logs/" >&2
  return 1
}

PASS=0 FAIL=0 SKIP=0 RESULTS=()
for case in "${CASES[@]}"; do
  IFS='|' read -r name needs_cluster expect cmd <<<"$case"
  selected "$name" || continue
  if [ "$needs_cluster" = 1 ] && [ "$DISTRIBUTED" = 0 ]; then
    RESULTS+=("SKIP  $name (--no-distributed)"); SKIP=$((SKIP + 1)); continue
  fi
  if [ "$needs_cluster" = 1 ] && ! cluster_up; then
    RESULTS+=("FAIL  $name (cluster did not start)"); FAIL=$((FAIL + 1)); continue
  fi
  log="$WORK/logs/$(echo "$name" | tr ' ()' '_' | tr -s '_' | sed 's/_$//').log"
  printf -- "-- %-36s" "$name"
  start=$(date +%s)
  if $cmd >"$log" 2>&1; then status=0; else status=$?; fi
  secs=$(( $(date +%s) - start ))
  if [ "$status" -ne 0 ]; then
    echo "FAIL (exit $status, ${secs}s)"; RESULTS+=("FAIL  $name: exit $status, see $log"); FAIL=$((FAIL + 1))
  elif ! grep -Eq "$expect" "$log"; then
    echo "FAIL (${secs}s)"; RESULTS+=("FAIL  $name: output did not match /$expect/, see $log"); FAIL=$((FAIL + 1))
  else
    echo "ok (${secs}s)"; RESULTS+=("PASS  $name"); PASS=$((PASS + 1))
  fi
done

echo
echo "== summary: $PASS passed, $FAIL failed, $SKIP skipped (logs and output: $WORK)"
[ ${#RESULTS[@]} -gt 0 ] && printf '   %s\n' "${RESULTS[@]}"
[ "$FAIL" -eq 0 ]
