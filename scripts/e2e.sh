#!/usr/bin/env bash
# Bring up the compose test databases, run the full suite against them, then tear down.
# Requires Docker: `docker compose` must be on PATH.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose"
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:5432/test}"
export MYSQL_URL="${MYSQL_URL:-mysql://root:password@127.0.0.1:3306/test}"

# Install the teardown BEFORE `up`: if `up` fails part-way (one service unhealthy, seed error),
# the containers it did start are still removed.
trap '$COMPOSE -f tests/docker/compose.yaml down -v' EXIT
$COMPOSE -f tests/docker/compose.yaml up -d --wait

# Serial: the hostile fixtures and per-test schemas/databases share one server.
cargo test --all -- --test-threads=1
