#!/usr/bin/env bash
# Bring up the compose test databases, run the full suite against them, then tear down.
# Override the container engine with COMPOSE, e.g.  COMPOSE='podman compose' scripts/e2e.sh
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="${COMPOSE:-docker compose}"
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:5432/test}"
export MYSQL_URL="${MYSQL_URL:-mysql://root:password@127.0.0.1:3306/test}"

$COMPOSE -f tests/docker/compose.yaml up -d --wait
trap '$COMPOSE -f tests/docker/compose.yaml down -v' EXIT

# Serial: the hostile fixtures and per-test schemas/databases share one server.
cargo test --all -- --test-threads=1
