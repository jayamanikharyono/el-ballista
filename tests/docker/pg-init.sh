#!/usr/bin/env bash
# Postgres compose init: restore the native dvdrental directory dump into $POSTGRES_DB, then
# ANALYZE it. This database is both the integration-test fixture and the demo database the
# examples and `el-ballista` commands point at.
# Runs once, before the server accepts TCP connections (the entrypoint opens the network
# only after every /docker-entrypoint-initdb.d script completes), so the healthcheck's
# TCP `pg_isready -h 127.0.0.1` only passes once the data is fully loaded.
set -euo pipefail
echo "[pg-init] restoring dvdrental into ${POSTGRES_DB} ..."
pg_restore --no-owner --no-privileges -d "${POSTGRES_DB}" -U "${POSTGRES_USER}" /dvdrental
# Fresh statistics (row counts, histograms) so the cost-based pushdown decisions in the
# examples are deterministic from the first run instead of waiting for autovacuum.
psql -v ON_ERROR_STOP=1 -d "${POSTGRES_DB}" -U "${POSTGRES_USER}" -c "ANALYZE"
echo "[pg-init] done."
