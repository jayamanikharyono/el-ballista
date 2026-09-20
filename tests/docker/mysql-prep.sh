#!/bin/sh
# MySQL compose init: strip Postgres COPY framing from the shared .dat files.
# Runs before 10-dvdrental.sql (entrypoint executes init files in sorted order).
# The .dat files under /dvdrental are pg_restore's source of truth and are
# mounted read-only, so cleaned copies go to /tmp/dvdrental/, which is what
# dvdrental_mysql.sql LOADs. Removed lines:
#   * '\.'  — Postgres COPY terminator; LOAD DATA would ingest it as a data
#             row (PK coerced to 0 -> "Duplicate entry '0'").
#   * empty lines — trailing blanks would likewise become bogus 0-PK rows.
# A legitimate data row always contains tabs (every table has >= 2 columns),
# and embedded newlines inside TEXT arrive backslash-escaped, so dropping
# exactly-'\.' and empty physical lines cannot remove real data.
set -eu
mkdir -p /tmp/dvdrental
for f in /dvdrental/*.dat; do
  b=$(basename "$f")
  sed -e '/^\\\.$/d' -e '/^$/d' "$f" > "/tmp/dvdrental/$b"
done
echo "[mysql-prep] cleaned $(ls /tmp/dvdrental/*.dat | wc -l) files into /tmp/dvdrental/"
