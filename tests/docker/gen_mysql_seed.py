import re, sys
from pathlib import Path

src = open('tests/data/dvdrental/restore.sql', encoding='utf-8').read()

# 1) table -> (datafile, [cols]) from the COPY ... FROM '$$PATH$$/N.dat' lines
copy_map = {}
for m in re.finditer(r"COPY public\.(\w+) \(([^)]*)\) FROM '\$\$PATH\$\$/(\d+\.dat)';", src):
    tbl, cols, f = m.group(1), m.group(2), m.group(3)
    collist = [c.strip() for c in cols.split(',')]
    copy_map[tbl] = (f, collist)

# 2) CREATE TABLE blocks -> {table: {col: pgtype}}
tables = {}
for m in re.finditer(r"CREATE TABLE public\.(\w+) \((.*?)\n\);", src, re.S):
    tbl, body = m.group(1), m.group(2)
    cols = {}
    for line in body.split('\n'):
        line = line.strip().rstrip(',')
        if not line:
            continue
        # column name is first token (may be quoted)
        mm = re.match(r'("?[\w]+"?)\s+(.*)', line)
        if not mm:
            continue
        name = mm.group(1).strip('"')
        rest = mm.group(2)
        cols[name] = rest
    tables[tbl] = cols

# 3) primary keys
pk = {}
for m in re.finditer(r"ALTER TABLE ONLY public\.(\w+)\s+ADD CONSTRAINT \w+ PRIMARY KEY \(([^)]*)\);", src):
    pk[m.group(1)] = [c.strip() for c in m.group(2).split(',')]

def mysql_type(pgtype):
    t = pgtype.lower()
    if 'text[]' in t: return 'TEXT'          # special_features array -> raw text
    if 'tsvector' in t: return 'TEXT'        # fulltext -> raw text
    if 'mpaa_rating' in t: return "ENUM('G','PG','PG-13','R','NC-17')"
    if 'public.year' in t: return 'SMALLINT'
    if t.startswith('character varying'):
        n = re.search(r'\((\d+)\)', t); return f'VARCHAR({n.group(1)})' if n else 'VARCHAR(255)'
    if t.startswith('character('):
        n = re.search(r'\((\d+)\)', t); return f'CHAR({n.group(1)})'
    if t.startswith('numeric'):
        n = re.search(r'\((\d+),(\d+)\)', t); return f'DECIMAL({n.group(1)},{n.group(2)})' if n else 'DECIMAL(10,2)'
    if t.startswith('smallint'): return 'SMALLINT'
    if t.startswith('bigint'): return 'BIGINT'
    if t.startswith('integer'): return 'INT'
    if t.startswith('boolean'): return 'TINYINT(1)'
    if t.startswith('timestamp'): return 'DATETIME(6)'
    if t.startswith('date'): return 'DATE'
    if t.startswith('bytea'): return 'LONGBLOB'
    if t.startswith('text'): return 'TEXT'
    return 'TEXT'

def is_bool(pgtype): return pgtype.lower().startswith('boolean')

out = []
out.append("-- GENERATED from tests/data/dvdrental (PostgreSQL dump) by tests/docker/gen_mysql_seed.py.")
out.append("-- Do not edit by hand. Single source of truth = the .dat files under tests/data/dvdrental/;")
out.append("-- this file only maps the same tab-separated COPY data into MySQL via LOAD DATA.")
out.append("-- Postgres loads the identical .dat files natively via pg_restore.")
out.append("-- The .dat files carry Postgres COPY framing (a trailing '\\.' terminator line plus")
out.append("-- blank lines) which pg_restore consumes but LOAD DATA would ingest as data rows")
out.append("-- (text PK coerced to 0 -> duplicate-key error). tests/docker/mysql-prep.sh strips")
out.append("-- that framing into /tmp/dvdrental/ at container init; this file reads the cleaned")
out.append("-- copies, never /dvdrental/ directly.")
out.append("")
out.append("SET FOREIGN_KEY_CHECKS = 0;")
out.append("-- Keep STRICT so bad rows fail loudly instead of being coerced (e.g. text -> 0).")
out.append("SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';")
out.append("")

LOAD_DIR = "/tmp/dvdrental"

order = ['actor','category','country','city','address','language','film','film_actor',
         'film_category','customer','store','staff','inventory','rental','payment']
order += [t for t in copy_map if t not in order]

for tbl in order:
    if tbl not in tables or tbl not in copy_map:
        continue
    coldefs = tables[tbl]
    datafile, copycols = copy_map[tbl]
    lines = []
    for c in copycols:
        pgt = coldefs.get(c, 'text')
        lines.append(f"  `{c}` {mysql_type(pgt)}")
    pk_clause = ""
    if tbl in pk:
        pk_clause = ",\n  PRIMARY KEY (" + ", ".join(f"`{c}`" for c in pk[tbl]) + ")"
    out.append(f"DROP TABLE IF EXISTS `{tbl}`;")
    out.append(f"CREATE TABLE `{tbl}` (\n" + ",\n".join(lines) + pk_clause + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
    out.append("")

out.append("-- Data load: identical tab-separated .dat files, \\N = NULL (MySQL LOAD DATA default).")
for tbl in order:
    if tbl not in tables or tbl not in copy_map:
        continue
    datafile, copycols = copy_map[tbl]
    coldefs = tables[tbl]
    bools = [c for c in copycols if is_bool(coldefs.get(c,''))]
    if bools:
        # route bool cols through @vars, convert 't'/'f' -> 1/0
        target = []
        setparts = []
        for c in copycols:
            if c in bools:
                target.append(f"@{c}")
                setparts.append(f"`{c}` = IF(@{c} = 't', 1, IF(@{c} = 'f', 0, NULL))")
            else:
                target.append(f"`{c}`")
        collist = "(" + ", ".join(target) + ")"
        setclause = "\n  SET " + ", ".join(setparts)
    else:
        collist = "(" + ", ".join(f"`{c}`" for c in copycols) + ")"
        setclause = ""
    out.append(
        f"LOAD DATA INFILE '{LOAD_DIR}/{datafile}' INTO TABLE `{tbl}`\n"
        f"  FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\'\n"
        f"  LINES TERMINATED BY '\\n'\n"
        f"  {collist}{setclause};"
    )
out.append("")
out.append("-- staff.picture parity: Postgres COPY text format encodes bytea as '\\\\x<hex>';")
out.append("-- LOAD DATA's ESCAPED BY only collapses the leading '\\\\\\\\' to '\\', leaving the")
out.append("-- 18 ASCII chars '\\x8950…' in the blob while Postgres holds the decoded 8 bytes.")
out.append("-- Decode here so both engines hold identical bytes. The LEFT(..) guard makes this")
out.append("-- a no-op on re-run (decoded bytes start with 0x89, never '\\x'). Only staff.picture")
out.append("-- is bytea in this dataset (tsvector/text[] stay text by design — see extraction_matrix).")
out.append("UPDATE `staff` SET `picture` = UNHEX(SUBSTRING(`picture`, 3))")
out.append("  WHERE LEFT(`picture`, 2) = '\\\\x' AND LENGTH(`picture`) > 2;")
out.append("")
out.append("SET FOREIGN_KEY_CHECKS = 1;")
out.append("")

open('tests/data/dvdrental_mysql.sql','w',encoding='utf-8').write("\n".join(out))
print("tables:", len(copy_map), "-> tests/data/dvdrental_mysql.sql")
print("bool cols per table:", {t:[c for c in copy_map[t][1] if is_bool(tables[t].get(c,''))] for t in copy_map if any(is_bool(tables[t].get(c,'')) for c in copy_map[t][1])})
