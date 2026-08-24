# Prove Your Data Pipeline Is Safe to Re-Run

An incremental pipeline over three deliberately incompatible marketplace feeds, built to
demonstrate one failure and prove it fixed:

> **Re-run the pipeline over the exact same input, and last Tuesday's revenue changes.**

No error. No failed job. No bad row count. A number you already reported quietly moves.

This repo builds the same seven days of orders **twice**, through two tables that differ by a
single `MERGE` condition, then re-processes an old day and shows you what each one did.

```
                                  NAIVE          GUARDED
  2026-08-02 revenue         139,640.97       139,640.97
  after replaying it         143,964.19       139,640.97
                                  +3.1%        unchanged

  141 orders reverted to an older version by the naive MERGE
```

---

## Contents

1. [What it builds](#what-it-builds)
2. [The three feeds](#the-three-feeds)
3. [Setup](#setup)
4. [The commands](#the-commands)
5. [Walking through a single interval](#walking-through-a-single-interval)
6. [The gate](#the-gate)
7. [How the fix works](#how-the-fix-works)
8. [What it still doesn't fix](#what-it-still-doesnt-fix)
9. [Repo layout](#repo-layout)
10. [Tested with](#tested-with)

---

## What it builds

```
PostgreSQL — three marketplace feeds, append-only version logs
   │  sliced by updated_at  (SYSTEM time)      src/ingestion/bronze.py
   ▼
BRONZE   Delta · append-only · partitioned by _ingest_date
   │  three shapes → one shape                 src/transform/normalize.py
   │  dedup + MERGE with restatement guard     src/ingestion/silver.py
   ▼
SILVER   one row per order · partitioned by order_date  (BUSINESS time)
```

Two silver tables are written, from the same batch:

| Table | MERGE condition | Expected behaviour |
|---|---|---|
| `data/silver/orders` | `WHEN MATCHED AND s.updated_at_utc > t.updated_at_utc` | survives a replay unchanged |
| `data/silver/orders_naive` | `WHEN MATCHED` | reverts corrections on a replay |

Everything else about them is identical — same read, same dedup, same keys, same partitioning.

### Two clocks, never mixed

| Column | Meaning | Job |
|---|---|---|
| `updated_at` | when the record was written or restated | **which slice a row belongs to** |
| `order_date` | when the order was placed | **which day's revenue it counts toward** |

A refund written today belongs to the day the order was placed. Slice on one, aggregate on the
other, and never let them swap.

### What "revenue" means here

`net_amount_usd` — the gross amount converted to USD at **the order's own date's** FX rate, and
zero if the order is `refunded` or `cancelled`.

This is a stored column, not a filter applied at query time, so every consumer agrees on the
definition. It also matters more than it looks: amounts never change between versions of an
order, only the status does. If revenue were `SUM(gross_amount_usd)`, reverting a refund would
change the status and leave the number identical — and the whole demonstration would prove
nothing. `tests/test_normalize.py::test_refunded_orders_contribute_zero_revenue` pins it.

---

## The three feeds

Normalising them **is** the work:

| | Shopify | Amazon | Lazada |
|---|---|---|---|
| Order id | `5000001` bigint | `111-7000034-0034` string | `8000001` bigint |
| Naming | `snake_case` | `PascalCase` | `snake_case` |
| Amounts | `128.40` numeric | `128.40` numeric | `"128.40"` **a string** |
| Timestamps | `timestamptz` UTC | `timestamptz` UTC | **text**, `'... +0800'` local |
| Status | `paid`, `refunded` | `Shipped`, `Canceled` | `"delivered,returned"` a list |
| Currency | USD | USD | SGD |

All of it lands as: USD, UTC, `Decimal`, one status vocabulary
(`placed | paid | refunded | cancelled`), one key (`marketplace:marketplace_order_id`).

**The source tables are append-only version logs, not current-state tables.** Each row is one
version of an order as of its `updated_at`. That's what a CDC feed looks like, and it's what
makes replaying an interval meaningful — a current-state table would return nothing on re-read.

---

## Setup

### Prerequisites

| | Why |
|---|---|
| **Docker Desktop**, running | Postgres and Spark both run in containers |
| **Python 3** on the host | only to run `run.py`, which drives `docker compose` |
| ~2 GB free disk | the Spark image is large; PySpark compiles from source on first build |

Spark runs in a container rather than on the host **on purpose**. Native PySpark on Windows
needs `winutils.exe` and `HADOOP_HOME` and fails on local file writes; containerising it also
means a reader on any OS gets the same result from a clean clone.

`run.py` checks the Docker daemon before it does anything else — including before `--fresh`
tears the lake down — so a stopped Docker Desktop costs you one line, not a named-pipe error:

```
============================================================
  BLOCKED  Docker cannot be reached — every step here shells out to `docker compose`.
  Start Docker Desktop and wait for the whale icon to stop animating, then re-run.
  If it is already running, `docker info` in a new shell will show the real error.
============================================================
```

Exit code `2`, and `.runs/summary.json` records `"overall": "BLOCKED"`. Nothing is deleted and
nothing half-runs.

The check is on `docker info`'s **output**, not its exit code, and there is a test for that.
On Windows the Docker CLI exits `0` with Docker Desktop stopped — it prints the connect error
to stderr and returns success anyway — so a preflight that trusts `returncode` passes and the
named-pipe error comes back one step later. See `tests/test_run_preflight.py`.

### First run

```bash
git clone https://github.com/sonawalaM/incremental-marketplace-pipeline
cd incremental-marketplace-pipeline

python run.py --fresh
```

That single command does everything below in order and stops at the first failure. **First run
takes 5–8 minutes**, almost all of it building the Spark image. Subsequent runs take about 90
seconds.

---

## The commands

`run.py` runs on your host and drives `docker compose`. Every step can be run on its own.

| Command | What it does | Typical time |
|---|---|---|
| `python run.py --fresh` | Everything, from a clean slate | 5–8 min first time |
| `python run.py` | Everything, reusing the built image and existing DB | ~90 s |
| `python run.py --steps up` | Start Postgres, wait until it answers `pg_isready` | 10 s |
| `python run.py --steps seed` | Generate the synthetic source data | 15 s |
| `python run.py --steps build` | Build the Spark image (Delta + JDBC jars baked in) | 4–6 min |
| `python run.py --steps bronze` | Ingest one interval into Bronze | 30 s |
| `python run.py --steps normalize` | Three feeds → one shape, and print what it produced | 30 s |
| `python run.py --steps silver` | Dedup + guarded MERGE into Silver | 40 s |
| `python run.py --steps gate` | **The proof.** 7-day build ×2, then replay | 3–4 min |
| `python run.py --steps tests` | 27 unit tests, in the container | 30 s |

The tests themselves need neither Delta nor Postgres — only Spark — so you can also run them on
the host if you have PySpark installed: `python -m pytest tests/ -q`. Nine of them are pure
stdlib and check that this README has not drifted from the code.

Steps are comma-separated and run in the order you give them:

```bash
python run.py --steps up,seed,bronze,normalize,silver
```

The single-interval steps default to **2026-08-02**. To point them at another day:

```bash
python run.py --steps bronze,normalize,silver \
  --interval-start 2026-08-03T00:00:00+00:00 \
  --interval-end   2026-08-04T00:00:00+00:00
```

Or call a job directly, bypassing `run.py`:

```bash
docker compose run --rm jobs -m src.ingestion.bronze \
  --interval-start 2026-08-03T00:00:00+00:00 \
  --interval-end   2026-08-04T00:00:00+00:00
```

### What every run writes

| File | Contents |
|---|---|
| `.runs/latest.log` | Full transcript, UTF-8, ANSI stripped |
| `.runs/summary.json` | Per-step pass/fail, plus every `METRIC` line the jobs emitted |

Jobs print machine-readable metrics alongside human logs:

```
METRIC {"job": "silver", "rows_in": 981, "rows_after_dedup": 828, "duplicate_keys": 0}
```

Reviewing a run should mean reading a structured summary, not scrolling several hundred lines
of Spark warnings looking for a row count.

---

## Walking through a single interval

### 1. Start the database

```bash
python run.py --steps up
```

Starts `source-db` (Postgres 16) on host port **5433**, and polls `pg_isready` until it
answers. Port 5433 avoids colliding with a local Postgres on 5432.

### 2. Seed the source

```bash
python run.py --steps seed
```

Runs `src/generator/seed.py` with `--start 2026-08-01 --days 7 --orders 5000 --seed 42`. The
generator is **deterministic** — same seed and start produce byte-identical data, which is what
lets the gate compare content hashes across runs at all.

```
seeded  shopify=  2416  amazon=  2421  lazada=  2393  fx=  18
window  2026-08-01 .. 2026-08-08  seed=42
```

5,000 orders become **7,230 version rows**: 1,784 orders (36%) are restated at least once, and
**438 of those — 25% — are restated 3 to 6 days after the sale.** Those late ones are the whole
point: they are what a replay collides with.

### 3. Ingest one interval into Bronze

```bash
python run.py --steps bronze
```

Reads `[2026-08-02T00:00:00+00:00, 2026-08-03T00:00:00+00:00)` from each feed and appends to
Delta. **Both bounds are required arguments — there is no default and no clock read anywhere in
the job.**

```
bronze | run_id=manual__20260802T000000Z interval=[2026-08-02T00:00:00+00:00, 2026-08-03T00:00:00+00:00)
bronze | shopify_orders | appended 348 rows -> /app/data/bronze/shopify_orders
bronze | amazon_orders  | appended 321 rows -> /app/data/bronze/amazon_orders
bronze | lazada_orders  | appended 312 rows -> /app/data/bronze/lazada_orders
bronze | done | 981 rows across 3 feeds
```

The predicate is pushed into the source query, so Postgres does the filtering. Lazada's text
timestamps are cast in SQL before comparison:

```sql
WHERE to_timestamp(updated_at, 'YYYY-MM-DD HH24:MI:SS TZHTZM') >= TIMESTAMPTZ '...'
```

Bronze is **append-only**. Running this twice gives you 1,962 rows, and that is correct — it is
an audit log of what was read and when. Dedup happens in Silver.

### 4. Normalise

```bash
python run.py --steps normalize
```

Reads the Bronze rows for that interval, maps all three shapes to one, converts to USD, and
prints what it produced:

```
normalize | normalized 981 rows
+-----------+----+---------------+---------+
|marketplace|rows|distinct_orders|gross_usd|
+-----------+----+---------------+---------+
|amazon     |321 |274            |67191.82 |
|lazada     |312 |267            |49317.73 |
|shopify    |348 |287            |80546.77 |
+-----------+----+---------------+---------+

order_status | count            null check (all must be 0)
paid         | 239              order_key | event_time_utc | ... | net_amount_usd
placed       | 725                      0 |              0 | ... |              0
refunded     |  17
```

Three things to check in that output:

- **`order_status` contains only** `placed`, `paid`, `refunded`, `cancelled`. Anything else is
  a mapping bug.
- **Every null count is 0.** A non-zero on `net_amount_usd` means an FX rate is missing for
  some date.
- **Lazada's `gross_usd` is roughly 74% of its local amount** — the SGD conversion happened.

Note `rows` (321) vs `distinct_orders` (274): 47 Amazon orders already have more than one
version inside this single day. Normalisation deliberately keeps all of them.

### 5. MERGE into Silver

```bash
python run.py --steps silver
```

```
silver | batch | 981 rows in -> 828 after dedup (153 restatements collapsed)
silver | 828 rows, 828 distinct keys, revenue_usd=161892.20
```

- **In-batch dedup first.** Delta's `MERGE` raises on duplicate source keys rather than picking
  one arbitrarily, so the batch collapses to the latest version per order before it goes near
  the target.
- **`duplicate_keys` must be 0.** One row per order is Silver's entire contract.

---

## The gate

```bash
python run.py --steps gate
```

This is the proof, and the reason the repo exists rather than a blog post. It:

1. Processes **2026-08-01 → 2026-08-07** in order, into **both** silver tables
2. Snapshots each — row count, revenue per marketplace per day, and a SHA-256 of every
   business column
3. **Re-processes 2026-08-02**, a day whose orders were corrected on Aug 5, 6 and 7
4. Snapshots again and diffs

Step 3 is not contrived. It is what a retry, a backfill, or a bug-fix rerun does.

```
==================================================================
  Replaying 2026-08-02 -- one MERGE condition apart
==================================================================
                                  NAIVE          GUARDED
------------------------------------------------------------------
  2026-08-02 revenue         139,640.97       139,640.97
  after replaying it         143,964.19       139,640.97
                                  +3.1%        unchanged
------------------------------------------------------------------
  141 orders reverted to an older version by the naive MERGE
  content hash  <sha256> -> <identical sha256>
  123 back-dated orders present, 0 duplicate keys, 0 wrong-day rows
==================================================================
  GATE PASSED
==================================================================
```

Across all days the naive table ends up **$4,609.39** above the truth — not because sales
appeared, but because refunds disappeared.

### What it asserts

| | Check | Catches |
|---|---|---|
| A | Row count and distinct keys unchanged | gross duplication |
| B | Revenue per marketplace per day identical | silent inflation |
| C | Content hash identical | anything A and B would miss |
| D | Invariants hold | duplicate keys, wrong-day rows, **an untested test** |

Assertion **D** includes `backdated_orders_present > 0`. If the replay collides with no late
corrections, the suite passes while proving nothing — and a test that cannot fail is worse than
no test, because it tells you the thing is safe.

The gate exits non-zero if the guarded table changed **or** if the naive one didn't. Both halves
have to hold for the demonstration to mean anything.

---

## How the fix works

Two changes. Neither is clever.

**1. Bounds are arguments, never clock reads.**

```sql
WHERE updated_at >= :interval_start
  AND updated_at <  :interval_end     -- half-open: no boundary double-read
```

Nothing calls `now()`. Nothing reads `MAX(updated_at)` from the table it's about to write. The
same run always reads the same slice, which is what makes a retry meaningless and a backfill
boring.

**2. Refuse to move backwards.**

```python
# src/ingestion/silver.py
RESTATEMENT_GUARD = "s.updated_at_utc > t.updated_at_utc"

merge = (merge.whenMatchedUpdateAll(condition=RESTATEMENT_GUARD) if guarded
         else merge.whenMatchedUpdateAll())          # <- naive: any version overwrites
```

That condition is the entire fix. An older version can no longer overwrite a newer one, so
replaying any interval becomes a no-op.

---

## What it still doesn't fix

**Ties break both operators.** Feeds stamp `updated_at` to the second. Two real changes inside
one second tie, and neither operator is correct: `>` silently drops the second change, `>=`
handles it and reintroduces the replay bug. A timestamp is not a total order. The real answer is
a sequence from the source — a CDC log offset, an LSN, a version counter — ordering on
`(updated_at, sequence)`. Many marketplace APIs don't provide one.

**And this repo cannot catch that.** The generator spaces versions a minute apart, so there are
zero ties by construction.

**The guard trusts `updated_at` absolutely.** A row that changes without bumping it is an
upstream bug, and the fix belongs upstream. But the failure is silent — if you want to detect
it, hash the business columns and compare.

**Recent days are provisional.** The pipeline reports **$973,257.90**; a full recompute of the
raw version log says **$961,641.91**. Every dollar of the $11,615.99 gap is refunds whose
`updated_at` falls outside the processed window — and the gap grows the closer you get to today:

| Order day | Pipeline | Full recompute | Overstated by |
|---|---:|---:|---:|
| 2026-08-01 | 142,437.80 | 141,979.48 | 458.32 |
| 2026-08-04 | 129,894.42 | 128,679.22 | 1,215.20 |
| 2026-08-07 | 142,723.62 | 138,563.24 | **4,160.38** |

Not a bug — the world hasn't finished happening. But the last few days are estimates that firm
up over about a week, and reporting them as final is a way to be wrong that nobody warns you
about.

**At this size none of this is worth doing.** A full recompute is about twice as fast here and
strictly more correct, because it sees the refunds the window missed. Incremental machinery only
earns its place when a full recompute costs more than you're willing to pay.

---

## Repo layout

```
├── run.py                        host-side runner; writes .runs/
├── docker-compose.yml            postgres + spark job runner
├── Dockerfile                    spark image, Delta + Postgres JDBC jars baked in
├── requirements.txt              exact pins
├── sql/001_source_schema.sql     the three feeds + FX rates
├── src/
│   ├── common/
│   │   ├── config.py             feed definitions + per-feed slicing SQL
│   │   └── spark.py              session, interval parsing, METRIC emission
│   ├── generator/seed.py         deterministic synthetic source data
│   ├── ingestion/
│   │   ├── bronze.py             interval-bound JDBC read → Delta append
│   │   └── silver.py             dedup + MERGE, guarded or naive
│   ├── transform/normalize.py    three shapes → one order table
│   └── gate.py                   the proof
├── tests/                        27 unit tests, incl. docs-vs-code drift checks
└── data/                         Delta lake root (gitignored)
```

---

## Tested with

| | Version |
|---|---|
| PySpark | 4.0.1 |
| delta-spark | 4.0.0 |
| Java | 21 (OpenJDK) |
| Postgres | 16 |
| Postgres JDBC | 42.7.4 |
| Python | 3.11 (container) / 3.12 (host) |
| Docker Compose | v2 |

**PySpark and delta-spark must be a matched pair** — each Delta release targets one Spark minor,
and a mismatch fails at runtime with an unhelpful error rather than at install time.

The base image is a known drift hazard. `python:3.11-slim` moved from Debian bookworm to trixie
mid-development and dropped the Java 17 packages, breaking the build. It is pinned to Java 21 as
a result; pinning the image to a digest is the stronger fix.

---

## License

MIT
