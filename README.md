# Incremental Marketplace Revenue Pipeline

A re-runnable incremental pipeline over three heterogeneous marketplace feeds, built to
demonstrate one thing:

> **"Re-running your pipeline changed last Tuesday's revenue."**

Most incremental-pipeline tutorials produce pipelines that cannot be safely re-run. This repo
shows the four ways that happens, fixes each one, and — crucially — **ships a test that proves
the fix**, plus a mode that reproduces the failure on demand.

Built with PySpark, Delta Lake, Airflow, dbt and Postgres. Synthetic data throughout.

---

## The problem

A refund does not arrive as a new row. It arrives as a **restatement of the original order**.
That single fact breaks four common implementations:

| Naive implementation | What breaks |
|---|---|
| `WHERE updated_at > (SELECT max(updated_at) FROM target)` | The slice depends on pipeline history. A retry re-counts or skips orders. |
| `WHERE updated_at > now() - interval '1 hour'` | Backfill is impossible. A retry 40 minutes later reads a different slice. |
| Partition and filter on **ingest date** | A refund for a 5-day-old order lands on today instead of the order's own day. |
| Plain `append` into the lake | Restatements accumulate instead of superseding. Revenue counts twice. |

### The fix: interval-bound ingestion

Every run derives its bounds from the orchestrator's data interval, passed in as arguments.
**Nothing in the pipeline reads a clock.**

```sql
WHERE updated_at >= :interval_start
  AND updated_at <  :interval_end   -- half-open, no boundary double-read
```

That one constraint buys safe retries, free backfill, and reproducible history.

### The restatement guard

```sql
WHEN MATCHED AND s.updated_at_utc > t.updated_at_utc THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

Drop that condition and replaying an old interval overwrites corrected orders with superseded
ones — silently, with no error, surfacing weeks later as revenue that changed for no reason.

---

## Architecture

```
Postgres — three CDC-style marketplace feeds
   │  sliced by updated_at        ← SYSTEM time
   ▼
BRONZE   Delta · append-only · partitioned by _ingest_date
   │  normalize → in-batch dedup → MERGE with restatement guard
   ▼
SILVER   one row per order · partitioned by order_date   ← BUSINESS time
   │
   ▼
GOLD     fct_orders · agg_daily_revenue
```

### Two clocks, kept separate

| Column | Meaning | Role |
|---|---|---|
| `updated_at` | when the source row was written or restated | **ingestion slicing** — nothing is missed |
| `order_date` | the day the order was placed | **partitioning** — where revenue belongs |

A refund written today for a five-day-old order is picked up by today's `updated_at` slice and
merged into the five-day-old `order_date` partition. Slice on ingest date instead and the refund
either vanishes or lands on the wrong day.

---

## The three feeds disagree on purpose

Normalizing them *is* the work:

| | Shopify | Amazon | Lazada |
|---|---|---|---|
| **Naming** | `snake_case` | `PascalCase` | `snake_case` |
| **Id** | `id` (bigint) | `AmazonOrderId` (string) | `order_id` (bigint) |
| **Amounts** | numeric | numeric | **string** `'128.40'` |
| **Time** | timestamptz UTC | timestamptz UTC | **text** `'... +0800'` |
| **Status** | `paid`, `refunded` | `Shipped`, `Canceled` | `'delivered,returned'` list |
| **Currency** | USD | USD | SGD |

All of it lands as: USD, UTC, `Decimal`, one status vocabulary (`placed | paid | refunded |
cancelled`), one key (`marketplace:marketplace_order_id`).

Source tables are **append-only version logs**, not current-state tables — each row is one
version of an order as of its `updated_at`. That's what a CDC feed looks like, and it's what
makes interval replay meaningful.

---

## Quickstart

Requires Docker and Python 3 on the host. Spark runs in a container — never natively on
Windows, where `winutils`/`HADOOP_HOME` breaks local file writes.

```bash
git clone https://github.com/sonawalaM/incremental-marketplace-pipeline
cd incremental-marketplace-pipeline

python run.py --fresh          # everything: db, seed, build, bronze, normalize, silver, tests
```

Then prove it is safely re-runnable:

```bash
python run.py --steps up,gate
```

Runs in about two minutes on 8 GB RAM.

### What `run.py` writes

| File | Contents |
|---|---|
| `.runs/latest.log` | Full transcript, UTF-8 |
| `.runs/summary.json` | Per-step pass/fail plus every `METRIC` line the jobs emitted |

Jobs emit machine-readable metrics alongside human logs:

```
METRIC {"job": "silver", "rows_in": 981, "rows_after_dedup": 828, "duplicate_keys": 0}
```

Scraping row counts out of prose is how verification quietly rots.

---

## Running individual steps

```bash
python run.py --steps up,seed          # database + deterministic data
python run.py --steps bronze           # interval-bound ingest
python run.py --steps normalize        # three feeds -> one shape
python run.py --steps silver           # dedup + guarded MERGE
python run.py --steps tests            # unit tests
python run.py --steps gate             # the proof
```

Any step directly, with a custom interval:

```bash
docker compose run --rm jobs -m src.ingestion.bronze \
  --interval-start 2026-08-03T00:00:00+00:00 \
  --interval-end   2026-08-04T00:00:00+00:00
```

---

## The gate

`src/gate.py` is the proof section, and the reason this repo exists rather than a blog post.

```
1. process Aug 1 .. Aug 7 in order
2. snapshot silver — rows, revenue by marketplace × day, content hash
3. RE-PROCESS AUG 2   ← orders from that day were corrected on Aug 5-7
4. snapshot again, diff
```

Step 3 is the destructive scenario. It is what a retry, a backfill, or a bug-fix rerun actually
does.

**Four assertions:**

| | Check | Catches |
|---|---|---|
| A | Row count and distinct keys unchanged | Gross duplication |
| B | Per-marketplace-per-day revenue identical | Silent inflation |
| C | Content hash identical | Anything A and B would miss |
| D | Invariants hold | Duplicate keys, wrong-day rows, an untested test |

Assertion D includes `backdated_orders_present > 0`. If the replay scenario isn't actually
exercised, the suite goes green while testing nothing — a test that cannot fail is worse than
no test.

---

## Determinism

The generator is seeded. Same `--seed` and `--start` produce byte-identical source data, which
is what lets the gate compare content hashes across runs at all.

Two rules follow, and both are enforced in code:

- Nothing calls `now()` or unseeded random inside the pipeline.
- `_ingested_at` (wall clock) is **diagnostic only**. Nothing orders, dedups, or hashes by it —
  doing so would make two runs of the same interval differ for reasons unrelated to correctness.

---

## Layout

```
├── run.py                      one-command runner, writes .runs/
├── docker-compose.yml          postgres + spark job runner
├── Dockerfile                  spark image, Delta + JDBC jars baked in
├── sql/001_source_schema.sql   the three feeds + FX rates
├── src/
│   ├── common/
│   │   ├── config.py           feed definitions + per-feed slicing SQL
│   │   └── spark.py            session, interval parsing, METRIC emission
│   ├── generator/seed.py       deterministic synthetic data
│   ├── ingestion/
│   │   ├── bronze.py           interval-bound JDBC read -> Delta append
│   │   └── silver.py           dedup + guarded MERGE
│   ├── transform/normalize.py  three shapes -> one order table
│   └── gate.py                 the proof
├── tests/                      unit tests (no Docker, no Delta, no Postgres needed)
└── data/                       Delta lake root (gitignored)
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
| Python | 3.11 |
| Docker Compose | v2 |

**PySpark and delta-spark must be a matched pair** — Delta releases target one Spark minor.
Mismatches fail at runtime with unhelpful errors.

The base image is a known drift hazard: `python:3.11-slim` moved from Debian bookworm to trixie
mid-development and dropped the Java 17 packages, breaking the build. Pinned to Java 21 as a
result. Pinning to a digest is the stronger fix.

---

## Build status

| Step | |
|---|---|
| 1. Source DB + deterministic feeds | ✅ |
| 2. Bronze ingestion | ✅ |
| 3. Normalize three feeds | ✅ |
| 4. Silver MERGE + restatement guard | ✅ |
| 5. **Gate — proof of re-runnability** | ✅ |
| 6. Gold — `fct_orders`, `agg_daily_revenue` | ⬜ |
| 7. Broken mode (`make demo-broken`) | ⬜ |
| 8. Airflow DAG + backfill demo | ⬜ |
| 9. dbt marts + data quality tests | ⬜ |
| 10. CI + clean-clone verification | ⬜ |

---

## License

MIT
