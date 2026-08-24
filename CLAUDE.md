# Incremental Marketplace Revenue Pipeline

Reference implementation of a re-runnable incremental pipeline across heterogeneous
marketplace feeds. The repo must be good enough that a developer-tool content team reads it
and commissions an article — treat every file as public-facing work product.

## What this is

Three heterogeneous marketplace order feeds (Shopify / Amazon / Lazada shapes) → one clean
order table → daily revenue, built to be **safely re-runnable**.

```
Postgres CDC-style feeds
   │  sliced by updated_at        ← system time
   ▼
BRONZE  Delta · append-only · partitioned by _ingest_date · emits slice_fingerprint
   │  normalize → dedup → MERGE
   ▼
SILVER  data/silver/orders        guarded  (WHEN MATCHED AND s.updated_at_utc > t.updated_at_utc)
        data/silver/orders_naive  naive    (WHEN MATCHED)
        one row per order · partitioned by order_date   ← business time
   │
   ▼
GOLD    fct_orders, agg_daily_revenue  — NOT BUILT YET (step 7)
```

**Revenue = `net_amount_usd`**, a stored column: USD at the order's own date's FX rate, zero if
refunded or cancelled. Never `SUM(gross_amount_usd)` — see build step 6.

## The thesis — do not lose this

**"Re-running your pipeline changed last Tuesday's revenue."**

That is the failure every data team has felt and nobody writes about properly. Four causes,
each demonstrated and then fixed:

1. **Stateful watermark** (`max(updated_at)` from a control table) → reruns aren't reproducible
2. **Wall-clock bounds** (`now() - interval`) → backfill impossible
3. **Ingest-date partitioning** → a refund for a 5-day-old order lands on today instead of
   the day the order belongs to
4. **Plain append** → restatements accumulate instead of superseding, so revenue inflates

### The fix: interval-bound ingestion

Bounds come from Airflow's data interval, passed explicitly into the Spark job. **Never read a
clock inside the job.**

```sql
WHERE updated_at >= :data_interval_start
  AND updated_at <  :data_interval_end   -- half-open
```

### The detail that makes the article

A refund arrives as a **restatement of the original order**, not as a new row. So:
- Naive append → both the paid version and the refunded version survive → revenue counted twice
- Naive dedup on `order_id` → drops the correction → refund never applied
- Correct → latest version by `updated_at` wins, applied to the **order's own date**

A back-dated refund must reduce **that day's** revenue, not today's. That is the whole
two-clock argument in one sentence a reader already understands.

## README is part of every step — not a task for the end

`README.md` is the first thing a buyer opens, so it is a deliverable, not documentation debt.
**Every build step updates it in the same commit as the code.** Specifically:

- Tick the step off in the **Build status** table
- Add any new command to **Running individual steps**
- Update **Tested with** whenever a version is pinned or bumped
- If the step revealed a real trap (version drift, partition-pruning hazard, encoding), write
  it down — those paragraphs are what separate this from a tutorial, and they become article
  material for free

A step is not done until the README reflects it.

## Hard rules

- **Interval bounds are arguments, never clock reads.**
- **MERGE always carries the restatement guard:**
  `WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE`
  Without it, replaying an old interval overwrites corrected rows with stale ones.
- **MERGE always carries a partition predicate** on `order_date`, or it full-scans.
- **Two clocks stay separate.** Slice on `updated_at` (system). Partition on `order_date`
  (business). Never mix them.
- **The repo must run broken on demand.** `src/gate.py` builds BOTH silver tables from the same
  batch — guarded and naive, one MERGE condition apart — and the gate FAILS if the naive one
  does not break. A demonstration that cannot fail proves nothing.
- **`run.py` preflights the Docker daemon before anything else**, including before `--fresh`
  tears down the lake. Daemon down → exit 2, `overall: BLOCKED`, nothing deleted. Every step
  shells out to `docker compose`, so the check belongs at the top, not per-step.
- **Generator stays deterministic.** Same seed → byte-identical output. No `now()`, no unseeded
  random. CI asserts this.
- **Runtime budget: under 5 minutes on 8 GB RAM** after the image is built. If a change blows
  that, the change is wrong.
- **`demo.py` was removed (Aug 2026) and must not come back.** Readers of a detailed tutorial
  want the real pipeline; a second toy implementation only creates drift between the article
  and the code.
- **README is a deliverable, updated in the same commit as the code.** Every command it lists
  must exist in `run.py`'s plan, every number must be traceable, every code snippet must appear
  verbatim in the source.
- **No double-entry accounting.** Deliberately removed — it added conceptual load for readers
  who aren't finance people. Revenue is the metric.

## Build order — step 5 is a gate

1. ✅ Source DB + deterministic feeds — *verified: 2416/2421/2393 rows*
2. ✅ Bronze ingestion — *verified: 348/321/312 for 2026-08-02*
3. ✅ Normalize 3 feeds → one shape — *verified vs source: 981 rows, gross $197,056.32*
4. ✅ Silver MERGE + restatement guard — *verified: 981→828, 153 collapsed, 0 dup keys,
   net $161,892.20 (gross was $165,739.46 before `net_amount_usd` existed)*
5. ✅ Gate — builds guarded AND naive silver from one batch, 7-day pass then replay; 4
   assertions plus "the naive table must break" (`src/gate.py`)
6. ✅ `net_amount_usd` — revenue as a stored column. Load-bearing: amounts do not change
   between versions, only status does, so `SUM(gross_amount_usd)` would make the replay
   demonstration print "unchanged" in both columns and prove nothing.
7. Gold — `fct_orders`, `agg_daily_revenue`
8. Airflow DAG + backfill demo
9. dbt marts + data quality tests
10. CI + clean-clone verification

Nothing past step 5 matters if step 5 fails.

## Versions — pin exactly, no ranges

`pyspark` and `delta-spark` **must be a matched pair** — Delta releases target one Spark minor.
Verified working: `pyspark==4.0.1` + `delta-spark==4.0.0` + Java 21 + Postgres 16 +
JDBC 42.7.4. Pinned in `requirements.txt` and recorded in the README's "Tested with" block.

## Conventions

- Source feeds live in schema `src`, are append-only version logs, PK `(natural_key, updated_at)`
- Bronze carries `_ingest_run_id`, `_ingested_at`, `_ingest_date`, `_interval_start`, `_interval_end`
- `_ingested_at` is wall clock and is **diagnostic only** — nothing may order or dedup by it
- Natural key after normalization: `(marketplace, marketplace_order_id)`
- Money is `Decimal`, never float. Base currency USD.
- Canonical order status: `placed | paid | refunded | cancelled`
- Lazada amounts arrive as strings and timestamps as local text with `+0800` — parse explicitly
- Spark runs in Docker, never natively on Windows (winutils/HADOOP_HOME breaks file writes)

## Publishing guardrail

Synthetic data only. No employer specifics, no real figures, nothing tied to a former employer's
architecture. Do not add Snowflake anywhere.
