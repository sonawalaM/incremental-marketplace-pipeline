# Incremental Marketplace Revenue Pipeline

Portfolio project for Mansukh Sonawala's paid technical writing business. The repo must be
good enough that a developer-tool content team reads it and commissions an article.

## What this is

Three heterogeneous marketplace order feeds (Shopify / Amazon / Lazada shapes) → one clean
order table → daily revenue, built to be **safely re-runnable**.

```
Postgres CDC-style feeds
   │  sliced by updated_at        ← system time
   ▼
BRONZE  Delta · append-only · partitioned by _ingest_date
   │  dedup + MERGE with restatement guard
   ▼
SILVER  orders · latest version per order · partitioned by order_date   ← business time
   │
   ▼
GOLD    fct_orders          one row per order, current state
        agg_daily_revenue   marketplace × order_date → orders, gross, refunds, net, AOV
```

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

## Hard rules

- **Interval bounds are arguments, never clock reads.**
- **MERGE always carries the restatement guard:**
  `WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE`
  Without it, replaying an old interval overwrites corrected rows with stale ones.
- **MERGE always carries a partition predicate** on `order_date`, or it full-scans.
- **Two clocks stay separate.** Slice on `updated_at` (system). Partition on `order_date`
  (business). Never mix them.
- **The repo must run broken on demand.** `make demo-broken` vs `make demo-fixed`, one config
  flag apart. The failure has to be reproducible by a reader.
- **Generator stays deterministic.** Same seed → byte-identical output. No `now()`, no unseeded
  random. CI asserts this.
- **Runtime budget: under 5 minutes on 8 GB RAM.** If a change blows that, the change is wrong.
- **No double-entry accounting.** Deliberately removed — it added conceptual load for readers
  who aren't finance people. Revenue is the metric.

## Build order — step 5 is a gate

1. ✅ Source DB + deterministic feeds *(done, verified)*
2. ✅ Bronze ingestion — interval-bound JDBC read, Delta append *(written; first run pending)*
3. Normalize 3 feeds → one order shape (currency → USD, status → canonical, TZ → UTC)
4. Silver MERGE — dedup + restatement guard
5. **GATE: verification suite passes**
   - order count unchanged on rerun
   - **per-marketplace-per-day net revenue equals baseline** ← the check that matters
   - no duplicate order keys in `fct_orders`
   - a back-dated refund lands on the order's date, not today's
6. Gold — `fct_orders`, `agg_daily_revenue`
7. Broken mode (`make demo-broken`)
8. Airflow DAG + backfill demo
9. dbt marts + tests
10. CI, README, clean-clone test

Nothing past step 5 matters if step 5 fails.

## Versions — pin exactly, no ranges

`pyspark` and `delta-spark` **must be a matched pair** — Delta releases target one Spark minor.
Currently building on `pyspark==4.0.1` + `delta-spark==4.0.0` + Java 17 (see `Dockerfile`).
Once a run succeeds, record the exact versions in the README's "tested with" block.

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
