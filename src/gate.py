"""THE GATE — proves the pipeline is safe to re-run, by showing what happens when it isn't.

    docker compose run --rm jobs -m src.gate

Everything after this step is decoration if this fails.

WHAT IT DOES
------------
It builds the same seven days of orders twice, through two silver tables that differ by
exactly one MERGE condition:

    guarded   WHEN MATCHED AND s.updated_at_utc > t.updated_at_utc THEN UPDATE ALL
    naive     WHEN MATCHED                                          THEN UPDATE ALL

Then it does the destructive thing on purpose:

    1. process 2026-08-01 .. 2026-08-07 in order, into both tables
    2. snapshot both — row count, revenue per marketplace per day, content hash
    3. RE-PROCESS 2026-08-02, a day whose orders were corrected on Aug 5-7
    4. snapshot both again and diff

Step 3 is not a contrived case. It is what a retry, a backfill, or a bug-fix rerun does.

WHAT IT ASSERTS (guarded table only — the naive one is expected to fail)
-----------------------------------------------------------------------
    A  row count and distinct keys unchanged by the replay
    B  revenue per marketplace per day identical
    C  content hash identical
    D  invariants: no duplicate keys, order_date always equals the order's own event date,
       and back-dated orders are actually present -- a replay that collides with nothing
       proves nothing, so a suite that passes on empty input is worse than no suite.

Exit code 0 only if the guarded table survived AND the naive one demonstrably did not.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config
from src.common.spark import build_spark, emit_metric, get_logger
from src.ingestion.bronze import ingest_feed
from src.ingestion.silver import dedupe_batch, merge_into_silver
from src.transform.normalize import normalize_all

log = get_logger("gate")

BASELINE_DIR = Path("tests/baselines")
GUARDED_PATH = f"{config.SILVER_ROOT}/orders"
NAIVE_PATH = f"{config.SILVER_ROOT}/orders_naive"

# Business columns only. Ingest metadata is excluded from the fingerprint because
# _ingested_at is wall clock -- including it would make every rerun "differ" for a reason
# that has nothing to do with correctness.
BUSINESS_COLS = ["order_key", "marketplace", "marketplace_order_id", "event_time_utc",
                 "order_date", "updated_at_utc", "order_status", "currency",
                 "gross_amount", "shipping_amount", "gross_amount_usd", "net_amount_usd"]


def process_interval(spark: SparkSession, start: datetime, end: datetime, run_id: str) -> int:
    """Ingest one interval ONCE, then merge the identical batch into both silver tables."""
    s, e = start.isoformat(), end.isoformat()
    rows = sum(ingest_feed(spark, feed, run_id, s, e) for feed in config.FEEDS)
    if rows == 0:
        return 0
    batch = dedupe_batch(normalize_all(spark, s, e)).cache()
    merge_into_silver(spark, batch, path=GUARDED_PATH, guarded=True)
    merge_into_silver(spark, batch, path=NAIVE_PATH, guarded=False)
    batch.unpersist()
    return rows


def snapshot(spark: SparkSession, path: str) -> dict:
    df = spark.read.format("delta").load(path).cache()
    revenue = {
        f"{r['marketplace']}|{r['order_date']}": str(r["revenue"])
        for r in df.groupBy("marketplace", "order_date")
                   .agg(F.sum("net_amount_usd").alias("revenue")).collect()
    }
    # Sorted in Python so the fingerprint is independent of Spark's partition ordering.
    row_hashes = sorted(
        r["h"] for r in df.select(
            F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in BUSINESS_COLS]),
                   256).alias("h")).collect()
    )
    snap = {
        "total_rows": df.count(),
        "distinct_keys": df.select("order_key").distinct().count(),
        "total_revenue_usd": str(df.agg(F.sum("net_amount_usd")).collect()[0][0]),
        "revenue_by_marketplace_day": dict(sorted(revenue.items())),
        "content_hash": hashlib.sha256("".join(row_hashes).encode()).hexdigest(),
    }
    df.unpersist()
    return snap


def revenue_on(spark: SparkSession, path: str, day: str) -> float:
    df = spark.read.format("delta").load(path).where(F.col("order_date") == day)
    v = df.agg(F.sum("net_amount_usd")).collect()[0][0]
    return float(v or 0)


def invariants(spark: SparkSession) -> dict:
    df = spark.read.format("delta").load(GUARDED_PATH)
    total, distinct = df.count(), df.select("order_key").distinct().count()
    return {
        "duplicate_keys": total - distinct,
        # order_date must always be the date of the order's own event time
        "rows_with_wrong_order_date":
            df.where(F.col("order_date") != F.to_date("event_time_utc")).count(),
        # if this is 0 the replay collides with nothing and the suite proves nothing
        "backdated_orders_present":
            df.where(F.to_date("_interval_start") > F.date_add(F.col("order_date"), 2)).count(),
    }


def diff(before: dict, after: dict) -> list[str]:
    problems = [f"{k}: {before[k]} -> {after[k]}"
                for k in ("total_rows", "distinct_keys", "total_revenue_usd", "content_hash")
                if before[k] != after[k]]
    b, a = before["revenue_by_marketplace_day"], after["revenue_by_marketplace_day"]
    problems += [f"revenue[{k}]: {b.get(k)} -> {a.get(k)}"
                 for k in sorted(set(b) | set(a)) if b.get(k) != a.get(k)]
    return problems


BAR, RULE, COL = "=" * 66, "-" * 66, 17


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--replay", default="2026-08-02",
                   help="the OLD interval to re-process after all the others")
    p.add_argument("--keep-lake", action="store_true", help="skip the clean rebuild")
    args = p.parse_args()

    if not args.keep_lake:
        shutil.rmtree(config.LAKE_ROOT, ignore_errors=True)
        log.info("lake reset: %s", config.LAKE_ROOT)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    replay = datetime.fromisoformat(args.replay).replace(tzinfo=timezone.utc)
    spark = build_spark("gate")
    failures: list[str] = []

    try:
        # 1 -- forward pass
        for d in range(args.days):
            s = start + timedelta(days=d)
            n = process_interval(spark, s, s + timedelta(days=1), f"gate__{s:%Y%m%d}")
            log.info("interval %s | %d source rows", s.date(), n)

        inv = invariants(spark)
        log.info("invariants: %s", inv)
        if inv["duplicate_keys"]:
            failures.append(f"D: {inv['duplicate_keys']} duplicate order keys")
        if inv["rows_with_wrong_order_date"]:
            failures.append(f"D: {inv['rows_with_wrong_order_date']} rows have the wrong order_date")
        if inv["backdated_orders_present"] == 0:
            failures.append("D: no back-dated orders present -- the replay proves nothing")

        g_before, n_before = snapshot(spark, GUARDED_PATH), snapshot(spark, NAIVE_PATH)
        BASELINE_DIR.mkdir(parents=True, exist_ok=True)
        (BASELINE_DIR / "silver_baseline.json").write_text(json.dumps(g_before, indent=2))

        day = str(replay.date())
        g_rev_before, n_rev_before = revenue_on(spark, GUARDED_PATH, day), revenue_on(spark, NAIVE_PATH, day)

        # 2 -- replay an OLD interval, after later corrections have already landed
        log.info("REPLAYING %s -- orders from this day were corrected on later days", day)
        process_interval(spark, replay, replay + timedelta(days=1), f"gate__replay__{replay:%Y%m%d}")

        g_after, n_after = snapshot(spark, GUARDED_PATH), snapshot(spark, NAIVE_PATH)
        g_rev_after, n_rev_after = revenue_on(spark, GUARDED_PATH, day), revenue_on(spark, NAIVE_PATH, day)

        failures += [f"A/B/C: {x}" for x in diff(g_before, g_after)]
        naive_broke = bool(diff(n_before, n_after))
        if not naive_broke:
            failures.append("naive table did not change on replay -- the demonstration is broken")

        reverted = (spark.read.format("delta").load(GUARDED_PATH).alias("g")
                    .join(spark.read.format("delta").load(NAIVE_PATH).alias("n"), "order_key")
                    .where(F.col("n.updated_at_utc") < F.col("g.updated_at_utc")).count())

        ok = not failures
        emit_metric(job="gate", ok=ok, failures=failures, naive_broke=naive_broke,
                    guarded_rows=g_after["total_rows"],
                    guarded_revenue_usd=g_after["total_revenue_usd"],
                    guarded_hash_before=g_before["content_hash"][:16],
                    guarded_hash_after=g_after["content_hash"][:16],
                    naive_revenue_before=n_before["total_revenue_usd"],
                    naive_revenue_after=n_after["total_revenue_usd"],
                    replay_day=day,
                    replay_day_revenue_naive_before=round(n_rev_before, 2),
                    replay_day_revenue_naive_after=round(n_rev_after, 2),
                    replay_day_revenue_guarded_before=round(g_rev_before, 2),
                    replay_day_revenue_guarded_after=round(g_rev_after, 2),
                    orders_reverted_by_naive=reverted, **inv)

        drift = ((n_rev_after - n_rev_before) / n_rev_before * 100) if n_rev_before else 0
        print(f"\n{BAR}")
        print(f"  Replaying {day} -- one MERGE condition apart")
        print(BAR)
        print(f"  {'':<26}{'NAIVE':>{COL}}{'GUARDED':>{COL}}")
        print(RULE)
        print(f"  {day + ' revenue':<26}{n_rev_before:>{COL},.2f}{g_rev_before:>{COL},.2f}")
        print(f"  {'after replaying it':<26}{n_rev_after:>{COL},.2f}{g_rev_after:>{COL},.2f}")
        print(f"  {'':<26}{('%+.1f%%' % drift):>{COL}}{'unchanged':>{COL}}")
        print(RULE)
        print(f"  {reverted} orders reverted to an older version by the naive MERGE")
        print(f"  content hash  {g_before['content_hash'][:16]} -> {g_after['content_hash'][:16]}")
        print(f"  {inv['backdated_orders_present']} back-dated orders present, "
              f"{inv['duplicate_keys']} duplicate keys, "
              f"{inv['rows_with_wrong_order_date']} wrong-day rows")
        print(BAR)
        print("  GATE PASSED" if ok else "  GATE FAILED")
        for f in failures:
            print(f"    - {f}")
        print(f"{BAR}\n")
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
