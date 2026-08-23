"""THE GATE — proof that the pipeline is safely re-runnable.

    docker compose run --rm jobs -m src.gate --start 2026-08-01 --days 7 --replay 2026-08-02

Everything after this step is decoration if this fails.

The sequence is deliberately the destructive one:

  1. process Aug 1 .. Aug 7 in order
  2. snapshot silver
  3. RE-PROCESS AUG 2 — an interval whose orders have since been corrected in later intervals
  4. snapshot again and diff

Step 3 is the scenario that breaks naive pipelines. Re-running an old interval is not a
contrived case; it is what a retry, a backfill, or a late bug-fix rerun actually does. Without
the restatement guard, Aug 2's stale versions overwrite corrections made on Aug 5, revenue
silently changes, and nothing errors.

Four assertions:
  A  row count and distinct keys unchanged
  B  per-marketplace-per-day revenue identical      <- the one that matters
  C  content hash identical                          <- catches changes A and B would miss
  D  invariants hold (no dup keys, order_date == event date, back-dated data present)

Runs everything in ONE Spark session. Fourteen container starts would take ten minutes;
this takes about one.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import config
from src.common.spark import build_spark, emit_metric, get_logger
from src.ingestion.bronze import ingest_feed
from src.ingestion.silver import SILVER_PATH, dedupe_batch, merge_into_silver
from src.transform.normalize import normalize_all

log = get_logger("gate")

BASELINE_DIR = Path("tests/baselines")


def process_interval(spark: SparkSession, start: datetime, end: datetime, run_id: str) -> int:
    s, e = start.isoformat(), end.isoformat()
    rows = sum(ingest_feed(spark, feed, run_id, s, e) for feed in config.FEEDS)
    if rows == 0:
        return 0
    batch = dedupe_batch(normalize_all(spark, s, e))
    merge_into_silver(spark, batch)
    return rows


def snapshot(spark: SparkSession) -> dict:
    """A fingerprint of silver, precise enough that any change shows up as a diff."""
    df = spark.read.format("delta").load(SILVER_PATH).cache()

    revenue = {
        f"{r['marketplace']}|{r['order_date']}": str(r["revenue"])
        for r in df.groupBy("marketplace", "order_date")
                   .agg(F.sum("gross_amount_usd").alias("revenue"))
                   .collect()
    }

    # Per-row hash over the business columns only. Ingest metadata is excluded on purpose:
    # _ingested_at is wall clock, so including it would make every rerun "differ" for a reason
    # that has nothing to do with correctness.
    business_cols = ["order_key", "marketplace", "marketplace_order_id", "event_time_utc",
                     "order_date", "updated_at_utc", "order_status", "currency",
                     "gross_amount", "shipping_amount", "gross_amount_usd"]
    # Sorted in Python so the fingerprint is independent of Spark's partition ordering —
    # two runs that produce the same rows must produce the same hash regardless of layout.
    row_hashes = sorted(
        r["h"] for r in df.select(
            F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in business_cols]),
                   256).alias("h")
        ).collect()
    )

    return {
        "total_rows": df.count(),
        "distinct_keys": df.select("order_key").distinct().count(),
        "total_revenue_usd": str(df.agg(F.sum("gross_amount_usd")).collect()[0][0]),
        "revenue_by_marketplace_day": dict(sorted(revenue.items())),
        "content_hash": hashlib.sha256("".join(row_hashes).encode()).hexdigest(),
    }


def invariants(spark: SparkSession) -> dict:
    df = spark.read.format("delta").load(SILVER_PATH)

    total = df.count()
    distinct = df.select("order_key").distinct().count()

    # order_date must always equal the date of the order's own event time.
    wrong_date = df.where(F.col("order_date") != F.to_date("event_time_utc")).count()

    # Prove the back-dated scenario is actually exercised: orders whose winning version was
    # ingested in an interval well after the day the order belongs to. If this is 0, the test
    # suite looks green while testing nothing interesting.
    backdated = df.where(
        F.to_date("_interval_start") > F.date_add(F.col("order_date"), 2)
    ).count()

    return {
        "duplicate_keys": total - distinct,
        "rows_with_wrong_order_date": wrong_date,
        "backdated_orders_present": backdated,
    }


def diff(before: dict, after: dict) -> list[str]:
    problems: list[str] = []
    for key in ("total_rows", "distinct_keys", "total_revenue_usd", "content_hash"):
        if before[key] != after[key]:
            problems.append(f"{key}: {before[key]} -> {after[key]}")

    b, a = before["revenue_by_marketplace_day"], after["revenue_by_marketplace_day"]
    for k in sorted(set(b) | set(a)):
        if b.get(k) != a.get(k):
            problems.append(f"revenue[{k}]: {b.get(k)} -> {a.get(k)}")
    return problems


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--replay", default="2026-08-02",
                   help="the OLD interval to re-process after all others")
    p.add_argument("--keep-lake", action="store_true",
                   help="do not wipe the lake first (default is a clean build)")
    args = p.parse_args()

    if not args.keep_lake:
        shutil.rmtree(config.LAKE_ROOT, ignore_errors=True)
        log.info("lake reset: %s", config.LAKE_ROOT)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    spark = build_spark("gate")
    failures: list[str] = []

    try:
        # --- 1. forward pass -------------------------------------------------------------
        for d in range(args.days):
            s = start + timedelta(days=d)
            e = s + timedelta(days=1)
            n = process_interval(spark, s, e, f"gate__{s:%Y%m%d}")
            log.info("interval %s | %d source rows", s.date(), n)

        inv = invariants(spark)
        log.info("invariants: %s", inv)
        if inv["duplicate_keys"]:
            failures.append(f"D: {inv['duplicate_keys']} duplicate order keys in silver")
        if inv["rows_with_wrong_order_date"]:
            failures.append(f"D: {inv['rows_with_wrong_order_date']} rows have order_date != event date")
        if inv["backdated_orders_present"] == 0:
            failures.append("D: no back-dated orders present — the test proves nothing")

        before = snapshot(spark)
        BASELINE_DIR.mkdir(parents=True, exist_ok=True)
        (BASELINE_DIR / "silver_baseline.json").write_text(json.dumps(before, indent=2))
        log.info("baseline | %d rows, revenue=%s, hash=%s",
                 before["total_rows"], before["total_revenue_usd"],
                 before["content_hash"][:16])

        # --- 2. replay an OLD interval ---------------------------------------------------
        replay = datetime.fromisoformat(args.replay).replace(tzinfo=timezone.utc)
        log.info("REPLAYING %s — orders from this day have since been corrected", replay.date())
        process_interval(spark, replay, replay + timedelta(days=1), f"gate__replay__{replay:%Y%m%d}")

        after = snapshot(spark)
        problems = diff(before, after)
        failures.extend(f"A/B/C: {x}" for x in problems)

        ok = not failures
        emit_metric(job="gate", ok=ok, failures=failures,
                    rows=after["total_rows"], distinct_keys=after["distinct_keys"],
                    revenue_usd=after["total_revenue_usd"],
                    content_hash_before=before["content_hash"][:16],
                    content_hash_after=after["content_hash"][:16],
                    **inv)

        print("\n" + "=" * 64)
        if ok:
            print("  GATE PASSED")
            print(f"    {after['total_rows']} orders, revenue ${after['total_revenue_usd']}")
            print(f"    replaying {replay.date()} changed nothing — content hash identical")
            print(f"    {inv['backdated_orders_present']} back-dated orders handled correctly")
        else:
            print("  GATE FAILED")
            for f in failures:
                print(f"    - {f}")
        print("=" * 64)
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
