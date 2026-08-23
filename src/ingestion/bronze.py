"""Bronze ingestion — interval-bound read, Delta append.

    python -m src.ingestion.bronze \
        --interval-start 2026-08-02T00:00:00+00:00 \
        --interval-end   2026-08-03T00:00:00+00:00

Both bounds are REQUIRED. There is no default and no clock read anywhere in this module.
That single constraint is what buys:

  * safe retries   — rerunning an interval reads exactly the same source slice
  * free backfill  — `airflow dags backfill` needs no special code path
  * auditability   — every bronze row carries the interval that produced it

The naive alternative (`WHERE updated_at > (SELECT max(updated_at) FROM target)`) makes the
slice a function of pipeline history: not reproducible, races under concurrency, and backfill
means mutating state. That version lands in step 5 behind --strategy=naive so readers can run
the failure themselves.
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config
from src.common.spark import build_spark, default_run_id, get_logger, parse_interval

log = get_logger("bronze")


def read_slice(spark: SparkSession, feed: config.Feed, start_iso: str, end_iso: str) -> DataFrame:
    """Predicate is pushed into the source query, so Postgres does the filtering, not Spark."""
    query = feed.bound_query(start_iso, end_iso)
    log.info("%s | %s", feed.name, " ".join(query.split()))
    return (
        spark.read.format("jdbc")
        .option("url", config.SOURCE_JDBC_URL)
        .option("user", config.SOURCE_USER)
        .option("password", config.SOURCE_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .load()
    )


def add_metadata(df: DataFrame, run_id: str, start_iso: str, end_iso: str) -> DataFrame:
    """_ingest_date partitions on the INTERVAL's date, not on wall clock — otherwise the
    same interval replayed tomorrow would land in a different partition and the rerun would
    not be comparable.

    _ingested_at is wall clock and is therefore the one non-deterministic column here. It is
    diagnostic only: nothing downstream may order or dedup by it, or reruns stop matching.
    """
    return (
        df.withColumn("_ingest_run_id", F.lit(run_id))
        .withColumn("_interval_start", F.lit(start_iso).cast("timestamp"))
        .withColumn("_interval_end", F.lit(end_iso).cast("timestamp"))
        .withColumn("_ingest_date", F.to_date(F.lit(start_iso).cast("timestamp")))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_feed(spark: SparkSession, feed: config.Feed, run_id: str,
                start_iso: str, end_iso: str) -> int:
    df = add_metadata(read_slice(spark, feed, start_iso, end_iso), run_id, start_iso, end_iso)
    n = df.count()
    if n == 0:
        log.warning("%s | 0 rows in interval — nothing written", feed.name)
        return 0
    (df.write.format("delta")
       .mode("append")
       .partitionBy("_ingest_date")
       .option("mergeSchema", "false")     # schema evolution is article #2, not a silent default
       .save(feed.bronze_path))
    log.info("%s | appended %d rows -> %s", feed.name, n, feed.bronze_path)
    return n


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--interval-start", required=True, help="ISO8601, inclusive")
    p.add_argument("--interval-end", required=True, help="ISO8601, exclusive")
    p.add_argument("--run-id", default=None, help="Airflow run_id; derived if omitted")
    p.add_argument("--feeds", default="all", help="comma-separated feed names, or 'all'")
    args = p.parse_args()

    start, end = parse_interval(args.interval_start, args.interval_end)
    start_iso, end_iso = start.isoformat(), end.isoformat()
    run_id = args.run_id or default_run_id(start)

    feeds = (config.FEEDS if args.feeds == "all"
             else tuple(config.FEEDS_BY_NAME[n.strip()] for n in args.feeds.split(",")))

    spark = build_spark("bronze-ingest")
    log.info("run_id=%s interval=[%s, %s)", run_id, start_iso, end_iso)

    total = 0
    try:
        for feed in feeds:
            total += ingest_feed(spark, feed, run_id, start_iso, end_iso)
    finally:
        spark.stop()

    log.info("done | %d rows across %d feeds", total, len(feeds))


if __name__ == "__main__":
    main()
