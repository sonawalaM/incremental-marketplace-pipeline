"""Silver MERGE — dedup within the batch, then upsert with a restatement guard.

    docker compose run --rm jobs -m src.ingestion.silver \
        --interval-start 2026-08-02T00:00:00+00:00 \
        --interval-end   2026-08-03T00:00:00+00:00

Silver holds ONE row per order: its latest known version. Two mechanisms get it there, and
both are load-bearing.

1. IN-BATCH DEDUP
   A single interval can contain several versions of the same order (placed at 09:00, paid at
   11:00). MERGE cannot accept duplicate keys in the source — Delta raises rather than picking
   one arbitrarily — so the batch is collapsed to the latest version per order first.

2. THE RESTATEMENT GUARD
       WHEN MATCHED AND source.updated_at_utc > target.updated_at_utc THEN UPDATE

   Without that condition, replaying an older interval overwrites a corrected order with a
   superseded version. Backfill stops being safe and starts being destructive — silently, with
   no error, and only visible later as revenue that changed for no reason.

Partition pruning: the MERGE restricts the target to the order_date partitions present in the
batch. Without it Delta scans the whole table on every run.
"""
from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import config
from src.common.spark import build_spark, emit_metric, get_logger, parse_interval
from src.transform.normalize import CANONICAL_COLUMNS, normalize_all

log = get_logger("silver")

SILVER_PATH = f"{config.SILVER_ROOT}/orders"
KEY = "order_key"


def dedupe_batch(df: DataFrame) -> DataFrame:
    """Collapse to the latest version per order.

    Ordering is deterministic on purpose: `updated_at_utc` alone is already unique per order
    (source PKs are (natural_key, updated_at)), and `gross_amount` is a stable tiebreak if that
    ever stops holding. Note what is NOT used — `_ingested_at`. It is wall clock, so ordering by
    it would make two runs of the same interval pick different rows, and the idempotency test
    would fail for a reason that has nothing to do with the pipeline's logic.
    """
    w = Window.partitionBy(KEY).orderBy(
        F.col("updated_at_utc").desc(), F.col("gross_amount").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def merge_into_silver(spark: SparkSession, batch: DataFrame) -> dict:
    batch = batch.select(*CANONICAL_COLUMNS).cache()

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        n = batch.count()
        (batch.write.format("delta").mode("overwrite")
              .partitionBy("order_date").save(SILVER_PATH))
        log.info("created silver with %d rows", n)
        return {"created": True, "rows_written": n}

    # Restrict the target to the partitions this batch actually touches. A MERGE without this
    # predicate rewrites far more of the table than it needs to.
    dates = [r["order_date"] for r in batch.select("order_date").distinct().collect()]
    date_list = ", ".join(f"'{d}'" for d in dates)
    partition_pred = f"t.order_date IN ({date_list})"

    # NOTE: this pruning is only safe because order_date is STABLE for a given order — it comes
    # from event time, which never changes. If a restatement could move an order to a different
    # day, the pruned target would miss the existing row and MERGE would insert a duplicate.
    # Anything that can change its own partition key cannot be pruned on it.
    target = DeltaTable.forPath(spark, SILVER_PATH)
    (target.alias("t")
        .merge(batch.alias("s"), f"t.{KEY} = s.{KEY} AND {partition_pred}")
        .whenMatchedUpdateAll(condition="s.updated_at_utc > t.updated_at_utc")
        .whenNotMatchedInsertAll()
        .execute())

    m = target.history(1).select("operationMetrics").collect()[0][0]
    return {
        "created": False,
        "partitions_touched": len(dates),
        "rows_inserted": int(m.get("numTargetRowsInserted", 0)),
        "rows_updated": int(m.get("numTargetRowsUpdated", 0)),
        "rows_unchanged": int(m.get("numTargetRowsCopied", 0)),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--interval-start", required=True)
    p.add_argument("--interval-end", required=True)
    args = p.parse_args()

    start, end = parse_interval(args.interval_start, args.interval_end)
    spark = build_spark("silver-merge")
    try:
        normalized = normalize_all(spark, start.isoformat(), end.isoformat()).cache()
        rows_in = normalized.count()

        batch = dedupe_batch(normalized).cache()
        rows_deduped = batch.count()
        log.info("batch | %d rows in -> %d after dedup (%d restatements collapsed)",
                 rows_in, rows_deduped, rows_in - rows_deduped)

        result = merge_into_silver(spark, batch)

        silver = spark.read.format("delta").load(SILVER_PATH)
        total = silver.count()
        distinct = silver.select(KEY).distinct().count()
        revenue = silver.agg(F.sum("gross_amount_usd")).collect()[0][0]

        log.info("silver | %d rows, %d distinct keys, revenue_usd=%s", total, distinct, revenue)
        emit_metric(job="silver", rows_in=rows_in, rows_after_dedup=rows_deduped,
                    restatements_collapsed=rows_in - rows_deduped,
                    silver_rows=total, silver_distinct_keys=distinct,
                    silver_revenue_usd=str(revenue),
                    # This must always hold. One row per order is silver's entire contract.
                    duplicate_keys=(total - distinct), **result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
