"""Dedup and restatement-guard tests.

`dedupe_batch` needs no Delta, so it is tested directly. The MERGE guard itself is expressed
here as the equivalent predicate, so the *semantics* are pinned even in environments without
the Delta jars; the end-to-end MERGE is covered by the idempotency suite at build step 5.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ingestion.silver import dedupe_batch

UTC = timezone.utc

SCHEMA = (
    "order_key string, marketplace string, marketplace_order_id string, "
    "event_time_utc timestamp, order_date date, updated_at_utc timestamp, "
    "order_status string, currency string, gross_amount decimal(12,2), "
    "shipping_amount decimal(12,2), gross_amount_usd decimal(12,2), "
    "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp"
)


def row(key, updated_h, status, amount="100.00", order_day=2, run="run-1"):
    return (key, "shopify", key.split(":")[1],
            datetime(2026, 8, order_day, 9, 0, tzinfo=UTC), date(2026, 8, order_day),
            datetime(2026, 8, order_day, updated_h, 0, tzinfo=UTC),
            status, "USD", Decimal(amount), Decimal("0.00"), Decimal(amount),
            run, datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 3, tzinfo=UTC))


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("tests-silver").master("local[2]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_dedup_keeps_latest_version(spark):
    df = spark.createDataFrame(
        [row("shopify:1", 9, "placed"),
         row("shopify:1", 14, "refunded"),      # latest — this one must survive
         row("shopify:1", 11, "paid"),
         row("shopify:2", 10, "paid")],
        SCHEMA,
    )
    out = {r.order_key: r.order_status for r in dedupe_batch(df).collect()}
    assert out == {"shopify:1": "refunded", "shopify:2": "paid"}


def test_dedup_leaves_one_row_per_key(spark):
    df = spark.createDataFrame(
        [row(f"shopify:{k}", h, "paid") for k in (1, 2, 3) for h in (9, 10, 11)], SCHEMA
    )
    out = dedupe_batch(df)
    assert out.count() == 3
    assert out.select("order_key").distinct().count() == 3


def test_dedup_is_deterministic_across_runs(spark):
    """Same input twice must give the same winner — otherwise the idempotency gate at step 5
    fails for reasons unrelated to the pipeline's actual logic."""
    rows = [row("shopify:1", h, s) for h, s in ((9, "placed"), (12, "paid"), (15, "refunded"))]
    a = [tuple(r) for r in dedupe_batch(spark.createDataFrame(rows, SCHEMA)).collect()]
    b = [tuple(r) for r in dedupe_batch(spark.createDataFrame(rows, SCHEMA)).collect()]
    assert a == b


def test_restatement_guard_rejects_stale_versions(spark):
    """The MERGE condition `s.updated_at_utc > t.updated_at_utc` in predicate form.

    This is the bug that makes backfills destructive: replay an old interval without the
    guard and a corrected order is overwritten by a superseded one.
    """
    target = spark.createDataFrame([row("shopify:1", 14, "refunded")], SCHEMA).alias("t")
    stale = spark.createDataFrame([row("shopify:1", 9, "placed")], SCHEMA).alias("s")

    would_update = (
        target.join(stale, F.col("t.order_key") == F.col("s.order_key"))
        .where(F.col("s.updated_at_utc") > F.col("t.updated_at_utc"))
        .count()
    )
    assert would_update == 0, "stale version must not overwrite a newer one"

    newer = spark.createDataFrame([row("shopify:1", 20, "cancelled")], SCHEMA).alias("s")
    should_update = (
        target.join(newer, F.col("t.order_key") == F.col("s.order_key"))
        .where(F.col("s.updated_at_utc") > F.col("t.updated_at_utc"))
        .count()
    )
    assert should_update == 1, "a genuinely newer version must be applied"


def test_backdated_refund_keeps_the_orders_own_date(spark):
    """A refund written days later still belongs to the day the order was placed. If this
    slips, revenue moves between days and every historical number stops matching."""
    df = spark.createDataFrame(
        [row("shopify:1", 9, "paid", order_day=2),
         # same order, restated on Aug 6 — but order_date must stay Aug 2
         ("shopify:1", "shopify", "1",
          datetime(2026, 8, 2, 9, 0, tzinfo=UTC), date(2026, 8, 2),
          datetime(2026, 8, 6, 3, 0, tzinfo=UTC), "refunded", "USD",
          Decimal("100.00"), Decimal("0.00"), Decimal("100.00"),
          "run-2", datetime(2026, 8, 6, tzinfo=UTC), datetime(2026, 8, 7, tzinfo=UTC))],
        SCHEMA,
    )
    r = dedupe_batch(df).collect()[0]
    assert r.order_status == "refunded"
    assert str(r.order_date) == "2026-08-02"
