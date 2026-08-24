"""Normalize three marketplace shapes into one order table.

    docker compose run --rm jobs -m src.transform.normalize \
        --interval-start 2026-08-02T00:00:00+00:00 \
        --interval-end   2026-08-03T00:00:00+00:00

This is a pure transform: bronze in, canonical DataFrame out. Step 4 (silver MERGE) consumes
`normalize_all()` directly; the CLI here exists so the mapping can be inspected and verified on
its own.

What the three feeds disagree about, and what this module settles:

    concern        shopify              amazon                    lazada
    ------------------------------------------------------------------------------------
    id             id (bigint)          AmazonOrderId (string)    order_id (bigint)
    amount         numeric              numeric                   STRING '128.40'
    time           timestamptz UTC      timestamptz UTC           TEXT '... +0800'
    status         paid/refunded/...    Shipped/Canceled/...      'delivered,returned' list
    currency       USD                  USD                       SGD

Everything lands as: USD, UTC, Decimal, one status vocabulary, one key.
"""
from __future__ import annotations

import argparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.common import config
from src.common.spark import build_spark, emit_metric, get_logger, parse_interval

log = get_logger("normalize")

AMOUNT = DecimalType(12, 2)
RATE = DecimalType(18, 8)

# '2026-08-02 16:25:39 +0800' — pattern letter Z matches the +HHMM offset form.
LAZADA_TS_FMT = "yyyy-MM-dd HH:mm:ss Z"

CANONICAL_COLUMNS = [
    "order_key", "marketplace", "marketplace_order_id",
    "event_time_utc", "order_date", "updated_at_utc",
    "order_status", "currency",
    "gross_amount", "shipping_amount", "gross_amount_usd", "net_amount_usd",
    "_ingest_run_id", "_interval_start", "_interval_end",
]

# Statuses that contribute nothing to revenue. A refunded order still has a gross amount --
# the customer was charged -- but the money went back, so it must not be counted.
NON_REVENUE_STATUSES = ("refunded", "cancelled")


def _finalize(df: DataFrame, marketplace: str) -> DataFrame:
    """Shared tail of every feed's mapping: key, business date, column order."""
    return (
        df.withColumn("marketplace", F.lit(marketplace))
        .withColumn("marketplace_order_id", F.col("marketplace_order_id").cast("string"))
        .withColumn("order_key", F.concat_ws(":", F.lit(marketplace),
                                             F.col("marketplace_order_id")))
        # order_date is BUSINESS time, derived from when the order happened — never from
        # when we ingested it. This is the column silver partitions on, and the reason a
        # refund processed today still reduces the day the order belongs to.
        .withColumn("order_date", F.to_date("event_time_utc"))
        .select(*[c for c in CANONICAL_COLUMNS
                  if c not in ("gross_amount_usd", "net_amount_usd")])
    )


def _status(mapping: dict[str, str], src: Column, default: str = "placed") -> Column:
    out = F.when(F.lit(False), F.lit(default))
    for raw, canonical in mapping.items():
        out = out.when(src == F.lit(raw), F.lit(canonical))
    return out.otherwise(F.lit(default))


SHOPIFY_STATUS = {
    "pending": "placed", "paid": "paid",
    "partially_refunded": "refunded", "refunded": "refunded", "voided": "cancelled",
}
AMAZON_STATUS = {
    "Pending": "placed", "Unshipped": "paid", "Shipped": "paid",
    "Canceled": "cancelled", "Refunded": "refunded",
}


def normalize_shopify(df: DataFrame) -> DataFrame:
    return _finalize(
        df.select(
            F.col("id").alias("marketplace_order_id"),
            F.col("created_at").alias("event_time_utc"),
            F.col("updated_at").alias("updated_at_utc"),
            _status(SHOPIFY_STATUS, F.col("financial_status")).alias("order_status"),
            F.col("currency"),
            F.col("total_price").cast(AMOUNT).alias("gross_amount"),
            F.col("total_shipping").cast(AMOUNT).alias("shipping_amount"),
            "_ingest_run_id", "_interval_start", "_interval_end",
        ),
        "shopify",
    )


def normalize_amazon(df: DataFrame) -> DataFrame:
    return _finalize(
        df.select(
            F.col("AmazonOrderId").alias("marketplace_order_id"),
            F.col("PurchaseDate").alias("event_time_utc"),
            F.col("LastUpdateDate").alias("updated_at_utc"),
            _status(AMAZON_STATUS, F.col("OrderStatus")).alias("order_status"),
            F.col("OrderTotal_CurrencyCode").alias("currency"),
            F.col("OrderTotal_Amount").cast(AMOUNT).alias("gross_amount"),
            F.col("ShippingPrice_Amount").cast(AMOUNT).alias("shipping_amount"),
            "_ingest_run_id", "_interval_start", "_interval_end",
        ),
        "amazon",
    )


def normalize_lazada(df: DataFrame) -> DataFrame:
    """Three separate problems in one feed: text timestamps with a local offset, amounts as
    strings, and status as a comma-joined list where the LAST meaningful state wins."""
    statuses = F.col("statuses")
    status = (
        F.when(statuses.contains("returned"), F.lit("refunded"))
        .when(statuses.contains("canceled") | statuses.contains("cancelled"), F.lit("cancelled"))
        .when(statuses.contains("delivered") | statuses.contains("shipped"), F.lit("paid"))
        .otherwise(F.lit("placed"))
    )
    return _finalize(
        df.select(
            F.col("order_id").alias("marketplace_order_id"),
            F.to_timestamp("created_at", LAZADA_TS_FMT).alias("event_time_utc"),
            F.to_timestamp("updated_at", LAZADA_TS_FMT).alias("updated_at_utc"),
            status.alias("order_status"),
            F.col("currency"),
            F.col("price").cast(AMOUNT).alias("gross_amount"),        # string -> Decimal
            F.col("shipping_fee").cast(AMOUNT).alias("shipping_amount"),
            "_ingest_run_id", "_interval_start", "_interval_end",
        ),
        "lazada",
    )


NORMALIZERS = {
    "shopify_orders": normalize_shopify,
    "amazon_orders": normalize_amazon,
    "lazada_orders": normalize_lazada,
}


def read_fx(spark: SparkSession) -> DataFrame:
    """FX is reference data, loaded as a FULL SNAPSHOT — not interval-bound like the feeds.

    An interval-bound read would only return rates *published* inside that window, but an
    order needs the rate for its own date, which was published earlier. Facts are sliced;
    small dimensions are snapshotted. (v1 assumes rates are never restated — a restated rate
    invalidates already-converted amounts, which is a different and harder problem.)
    """
    return (
        spark.read.format("jdbc")
        .option("url", config.SOURCE_JDBC_URL)
        .option("user", config.SOURCE_USER)
        .option("password", config.SOURCE_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("query", "SELECT currency_pair, rate_date, rate FROM src.raw_fx_rates")
        .load()
        .select(
            F.col("currency_pair"),
            F.col("rate_date"),
            F.col("rate").cast(RATE).alias("rate"),
        )
    )


def convert_to_usd(orders: DataFrame, fx: DataFrame) -> DataFrame:
    """Rate is looked up by the order's OWN date, not today's. Same principle as partitioning:
    a value that belongs to a business day must be computed with that day's inputs."""
    joined = orders.withColumn("_pair", F.concat(F.col("currency"), F.lit("/USD"))).join(
        fx,
        (F.col("_pair") == fx["currency_pair"]) & (F.col("order_date") == fx["rate_date"]),
        "left",
    )
    return (
        joined.withColumn(
            "gross_amount_usd",
            (F.col("gross_amount") * F.coalesce(F.col("rate"), F.lit(1).cast(RATE))).cast(AMOUNT),
        )
        # net_amount_usd is what "revenue" means everywhere downstream. Keeping it as a stored
        # column rather than a filter at query time means every consumer -- the gate, the
        # metrics, gold -- agrees on the definition instead of each re-deriving it.
        .withColumn(
            "net_amount_usd",
            F.when(F.col("order_status").isin(*NON_REVENUE_STATUSES), F.lit(0).cast(AMOUNT))
             .otherwise(F.col("gross_amount_usd")),
        )
        .drop("_pair", "currency_pair", "rate_date", "rate")
        .select(*CANONICAL_COLUMNS)
    )


def normalize_all(spark: SparkSession, start_iso: str, end_iso: str) -> DataFrame:
    """Read each bronze feed's slice, map it, union, convert currency."""
    parts: list[DataFrame] = []
    for feed_name, fn in NORMALIZERS.items():
        feed = config.FEEDS_BY_NAME[feed_name]
        bronze = (
            spark.read.format("delta").load(feed.bronze_path)
            .where((F.col("_interval_start") == F.lit(start_iso).cast("timestamp"))
                   & (F.col("_interval_end") == F.lit(end_iso).cast("timestamp")))
        )
        parts.append(fn(bronze))

    unioned = parts[0]
    for p in parts[1:]:
        unioned = unioned.unionByName(p)

    return convert_to_usd(unioned, read_fx(spark))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--interval-start", required=True)
    p.add_argument("--interval-end", required=True)
    p.add_argument("--show", type=int, default=8)
    args = p.parse_args()

    start, end = parse_interval(args.interval_start, args.interval_end)
    spark = build_spark("normalize")
    try:
        df = normalize_all(spark, start.isoformat(), end.isoformat()).cache()
        total = df.count()
        log.info("normalized %d rows", total)

        log.info("rows and revenue by marketplace:")
        by_mkt = (df.groupBy("marketplace")
                    .agg(F.count("*").alias("rows"),
                         F.countDistinct("order_key").alias("distinct_orders"),
                         F.sum("gross_amount_usd").alias("gross_usd"),
                         F.sum("net_amount_usd").alias("net_usd"))
                    .orderBy("marketplace"))
        by_mkt.show(truncate=False)
        for r in by_mkt.collect():
            emit_metric(job="normalize", marketplace=r["marketplace"], rows=r["rows"],
                        distinct_orders=r["distinct_orders"], gross_usd=str(r["gross_usd"]), net_usd=str(r["net_usd"]))

        log.info("status vocabulary after mapping (must be placed/paid/refunded/cancelled):")
        status = df.groupBy("order_status").count().orderBy("order_status")
        status.show(truncate=False)
        statuses = {r["order_status"]: r["count"] for r in status.collect()}
        unexpected = sorted(set(statuses) - {"placed", "paid", "refunded", "cancelled"})

        log.info("null check on columns that must never be null:")
        required = ["order_key", "event_time_utc", "updated_at_utc", "order_date",
                    "gross_amount", "gross_amount_usd", "net_amount_usd"]
        nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                           for c in required]).collect()[0].asDict()
        df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required]).show()

        emit_metric(job="normalize", total_rows=total, statuses=statuses,
                    unexpected_statuses=unexpected, nulls=nulls,
                    ok=(not unexpected and not any(nulls.values())))

        df.orderBy("order_key").show(args.show, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
