"""Mapping tests for the three marketplace shapes.

These run on hand-built DataFrames, not on bronze — so they need no Delta, no Postgres, and
no Docker. Fast enough to run on every push, which is what makes them worth having.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transform.normalize import (
    convert_to_usd,
    normalize_amazon,
    normalize_lazada,
    normalize_shopify,
)

UTC = timezone.utc
META = ("run-1", datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 3, tzinfo=UTC))


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("tests").master("local[2]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_shopify_maps_to_canonical(spark):
    df = spark.createDataFrame(
        [(5000001, datetime(2026, 8, 2, 10, 0, tzinfo=UTC), datetime(2026, 8, 2, 11, 0, tzinfo=UTC),
          Decimal("128.40"), Decimal("6.88"), "USD", "partially_refunded", *META)],
        "id long, created_at timestamp, updated_at timestamp, total_price decimal(12,2), "
        "total_shipping decimal(12,2), currency string, financial_status string, "
        "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp",
    )
    r = normalize_shopify(df).collect()[0]
    assert r.order_key == "shopify:5000001"
    assert r.order_status == "refunded"          # partially_refunded collapses to refunded
    assert r.gross_amount == Decimal("128.40")
    assert str(r.order_date) == "2026-08-02"


def test_amazon_status_vocabulary(spark):
    rows = [("111-1", datetime(2026, 8, 2, 9, 0, tzinfo=UTC), datetime(2026, 8, 2, 9, 0, tzinfo=UTC),
             Decimal("50.00"), "USD", Decimal("3.99"), s, *META)
            for s in ("Pending", "Unshipped", "Shipped", "Canceled", "Refunded")]
    df = spark.createDataFrame(
        rows,
        "AmazonOrderId string, PurchaseDate timestamp, LastUpdateDate timestamp, "
        "OrderTotal_Amount decimal(12,2), OrderTotal_CurrencyCode string, "
        "ShippingPrice_Amount decimal(12,2), OrderStatus string, "
        "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp",
    )
    got = [r.order_status for r in normalize_amazon(df).collect()]
    assert got == ["placed", "paid", "paid", "cancelled", "refunded"]


def test_lazada_parses_local_time_and_string_amounts(spark):
    df = spark.createDataFrame(
        [(8000001, "2026-08-02 16:25:39 +0800", "2026-08-03 01:10:00 +0800",
          "128.40", "6.88", "SGD", "delivered,returned", *META)],
        "order_id long, created_at string, updated_at string, price string, "
        "shipping_fee string, currency string, statuses string, "
        "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp",
    )
    r = normalize_lazada(df).collect()[0]
    # 16:25 SGT is 08:25 UTC — and the order therefore belongs to Aug 2, not Aug 3.
    assert r.event_time_utc == datetime(2026, 8, 2, 8, 25, 39)
    assert str(r.order_date) == "2026-08-02"
    # updated_at 01:10 +0800 on Aug 3 is 17:10 UTC on Aug 2 — the offset genuinely moves the day
    assert r.updated_at_utc == datetime(2026, 8, 2, 17, 10, 0)
    assert r.gross_amount == Decimal("128.40")   # parsed from a string, exact, not a float
    assert r.order_status == "refunded"          # 'returned' wins over 'delivered'


def test_lazada_status_precedence(spark):
    cases = {"unpaid": "placed", "shipped": "paid", "delivered": "paid",
             "delivered,returned": "refunded", "canceled": "cancelled"}
    df = spark.createDataFrame(
        [(8000001 + i, "2026-08-02 10:00:00 +0800", "2026-08-02 10:00:00 +0800",
          "10.00", "0.00", "SGD", s, *META) for i, s in enumerate(cases)],
        "order_id long, created_at string, updated_at string, price string, "
        "shipping_fee string, currency string, statuses string, "
        "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp",
    )
    got = {r.marketplace_order_id: r.order_status
           for r in normalize_lazada(df).orderBy("marketplace_order_id").collect()}
    assert list(got.values()) == list(cases.values())


def test_fx_uses_the_orders_own_date(spark):
    """A rate from the wrong day is the same class of bug as a partition on the wrong day."""
    orders = spark.createDataFrame(
        [("lazada:1", "lazada", "1", datetime(2026, 8, 2, 8, 0, tzinfo=UTC), date(2026, 8, 2),
          datetime(2026, 8, 2, 8, 0, tzinfo=UTC), "paid", "SGD",
          Decimal("100.00"), Decimal("0.00"), *META),
         ("shopify:2", "shopify", "2", datetime(2026, 8, 2, 8, 0, tzinfo=UTC), date(2026, 8, 2),
          datetime(2026, 8, 2, 8, 0, tzinfo=UTC), "paid", "USD",
          Decimal("100.00"), Decimal("0.00"), *META)],
        "order_key string, marketplace string, marketplace_order_id string, "
        "event_time_utc timestamp, order_date date, updated_at_utc timestamp, "
        "order_status string, currency string, gross_amount decimal(12,2), "
        "shipping_amount decimal(12,2), _ingest_run_id string, "
        "_interval_start timestamp, _interval_end timestamp",
    )
    fx = spark.createDataFrame(
        [("SGD/USD", date(2026, 8, 2), Decimal("0.74000000")),
         ("SGD/USD", date(2026, 8, 3), Decimal("0.99000000")),  # wrong day — must NOT be used
         ("USD/USD", date(2026, 8, 2), Decimal("1.00000000"))],
        "currency_pair string, rate_date date, rate decimal(18,8)",
    )
    out = {r.order_key: r.gross_amount_usd for r in convert_to_usd(orders, fx).collect()}
    assert out["lazada:1"] == Decimal("74.00")
    assert out["shopify:2"] == Decimal("100.00")


def test_union_keeps_one_row_per_source_version(spark):
    """Normalization must not add or drop rows — dedup is silver's job, not this module's."""
    shopify = spark.createDataFrame(
        [(1, datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC),
          Decimal("10.00"), Decimal("0.00"), "USD", "paid", *META),
         (1, datetime(2026, 8, 2, tzinfo=UTC), datetime(2026, 8, 2, 5, tzinfo=UTC),
          Decimal("10.00"), Decimal("0.00"), "USD", "refunded", *META)],
        "id long, created_at timestamp, updated_at timestamp, total_price decimal(12,2), "
        "total_shipping decimal(12,2), currency string, financial_status string, "
        "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp",
    )
    out = normalize_shopify(shopify)
    assert out.count() == 2
    assert out.select("order_key").distinct().count() == 1


def test_refunded_orders_contribute_zero_revenue(spark):
    """The single most load-bearing column in the project.

    Amounts do not change between versions of an order — only the status does. If revenue were
    SUM(gross_amount_usd), reverting a refund would change the status and leave the number
    identical, and the whole replay demonstration would silently prove nothing.
    """
    rows = [(f"{i}", datetime(2026, 8, 2, 9, 0, tzinfo=UTC), datetime(2026, 8, 2, 9, 0, tzinfo=UTC),
             Decimal("100.00"), "USD", Decimal("0.00"), s, *META)
            for i, s in enumerate(("Unshipped", "Refunded", "Canceled"))]
    df = spark.createDataFrame(
        rows,
        "AmazonOrderId string, PurchaseDate timestamp, LastUpdateDate timestamp, "
        "OrderTotal_Amount decimal(12,2), OrderTotal_CurrencyCode string, "
        "ShippingPrice_Amount decimal(12,2), OrderStatus string, "
        "_ingest_run_id string, _interval_start timestamp, _interval_end timestamp",
    )
    fx = spark.createDataFrame(
        [("USD/USD", date(2026, 8, 2), Decimal("1.00000000"))],
        "currency_pair string, rate_date date, rate decimal(18,8)",
    )
    out = {r.order_status: (r.gross_amount_usd, r.net_amount_usd)
           for r in convert_to_usd(normalize_amazon(df), fx).collect()}

    assert out["paid"] == (Decimal("100.00"), Decimal("100.00"))
    assert out["refunded"] == (Decimal("100.00"), Decimal("0.00"))
    assert out["cancelled"] == (Decimal("100.00"), Decimal("0.00"))
