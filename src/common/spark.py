"""Spark session + shared helpers."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession


def emit_metric(**fields) -> None:
    """Print one machine-readable METRIC line.

    Human logs are for humans; these lines are for the runner, for CI assertions, and for
    anyone reviewing a run without reading 400 lines of Spark chatter. Scraping row counts
    out of prose is how verification quietly rots.
    """
    print("METRIC " + json.dumps(fields, default=str), flush=True)


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s | %(message)s",
    )
    return logging.getLogger(name)


def build_spark(app_name: str) -> SparkSession:
    """Delta jars are baked into the image, so no --packages / Maven call at runtime."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")   # never depend on host TZ
        .config("spark.sql.shuffle.partitions", "8")   # small local data; 200 is waste
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_interval(start: str, end: str) -> tuple[datetime, datetime]:
    """Bounds arrive as ISO strings from the caller (Airflow's data interval). Validated,
    never defaulted — a job that can invent its own bounds is a job that can't be replayed."""
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    if s.tzinfo is None:
        s = s.replace(tzinfo=timezone.utc)
    if e.tzinfo is None:
        e = e.replace(tzinfo=timezone.utc)
    if e <= s:
        raise ValueError(f"interval_end ({e}) must be after interval_start ({s})")
    return s, e


def default_run_id(interval_start: datetime) -> str:
    """Deterministic by design: same interval → same run id → reruns are comparable.
    Airflow passes its real run_id in; this is the manual-invocation equivalent."""
    return f"manual__{interval_start.strftime('%Y%m%dT%H%M%SZ')}"
