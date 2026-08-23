"""Feed definitions and paths.

The per-feed `bound_sql` is the whole point of this module: each marketplace expresses its
version timestamp differently, so the slicing predicate has to be written per feed — but the
*bounds themselves* are always the same two arguments, handed in from the caller. Nothing here
reads a clock.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

SOURCE_JDBC_URL = os.environ.get("SOURCE_JDBC_URL", "jdbc:postgresql://source-db:5432/marketplace")
SOURCE_USER = os.environ.get("SOURCE_USER", "pipeline")
SOURCE_PASSWORD = os.environ.get("SOURCE_PASSWORD", "pipeline")

LAKE_ROOT = os.environ.get("LAKE_ROOT", "/app/data")
BRONZE_ROOT = f"{LAKE_ROOT}/bronze"
SILVER_ROOT = f"{LAKE_ROOT}/silver"

# Lazada stores its version timestamp as TEXT with a local offset, so it must be cast before
# it can be compared. Verified against Postgres 16: '2026-08-02 16:25:39 +0800' parses to
# 2026-08-02 08:25:39+00.
LAZADA_TS = "to_timestamp({col}, 'YYYY-MM-DD HH24:MI:SS TZHTZM')"


@dataclass(frozen=True)
class Feed:
    name: str            # bronze table name
    source_table: str
    version_col_sql: str  # SQL expression yielding a timestamptz to slice on

    @property
    def bronze_path(self) -> str:
        return f"{BRONZE_ROOT}/{self.name}"

    def bound_query(self, start_iso: str, end_iso: str) -> str:
        """Half-open [start, end). Bounds are parameters, never clock reads."""
        return (
            f"SELECT * FROM {self.source_table} "
            f"WHERE {self.version_col_sql} >= TIMESTAMPTZ '{start_iso}' "
            f"AND {self.version_col_sql} <  TIMESTAMPTZ '{end_iso}'"
        )


FEEDS: tuple[Feed, ...] = (
    Feed("shopify_orders", "src.raw_shopify_orders", "updated_at"),
    Feed("amazon_orders", "src.raw_amazon_orders", '"LastUpdateDate"'),
    Feed("lazada_orders", "src.raw_lazada_orders", LAZADA_TS.format(col="updated_at")),
)

FEEDS_BY_NAME = {f.name: f for f in FEEDS}
