-- Source schema: three marketplace CDC-style extract feeds + FX + chart of accounts.
--
-- These are APPEND-ONLY VERSION LOGS, not current-state OLTP tables. Each row is one version
-- of an order as of `updated_at`. That is what a CDC feed or a dated extract actually looks
-- like, and it is what makes interval-bound replay meaningful: re-reading interval [t0, t1)
-- returns the same versions it returned the first time. A current-state table would return
-- nothing on replay, and the backfill demo would be impossible.
--
-- The three feeds are DELIBERATELY heterogeneous. That heterogeneity is the article:
--   * naming     snake_case vs PascalCase
--   * types      amounts as numeric vs as strings
--   * time       UTC vs local offset
--   * status     three different vocabularies for the same lifecycle
--   * identity   bigint ids vs opaque strings

DROP SCHEMA IF EXISTS src CASCADE;
CREATE SCHEMA src;

-- ---------------------------------------------------------------------------
-- Shopify-shaped: snake_case, numeric amounts, UTC timestamps
-- ---------------------------------------------------------------------------
CREATE TABLE src.raw_shopify_orders (
    id                  BIGINT        NOT NULL,
    order_number        TEXT          NOT NULL,
    created_at          TIMESTAMPTZ   NOT NULL,   -- event time, UTC
    processed_at        TIMESTAMPTZ,              -- posting time, UTC
    updated_at          TIMESTAMPTZ   NOT NULL,   -- version time, UTC
    total_price         NUMERIC(12,2) NOT NULL,
    subtotal_price      NUMERIC(12,2) NOT NULL,
    total_shipping      NUMERIC(12,2) NOT NULL,
    total_discounts     NUMERIC(12,2) NOT NULL,
    currency            TEXT          NOT NULL,
    financial_status    TEXT          NOT NULL,   -- pending|paid|partially_refunded|refunded|voided
    customer_id         BIGINT,
    PRIMARY KEY (id, updated_at)
);

-- ---------------------------------------------------------------------------
-- Amazon SP-API-shaped: PascalCase, flattened nested money, UTC, own vocabulary
-- ---------------------------------------------------------------------------
CREATE TABLE src.raw_amazon_orders (
    "AmazonOrderId"           TEXT          NOT NULL,
    "PurchaseDate"            TIMESTAMPTZ   NOT NULL,   -- event time, UTC
    "LastUpdateDate"          TIMESTAMPTZ   NOT NULL,   -- version time, UTC
    "OrderTotal_Amount"       NUMERIC(12,2) NOT NULL,
    "OrderTotal_CurrencyCode" TEXT          NOT NULL,
    "ShippingPrice_Amount"    NUMERIC(12,2) NOT NULL,
    "OrderStatus"             TEXT          NOT NULL,   -- Pending|Unshipped|Shipped|Canceled|Refunded
    "MarketplaceId"           TEXT          NOT NULL,
    "BuyerInfoHash"           TEXT,
    PRIMARY KEY ("AmazonOrderId", "LastUpdateDate")
);

-- ---------------------------------------------------------------------------
-- Lazada-shaped: amounts as STRINGS, LOCAL time with offset, status as a list
-- ---------------------------------------------------------------------------
CREATE TABLE src.raw_lazada_orders (
    order_id            BIGINT        NOT NULL,
    created_at          TEXT          NOT NULL,   -- 'YYYY-MM-DD HH:MM:SS +0800' — local, as text
    updated_at          TEXT          NOT NULL,   -- same format
    price               TEXT          NOT NULL,   -- amount as a string, e.g. '128.40'
    shipping_fee        TEXT          NOT NULL,
    currency            TEXT          NOT NULL,
    statuses            TEXT          NOT NULL,   -- comma-joined list: 'unpaid', 'delivered,returned'
    customer_first_name TEXT,
    PRIMARY KEY (order_id, updated_at)
);

-- ---------------------------------------------------------------------------
-- FX. v1: rates are published before use and are never restated.
-- Restated rates are article #3 — they invalidate already-computed base amounts,
-- which is dependency-driven reprocessing, a harder problem than row-level late arrival.
-- ---------------------------------------------------------------------------
CREATE TABLE src.raw_fx_rates (
    currency_pair       TEXT          NOT NULL,   -- e.g. 'SGD/USD'
    rate_date           DATE          NOT NULL,
    rate                NUMERIC(18,8) NOT NULL,
    updated_at          TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (currency_pair, rate_date)
);

-- Slicing predicate hits updated_at on every feed. Index accordingly.
CREATE INDEX ix_shopify_updated ON src.raw_shopify_orders (updated_at);
CREATE INDEX ix_amazon_updated  ON src.raw_amazon_orders  ("LastUpdateDate");
CREATE INDEX ix_lazada_updated  ON src.raw_lazada_orders  (updated_at);
