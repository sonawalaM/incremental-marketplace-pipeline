"""
Deterministic synthetic marketplace order feeds.

Generates the FULL window up front, including restatements and back-dated adjustments,
then writes append-only version rows. The pipeline slices that window by `updated_at`
interval, so replaying an interval always reads the same versions.

Determinism is the point: same --seed and --start produce byte-identical output, which is
what lets the article claim reproducibility and lets CI assert it.

    python -m src.generator.seed --start 2026-08-01 --days 7 --orders 5000 --seed 42
"""
from __future__ import annotations

import argparse
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import execute_values

# --- lifecycle vocabularies, deliberately different per marketplace -----------------
SHOPIFY_FLOW = ["pending", "paid", "partially_refunded", "refunded"]
AMAZON_FLOW = ["Pending", "Unshipped", "Shipped", "Refunded"]
LAZADA_FLOW = ["unpaid", "shipped", "delivered", "delivered,returned"]

CURRENCIES = {"shopify": "USD", "amazon": "USD", "lazada": "SGD"}
LAZADA_OFFSET = timedelta(hours=8)  # SGT, and the feed reports local time as text

TWO_DP = Decimal("0.01")


@dataclass
class Config:
    start: datetime
    days: int
    orders: int
    seed: int
    restatement_rate: float      # share of orders that get >=1 later version
    backdated_rate: float        # share of restatements that land in a much older period
    backdate_lag_days: tuple[int, int]


def money(rng: random.Random, lo: float, hi: float) -> Decimal:
    return Decimal(str(rng.uniform(lo, hi))).quantize(TWO_DP, rounding=ROUND_HALF_UP)


def build_orders(cfg: Config):
    """Yield (marketplace, versions[]) where each version is a dict of source-shaped fields."""
    rng = random.Random(cfg.seed)
    window = timedelta(days=cfg.days)

    for i in range(cfg.orders):
        marketplace = rng.choice(["shopify", "amazon", "lazada"])
        # event time: uniformly across the window
        event_time = cfg.start + timedelta(seconds=rng.randrange(int(window.total_seconds())))

        subtotal = money(rng, 15, 400)
        shipping = money(rng, 0, 25)
        discount = money(rng, 0, 20) if rng.random() < 0.25 else Decimal("0.00")
        total = (subtotal + shipping - discount).quantize(TWO_DP)

        flow = {"shopify": SHOPIFY_FLOW, "amazon": AMAZON_FLOW, "lazada": LAZADA_FLOW}[marketplace]

        # v0: created, updated_at == event_time
        versions = [dict(status=flow[0], updated_at=event_time)]
        last_ts = event_time

        if rng.random() < cfg.restatement_rate:
            n_restatements = rng.choice([1, 1, 1, 2])
            for step in range(1, n_restatements + 1):
                status = flow[min(step, len(flow) - 1)]
                if rng.random() < cfg.backdated_rate:
                    # Back-dated adjustment: written LATE, but the order's event_time — and so
                    # its accounting period — stays old. This is the row a naive ingest-date
                    # partition scheme files under the wrong day, or loses entirely.
                    lag = timedelta(days=rng.randint(*cfg.backdate_lag_days),
                                    hours=rng.randrange(24))
                else:
                    lag = timedelta(hours=rng.randint(1, 20))
                # Versions of one order are strictly ordered in system time. Without this,
                # two restatements can collide on updated_at and violate (id, updated_at).
                updated_at = max(event_time + lag, last_ts + timedelta(minutes=1))
                versions.append(dict(status=status, updated_at=updated_at))
                last_ts = updated_at

        yield marketplace, dict(
            idx=i,
            event_time=event_time,
            subtotal=subtotal,
            shipping=shipping,
            discount=discount,
            total=total,
            versions=sorted(versions, key=lambda v: v["updated_at"]),
        )


def to_rows(cfg: Config):
    shopify, amazon, lazada = [], [], []

    for marketplace, o in build_orders(cfg):
        for v in o["versions"]:
            if marketplace == "shopify":
                shopify.append((
                    5_000_000 + o["idx"],
                    f"#{1000 + o['idx']}",
                    o["event_time"],
                    o["event_time"] + timedelta(minutes=7),
                    v["updated_at"],
                    o["total"], o["subtotal"], o["shipping"], o["discount"],
                    CURRENCIES["shopify"],
                    v["status"],
                    900_000 + (o["idx"] % 1500),
                ))
            elif marketplace == "amazon":
                amazon.append((
                    f"111-{7000000 + o['idx']}-{o['idx'] % 9999:04d}",
                    o["event_time"],
                    v["updated_at"],
                    o["total"], CURRENCIES["amazon"], o["shipping"],
                    v["status"],
                    "ATVPDKIKX0DER",
                    f"buyer-{o['idx'] % 1500:05d}",
                ))
            else:
                # local time, rendered as TEXT with a +0800 offset; amounts as strings
                def local(ts: datetime) -> str:
                    return (ts + LAZADA_OFFSET).strftime("%Y-%m-%d %H:%M:%S +0800")

                lazada.append((
                    8_000_000 + o["idx"],
                    local(o["event_time"]),
                    local(v["updated_at"]),
                    str(o["total"]),
                    str(o["shipping"]),
                    CURRENCIES["lazada"],
                    v["status"],
                    f"cust{o['idx'] % 1500}",
                ))

    return shopify, amazon, lazada


def fx_rows(cfg: Config):
    """SGD/USD published one day ahead of use. v1: never restated."""
    rng = random.Random(cfg.seed + 1)
    rows = []
    for d in range(-1, cfg.days + 1):
        rate_date = (cfg.start + timedelta(days=d)).date()
        rate = Decimal(str(0.74 + rng.uniform(-0.01, 0.01))).quantize(Decimal("0.00000001"))
        published = cfg.start + timedelta(days=d - 1)
        rows.append(("SGD/USD", rate_date, rate, published))
        rows.append(("USD/USD", rate_date, Decimal("1.00000000"), published))
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--orders", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--restatement-rate", type=float, default=0.35)
    p.add_argument("--backdated-rate", type=float, default=0.20)
    p.add_argument("--dsn", default=os.environ.get(
        "SOURCE_DSN", "postgresql://pipeline:pipeline@localhost:5433/marketplace"))
    args = p.parse_args()

    cfg = Config(
        start=datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc),
        days=args.days,
        orders=args.orders,
        seed=args.seed,
        restatement_rate=args.restatement_rate,
        backdated_rate=args.backdated_rate,
        backdate_lag_days=(3, 6),
    )

    shopify, amazon, lazada = to_rows(cfg)
    fx = fx_rows(cfg)

    with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE src.raw_shopify_orders, src.raw_amazon_orders, "
                    "src.raw_lazada_orders, src.raw_fx_rates")
        execute_values(cur, """
            INSERT INTO src.raw_shopify_orders
              (id, order_number, created_at, processed_at, updated_at, total_price,
               subtotal_price, total_shipping, total_discounts, currency,
               financial_status, customer_id)
            VALUES %s ON CONFLICT DO NOTHING""", shopify)
        execute_values(cur, """
            INSERT INTO src.raw_amazon_orders
              ("AmazonOrderId", "PurchaseDate", "LastUpdateDate", "OrderTotal_Amount",
               "OrderTotal_CurrencyCode", "ShippingPrice_Amount", "OrderStatus",
               "MarketplaceId", "BuyerInfoHash")
            VALUES %s ON CONFLICT DO NOTHING""", amazon)
        execute_values(cur, """
            INSERT INTO src.raw_lazada_orders
              (order_id, created_at, updated_at, price, shipping_fee, currency,
               statuses, customer_first_name)
            VALUES %s ON CONFLICT DO NOTHING""", lazada)
        execute_values(cur, """
            INSERT INTO src.raw_fx_rates (currency_pair, rate_date, rate, updated_at)
            VALUES %s ON CONFLICT DO NOTHING""", fx)

    print(f"seeded  shopify={len(shopify):>6}  amazon={len(amazon):>6}  "
          f"lazada={len(lazada):>6}  fx={len(fx):>4}")
    print(f"window  {cfg.start.date()} .. {(cfg.start + timedelta(days=cfg.days)).date()}  "
          f"seed={cfg.seed}")


if __name__ == "__main__":
    main()
