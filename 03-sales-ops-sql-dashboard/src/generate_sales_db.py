from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
rng = np.random.default_rng(42)

BASE_DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "sales_ops.db")

def rand_date(start: datetime, end: datetime) -> datetime:
    seconds = max(int((end - start).total_seconds()), 1)
    return start + timedelta(seconds=int(rng.integers(0, seconds)))

def main() -> None:
    now = datetime.now().replace(microsecond=0)
    start_18m = now - timedelta(days=30 * 18)

    # ---- Dimensions
    n_customers = 3000
    n_products = 250
    n_reps = 18
    n_orders = 22000

    regions = ["Midwest", "South", "Northeast", "West"]
    channels = ["Web", "Sales", "Partner"]
    segments = ["SMB", "Mid-Market", "Enterprise"]
    categories = ["Hardware", "Software", "Services", "Accessories"]

    reps = pd.DataFrame([{
        "rep_id": f"R{1000+i}",
        "rep_name": fake.name(),
        "region": rng.choice(regions, p=[0.30, 0.25, 0.25, 0.20])
    } for i in range(n_reps)])

    customers = pd.DataFrame([{
        "customer_id": f"C{100000+i}",
        "customer_name": fake.company(),
        "segment": rng.choice(segments, p=[0.55, 0.30, 0.15]),
        "region": rng.choice(regions, p=[0.30, 0.25, 0.25, 0.20]),
        "signup_date": rand_date(start_18m - timedelta(days=365), start_18m).date().isoformat()
    } for i in range(n_customers)])

    # product pricing + cost to compute margin
    products = pd.DataFrame([{
        "product_id": f"P{2000+i}",
        "product_name": f"{fake.word().title()} {rng.choice(['Pro','Plus','Max','Edge','Core'])}",
        "category": rng.choice(categories, p=[0.25, 0.35, 0.20, 0.20]),
        "list_price": float(np.round(rng.uniform(15, 900), 2)),
    } for i in range(n_products)])
    products["unit_cost"] = (products["list_price"] * rng.uniform(0.45, 0.75, size=n_products)).round(2)

    # ---- Orders + order items
    cust_ids = customers["customer_id"].tolist()
    rep_ids = reps["rep_id"].tolist()

    orders_rows = []
    items_rows = []
    return_rows = []

    for i in range(n_orders):
        order_id = f"O{500000+i}"
        customer_id = rng.choice(cust_ids)
        rep_id = rng.choice(rep_ids)
        order_date = rand_date(start_18m, now).date().isoformat()
        channel = rng.choice(channels, p=[0.55, 0.35, 0.10])

        # discount rate influenced by channel/segment
        base_disc = rng.uniform(0.00, 0.18)
        if channel == "Partner":
            base_disc += rng.uniform(0.03, 0.10)
        disc_rate = float(np.clip(base_disc, 0, 0.30))

        orders_rows.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "rep_id": rep_id,
            "order_date": order_date,
            "channel": channel,
            "discount_rate": disc_rate
        })

        # items per order
        n_items = int(rng.integers(1, 5))
        chosen = rng.choice(products.index, size=n_items, replace=False)

        for j, pidx in enumerate(chosen):
            product_id = products.loc[pidx, "product_id"]
            qty = int(rng.integers(1, 6))
            list_price = float(products.loc[pidx, "list_price"])
            unit_cost = float(products.loc[pidx, "unit_cost"])

            # apply discount
            unit_price = float(np.round(list_price * (1.0 - disc_rate), 2))

            items_rows.append({
                "order_item_id": f"{order_id}-{j+1}",
                "order_id": order_id,
                "product_id": product_id,
                "quantity": qty,
                "unit_price": unit_price,
                "unit_cost": unit_cost
            })

        # returns: small percent, higher for certain channels
        ret_prob = 0.03 + (0.02 if channel == "Web" else 0.00) + (0.01 if channel == "Partner" else 0.00)
        if rng.random() < ret_prob:
            return_rows.append({
                "return_id": f"RET{700000+i}",
                "order_id": order_id,
                "return_date": (pd.to_datetime(order_date) + pd.Timedelta(days=int(rng.integers(3, 45)))).date().isoformat(),
                "reason": rng.choice(["Damaged", "Wrong item", "Late delivery", "Customer changed mind", "Other"],
                                    p=[0.20, 0.20, 0.15, 0.30, 0.15])
            })

    orders = pd.DataFrame(orders_rows)
    order_items = pd.DataFrame(items_rows)
    returns = pd.DataFrame(return_rows)

    # ---- Write to SQLite
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = sqlite3.connect(DB_PATH)
    try:
        reps.to_sql("dim_reps", con, index=False)
        customers.to_sql("dim_customers", con, index=False)
        products.to_sql("dim_products", con, index=False)
        orders.to_sql("fact_orders", con, index=False)
        order_items.to_sql("fact_order_items", con, index=False)
        returns.to_sql("fact_returns", con, index=False)

        # helpful indexes
        con.execute("CREATE INDEX idx_orders_date ON fact_orders(order_date);")
        con.execute("CREATE INDEX idx_items_order ON fact_order_items(order_id);")
        con.execute("CREATE INDEX idx_orders_customer ON fact_orders(customer_id);")
        con.commit()
    finally:
        con.close()

    print(f"✅ Created SQLite DB: {DB_PATH}")
    print(f"Rows: customers={len(customers):,}, products={len(products):,}, orders={len(orders):,}, items={len(order_items):,}, returns={len(returns):,}")

if __name__ == "__main__":
    main()
