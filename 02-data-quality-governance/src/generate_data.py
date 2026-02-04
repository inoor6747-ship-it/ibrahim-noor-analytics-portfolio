from __future__ import annotations

import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
rng = np.random.default_rng(42)

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
os.makedirs(DATA_DIR, exist_ok=True)

def _rand_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = max(int(delta.total_seconds()), 1)
    return start + timedelta(seconds=int(rng.integers(0, seconds)))

def generate_customers(n: int = 5000) -> pd.DataFrame:
    now = datetime.now().replace(microsecond=0)
    start = now - timedelta(days=365 * 2)

    countries = ["US", "CA", "MX", "GB", "DE", "IN"]
    statuses = ["Active", "Inactive"]

    rows = []
    for i in range(n):
        cust_id = f"C{100000 + i}"
        email = fake.email()
        signup = _rand_dt(start, now - timedelta(days=1)).date().isoformat()
        country = rng.choice(countries, p=[0.55, 0.10, 0.05, 0.10, 0.10, 0.10])
        status = rng.choice(statuses, p=[0.85, 0.15])
        rows.append({
            "customer_id": cust_id,
            "email": email,
            "signup_date": signup,
            "country": country,
            "status": status,
        })

    df = pd.DataFrame(rows)

    # Inject data quality issues
    # 1) Missing emails (nulls)
    null_idx = rng.choice(df.index, size=int(0.03 * n), replace=False)
    df.loc[null_idx, "email"] = None

    # 2) Invalid emails
    invalid_idx = rng.choice(df.index.difference(null_idx), size=int(0.02 * n), replace=False)
    df.loc[invalid_idx, "email"] = "not-an-email"

    # 3) Duplicate customer_ids (2% duplicates)
    dup_count = int(0.02 * n)
    dup_idx = rng.choice(df.index, size=dup_count, replace=False)
    df.loc[dup_idx, "customer_id"] = df.loc[rng.choice(df.index, size=dup_count, replace=True), "customer_id"].values

    # 4) Invalid country codes
    bad_country_idx = rng.choice(df.index, size=int(0.01 * n), replace=False)
    df.loc[bad_country_idx, "country"] = "XX"

    # 5) Future signup dates
    future_idx = rng.choice(df.index, size=int(0.01 * n), replace=False)
    df.loc[future_idx, "signup_date"] = (now + timedelta(days=10)).date().isoformat()

    return df

def generate_transactions(customers: pd.DataFrame, n: int = 30000) -> pd.DataFrame:
    now = datetime.now().replace(microsecond=0)
    start = now - timedelta(days=365)

    currencies = ["USD", "CAD", "GBP", "EUR"]
    channels = ["Web", "Mobile", "Store", "Partner"]

    cust_ids = customers["customer_id"].astype(str).tolist()

    rows = []
    for i in range(n):
        tx_id = f"T{500000 + i}"
        cust_id = rng.choice(cust_ids)
        tx_dt = _rand_dt(start, now).date().isoformat()
        amount = float(np.round(max(0.5, rng.normal(65, 40)), 2))
        currency = rng.choice(currencies, p=[0.75, 0.10, 0.05, 0.10])
        channel = rng.choice(channels, p=[0.45, 0.35, 0.15, 0.05])
        rows.append({
            "transaction_id": tx_id,
            "customer_id": cust_id,
            "transaction_date": tx_dt,
            "amount": amount,
            "currency": currency,
            "channel": channel,
        })

    df = pd.DataFrame(rows)

    # Inject data quality issues
    # 1) Orphan customer_id (not in customers)
    orphan_idx = rng.choice(df.index, size=int(0.015 * n), replace=False)
    df.loc[orphan_idx, "customer_id"] = "C999999"

    # 2) Negative / zero amounts
    neg_idx = rng.choice(df.index, size=int(0.02 * n), replace=False)
    df.loc[neg_idx, "amount"] = -1 * df.loc[neg_idx, "amount"].abs()

    zero_idx = rng.choice(df.index.difference(neg_idx), size=int(0.01 * n), replace=False)
    df.loc[zero_idx, "amount"] = 0

    # 3) Future transaction dates (timeliness issue)
    future_tx_idx = rng.choice(df.index, size=int(0.01 * n), replace=False)
    df.loc[future_tx_idx, "transaction_date"] = (now + timedelta(days=7)).date().isoformat()

    # 4) Duplicate transaction IDs
    dup_count = int(0.01 * n)
    dup_idx = rng.choice(df.index, size=dup_count, replace=False)
    df.loc[dup_idx, "transaction_id"] = df.loc[rng.choice(df.index, size=dup_count, replace=True), "transaction_id"].values

    # 5) Invalid currency
    bad_curr_idx = rng.choice(df.index, size=int(0.01 * n), replace=False)
    df.loc[bad_curr_idx, "currency"] = "XXX"

    # 6) Missing required fields
    miss_idx = rng.choice(df.index, size=int(0.005 * n), replace=False)
    df.loc[miss_idx, "transaction_date"] = None

    return df

def main() -> None:
    customers = generate_customers()
    transactions = generate_transactions(customers)

    cust_path = os.path.join(DATA_DIR, "raw_customers.csv")
    tx_path = os.path.join(DATA_DIR, "raw_transactions.csv")

    customers.to_csv(cust_path, index=False)
    transactions.to_csv(tx_path, index=False)

    print(f"✅ Wrote customers: {len(customers):,} rows → {cust_path}")
    print(f"✅ Wrote transactions: {len(transactions):,} rows → {tx_path}")

if __name__ == "__main__":
    main()
