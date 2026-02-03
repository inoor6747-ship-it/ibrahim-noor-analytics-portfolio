from __future__ import annotations

import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
rng = np.random.default_rng(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

def _random_ts(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=int(rng.integers(0, max(seconds, 1))))

def _maybe_null(value, p_null: float):
    return None if rng.random() < p_null else value

def _maybe_duplicate_ids(ids: list[str], p_dup: float) -> list[str]:
    out = []
    for i, _id in enumerate(ids):
        if i > 0 and rng.random() < p_dup:
            out.append(out[int(rng.integers(0, i))])
        else:
            out.append(_id)
    return out

def generate_tickets(n: int = 20000, months_back: int = 6) -> pd.DataFrame:
    now = datetime.now().replace(microsecond=0, second=0)
    start_window = now - timedelta(days=30 * months_back)

    categories = ["Billing", "Access", "Payments", "Account", "Technical", "Fraud Review"]
    priorities = ["Low", "Medium", "High", "Critical"]
    channels = ["Email", "Web", "Phone", "Chat"]
    owners = [f"Team-{x}" for x in ["A", "B", "C", "D"]]
    sla_hours = {"Low": 72, "Medium": 48, "High": 24, "Critical": 8}

    base_ids = [f"T{100000 + i}" for i in range(n)]
    ticket_ids = _maybe_duplicate_ids(base_ids, p_dup=0.02)

    records = []
    for i in range(n):
        created = _random_ts(start_window, now - timedelta(hours=1))
        priority = rng.choice(priorities, p=[0.35, 0.40, 0.20, 0.05])
        category = rng.choice(categories)
        channel = rng.choice(channels, p=[0.35, 0.25, 0.20, 0.20])
        owner = rng.choice(owners)

        intake_h = max(0.1, rng.normal(1.5, 1.0))
        triage_h = max(0.1, rng.normal(3.0, 2.0))
        work_h = max(0.1, rng.normal(12.0, 8.0))
        review_h = max(0.1, rng.normal(4.0, 3.0))

        intake_ts = created + timedelta(hours=intake_h)
        triage_ts = intake_ts + timedelta(hours=triage_h)
        work_ts = triage_ts + timedelta(hours=work_h)
        resolved_ts = work_ts + timedelta(hours=review_h)

        records.append({
            "ticket_id": ticket_ids[i],
            "created_at": created,
            "intake_at": intake_ts,
            "triage_at": triage_ts,
            "work_at": work_ts,
            "resolved_at": resolved_ts,
            "category": category,
            "priority": priority,
            "channel": channel,
            "owner_team": owner,
            "sla_target_hours": int(sla_hours[priority]),
        })

    df = pd.DataFrame.from_records(records)
    return df

if __name__ == "__main__":
    df = generate_tickets()
    out = os.path.join(DATA_DIR, "tickets_raw.csv")
    df.to_csv(out, index=False)
    print(f"Generated {len(df)} rows → {out}")
