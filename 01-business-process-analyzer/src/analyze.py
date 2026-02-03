from __future__ import annotations

import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
RAW_PATH = os.path.join(DATA_DIR, "tickets_raw.csv")

def parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")

def hours_between(a: pd.Series, b: pd.Series) -> pd.Series:
    return (b - a).dt.total_seconds() / 3600.0

def main() -> None:
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Missing raw file: {RAW_PATH}. Run generate_data.py first.")

    df = pd.read_csv(RAW_PATH)

    # Parse timestamps
    for c in ["created_at", "intake_at", "triage_at", "work_at", "resolved_at"]:
        df[c] = parse_dt(df[c])

    # Drop exact duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    removed_dups = before - len(df)

    # Flag duplicate ticket IDs (data quality signal)
    df["is_duplicate_ticket_id"] = df.duplicated(subset=["ticket_id"], keep=False)

    # Validate timeline monotonicity (created <= intake <= triage <= work <= resolved)
    def monotonic_ok(row) -> bool:
        ts = [row["created_at"], row["intake_at"], row["triage_at"], row["work_at"], row["resolved_at"]]
        if any(pd.isna(x) for x in ts):
            return False
        return ts == sorted(ts)

    df["is_valid_timeline"] = df.apply(monotonic_ok, axis=1)

    rejected = df.loc[~df["is_valid_timeline"]].copy()
    clean = df.loc[df["is_valid_timeline"]].copy()

    # Metrics
    clean["cycle_time_hours"] = hours_between(clean["created_at"], clean["resolved_at"])
    clean["intake_hours"] = hours_between(clean["created_at"], clean["intake_at"])
    clean["triage_hours"] = hours_between(clean["intake_at"], clean["triage_at"])
    clean["work_hours"] = hours_between(clean["triage_at"], clean["work_at"])
    clean["review_hours"] = hours_between(clean["work_at"], clean["resolved_at"])

    # SLA breach
    clean["sla_breached"] = clean["cycle_time_hours"] > clean["sla_target_hours"]

    # Bottleneck stage (which stage took longest)
    stage_cols = ["intake_hours", "triage_hours", "work_hours", "review_hours"]
    clean["bottleneck_stage"] = clean[stage_cols].idxmax(axis=1).str.replace("_hours", "", regex=False)

    # KPI tables
    kpi_overall = pd.DataFrame([{
        "rows_raw": int(before),
        "rows_after_drop_duplicates": int(len(df)),
        "duplicate_rows_removed": int(removed_dups),
        "rows_valid_timeline": int(len(clean)),
        "rows_rejected_invalid_timeline": int(len(rejected)),
        "pct_valid": float(len(clean) / max(len(df), 1)),
        "avg_cycle_time_hours": float(clean["cycle_time_hours"].mean()),
        "median_cycle_time_hours": float(clean["cycle_time_hours"].median()),
        "sla_breach_rate": float(clean["sla_breached"].mean()),
    }])

    by_priority = (clean.groupby("priority", as_index=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_cycle_time_hours=("cycle_time_hours", "mean"),
            sla_breach_rate=("sla_breached", "mean"),
        )
        .sort_values(["sla_breach_rate", "avg_cycle_time_hours"], ascending=False)
    )

    by_category = (clean.groupby("category", as_index=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_cycle_time_hours=("cycle_time_hours", "mean"),
            sla_breach_rate=("sla_breached", "mean"),
        )
        .sort_values(["sla_breach_rate", "avg_cycle_time_hours"], ascending=False)
    )

    by_owner = (clean.groupby("owner_team", as_index=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_cycle_time_hours=("cycle_time_hours", "mean"),
            sla_breach_rate=("sla_breached", "mean"),
        )
        .sort_values(["sla_breach_rate", "avg_cycle_time_hours"], ascending=False)
    )

    bottlenecks = (clean.groupby("bottleneck_stage", as_index=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_cycle_time_hours=("cycle_time_hours", "mean"),
        )
        .sort_values("tickets", ascending=False)
    )

    # Export outputs
    os.makedirs(DATA_DIR, exist_ok=True)

    clean.to_csv(os.path.join(DATA_DIR, "tickets_clean.csv"), index=False)
    rejected.to_csv(os.path.join(DATA_DIR, "tickets_rejected.csv"), index=False)

    kpi_overall.to_csv(os.path.join(DATA_DIR, "kpi_overall.csv"), index=False)
    by_priority.to_csv(os.path.join(DATA_DIR, "kpi_by_priority.csv"), index=False)
    by_category.to_csv(os.path.join(DATA_DIR, "kpi_by_category.csv"), index=False)
    by_owner.to_csv(os.path.join(DATA_DIR, "kpi_by_owner.csv"), index=False)
    bottlenecks.to_csv(os.path.join(DATA_DIR, "kpi_bottlenecks.csv"), index=False)

    # Print summary
    print("✅ Analysis complete")
    print(kpi_overall.to_string(index=False))
    print("\nTop bottleneck stages:")
    print(bottlenecks.head(5).to_string(index=False))
    print(f"\nExports saved in: {os.path.abspath(DATA_DIR)}")

if __name__ == "__main__":
    main()
