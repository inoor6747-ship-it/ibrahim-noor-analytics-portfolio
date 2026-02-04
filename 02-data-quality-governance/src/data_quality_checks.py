from __future__ import annotations

import os
import re
from datetime import datetime
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))

CUSTOMERS_PATH = os.path.join(DATA_DIR, "raw_customers.csv")
TX_PATH = os.path.join(DATA_DIR, "raw_transactions.csv")

ISSUES_PATH = os.path.join(DATA_DIR, "dq_issues.csv")
SUMMARY_PATH = os.path.join(DATA_DIR, "dq_summary.csv")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

ALLOWED_COUNTRIES = {"US", "CA", "MX", "GB", "DE", "IN"}
ALLOWED_STATUS = {"Active", "Inactive"}
ALLOWED_CURRENCY = {"USD", "CAD", "GBP", "EUR"}
ALLOWED_CHANNEL = {"Web", "Mobile", "Store", "Partner"}

def _to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def add_issue(issues: list[dict], table: str, record_id: str, column: str, rule: str, severity: str, details: str) -> None:
    issues.append({
        "table": table,
        "record_id": record_id,
        "column": column,
        "rule": rule,
        "severity": severity,
        "details": details,
    })

def main() -> None:
    if not os.path.exists(CUSTOMERS_PATH) or not os.path.exists(TX_PATH):
        raise FileNotFoundError("Missing raw data. Run src/generate_data.py first.")

    customers = pd.read_csv(CUSTOMERS_PATH, dtype=str)
    tx = pd.read_csv(TX_PATH)

    # Normalize dtypes
    tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce")
    customers["signup_date_dt"] = _to_dt(customers.get("signup_date"))
    tx["transaction_date_dt"] = _to_dt(tx.get("transaction_date"))

    now = pd.Timestamp(datetime.now())

    issues: list[dict] = []

    # -------------------------
    # Customers rules
    # -------------------------
    # C1: customer_id not null
    null_id = customers["customer_id"].isna() | (customers["customer_id"].astype(str).str.strip() == "")
    for idx in customers.index[null_id]:
        add_issue(issues, "customers", f"row_{idx}", "customer_id", "not_null", "high", "customer_id is missing")

    # C2: customer_id unique
    dup_mask = customers["customer_id"].duplicated(keep=False)
    for idx in customers.index[dup_mask.fillna(False)]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "customer_id", "unique", "high", "duplicate customer_id")

    # C3: email not null
    email_null = customers["email"].isna() | (customers["email"].astype(str).str.strip() == "")
    for idx in customers.index[email_null]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "email", "not_null", "medium", "email is missing")

    # C4: email valid format (when present)
    email_present = ~email_null
    bad_email = email_present & ~customers["email"].astype(str).str.match(EMAIL_RE)
    for idx in customers.index[bad_email]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "email", "valid_email", "medium", f"invalid email: {customers.loc[idx, 'email']}")

    # C5: signup_date valid + not in future
    bad_signup = customers["signup_date_dt"].isna()
    for idx in customers.index[bad_signup]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "signup_date", "valid_date", "high", f"invalid signup_date: {customers.loc[idx, 'signup_date']}")

    future_signup = customers["signup_date_dt"].notna() & (customers["signup_date_dt"] > now)
    for idx in customers.index[future_signup]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "signup_date", "not_future", "medium", f"future signup_date: {customers.loc[idx, 'signup_date']}")

    # C6: country in allowed set
    bad_country = ~customers["country"].isin(ALLOWED_COUNTRIES)
    for idx in customers.index[bad_country.fillna(True)]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "country", "allowed_values", "low", f"unexpected country: {customers.loc[idx, 'country']}")

    # C7: status allowed
    bad_status = ~customers["status"].isin(ALLOWED_STATUS)
    for idx in customers.index[bad_status.fillna(True)]:
        cid = str(customers.loc[idx, "customer_id"])
        add_issue(issues, "customers", cid, "status", "allowed_values", "low", f"unexpected status: {customers.loc[idx, 'status']}")

    # -------------------------
    # Transactions rules
    # -------------------------
    # T1: transaction_id not null + unique
    tx_null_id = tx["transaction_id"].isna() | (tx["transaction_id"].astype(str).str.strip() == "")
    for idx in tx.index[tx_null_id]:
        add_issue(issues, "transactions", f"row_{idx}", "transaction_id", "not_null", "high", "transaction_id is missing")

    tx_dup = tx["transaction_id"].duplicated(keep=False)
    for idx in tx.index[tx_dup.fillna(False)]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "transaction_id", "unique", "high", "duplicate transaction_id")

    # T2: customer_id not null
    tx_cust_null = tx["customer_id"].isna() | (tx["customer_id"].astype(str).str.strip() == "")
    for idx in tx.index[tx_cust_null]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "customer_id", "not_null", "high", "customer_id is missing")

    # T3: referential integrity (customer exists)
    customer_set = set(customers["customer_id"].dropna().astype(str).tolist())
    orphan = ~tx["customer_id"].astype(str).isin(customer_set)
    for idx in tx.index[orphan.fillna(True)]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "customer_id", "fk_exists", "high", f"customer_id not found: {tx.loc[idx, 'customer_id']}")

    # T4: transaction_date valid + not null + not in future
    tx_date_null = tx["transaction_date"].isna() | (tx["transaction_date"].astype(str).str.strip() == "")
    for idx in tx.index[tx_date_null]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "transaction_date", "not_null", "high", "transaction_date is missing")

    tx_bad_date = tx["transaction_date_dt"].isna() & ~tx_date_null
    for idx in tx.index[tx_bad_date]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "transaction_date", "valid_date", "high", f"invalid transaction_date: {tx.loc[idx, 'transaction_date']}")

    tx_future = tx["transaction_date_dt"].notna() & (tx["transaction_date_dt"] > now)
    for idx in tx.index[tx_future]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "transaction_date", "not_future", "medium", f"future transaction_date: {tx.loc[idx, 'transaction_date']}")

    # T5: amount valid (not null, > 0)
    amt_null = tx["amount"].isna()
    for idx in tx.index[amt_null]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "amount", "not_null", "high", "amount is missing or non-numeric")

    amt_bad = tx["amount"].notna() & (tx["amount"] <= 0)
    for idx in tx.index[amt_bad]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "amount", "positive", "high", f"non-positive amount: {tx.loc[idx, 'amount']}")

    # T6: currency allowed
    bad_curr = ~tx["currency"].astype(str).isin(ALLOWED_CURRENCY)
    for idx in tx.index[bad_curr.fillna(True)]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "currency", "allowed_values", "low", f"unexpected currency: {tx.loc[idx, 'currency']}")

    # T7: channel allowed
    bad_channel = ~tx["channel"].astype(str).isin(ALLOWED_CHANNEL)
    for idx in tx.index[bad_channel.fillna(True)]:
        tid = str(tx.loc[idx, "transaction_id"])
        add_issue(issues, "transactions", tid, "channel", "allowed_values", "low", f"unexpected channel: {tx.loc[idx, 'channel']}")

    issues_df = pd.DataFrame(issues)

    # -------------------------
    # Scorecard summary
    # -------------------------
    def table_summary(table_name: str, df: pd.DataFrame, pk_col: str, required_cols: list[str]) -> dict:
        # Completeness: required fields non-null rate
        completeness_rates = {}
        for c in required_cols:
            non_null = df[c].notna() & (df[c].astype(str).str.strip() != "")
            completeness_rates[f"completeness_{c}"] = float(non_null.mean())

        completeness_overall = float(np.mean(list(completeness_rates.values()))) if completeness_rates else 1.0

        # Uniqueness: pk duplicates rate
        dup_rate = float(df[pk_col].duplicated(keep=False).mean())

        # Validity: from issues table for this table
        table_issues = issues_df[issues_df["table"] == table_name] if not issues_df.empty else pd.DataFrame()
        issue_rate = float(len(table_issues) / max(len(df), 1))

        # Simple quality score (0..100): 100 - penalties
        # (This is intentionally simple and explainable)
        score = 100.0
        score -= issue_rate * 60.0
        score -= (1.0 - completeness_overall) * 30.0
        score -= dup_rate * 10.0
        score = float(max(0.0, min(100.0, score)))

        return {
            "table": table_name,
            "rows": int(len(df)),
            "issue_count": int(len(table_issues)),
            "issue_rate": issue_rate,
            "duplicate_rate": dup_rate,
            "completeness_overall": completeness_overall,
            "quality_score": score,
            **completeness_rates,
        }

    cust_sum = table_summary("customers", customers, "customer_id", ["customer_id", "email", "signup_date", "country", "status"])
    tx_sum = table_summary("transactions", tx, "transaction_id", ["transaction_id", "customer_id", "transaction_date", "amount", "currency", "channel"])

    overall_score = float(np.mean([cust_sum["quality_score"], tx_sum["quality_score"]]))

    overall = {
        "table": "OVERALL",
        "rows": int(cust_sum["rows"] + tx_sum["rows"]),
        "issue_count": int(cust_sum["issue_count"] + tx_sum["issue_count"]),
        "issue_rate": float((cust_sum["issue_count"] + tx_sum["issue_count"]) / max(cust_sum["rows"] + tx_sum["rows"], 1)),
        "duplicate_rate": float(np.mean([cust_sum["duplicate_rate"], tx_sum["duplicate_rate"]])),
        "completeness_overall": float(np.mean([cust_sum["completeness_overall"], tx_sum["completeness_overall"]])),
        "quality_score": overall_score,
    }

    summary_df = pd.DataFrame([overall, cust_sum, tx_sum])

    # Issue breakdowns for Power BI convenience
    if not issues_df.empty:
        issues_df["severity_rank"] = issues_df["severity"].map({"high": 3, "medium": 2, "low": 1}).fillna(0).astype(int)

    # Write outputs
    issues_df.to_csv(ISSUES_PATH, index=False)
    summary_df.to_csv(SUMMARY_PATH, index=False)

    print("✅ Data quality checks complete")
    print(f"Violations: {len(issues_df):,} → {ISSUES_PATH}")
    print(f"Scorecard: {SUMMARY_PATH}")
    print("\nTop rules (by count):")
    if not issues_df.empty:
        print(issues_df.groupby(["table", "rule"], as_index=False).size().sort_values("size", ascending=False).head(10).to_string(index=False))
    else:
        print("No issues found (unexpected for this project).")

if __name__ == "__main__":
    main()
