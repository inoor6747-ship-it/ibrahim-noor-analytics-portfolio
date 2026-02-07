from __future__ import annotations

import os
import sqlite3

BASE_DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
DB_PATH = os.path.join(PROJECT_DIR, "data", "sales_ops.db")
SQL_DIR = os.path.join(PROJECT_DIR, "sql")

ORDER = [
    "10_views_core.sql",
    "20_kpi_tables.sql",
    "30_customer_cohorts.sql",
]

def read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def main() -> None:
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()

        for fname in ORDER:
            fpath = os.path.join(SQL_DIR, fname)
            print(f"Running: {fname}")
            cur.executescript(read_sql(fpath))
            con.commit()

        print("\n✅ KPI tables created successfully.\n")

        rows = cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type IN ('table','view')
            ORDER BY name;
        """).fetchall()

        for r in rows:
            if r[0].startswith("kpi_") or r[0] in ("customer_cohorts", "customer_first_order") or r[0].startswith("v_"):
                print(" -", r[0])

    finally:
        con.close()

if __name__ == "__main__":
    main()
