"""
seed_warehouse.py
-----------------
Generates a realistic synthetic SaaS finance warehouse in DuckDB.
Tables: customers, subscriptions, invoices, headcount, departments

Run once at app startup. Persists to /tmp/warehouse.duckdb (or in-memory).
"""
import duckdb
import random
from datetime import date, timedelta

random.seed(42)

# ============ DIMENSIONS ============
SEGMENTS = ["Enterprise", "Mid-Market", "SMB", "Startup"]
SEGMENT_WEIGHTS = [0.10, 0.25, 0.40, 0.25]  # # of customers
SEGMENT_ARR_RANGE = {
    "Enterprise": (250_000, 1_500_000),
    "Mid-Market": (50_000, 250_000),
    "SMB":        (8_000, 50_000),
    "Startup":    (1_200, 8_000),
}
SEGMENT_CHURN_RATE = {
    "Enterprise": 0.02,
    "Mid-Market": 0.05,
    "SMB":        0.12,
    "Startup":    0.20,
}
INDUSTRIES = ["SaaS", "Fintech", "Healthcare", "Retail", "Media", "Manufacturing"]
DEPARTMENTS = [
    ("Engineering", 85, 92),
    ("Sales",       42, 50),
    ("Marketing",   18, 22),
    ("Customer Success", 24, 28),
    ("Finance",     12, 14),
    ("People Ops",  8, 10),
    ("Product",     16, 20),
    ("Data",        9, 12),
]

START_DATE = date(2024, 1, 1)
TODAY = date(2026, 4, 1)


def build_warehouse(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Build the entire synthetic warehouse and return the connection."""
    con = duckdb.connect(db_path)

    # ============ CUSTOMERS ============
    con.execute("""
        CREATE OR REPLACE TABLE customers (
            customer_id    VARCHAR PRIMARY KEY,
            company_name   VARCHAR,
            segment        VARCHAR,
            industry       VARCHAR,
            country        VARCHAR,
            signup_date    DATE,
            churned_date   DATE
        )
    """)

    customers = []
    for i in range(1, 401):
        seg = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
        days_since_start = (TODAY - START_DATE).days
        signup_offset = random.randint(0, days_since_start - 30)
        signup = START_DATE + timedelta(days=signup_offset)

        # Churn?
        churned = None
        days_active = (TODAY - signup).days
        # Annualized churn rate
        churn_prob = SEGMENT_CHURN_RATE[seg] * (days_active / 365)
        if random.random() < churn_prob:
            churn_offset = random.randint(60, max(61, days_active))
            churned = signup + timedelta(days=churn_offset)
            if churned > TODAY:
                churned = None

        customers.append((
            f"CUST_{i:04d}",
            f"{random.choice(['Acme','Globex','Initech','Umbrella','Stark','Wayne','Hooli','Pied Piper','Wonka','Soylent','Tyrell','Cyberdyne','Massive Dynamic','Oscorp','LexCorp'])} {random.choice(['Inc','LLC','Corp','Group','Holdings','Labs','Systems','Technologies'])} #{i}",
            seg,
            random.choice(INDUSTRIES),
            random.choice(["US","US","US","US","UK","Canada","Germany","France","Australia","Japan"]),
            signup,
            churned,
        ))

    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?)", customers)

    # ============ SUBSCRIPTIONS ============
    con.execute("""
        CREATE OR REPLACE TABLE subscriptions (
            subscription_id  VARCHAR PRIMARY KEY,
            customer_id      VARCHAR,
            product          VARCHAR,
            mrr              DECIMAL(12,2),
            start_date       DATE,
            end_date         DATE,
            status           VARCHAR
        )
    """)

    PRODUCTS = ["Core Platform", "Analytics Add-on", "API Access", "Premium Support", "Compliance Pack"]
    subscriptions = []
    sub_id = 1
    for cust_id, _, seg, _, _, signup, churned in customers:
        # 1-3 subs per customer
        n_subs = random.choices([1,2,3], weights=[0.5,0.35,0.15])[0]
        annual_arr = random.uniform(*SEGMENT_ARR_RANGE[seg])

        for j in range(n_subs):
            product = random.choice(PRODUCTS) if j == 0 else random.choice(PRODUCTS[1:])
            # Split ARR across subs
            sub_mrr = (annual_arr / 12) * random.uniform(0.2, 0.6)

            # Sub start = signup + some delay
            sub_start = signup + timedelta(days=random.randint(0, 90) if j == 0 else random.randint(60, 400))
            if sub_start > TODAY:
                continue

            # Sub end = customer churn or active
            if churned:
                sub_end = churned
                status = "churned"
            else:
                # Some subs upgrade/swap before customer churns
                if random.random() < 0.15 and (TODAY - sub_start).days > 180:
                    sub_end = sub_start + timedelta(days=random.randint(180, (TODAY - sub_start).days))
                    status = "expired"
                else:
                    sub_end = None
                    status = "active"

            subscriptions.append((
                f"SUB_{sub_id:05d}",
                cust_id,
                product,
                round(sub_mrr, 2),
                sub_start,
                sub_end,
                status
            ))
            sub_id += 1

    con.executemany("INSERT INTO subscriptions VALUES (?,?,?,?,?,?,?)", subscriptions)

    # ============ INVOICES ============
    con.execute("""
        CREATE OR REPLACE TABLE invoices (
            invoice_id     VARCHAR PRIMARY KEY,
            customer_id    VARCHAR,
            subscription_id VARCHAR,
            invoice_date   DATE,
            amount         DECIMAL(12,2),
            status         VARCHAR
        )
    """)

    invoices = []
    inv_id = 1
    for sub_id_str, cust_id, _, mrr, start, end, status in subscriptions:
        # Monthly invoices from start to end (or today)
        billing_end = end if end else TODAY
        cur = date(start.year, start.month, 1)
        while cur <= billing_end:
            inv_status = random.choices(["paid","paid","paid","paid","paid","outstanding","late"], weights=[1,1,1,1,1,0.05,0.03])[0]
            invoices.append((
                f"INV_{inv_id:06d}",
                cust_id,
                sub_id_str,
                cur,
                round(float(mrr), 2),
                inv_status,
            ))
            inv_id += 1
            # Next month
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)

    con.executemany("INSERT INTO invoices VALUES (?,?,?,?,?,?)", invoices)

    # ============ HEADCOUNT ============
    con.execute("""
        CREATE OR REPLACE TABLE headcount (
            snapshot_date    DATE,
            department       VARCHAR,
            actual_headcount INTEGER,
            plan_headcount   INTEGER
        )
    """)

    hc_rows = []
    # Monthly snapshots from 2024-01 to current
    cur = date(2024, 1, 1)
    while cur <= TODAY:
        for dept, base_actual, base_plan in DEPARTMENTS:
            # Growth over time
            months_in = (cur.year - 2024) * 12 + cur.month - 1
            growth_factor = 1 + (months_in * 0.018)
            actual = int(base_actual * growth_factor + random.randint(-2, 2))
            plan = int(base_plan * growth_factor + random.randint(-1, 1))
            hc_rows.append((cur, dept, actual, plan))
        # Next month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

    con.executemany("INSERT INTO headcount VALUES (?,?,?,?)", hc_rows)

    # ============ FISCAL CALENDAR (helper view) ============
    con.execute("""
        CREATE OR REPLACE VIEW fiscal_calendar AS
        SELECT DISTINCT
            DATE_TRUNC('quarter', invoice_date) AS quarter_start,
            CONCAT('Q', QUARTER(invoice_date), ' ', EXTRACT(YEAR FROM invoice_date)) AS quarter_label,
            EXTRACT(YEAR FROM invoice_date) AS fiscal_year,
            QUARTER(invoice_date) AS fiscal_quarter
        FROM invoices
        ORDER BY quarter_start
    """)

    return con


def get_warehouse_stats(con) -> dict:
    """Quick stats for sanity checking."""
    return {
        "customers": con.execute("SELECT COUNT(*) FROM customers").fetchone()[0],
        "active_customers": con.execute("SELECT COUNT(*) FROM customers WHERE churned_date IS NULL").fetchone()[0],
        "subscriptions": con.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0],
        "active_subs": con.execute("SELECT COUNT(*) FROM subscriptions WHERE status='active'").fetchone()[0],
        "invoices": con.execute("SELECT COUNT(*) FROM invoices").fetchone()[0],
        "headcount_snapshots": con.execute("SELECT COUNT(*) FROM headcount").fetchone()[0],
        "current_arr": con.execute("""
            SELECT ROUND(SUM(mrr) * 12, 0)
            FROM subscriptions
            WHERE status = 'active'
        """).fetchone()[0],
    }


if __name__ == "__main__":
    con = build_warehouse()
    stats = get_warehouse_stats(con)
    print("Warehouse seeded successfully:")
    for k, v in stats.items():
        print(f"  {k:25s} = {v:,}" if isinstance(v, (int, float)) else f"  {k:25s} = {v}")
