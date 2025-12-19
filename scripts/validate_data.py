import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "demo_dw")
DB_USER = os.getenv("DB_USER", "demo_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "demo_pass")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    engine = create_engine(DATABASE_URL)
else:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

checks = [
    ("products_count", "SELECT COUNT(*) FROM public.products", 1),
    ("customers_count", "SELECT COUNT(*) FROM public.customers", 1),
    ("order_lines_count", "SELECT COUNT(*) FROM public.order_lines", 1),
    ("marketing_count", "SELECT COUNT(*) FROM public.marketing_spend", 1),
    ("returns_count", "SELECT COUNT(*) FROM public.returns", 0),
    ("no_negative_revenue", "SELECT COUNT(*) FROM public.order_lines WHERE net_revenue < 0", 0),
    ("no_negative_spend", "SELECT COUNT(*) FROM public.marketing_spend WHERE spend_eur < 0", 0),
]

def main():
    failures = []
    with engine.begin() as conn:
        for name, sql, min_expected in checks:
            val = conn.execute(text(sql)).scalar_one()
            ok = val >= min_expected if "count" in name and name != "returns_count" else (val == min_expected)
            print(f"{name}: {val} -> {'OK' if ok else 'FAIL'}")
            if not ok:
                failures.append((name, val))

    if failures:
        raise SystemExit(f"Validation failed: {failures}")

    print("✅ All validation checks passed.")

if __name__ == "__main__":
    main()
