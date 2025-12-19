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

def make_engine():
    if DATABASE_URL:
        return create_engine(DATABASE_URL)
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

# Each check returns a number.
# - "min" means it must be >= expected
# - "eq" means it must equal expected
CHECKS = [
    ("products_count", "min", 1, "SELECT COUNT(*) FROM public.products"),
    ("customers_count", "min", 1, "SELECT COUNT(*) FROM public.customers"),
    ("order_lines_count", "min", 1, "SELECT COUNT(*) FROM public.order_lines"),
    ("marketing_count", "min", 1, "SELECT COUNT(*) FROM public.marketing_spend"),
    ("returns_count", "min", 0, "SELECT COUNT(*) FROM public.returns"),

    # sanity rules
    ("no_negative_net_revenue", "eq", 0, "SELECT COUNT(*) FROM public.order_lines WHERE net_revenue < 0"),
    ("no_negative_qty", "eq", 0, "SELECT COUNT(*) FROM public.order_lines WHERE qty <= 0"),
    ("no_negative_spend", "eq", 0, "SELECT COUNT(*) FROM public.marketing_spend WHERE spend_eur < 0"),
    ("refund_not_negative", "eq", 0, "SELECT COUNT(*) FROM public.returns WHERE refund_amount < 0"),

    # referential integrity (should be 0 orphans)
    ("no_orphan_products", "eq", 0, """
        SELECT COUNT(*)
        FROM public.order_lines ol
        LEFT JOIN public.products p ON p.product_id = ol.product_id
        WHERE p.product_id IS NULL
    """),
    ("no_orphan_customers", "eq", 0, """
        SELECT COUNT(*)
        FROM public.order_lines ol
        LEFT JOIN public.customers c ON c.customer_id = ol.customer_id
        WHERE c.customer_id IS NULL
    """),
]

def check_ok(kind: str, expected: int, value: int) -> bool:
    if kind == "min":
        return value >= expected
    if kind == "eq":
        return value == expected
    return False

def main():
    engine = make_engine()
    failures = []

    with engine.begin() as conn:
        for name, kind, expected, sql in CHECKS:
            value = conn.execute(text(sql)).scalar_one()
            ok = check_ok(kind, expected, int(value))
            print(f"{name}: {value} -> {'OK' if ok else 'FAIL'}")
            if not ok:
                failures.append((name, value, kind, expected))

    if failures:
        raise SystemExit(f"❌ Validation failed: {failures}")

    print("✅ All validation checks passed.")

if __name__ == "__main__":
    main()
