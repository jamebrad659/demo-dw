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

SQL = text("""
SELECT 'products' AS table_name, COUNT(*) AS row_count FROM public.products
UNION ALL SELECT 'customers', COUNT(*) FROM public.customers
UNION ALL SELECT 'order_lines', COUNT(*) FROM public.order_lines
UNION ALL SELECT 'marketing_spend', COUNT(*) FROM public.marketing_spend
UNION ALL SELECT 'returns', COUNT(*) FROM public.returns
ORDER BY table_name;
""")


with engine.begin() as conn:
    rows = conn.execute(SQL).fetchall()

for r in rows:
    print(f"{r[0]:<16} {r[1]}")

