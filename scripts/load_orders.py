import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "demo_dw")
DB_USER = os.getenv("DB_USER", "demo_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "demo_pass")

ORDERS_PATH = "data/raw/orders_api.json"
SCHEMA = "public"
TABLE = "order_lines"

def main():
    # 1) Read JSON
    with open(ORDERS_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    df = pd.DataFrame(payload["data"])

    # 2) Clean / types
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])

    int_cols = ["order_line_id", "order_id", "customer_id", "product_id", "qty"]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="raise").astype(int)

    money_cols = ["gross_revenue", "discount_amount", "net_revenue"]
    for c in money_cols:
        df[c] = pd.to_numeric(df[c], errors="raise").round(2)

    # 3) Connect to Postgres
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    if  DATABASE_URL:
        engine = create_engine(DATABASE_URL)
    else:
        engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )    

    # 4) Beginner-safe approach: clear table then load fresh
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE};"))

    # 5) Load
    cols = [
        "order_line_id", "order_id", "order_timestamp",
        "customer_id", "product_id", "qty",
        "gross_revenue", "discount_amount", "net_revenue",
        "currency"
    ]

    df[cols].to_sql(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    print(f"✅ Loaded {len(df)} order lines into {SCHEMA}.{TABLE}")

if __name__ == "__main__":
    main()
