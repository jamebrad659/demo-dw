import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "demo_dw")
DB_USER = os.getenv("DB_USER", "demo_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "demo_pass")

RETURNS_PATH = "data/raw/returns.xlsx"
SCHEMA = "public"
TABLE = "returns"

def main():
    df = pd.read_excel(RETURNS_PATH, sheet_name="returns")

    # types
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["refund_timestamp"] = pd.to_datetime(df["refund_timestamp"])
    df["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="raise").round(2)

    int_cols = ["order_line_id", "order_id", "customer_id", "product_id"]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="raise").astype(int)

    DATABASE_URL = os.getenv("DATABASE_URL")
    
    if  DATABASE_URL:
        engine = create_engine(DATABASE_URL)
    else:
        engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )    

    # replace all rows each run
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE};"))

    df.to_sql(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    print(f"✅ Loaded {len(df)} returns into {SCHEMA}.{TABLE}")

if __name__ == "__main__":
    main()


