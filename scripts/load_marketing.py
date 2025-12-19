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

MARKETING_PATH = "data/raw/marketing.csv"
SCHEMA = "public"
TABLE = "marketing_spend"

def main():
    df = pd.read_csv(MARKETING_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["spend_eur"] = pd.to_numeric(df["spend_eur"], errors="raise").round(2)

    DATABASE_URL = os.getenv("DATABASE_URL")
    
    if  DATABASE_URL:
        engine = create_engine(DATABASE_URL)
    else:
        engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )    

    # simple: replace all rows each run
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE};"))

    df.to_sql(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    print(f"✅ Loaded {len(df)} rows into {SCHEMA}.{TABLE}")

if __name__ == "__main__":
    main()
