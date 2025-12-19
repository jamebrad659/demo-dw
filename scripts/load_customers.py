import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CUSTOMERS_PATH = "data/raw/customers.json"

def main():
    with open(CUSTOMERS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df["created_at"] = pd.to_datetime(df["created_at"])

    DATABASE_URL = os.getenv("DATABASE_URL")
    
    if  DATABASE_URL:
        engine = create_engine(DATABASE_URL)
    else:
        engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )    



    df[["customer_id", "full_name", "email", "country", "segment", "created_at"]].to_sql(
        "customers",
        engine,
        if_exists="append",
        index=False,
    )

    print(f"✅ Loaded {len(df)} customers into Postgres.")

if __name__ == "__main__":
    main()
