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

PRODUCTS_PATH = "data/raw/products_api.json"

def main():
    # 1) Read JSON
    with open(PRODUCTS_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    df = pd.DataFrame(payload["data"])

    # 2) Clean types
    df["updated_at"] = pd.to_datetime(df["updated_at"])

    # 3) Connect to Postgres
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # 4) Load into table (replace = overwrite table data each time)
    df[["product_id", "name", "category", "price", "is_active", "updated_at"]].to_sql(
        "products",
        engine,
        if_exists="append",
        index=False,
    )

    print(f"✅ Loaded {len(df)} products into Postgres.")

if __name__ == "__main__":
    main()
