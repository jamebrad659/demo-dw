import os
import json
import random
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
from faker import Faker

# -----------------------
# Config (edit as needed)
# -----------------------
SEED = 42
N_PRODUCTS = 200
N_CUSTOMERS = 500
N_ORDER_LINES = 8000          # each row = one product line (fact-style)
DAYS_BACK = 180               # generate orders in last N days
RETURN_RATE = 0.08            # 8% of order lines are returned/refunded
CHANNELS = ["google_ads", "meta_ads", "tiktok_ads", "email", "affiliate"]

RAW_DIR = "data/raw"

# -----------------------
# Helpers
# -----------------------
def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]

# -----------------------
# Main generator
# -----------------------
def main():
    random.seed(SEED)
    np.random.seed(SEED)
    fake = Faker()
    Faker.seed(SEED)

    ensure_dirs()

    today = datetime.now().date()
    start_day = today - timedelta(days=DAYS_BACK)

    # -----------------------
    # Products (API JSON)
    # -----------------------
    categories = ["electronics", "fashion", "home", "beauty", "sports", "books"]
    cat_weights = [0.18, 0.22, 0.20, 0.12, 0.16, 0.12]

    products = []
    for pid in range(1, N_PRODUCTS + 1):
        cat = weighted_choice(categories, cat_weights)
        base_price = {
            "electronics": (30, 1200),
            "fashion": (10, 250),
            "home": (8, 400),
            "beauty": (5, 150),
            "sports": (10, 600),
            "books": (5, 60),
        }[cat]
        price = round(random.uniform(*base_price), 2)

        products.append({
            "product_id": pid,
            "name": f"{fake.word().title()} {fake.word().title()}",
            "category": cat,
            "price": price,
            "is_active": random.random() > 0.03,  # 3% inactive
            "updated_at": fake.date_time_between(start_date="-30d", end_date="now").isoformat()
        })

    with open(os.path.join(RAW_DIR, "products_api.json"), "w", encoding="utf-8") as f:
        json.dump({"data": products, "source": "fake_api", "generated_at": datetime.now().isoformat()}, f, indent=2)

    products_df = pd.DataFrame(products)

    # -----------------------
    # Customers (JSON)
    # -----------------------
    countries = ["FR", "DE", "ES", "IT", "NL", "BE", "UK"]
    segments = ["consumer", "small_business", "enterprise"]

    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        created_at = fake.date_time_between(start_date=f"-{DAYS_BACK}d", end_date="now")
        customers.append({
            "customer_id": cid,
            "full_name": fake.name(),
            "email": fake.email(),
            "country": weighted_choice(countries, [0.22, 0.18, 0.14, 0.14, 0.10, 0.08, 0.14]),
            "segment": weighted_choice(segments, [0.78, 0.18, 0.04]),
            "created_at": created_at.isoformat()
        })

    with open(os.path.join(RAW_DIR, "customers.json"), "w", encoding="utf-8") as f:
        json.dump(customers, f, indent=2)

    customers_df = pd.DataFrame(customers)

    # -----------------------
    # Orders (API JSON) as "order lines"
    # Each row = one product line item (good for fact tables)
    # -----------------------
    # Make electronics/fashion slightly more popular
    cat_popularity = {c: w for c, w in zip(categories, [0.22, 0.26, 0.18, 0.10, 0.14, 0.10])}
    products_df["pop_w"] = products_df["category"].map(cat_popularity)

    order_lines = []
    for line_id in range(1, N_ORDER_LINES + 1):
        order_dt = fake.date_time_between(start_date=start_day, end_date=today)
        cust = customers_df.sample(1, random_state=random.randint(0, 10_000)).iloc[0]

        prod = products_df.sample(1, weights=products_df["pop_w"], random_state=random.randint(0, 10_000)).iloc[0]
        qty = int(np.random.choice([1, 1, 1, 2, 2, 3], p=[0.55, 0.0, 0.0, 0.30, 0.10, 0.05]))  # mostly 1–2

        gross = round(float(prod["price"]) * qty, 2)

        # discounts: more common for fashion
        discount_pct = 0.0
        if prod["category"] in ["fashion", "home"] and random.random() < 0.25:
            discount_pct = random.choice([0.05, 0.10, 0.15, 0.20])
        elif random.random() < 0.10:
            discount_pct = random.choice([0.03, 0.05, 0.08])

        discount_amt = round(gross * discount_pct, 2)
        net = round(gross - discount_amt, 2)

        order_lines.append({
            "order_line_id": line_id,
            "order_id": int((line_id - 1) / 2) + 1,  # ~2 lines per order id
            "order_timestamp": order_dt.isoformat(),
            "customer_id": int(cust["customer_id"]),
            "product_id": int(prod["product_id"]),
            "qty": qty,
            "gross_revenue": gross,
            "discount_amount": discount_amt,
            "net_revenue": net,
            "currency": "EUR"
        })

    with open(os.path.join(RAW_DIR, "orders_api.json"), "w", encoding="utf-8") as f:
        json.dump({"data": order_lines, "source": "fake_api", "generated_at": datetime.now().isoformat()}, f, indent=2)

    orders_df = pd.DataFrame(order_lines)

    # -----------------------
    # Returns / Refunds (Excel)
    # -----------------------
    n_returns = int(len(orders_df) * RETURN_RATE)
    return_sample = orders_df.sample(n_returns, random_state=SEED).copy()

    reasons = ["damaged", "wrong_size", "not_as_expected", "late_delivery", "changed_mind"]
    reason_w = [0.18, 0.22, 0.24, 0.12, 0.24]

    # Refund happens 1–21 days after purchase
    return_sample["refund_timestamp"] = pd.to_datetime(return_sample["order_timestamp"]) + pd.to_timedelta(
        np.random.randint(1, 22, size=n_returns), unit="D"
    )
    return_sample["reason"] = [weighted_choice(reasons, reason_w) for _ in range(n_returns)]
    # sometimes partial refund
    return_sample["refund_amount"] = return_sample["net_revenue"] * np.random.choice([1.0, 1.0, 1.0, 0.5, 0.8], size=n_returns, p=[0.70, 0.0, 0.0, 0.15, 0.15])
    return_sample["refund_amount"] = return_sample["refund_amount"].round(2)

    returns_cols = [
        "order_line_id", "order_id", "customer_id", "product_id",
        "order_timestamp", "refund_timestamp", "refund_amount", "reason"
    ]
    returns_df = return_sample[returns_cols].sort_values("refund_timestamp")

    # Save to Excel
    returns_path = os.path.join(RAW_DIR, "returns.xlsx")
    with pd.ExcelWriter(returns_path, engine="openpyxl") as writer:
        returns_df.to_excel(writer, sheet_name="returns", index=False)

    # -----------------------
    # Marketing spend (CSV)
    # Daily spend per channel
    # -----------------------
    rows = []
    for d in daterange(start_day, today):
        for ch in CHANNELS:
            base = {
                "google_ads": (200, 1200),
                "meta_ads": (150, 900),
                "tiktok_ads": (60, 500),
                "email": (10, 120),
                "affiliate": (20, 250)
            }[ch]
            spend = round(random.uniform(*base), 2)

            # weekends slightly lower for some channels
            if d.weekday() >= 5 and ch in ["google_ads", "meta_ads"]:
                spend = round(spend * random.uniform(0.75, 0.95), 2)

            rows.append({"date": d.isoformat(), "channel": ch, "spend_eur": spend})

    marketing_df = pd.DataFrame(rows)
    marketing_df.to_csv(os.path.join(RAW_DIR, "marketing.csv"), index=False)

    print("✅ Fake data generated in:", RAW_DIR)
    print(" - products_api.json")
    print(" - orders_api.json")
    print(" - customers.json")
    print(" - marketing.csv")
    print(" - returns.xlsx")


if __name__ == "__main__":
    main()
