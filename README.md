# End-to-End Data Engineering + Dashboard (Python, Postgres, Flask, Streamlit)

## What this project does
An end-to-end data platform:
- Extracts data from multiple formats (JSON, CSV, Excel)
- Loads into PostgreSQL (local or Supabase)
- Serves data through a Flask API
- Visualizes KPIs in a Streamlit dashboard
- Runs as an automated pipeline

## Architecture
Raw Files → ETL Loaders → PostgreSQL → Flask API → Streamlit Dashboard

## Data sources (fake data)
Files in `data/raw/`:
- `products_api.json`
- `orders_api.json`
- `customers.json`
- `marketing.csv`
- `returns.xlsx`

## Database tables
- `products`
- `customers`
- `order_lines`
- `marketing_spend`
- `returns`

## Run locally
1) Start Postgres (Docker)
2) Load data:
```bash
python scripts/run_pipeline.py
