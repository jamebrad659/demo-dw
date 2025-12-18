import os
from datetime import datetime, date

from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "demo_dw")
DB_USER = os.getenv("DB_USER", "demo_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "demo_pass")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

app = Flask(__name__)

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/kpis")
def kpis():
    # Read query params like /kpis?start=2025-01-01&end=2025-01-31
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    if not start_str or not end_str:
        return jsonify({"error": "Please provide start and end. Example: /kpis?start=2025-01-01&end=2025-01-31"}), 400

    start = parse_date(start_str)
    end = parse_date(end_str)

    sql = text("""
        WITH base AS (
          SELECT
            ol.order_id,
            ol.order_line_id,
            ol.net_revenue,
            ol.order_timestamp::date AS order_date
          FROM public.order_lines ol
          WHERE ol.order_timestamp::date BETWEEN :start AND :end
        ),
        refunds AS (
          SELECT
            r.order_line_id,
            r.refund_amount
          FROM public.returns r
        )
        SELECT
          -- revenue
          COALESCE(SUM(base.net_revenue), 0) AS revenue_net,
          COALESCE(SUM(COALESCE(refunds.refund_amount, 0)), 0) AS refunds_total,
          COALESCE(SUM(base.net_revenue) - SUM(COALESCE(refunds.refund_amount, 0)), 0) AS revenue_after_refunds,

          -- orders
          COUNT(DISTINCT base.order_id) AS orders,
          COUNT(base.order_line_id) AS order_lines,

          -- refund rate (by lines)
          ROUND(
            100.0 * COUNT(refunds.order_line_id) / NULLIF(COUNT(base.order_line_id), 0),
            2
          ) AS refund_rate_pct,

          -- aov (average order value)
          ROUND(
            (SUM(base.net_revenue) / NULLIF(COUNT(DISTINCT base.order_id), 0))::numeric,
            2
          ) AS aov
        FROM base
        LEFT JOIN refunds ON refunds.order_line_id = base.order_line_id;
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {"start": start, "end": end}).mappings().one()

    return jsonify({
        "start": start_str,
        "end": end_str,
        "kpis": dict(row)
    })

@app.get("/revenue/by-day")
def revenue_by_day():
    start_str = request.args.get("start")
    end_str = request.args.get("end")
    if not start_str or not end_str:
        return jsonify({"error": "Example: /revenue/by-day?start=YYYY-MM-DD&end=YYYY-MM-DD"}), 400

    start = parse_date(start_str)
    end = parse_date(end_str)

    sql = text("""
        SELECT
          ol.order_timestamp::date AS day,
          ROUND(SUM(ol.net_revenue)::numeric, 2) AS revenue_net,
          COUNT(DISTINCT ol.order_id) AS orders
        FROM public.order_lines ol
        WHERE ol.order_timestamp::date BETWEEN :start AND :end
        GROUP BY 1
        ORDER BY 1;
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"start": start, "end": end}).mappings().all()

    return jsonify({
        "start": start_str,
        "end": end_str,
        "data": [dict(r) for r in rows]
    })

@app.get("/revenue/by-category")
def revenue_by_category():
    start_str = request.args.get("start")
    end_str = request.args.get("end")
    if not start_str or not end_str:
        return jsonify({"error": "Example: /revenue/by-category?start=YYYY-MM-DD&end=YYYY-MM-DD"}), 400

    start = parse_date(start_str)
    end = parse_date(end_str)

    sql = text("""
        SELECT
          p.category,
          ROUND(SUM(ol.net_revenue)::numeric, 2) AS revenue_net,
          COUNT(DISTINCT ol.order_id) AS orders
        FROM public.order_lines ol
        JOIN public.products p ON p.product_id = ol.product_id
        WHERE ol.order_timestamp::date BETWEEN :start AND :end
        GROUP BY 1
        ORDER BY revenue_net DESC;
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"start": start, "end": end}).mappings().all()

    return jsonify({"start": start_str, "end": end_str, "data": [dict(r) for r in rows]})

@app.get("/top-products")
def top_products():
    start_str = request.args.get("start")
    end_str = request.args.get("end")
    limit = int(request.args.get("limit", 10))

    if not start_str or not end_str:
        return jsonify({"error": "Example: /top-products?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=10"}), 400

    start = parse_date(start_str)
    end = parse_date(end_str)

    sql = text("""
        SELECT
          p.product_id,
          p.name,
          p.category,
          SUM(ol.qty) AS units_sold,
          ROUND(SUM(ol.net_revenue)::numeric, 2) AS revenue_net
        FROM public.order_lines ol
        JOIN public.products p ON p.product_id = ol.product_id
        WHERE ol.order_timestamp::date BETWEEN :start AND :end
        GROUP BY 1,2,3
        ORDER BY revenue_net DESC
        LIMIT :limit;
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"start": start, "end": end, "limit": limit}).mappings().all()

    return jsonify({"start": start_str, "end": end_str, "data": [dict(r) for r in rows]})

@app.get("/marketing/roas-by-day")
def roas_by_day():
    start_str = request.args.get("start")
    end_str = request.args.get("end")
    if not start_str or not end_str:
        return jsonify({"error": "Example: /marketing/roas-by-day?start=YYYY-MM-DD&end=YYYY-MM-DD"}), 400

    start = parse_date(start_str)
    end = parse_date(end_str)

    sql = text("""
        WITH rev AS (
          SELECT
            ol.order_timestamp::date AS day,
            SUM(ol.net_revenue) AS revenue_net
          FROM public.order_lines ol
          WHERE ol.order_timestamp::date BETWEEN :start AND :end
          GROUP BY 1
        ),
        spend AS (
          SELECT
            ms.date AS day,
            SUM(ms.spend_eur) AS spend_eur
          FROM public.marketing_spend ms
          WHERE ms.date BETWEEN :start AND :end
          GROUP BY 1
        )
        SELECT
          COALESCE(rev.day, spend.day) AS day,
          ROUND(COALESCE(rev.revenue_net, 0)::numeric, 2) AS revenue_net,
          ROUND(COALESCE(spend.spend_eur, 0)::numeric, 2) AS spend_eur,
          ROUND(
            (COALESCE(rev.revenue_net, 0) / NULLIF(COALESCE(spend.spend_eur, 0), 0))::numeric,
            4
          ) AS roas
        FROM rev
        FULL OUTER JOIN spend ON spend.day = rev.day
        ORDER BY 1;
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"start": start, "end": end}).mappings().all()

    return jsonify({"start": start_str, "end": end_str, "data": [dict(r) for r in rows]})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
