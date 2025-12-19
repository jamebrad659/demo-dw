import requests
import pandas as pd
import streamlit as st
import os
import streamlit as st

API_BASE = st.secrets.get("API_BASE", os.getenv("API_BASE", "http://127.0.0.1:5000")).rstrip("/")



def to_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


st.set_page_config(page_title="Ecommerce Dashboard", layout="wide")
st.title("📊 Ecommerce Dashboard")

# Date filters (use your known data range)
col1, col2 = st.columns(2)
with col1:
    start = st.date_input("Start date", value=pd.to_datetime("2025-06-21").date())
with col2:
    end = st.date_input("End date", value=pd.to_datetime("2025-12-17").date())

params = {"start": start.isoformat(), "end": end.isoformat()}

# --- KPIs ---
kpi_resp = requests.get(f"{API_BASE}/kpis", params=params).json()
k = kpi_resp.get("kpis", {})

revenue_net = to_float(k.get("revenue_net"))
orders = int(to_float(k.get("orders")))
aov = to_float(k.get("aov"))
refund_rate = to_float(k.get("refund_rate_pct"))

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Revenue (Net)", f"€{revenue_net:,.2f}")
kpi2.metric("Orders", f"{orders:,}")
kpi3.metric("AOV", f"€{aov:,.2f}")
kpi4.metric("Refund Rate", f"{refund_rate:.2f}%")


st.divider()

# --- Revenue by day chart ---
rev_resp = requests.get(f"{API_BASE}/revenue/by-day", params=params).json()
df = pd.DataFrame(rev_resp.get("data", []))

if df.empty:
    st.warning("No data returned for this date range.")
else:
    df["day"] = pd.to_datetime(df["day"])
    df = df.sort_values("day")
    st.subheader("Revenue by day")
    st.line_chart(df.set_index("day")["revenue_net"])

    with st.expander("Show data table"):
        st.dataframe(df, use_container_width=True)

st.divider()

# --- Revenue by category ---
cat_resp = requests.get(f"{API_BASE}/revenue/by-category", params=params).json()
cat_df = pd.DataFrame(cat_resp.get("data", []))

st.subheader("Revenue by category")
if cat_df.empty:
    st.warning("No category data returned.")
else:
    cat_df["revenue_net"] = cat_df["revenue_net"].apply(to_float)
    st.bar_chart(cat_df.set_index("category")["revenue_net"])

st.divider()

# --- Top products ---
top_resp = requests.get(f"{API_BASE}/top-products", params={**params, "limit": 10}).json()
top_df = pd.DataFrame(top_resp.get("data", []))

st.subheader("Top products (by revenue)")
if top_df.empty:
    st.warning("No top products returned.")
else:
    top_df["revenue_net"] = top_df["revenue_net"].apply(to_float)
    st.dataframe(top_df, use_container_width=True)


st.divider()
st.subheader("Marketing performance (ROAS)")

roas_resp = requests.get(f"{API_BASE}/marketing/roas-by-day", params=params).json()
roas_df = pd.DataFrame(roas_resp.get("data", []))

if roas_df.empty:
    st.warning("No ROAS data returned.")
else:
    roas_df["day"] = pd.to_datetime(roas_df["day"])
    roas_df["revenue_net"] = roas_df["revenue_net"].apply(to_float)
    roas_df["spend_eur"] = roas_df["spend_eur"].apply(to_float)
    roas_df["roas"] = roas_df["roas"].apply(to_float)

    # simple charts
    st.line_chart(roas_df.set_index("day")[["revenue_net", "spend_eur"]])

    st.subheader("ROAS by day")
    st.line_chart(roas_df.set_index("day")["roas"])

    with st.expander("Show ROAS table"):
        st.dataframe(roas_df, use_container_width=True)
