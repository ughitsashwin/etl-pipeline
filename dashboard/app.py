# dashboard/app.py
#
# WHAT THIS FILE DOES:
# A Streamlit dashboard that reads from the SQLite database
# and visualises the weather data and pipeline run history.
#
# WHY STREAMLIT?
# Streamlit turns a Python script into an interactive web app
# with almost no extra code. No HTML, no JavaScript, no Flask.
# It is widely used in data engineering and data science teams
# for internal dashboards and demos.
#
# HOW TO RUN:
#   streamlit run dashboard/app.py
# Then open http://localhost:8501 in your browser.

import sys
import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# Add project root to path so we can import from src/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
# This must be the first Streamlit call in the script.
# layout="wide" uses the full browser width instead of a narrow centered column.
st.set_page_config(
    page_title="ETL Pipeline Dashboard",
    page_icon="🌦️",
    layout="wide",
)

# ── DATABASE CONNECTION ───────────────────────────────────────────────────────
DB_PATH = Path("data/weather.db")

def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}")

# @st.cache_data caches the query result for 60 seconds.
# Without this, every time the user interacts with the dashboard
# it would re-query the database. Caching makes it fast.
@st.cache_data(ttl=60)
def load_weather_data():
    """Load all weather data from the database."""
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM weather ORDER BY date DESC",
        engine,
        parse_dates=["date"],
    )

@st.cache_data(ttl=60)
def load_run_history():
    """Load pipeline run history from the database."""
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM pipeline_runs ORDER BY ran_at DESC LIMIT 20",
        engine,
    )

# ── CHECK DATABASE EXISTS ─────────────────────────────────────────────────────
if not DB_PATH.exists():
    st.error("Database not found. Run the pipeline first: PYTHONPATH=. python src/pipeline.py")
    st.stop()

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
df = load_weather_data()
runs = load_run_history()

# ── HEADER ────────────────────────────────────────────────────────────────────
st.title("🌦️ ETL Pipeline Dashboard")
st.caption("Live weather data pipeline — auto-refreshes every 60 seconds")
st.divider()

# ── KPI METRICS ROW ───────────────────────────────────────────────────────────
# st.columns() creates a row of equally-spaced columns.
# These "metric" cards are the first thing a viewer sees —
# they give an instant summary of the pipeline health.
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    label="Total rows",
    value=f"{len(df):,}",
)
col2.metric(
    label="Cities tracked",
    value=df["city"].nunique(),
)
col3.metric(
    label="Latest data",
    value=str(df["date"].max())[:10],
)
col4.metric(
    label="Rainy days",
    value=f"{df['is_rainy'].sum()}",
    delta=f"{df['is_rainy'].mean()*100:.0f}% of all days",
)
col5.metric(
    label="Pipeline runs",
    value=len(runs),
    delta="last 20 shown",
)

st.divider()

# ── CITY FILTER ───────────────────────────────────────────────────────────────
# A selectbox lets the user filter all charts by city.
# "All Cities" shows data for every city combined.
cities = ["All Cities"] + sorted(df["city"].unique().tolist())
selected_city = st.selectbox("Filter by city", cities)

if selected_city != "All Cities":
    filtered = df[df["city"] == selected_city].copy()
else:
    filtered = df.copy()

# Sort by date ascending for charts (oldest → newest)
filtered = filtered.sort_values("date")

st.divider()

# ── CHARTS ROW ────────────────────────────────────────────────────────────────
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("🌡️ Temperature over time")
    st.caption("Daily max, min, and average temperature (°C)")

    # Pivot the data so each city gets its own column if showing all cities
    if selected_city == "All Cities":
        # Show average temperature per day across all cities
        temp_data = (
            filtered.groupby("date")[["temp_max", "temp_min", "temp_avg"]]
            .mean()
            .round(1)
        )
    else:
        temp_data = filtered.set_index("date")[["temp_max", "temp_min", "temp_avg"]]

    st.line_chart(temp_data, color=["#ff6b6b", "#4dabf7", "#51cf66"])

with chart_col2:
    st.subheader("🌧️ Daily precipitation (mm)")
    st.caption("Bars above 1mm = rainy day")

    if selected_city == "All Cities":
        precip_data = filtered.groupby("date")["precip_mm"].mean().round(1)
    else:
        precip_data = filtered.set_index("date")["precip_mm"]

    st.bar_chart(precip_data, color="#4dabf7")

st.divider()

# ── CITY COMPARISON ───────────────────────────────────────────────────────────
st.subheader("🏙️ City comparison")
st.caption("Average temperature and total rainfall per city across all loaded data")

city_stats = (
    df.groupby("city")
    .agg(
        avg_temp=("temp_avg", "mean"),
        total_rain=("precip_mm", "sum"),
        rainy_days=("is_rainy", "sum"),
        days_tracked=("date", "count"),
    )
    .round(2)
    .reset_index()
)
city_stats.columns = ["City", "Avg Temp (°C)", "Total Rain (mm)", "Rainy Days", "Days Tracked"]

# Display as a styled table — use_container_width fills the full width
st.dataframe(city_stats, use_container_width=True, hide_index=True)

st.divider()

# ── PIPELINE RUN HISTORY ──────────────────────────────────────────────────────
# This section shows the operational health of the pipeline itself —
# when it ran, how many rows it loaded, and whether it succeeded or failed.
# This is what makes your project look production-grade.
st.subheader("⚙️ Pipeline run history")
st.caption("Last 20 runs — green = success, red = failure")

if len(runs) == 0:
    st.info("No pipeline runs recorded yet.")
else:
    # Colour-code the status column
    def colour_status(val):
        if val == "success":
            return "background-color: #2d6a4f; color: #d8f3dc"
        elif val == "failed":
            return "background-color: #6b2737; color: #ffd6dc"
        return ""

    styled_runs = runs[["ran_at", "rows_loaded", "status", "message"]].copy()
    styled_runs.columns = ["Ran at (UTC)", "Rows loaded", "Status", "Message"]

    st.dataframe(
        styled_runs.style.map(colour_status, subset=["Status"]),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ── RAW DATA TABLE ────────────────────────────────────────────────────────────
with st.expander("📋 View raw data"):
    st.dataframe(
        filtered.sort_values("date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {len(filtered)} rows")
