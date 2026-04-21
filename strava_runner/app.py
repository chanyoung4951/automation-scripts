from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
PARQUET_GLOB = str(BASE_DIR / "data" / "parquet" / "activities" / "year=*" / "month=*" / "*.parquet")

st.set_page_config(page_title="Strava Runner Dashboard", layout="wide")
st.title("🏃 Strava Runner Dashboard")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    con = duckdb.connect()
    query = f"""
        SELECT
            activity_id,
            name,
            sport_type,
            start_date_local,
            distance_m / 1000.0 AS distance_km,
            moving_time_s / 60.0 AS moving_time_min,
            CASE WHEN moving_time_s > 0 THEN (distance_m / moving_time_s) * 3.6 ELSE NULL END AS speed_kmh,
            average_heartrate,
            max_heartrate,
            total_elevation_gain_m
        FROM read_parquet('{PARQUET_GLOB}')
        WHERE sport_type = 'Run'
    """
    return con.execute(query).df()


def pace_min_per_km(speed_kmh: float) -> float | None:
    if speed_kmh is None or speed_kmh <= 0:
        return None
    return 60.0 / speed_kmh


try:
    df = load_data()
except Exception as exc:  # noqa: BLE001
    st.warning(f"No data found yet or query failed: {exc}")
    st.stop()

if df.empty:
    st.info("No running activities available yet. Run sync_strava.py first.")
    st.stop()

df["date"] = pd.to_datetime(df["start_date_local"], errors="coerce")
df = df.sort_values("date")

col1, col2, col3 = st.columns(3)
col1.metric("Total Runs", int(df["activity_id"].nunique()))
col2.metric("Total Distance (km)", f"{df['distance_km'].sum():.1f}")
col3.metric("Avg Distance / Run (km)", f"{df['distance_km'].mean():.2f}")

weekly = (
    df.set_index("date")
    .resample("W")["distance_km"]
    .sum()
    .reset_index()
    .rename(columns={"distance_km": "weekly_km"})
)

st.subheader("Weekly Distance")
st.line_chart(weekly.set_index("date")["weekly_km"])

st.subheader("Recent Runs")
show_cols = [
    "date",
    "name",
    "distance_km",
    "moving_time_min",
    "speed_kmh",
    "average_heartrate",
    "total_elevation_gain_m",
]
st.dataframe(df[show_cols].tail(30), use_container_width=True)
