"""Serving-layer dashboard: answers the real-time question directly --
"which cities have dangerous AQI in the last hour?" -- from the speed layer,
with historical context from the batch layer alongside it.

Run:
    streamlit run serving/dashboard.py
"""

import sys
import time
from pathlib import Path

# Streamlit executes this script with only serving/ on sys.path; make the
# repo root importable so common/config/serving resolve.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st

from common.aqi import DANGEROUS_AQI_THRESHOLD
from config import settings
from serving.merge import build_and_write_serving_view, dangerous_cities_now

st.set_page_config(page_title="Air Quality - Lambda Architecture Demo", layout="wide")
st.title("Which cities have dangerous AQI in the last hour?")
st.caption(
    f"Speed layer: rolling 60-minute sliding window, EPA max-sub-index AQI. "
    f'"Dangerous" = AQI >= {DANGEROUS_AQI_THRESHOLD} (Unhealthy or worse).'
)

col1, col2 = st.columns([1, 4])
with col1:
    refresh = st.button("Refresh now")
with col2:
    auto = st.checkbox("Auto-refresh every 30s", value=False)

if refresh or "serving_df" not in st.session_state:
    st.session_state["serving_df"] = build_and_write_serving_view()

serving_df = st.session_state["serving_df"]
dangerous_df = dangerous_cities_now(serving_df)

st.subheader(f"{len(dangerous_df)} city/ies dangerous right now")
if serving_df.empty:
    st.info("No data yet. Run the ingestion producer, then the batch and speed jobs (see README).")
elif dangerous_df.empty:
    st.success("No tracked city is currently at Unhealthy AQI or worse.")
else:
    show_cols = [c for c in ["city", "country", "aqi", "dominant_pollutant", "reading_count", "window_end", "historical_avg_aqi"] if c in dangerous_df.columns]
    st.dataframe(dangerous_df[show_cols], width="stretch")
    if {"latitude", "longitude"}.issubset(dangerous_df.columns):
        map_df = dangerous_df.dropna(subset=["latitude", "longitude"]).rename(columns={"latitude": "lat", "longitude": "lon"})
        if not map_df.empty:
            st.map(map_df[["lat", "lon"]])

st.subheader("All tracked cities - speed view + historical batch baseline")
if serving_df.empty:
    st.write("Nothing to show yet.")
else:
    st.dataframe(serving_df, width="stretch")

st.subheader("Historical worst city-hours (batch layer)")
try:
    summary = pd.read_parquet(settings.BATCH_VIEW_DIR / "city_summary")
    st.bar_chart(summary.set_index("city")["max_aqi"].head(15))
except Exception:
    st.info("Batch view not built yet. Run `python -m batch.batch_job`.")

if auto:
    time.sleep(30)
    st.rerun()
