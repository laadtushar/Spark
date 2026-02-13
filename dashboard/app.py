import streamlit as st
import pandas as pd
import plotly.express as px
import redis
import os
import json
import time

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)

st.set_page_config(page_title="Global Talent Pulse", layout="wide")

st.title("🌐 Global Talent Pulse")
st.markdown("### Real-Time Technology Adoption Trends (Streaming from Spark/Kafka)")

# Sidebar for controls
st.sidebar.header("Dashboard Settings")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 10, 2)
st.sidebar.markdown("---")
st.sidebar.info(
    "This dashboard displays real-time data processed from Kafka topics by Apache Spark."
)


@st.cache_resource
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


r = get_redis_client()


def get_live_data():
    # Fetch global trends
    skill_keys = r.keys("trend:skill:*")
    global_data = []
    for key in skill_keys:
        skill = key.split(":")[-1]
        count = r.get(key)
        global_data.append({"Skill": skill.capitalize(), "Count": int(count)})

    # Fetch location trends
    loc_keys = r.keys("trend:loc:*")
    location_data = []
    for key in loc_keys:
        parts = key.split(":")
        loc = parts[2]
        skill = parts[3]
        count = r.get(key)
        location_data.append(
            {"Location": loc, "Skill": skill.capitalize(), "Count": int(count)}
        )

    stats = {"total_hits": int(r.get("stats:total_skill_hits") or 0)}

    return pd.DataFrame(global_data), pd.DataFrame(location_data), stats


# Placeholders for dynamic content
metrics_placeholder = st.empty()
main_row_1 = st.columns([2, 1])
main_row_2 = st.columns([1, 1])

with main_row_1[0]:
    st.subheader("🔥 Global Skill Demand")
    bar_chart_placeholder = st.empty()

with main_row_1[1]:
    st.subheader("🍰 Skill Distribution")
    pie_chart_placeholder = st.empty()

with main_row_2[0]:
    st.subheader("📍 Demand by Location")
    location_chart_placeholder = st.empty()

with main_row_2[1]:
    st.subheader("📋 Top Postings Data")
    table_placeholder = st.empty()

# Real-time update loop
while True:
    df_global, df_loc, stats = get_live_data()

    if not df_global.empty:
        # 1. Update Metrics
        top_skill = df_global.sort_values("Count", ascending=False).iloc[0]["Skill"]
        with metrics_placeholder:
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Skill Extractions", f"{stats['total_hits']:,}")
            m2.metric("Trending Skill", top_skill)
            m3.metric(
                "Active Regions",
                len(df_loc["Location"].unique()) if not df_loc.empty else 0,
            )

        # 2. Global Bar Chart
        with bar_chart_placeholder:
            fig_bar = px.bar(
                df_global.sort_values("Count", ascending=False),
                x="Skill",
                y="Count",
                color="Count",
                color_continuous_scale="Viridis",
                template="plotly_dark",
                height=400,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # 3. Pie Chart
        with pie_chart_placeholder:
            fig_pie = px.pie(
                df_global,
                names="Skill",
                values="Count",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                template="plotly_dark",
                height=400,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # 4. Location Breakdown
        if not df_loc.empty:
            with location_chart_placeholder:
                # Group by location for a summary bar
                loc_summary = df_loc.groupby("Location")["Count"].sum().reset_index()
                fig_loc = px.bar(
                    loc_summary.sort_values("Count", ascending=False),
                    y="Location",
                    x="Count",
                    orientation="h",
                    color="Count",
                    color_continuous_scale="Magma",
                    template="plotly_dark",
                    height=400,
                )
                st.plotly_chart(fig_loc, use_container_width=True)

        # 5. Data Table
        with table_placeholder:
            st.dataframe(
                df_global.sort_values("Count", ascending=False).reset_index(drop=True),
                use_container_width=True,
                height=400,
            )

    time.sleep(refresh_rate)
