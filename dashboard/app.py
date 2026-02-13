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
st.sidebar.header("Filter & Settings")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 10, 2)


@st.cache_resource
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


r = get_redis_client()


def get_live_data():
    # In a full Spark -> Redis implementation, Spark would update keys like trend:spark
    # For this POC, we check common skills
    skills = [
        "python",
        "spark",
        "kafka",
        "react",
        "aws",
        "kubernetes",
        "docker",
        "sql",
        "java",
        "golang",
    ]
    data = []

    for skill in skills:
        count = r.get(f"trend:{skill}") or 0
        data.append({"Skill": skill.capitalize(), "Count": int(count)})

    return pd.DataFrame(data)


# Main Dashboard Layout
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("🔥 Top Requested Skills (Live)")
    chart_placeholder = st.empty()

with col2:
    st.subheader("📊 Raw Counts")
    table_placeholder = st.empty()

# Real-time update loop
while True:
    df = get_live_data()

    with chart_placeholder:
        fig = px.bar(
            df.sort_values("Count", ascending=False),
            x="Skill",
            y="Count",
            color="Count",
            color_continuous_scale="Viridis",
            template="plotly_dark",
        )
        st.plotly_chart(fig, use_container_width=True)

    with table_placeholder:
        st.table(df.sort_values("Count", ascending=False).reset_index(drop=True))

    time.sleep(refresh_rate)
