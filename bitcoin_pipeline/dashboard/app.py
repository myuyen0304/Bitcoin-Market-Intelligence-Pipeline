"""Streamlit dashboard for Bitcoin Gold marts in DuckDB."""

from pathlib import Path
import sys

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bitcoin_pipeline.config.settings import settings


st.set_page_config(page_title="Bitcoin Market Intelligence", layout="wide")


@st.cache_data(ttl=300)
def load_table(table_name: str) -> pd.DataFrame:
    """Load a Gold mart from DuckDB into a DataFrame."""
    if not settings.duckdb_path.exists():
        raise FileNotFoundError(
            f"DuckDB file not found: {settings.duckdb_path}. Run dbt before opening the dashboard."
        )

    with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
        return conn.execute(f"select * from {table_name} order by date_day").df()


def render_empty_state(error: Exception) -> None:
    """Render a concise recovery path when marts are not available yet."""
    st.error(str(error))
    st.code(
        "$env:DBT_PROFILES_DIR=(Get-Location).Path\n"
        "dbt run\n"
        "dbt test\n"
        "streamlit run bitcoin_pipeline/dashboard/app.py",
        language="powershell",
    )


try:
    price_df = load_table("mart_btc_price_analysis")
    anomaly_df = load_table("mart_btc_volume_anomalies")
    sentiment_df = load_table("mart_btc_sentiment_correlation")
except Exception as exc:
    render_empty_state(exc)
    st.stop()

if price_df.empty:
    render_empty_state(ValueError("Gold marts are empty. Run Bronze ingestion and dbt again."))
    st.stop()

latest = price_df.sort_values("date_day").iloc[-1]
latest_anomalies = anomaly_df[anomaly_df["is_volume_anomaly"] == True].sort_values("date_day", ascending=False)
latest_freshness = price_df["latest_ingested_at"].max()

st.title("Bitcoin Market Intelligence")

kpi_cols = st.columns(4)
kpi_cols[0].metric("Latest Close", f"${latest['close']:,.0f}")
kpi_cols[1].metric("Fear & Greed", f"{latest['fear_greed_value']:.0f}" if pd.notnull(latest["fear_greed_value"]) else "n/a")
kpi_cols[2].metric("30D Volatility", f"{latest['volatility_30d_pct']:.2f}%" if pd.notnull(latest["volatility_30d_pct"]) else "n/a")
kpi_cols[3].metric("Latest Ingested", str(latest_freshness)[:19])

fig_price = go.Figure()
fig_price.add_trace(go.Scatter(x=price_df["date_day"], y=price_df["close"], mode="lines", name="Close"))
fig_price.add_trace(go.Scatter(x=price_df["date_day"], y=price_df["ma7_close"], mode="lines", name="MA7"))
fig_price.add_trace(go.Scatter(x=price_df["date_day"], y=price_df["ma30_close"], mode="lines", name="MA30"))
fig_price.update_layout(title="BTC Close Price with Moving Averages", yaxis_title="USD", hovermode="x unified")
st.plotly_chart(fig_price, use_container_width=True)

sentiment_chart = price_df.dropna(subset=["fear_greed_value"])
fig_sentiment = go.Figure()
fig_sentiment.add_trace(go.Bar(x=sentiment_chart["date_day"], y=sentiment_chart["fear_greed_value"], name="Fear & Greed"))
fig_sentiment.add_trace(go.Scatter(x=sentiment_chart["date_day"], y=sentiment_chart["daily_return_pct"], mode="lines", name="Daily Return %", yaxis="y2"))
fig_sentiment.update_layout(
    title="Sentiment vs Daily Return",
    yaxis={"title": "Fear & Greed"},
    yaxis2={"title": "Daily Return %", "overlaying": "y", "side": "right"},
    hovermode="x unified",
)
st.plotly_chart(fig_sentiment, use_container_width=True)

left, right = st.columns([3, 2])
with left:
    fig_volume = px.bar(
        anomaly_df,
        x="date_day",
        y="volume",
        color="is_volume_anomaly",
        title="BTC Volume Anomaly Detection",
        labels={"volume": "Volume", "date_day": "Date", "is_volume_anomaly": "Anomaly"},
    )
    st.plotly_chart(fig_volume, use_container_width=True)

with right:
    st.subheader("Recent Volume Anomalies")
    table_cols = ["date_day", "close", "volume", "volume_z_score", "daily_return_pct", "sentiment_label"]
    st.dataframe(latest_anomalies[table_cols].head(10), use_container_width=True, hide_index=True)

st.subheader("Rolling Sentiment Correlation")
fig_corr = px.line(
    sentiment_df,
    x="date_day",
    y="rolling_30d_sentiment_return_corr",
    title="30D Rolling Correlation: Fear & Greed vs Daily Return",
)
st.plotly_chart(fig_corr, use_container_width=True)
