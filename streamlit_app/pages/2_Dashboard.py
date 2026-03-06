"""
Dashboard page - KPIs and charts.
"""
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except Exception:
    px = None
    PLOTLY_AVAILABLE = False

from core.database import get_case_stats, get_transaction_count, get_all_sessions, DATABASE_URL
from styles import apply_base_styles, render_sidebar


st.set_page_config(page_title="Dashboard - Sentinel", page_icon="", layout="wide")
apply_base_styles()

with st.sidebar:
    render_sidebar()
    st.divider()
    try:
        sessions = get_all_sessions()
        if len(sessions):
            opts = ["All Sessions"] + [str(x) for x in sessions["session_id"].tolist()]
            st.selectbox("Filter by Session", opts)
    except Exception:
        pass

st.markdown("# Dashboard")
st.divider()

try:
    stats = get_case_stats()
    tx_count = get_transaction_count()
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Transactions", f"{tx_count:,}")
    c2.metric("Total Cases", f"{stats.get('total', 0):,}")
    c3.metric("Open", f"{stats.get('open', 0):,}")
    c4.metric("Confirmed", f"{stats.get('confirmed', 0):,}")
    c5.metric("Investigating", f"{stats.get('investigating', 0):,}")
    c6.metric("At-Risk $", f"${float(stats.get('at_risk_usd', 0)):,.0f}")
except Exception as exc:
    st.warning(f"Database error: {exc}")
    st.stop()

st.divider()

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


@st.cache_data(ttl=60)
def load_tx_data() -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT data_source, tx_status, amount_usd, merchant, is_3d, tx_date
        FROM transactions
        WHERE tx_date IS NOT NULL
        ORDER BY tx_date DESC
        LIMIT 100000
        """,
        engine,
    )


@st.cache_data(ttl=30)
def load_cases_data() -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT alert_type, severity, amount_usd, status, created_at
        FROM fraud_cases
        ORDER BY created_at DESC
        """,
        engine,
    )


try:
    tx_df = load_tx_data()
    case_df = load_cases_data()
except Exception as exc:
    st.error(f"Could not load data: {exc}")
    st.stop()

if not PLOTLY_AVAILABLE:
    st.warning("Plotly is not installed. Charts are disabled. Install with: `python -m pip install plotly==5.20.0`")
    st.dataframe(case_df.head(50), use_container_width=True, hide_index=True)
    st.dataframe(tx_df.head(50), use_container_width=True, hide_index=True)
    st.stop()

col1, col2 = st.columns(2)
with col1:
    st.markdown("#### Cases by Alert Type")
    if len(case_df):
        agg = case_df.groupby("alert_type").size().reset_index(name="count").sort_values("count", ascending=False)
        fig = px.bar(agg, x="alert_type", y="count", color="alert_type", template="plotly_dark")
        fig.update_layout(showlegend=False, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("#### Case Status Distribution")
    if len(case_df):
        status_agg = case_df.groupby("status").size().reset_index(name="count")
        fig = px.pie(status_agg, names="status", values="count", template="plotly_dark", hole=0.45)
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    st.markdown("#### Transaction Volume by Source & Status")
    if len(tx_df):
        agg = tx_df.groupby(["data_source", "tx_status"]).size().reset_index(name="count")
        fig = px.bar(agg, x="data_source", y="count", color="tx_status", barmode="stack", template="plotly_dark")
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.markdown("#### Cases by Severity")
    if len(case_df):
        sev = case_df.groupby("severity")["amount_usd"].agg(["sum", "count"]).reset_index()
        sev.columns = ["severity", "total_risk_usd", "case_count"]
        fig = px.bar(sev, x="severity", y="total_risk_usd", color="severity", text="case_count", template="plotly_dark")
        fig.update_traces(texttemplate="%{text} cases", textposition="outside")
        fig.update_layout(showlegend=False, margin=dict(t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("#### Daily Transaction Volume (Last 30 Days)")
if len(tx_df):
    tx_df["date"] = pd.to_datetime(tx_df["tx_date"]).dt.date
    daily = tx_df.groupby(["date", "data_source"]).size().reset_index(name="count")
    daily = daily[daily["date"] >= (pd.Timestamp.now() - pd.Timedelta(days=30)).date()]
    if len(daily):
        fig = px.line(daily, x="date", y="count", color="data_source", template="plotly_dark", markers=True)
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
