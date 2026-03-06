"""
pages/8_Merchant_Risk.py – Merchant risk dashboard with approval ratios and trends.
"""
import streamlit as st
import pandas as pd
import json
from sqlalchemy import text

from core.database import get_engine, get_all_sessions
from styles import apply_base_styles, render_sidebar

st.set_page_config(page_title="Merchant Risk – Sentinel", page_icon="", layout="wide")
apply_base_styles()
st.divider()
st.markdown("**Analysis Filter**")
sessions = get_all_sessions()
if sessions is not None and len(sessions) > 0:
    session_id = st.selectbox("Select Session", sessions["session_id"].tolist(), index=0)
else:
    st.warning("No sessions available")
    st.stop()

st.markdown("# Merchant Risk Dashboard")
st.divider()

try:
    # Load analysis results for the selected session
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT results FROM analysis_results WHERE session_id = :sid LIMIT 1"),
            {"sid": str(session_id)},
        ).fetchone()
    
    if not result:
        st.error("No analysis data found for this session")
        st.stop()
    
    analysis_json = result[0]
    if isinstance(analysis_json, str):
        analysis = json.loads(analysis_json)
    else:
        analysis = analysis_json
    
except Exception as e:
    st.error(f"Failed to load merchant analysis: {e}")
    st.stop()

# ── Extract merchant analysis ───────────────────────────────────
merchant_analysis = analysis.get("merchant_analysis", {})
stats = merchant_analysis.get("stats", {})
risky_merchants = merchant_analysis.get("risky_merchants", pd.DataFrame())

# ── KPIs ────────────────────────────────────────────────────────
st.markdown("## 📊 Merchant Statistics")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Merchants", stats.get("total_merchants", 0))
c2.metric("Risky (<30% approval)", stats.get("risky_low_approval", 0))
c3.metric("Suspicious (>95% approval)", stats.get("risky_high_approval", 0))
c4.metric("Avg Approval Rate", f"{stats.get('avg_approval_rate', 0):.1f}%")
c5.metric("At-Risk Merchants", stats.get("total_merchants", 0) - stats.get("safe_merchants", 1))

st.divider()

# ── Risky Merchants Table ───────────────────────────────────────
st.markdown("## ⚠️  Risk Merchants (Approval <30% or >95%)")

if risky_merchants is not None and len(risky_merchants) > 0:
    display_df = risky_merchants.copy()
    if isinstance(display_df, pd.Series):
        display_df = display_df.to_frame().T
    
    # Format columns
    if "approval_rate" in display_df.columns:
        display_df["approval_rate"] = display_df["approval_rate"].apply(lambda x: f"{float(x):.1f}%")
    if "approval_amount" in display_df.columns:
        display_df["approval_amount"] = display_df["approval_amount"].apply(lambda x: f"${float(x):,.0f}")
    if "decline_amount" in display_df.columns:
        display_df["decline_amount"] = display_df["decline_amount"].apply(lambda x: f"${float(x):,.0f}")
    if "total_transactions" in display_df.columns:
        display_df["total_transactions"] = display_df["total_transactions"].apply(lambda x: int(x))
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("✅ No risky merchants detected in this session.")

st.divider()

# ── Merchant Trend Analysis ─────────────────────────────────────
st.markdown("## 📈 Merchant Volume Trends")

merchant_trends = analysis.get("merchant_trend_analysis", {})
trend_merchants = merchant_trends.get("trend_merchants", pd.DataFrame())
trend_stats = merchant_trends.get("stats", {})

if trend_merchants is not None and len(trend_merchants) > 0:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Increasing Volume", trend_stats.get("increasing_volume", 0))
    c2.metric("Decreasing Volume", trend_stats.get("decreasing_volume", 0))
    c3.metric("Volatility Spikes", trend_stats.get("volatility_spikes", 0))
    c4.metric("Avg Change", f"{trend_stats.get('avg_change_pct', 0):.1f}%")
    
    st.write("**Merchants with Notable Trends:**")
    display_trend = trend_merchants.copy()
    if "volume_change_pct" in display_trend.columns:
        display_trend["volume_change_pct"] = display_trend["volume_change_pct"].apply(lambda x: f"{float(x):.1f}%")
    if "transaction_count" in display_trend.columns:
        display_trend["transaction_count"] = display_trend["transaction_count"].apply(lambda x: int(x))
    
    st.dataframe(display_trend.head(20), use_container_width=True, hide_index=True)
else:
    st.info("No trend data available for analysis period.")

st.divider()

# ── Merchant Performance Breakdown ──────────────────────────────
st.markdown("## 💰 Top Merchants by Volume")

top_by_volume = merchant_analysis.get("top_merchants_by_volume", pd.DataFrame())

if top_by_volume is not None and len(top_by_volume) > 0:
    display_top = top_by_volume.copy()
    if "approval_amount" in display_top.columns:
        display_top["approval_amount"] = display_top["approval_amount"].apply(lambda x: f"${float(x):,.0f}")
    if "decline_amount" in display_top.columns:
        display_top["decline_amount"] = display_top["decline_amount"].apply(lambda x: f"${float(x):,.0f}")
    
    st.dataframe(display_top.head(15), use_container_width=True, hide_index=True)
else:
    st.info("No volume breakdown available.")

st.divider()

# ── Risk Summary ────────────────────────────────────────────────
st.markdown("## ⚡ Merchant Risk Assessment")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🔴 High-Risk Assessment")
    st.write("**Merchants flagged for:**")
    st.write("• Approval rate <30% (Rejection Risk)")
    st.write("• Approval rate >95% (Fraud Risk)")
    st.write("• High velocity changes (Market Risk)")
    st.write("• Associated with fraud cases")

with col2:
    st.markdown("### ✅ Recommended Actions")
    st.write("1. **Review & Approve** merchants <30% if legitimate")
    st.write("2. **Investigate >95%** merchants for potential MCC abuse")
    st.write("3. **Monitor trends** merchants with sudden volume spikes")
    st.write("4. **Lower limits** on first-time or risky merchants")

st.divider()

# ── Export merchant report ──────────────────────────────────────
st.markdown("### 📥  Export Merchant Report")

if risky_merchants is not None and len(risky_merchants) > 0:
    csv_data = risky_merchants.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️  Export Risky Merchants to CSV",
        data=csv_data,
        file_name=f"sentinel_merchant_risk_{session_id}.csv",
        mime="text/csv",
        use_container_width=True,
    )
