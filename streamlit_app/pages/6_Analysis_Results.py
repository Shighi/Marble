"""
pages/6_Analysis_Results.py – Comprehensive forensic analysis results viewer.
Shows all 12 analysis methods with detailed findings for a selected session.
"""
import streamlit as st
import pandas as pd
import json
from core.database import get_all_sessions, get_analysis_results, get_cases
from core.written_report import build_written_report
from styles import apply_base_styles

st.set_page_config(page_title="Analysis Results – Sentinel", page_icon="", layout="wide")

apply_base_styles()

st.markdown("# Forensic Analysis Results")
st.markdown("View all 12 fraud detection analyses for a selected upload session.")
st.divider()

# Load sessions
try:
    sessions = get_all_sessions()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.stop()

if len(sessions) == 0:
    st.info("No sessions yet. Go to **📤 Upload Data** to run your first analysis.")
    st.stop()

# Session selector
import urllib.parse
query_params = st.experimental_get_query_params()
preselect_sid = query_params.get("session_id", [None])[0]
session_labels = [
    f"{row['uploaded_at']} | {int(row['card_rows']+row['apm_rows']):,} rows"
    for _, row in sessions.iterrows()
]
if preselect_sid and preselect_sid in sessions["session_id"].astype(str).values:
    preselect_idx = sessions["session_id"].astype(str).tolist().index(preselect_sid)
    selected_label = session_labels[preselect_idx]
    selected_idx = preselect_idx
else:
    selected_label = st.selectbox("Select a session", session_labels)
    selected_idx = session_labels.index(selected_label)
selected_row = sessions.iloc[selected_idx]
sid = str(selected_row["session_id"])

st.markdown(f"**Session ID:** `{sid}`")
st.markdown(f"**Uploaded:** {selected_row['uploaded_at']} | **Rows:** {int(selected_row['card_rows']+selected_row['apm_rows']):,}")
st.divider()

# Load analysis results
try:
    results = get_analysis_results(sid)
    cases_df = get_cases(session_id=sid)
except Exception as e:
    st.error(f"Failed to load analysis results: {e}")
    st.stop()

if not results:
    st.warning("No analysis results found for this session.")
    st.stop()

# Extract and display date range metadata
metadata = results.get("_metadata", {})
if isinstance(metadata, str):
    try:
        import json
        metadata = json.loads(metadata)
    except:
        metadata = {}

date_range = metadata.get("date_range", {})

# Display date range information
st.markdown("### 📅 Analysis Time Period")
col1, col2, col3, col4 = st.columns(4)

if date_range.get("Card"):
    with col1:
        st.metric("💳 Card Data", f"{date_range['Card']['start']} to {date_range['Card']['end']}")
if date_range.get("APM"):
    with col2:
        st.metric("📱 APM Data", f"{date_range['APM']['start']} to {date_range['APM']['end']}")
if metadata.get("total_card_rows"):
    with col3:
        st.metric("💳 Card Rows", f"{int(metadata['total_card_rows']):,}")
if metadata.get("total_apm_rows"):
    with col4:
        st.metric("📱 APM Rows", f"{int(metadata['total_apm_rows']):,}")

# Overall date range
all_dates = []
for source, dr in date_range.items():
    if dr.get("start"):
        all_dates.append(dr["start"])
    if dr.get("end"):
        all_dates.append(dr["end"])

if all_dates:
    min_date = min(all_dates)
    max_date = max(all_dates)
    st.info(f"**Analysis covers transactions from {min_date} to {max_date}**")

st.divider()

# Define the 12 analyses with their display names and icons
analyses_config = {
    "enhanced_bin_analysis": {"icon": "🏧", "title": "BIN Analysis", "description": "Bank Identification Number patterns and risk indicators"},
    "enhanced_card_analysis": {"icon": "💳", "title": "Card Analysis", "description": "Card transaction patterns and velocity"},
    "enhanced_phone_analysis": {"icon": "📱", "title": "Phone Analysis", "description": "Phone number transaction patterns"},
    "enhanced_email_analysis": {"icon": "📧", "title": "Email Analysis", "description": "Email address transaction patterns"},
    "payout_only_cross_analysis": {"icon": "🚨", "title": "Payout-Only Networks", "description": "Entities with only payout transactions (fraud networks)"},
    "recurring_card_patterns": {"icon": "🔄", "title": "Recurring Patterns", "description": "Suspicious recurring transaction patterns"},
    "velocity_violations": {"icon": "⚡", "title": "Velocity Violations", "description": "Daily and hourly transaction velocity anomalies"},
    "suspicious_timing": {"icon": "⏰", "title": "Suspicious Timing", "description": "Unusual transaction timing patterns"},
    "merchant_trends": {"icon": "📈", "title": "Merchant Trends", "description": "Merchant volume and approval trends"},
    "merchant_analysis": {"icon": "🏪", "title": "Merchant Analysis", "description": "Merchant risk assessment and approval ratios"},
    "blocked_countries": {"icon": "🌍", "title": "Blocked Countries", "description": "Transactions from sanctioned jurisdictions"},
    "secure_3d_analysis": {"icon": "🔐", "title": "3D Secure Analysis", "description": "3D Secure transaction patterns and risks"},
}

# Summary metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("🚨 Total Alerts Generated", len(cases_df) if len(cases_df) > 0 else 0)
col2.metric("🔴 Critical", len(cases_df[cases_df["severity"] == "critical"]) if len(cases_df) > 0 else 0)
col3.metric("🟠 High", len(cases_df[cases_df["severity"] == "high"]) if len(cases_df) > 0 else 0)
col4.metric("🟡 Medium", len(cases_df[cases_df["severity"] == "medium"]) if len(cases_df) > 0 else 0)

st.divider()

# Tabs for each analysis
tabs = st.tabs([f"{cfg['icon']} {cfg['title']}" for cfg in analyses_config.values()])

for (analysis_key, analysis_cfg), tab in zip(analyses_config.items(), tabs):
    with tab:
        try:
            st.markdown(f"## {analysis_cfg['icon']} {analysis_cfg['title']}")
            st.caption(analysis_cfg['description'])
            st.divider()

            if analysis_key not in results or not results[analysis_key]:
                st.info("No findings in this analysis")
                continue

            # Parse analysis_data with multiple fallback strategies
            analysis_data = results[analysis_key]

            if isinstance(analysis_data, dict):
                pass
            elif isinstance(analysis_data, str):
                try:
                    analysis_data = json.loads(analysis_data)
                except json.JSONDecodeError:
                    # Older sessions may contain stringified Python objects
                    # (e.g., DataFrame repr) that are not valid JSON.
                    st.warning("This session contains legacy analysis payloads that are not JSON-parsable.")
                    st.text_area("Raw analysis payload", analysis_data[:5000], height=220, key=f"raw_{analysis_key}")
                    continue
            elif isinstance(analysis_data, list):
                analysis_data = {"data": analysis_data}
            else:
                st.warning(f"Invalid data format for analysis: {type(analysis_data)}")
                continue

            if not isinstance(analysis_data, dict):
                st.warning("Data is still not a dict after parsing")
                continue

            if analysis_key == "enhanced_bin_analysis":
                col1, col2 = st.columns(2)
                with col1:
                    stats = analysis_data.get("stats", {})
                    st.metric("Total Unique BINs", stats.get("unique_bins", analysis_data.get("total_unique_bins", 0)))

                if "bins_highest" in analysis_data:
                    st.markdown("### Top BINs by Approved Amount")
                    bins_df = pd.DataFrame(analysis_data["bins_highest"])
                    st.dataframe(bins_df, use_container_width=True, hide_index=True)

                if "bins_declines" in analysis_data:
                    st.markdown("### Top BINs by Declines")
                    declines_df = pd.DataFrame(analysis_data["bins_declines"])
                    st.dataframe(declines_df, use_container_width=True, hide_index=True)

            elif analysis_key == "enhanced_card_analysis":
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Unique Cards", analysis_data.get("total_cards", 0))

                if "cards_highest" in analysis_data:
                    st.markdown("### Top Cards by Approved Amount")
                    cards_df = pd.DataFrame(analysis_data["cards_highest"])
                    st.dataframe(cards_df, use_container_width=True, hide_index=True)

                if "cards_declines" in analysis_data:
                    st.markdown("### Top Cards by Declines")
                    declines_df = pd.DataFrame(analysis_data["cards_declines"])
                    st.dataframe(declines_df, use_container_width=True, hide_index=True)

            elif analysis_key == "enhanced_phone_analysis":
                if "total_unique_phones" in analysis_data:
                    st.metric("Total Unique Phones", analysis_data["total_unique_phones"])

                if "phones_only_payouts" in analysis_data and analysis_data["phones_only_payouts"] is not None:
                    phones_df = pd.DataFrame(analysis_data["phones_only_payouts"]) if isinstance(analysis_data["phones_only_payouts"], list) else analysis_data["phones_only_payouts"]
                    if len(phones_df) > 0:
                        st.markdown(f"### Phones with ONLY Payouts ({len(phones_df)} found)")
                        st.dataframe(phones_df.head(20), use_container_width=True, hide_index=True)

            elif analysis_key == "enhanced_email_analysis":
                if "emails_only_payouts" in analysis_data and analysis_data["emails_only_payouts"] is not None:
                    emails_df = pd.DataFrame(analysis_data["emails_only_payouts"]) if isinstance(analysis_data["emails_only_payouts"], list) else analysis_data["emails_only_payouts"]
                    if len(emails_df) > 0:
                        st.markdown(f"### Emails with ONLY Payouts ({len(emails_df)} found)")
                        st.dataframe(emails_df.head(20), use_container_width=True, hide_index=True)

            elif analysis_key == "payout_only_cross_analysis":
                if "fraud_networks" in analysis_data:
                    networks = analysis_data["fraud_networks"]
                    if networks:
                        st.markdown(f"### Detected Fraud Networks ({len(networks)} found)")
                        for net in networks[:20]:
                            st.write(f"**{net.get('entity1', 'Unknown')}** <-> **{net.get('entity2', 'Unknown')}**")
                            st.caption(f"Combined payout: ${float(net.get('combined_payout', 0)):,.2f}")

                if "payout_only_count" in analysis_data:
                    st.metric("Entities with ONLY Payouts", analysis_data["payout_only_count"])
                    st.metric("Total Payout Risk", f"${float(analysis_data.get('total_payout_usd', 0)):,.2f}")

            elif analysis_key == "velocity_violations":
                if "daily_violations" in analysis_data:
                    daily_df = pd.DataFrame(analysis_data["daily_violations"]) if isinstance(analysis_data["daily_violations"], list) else analysis_data["daily_violations"]
                    if len(daily_df) > 0:
                        st.markdown(f"### Daily Velocity Violations ({len(daily_df)} found)")
                        st.dataframe(daily_df, use_container_width=True, hide_index=True)

                if "hourly_violations" in analysis_data:
                    hourly_df = pd.DataFrame(analysis_data["hourly_violations"]) if isinstance(analysis_data["hourly_violations"], list) else analysis_data["hourly_violations"]
                    if len(hourly_df) > 0:
                        st.markdown(f"### Hourly Velocity Violations ({len(hourly_df)} found)")
                        st.dataframe(hourly_df, use_container_width=True, hide_index=True)

            elif analysis_key == "merchant_analysis":
                if "merchant_stats" in analysis_data:
                    merchant_df = pd.DataFrame(analysis_data["merchant_stats"]) if isinstance(analysis_data["merchant_stats"], list) else analysis_data["merchant_stats"]
                    if len(merchant_df) > 0:
                        st.markdown("### Merchant Analysis Summary")
                        st.dataframe(merchant_df.head(20), use_container_width=True)

            elif analysis_key == "blocked_countries":
                if "blocked_transactions" in analysis_data:
                    blocked_df = pd.DataFrame(analysis_data["blocked_transactions"]) if isinstance(analysis_data["blocked_transactions"], list) else analysis_data["blocked_transactions"]
                    if len(blocked_df) > 0:
                        st.markdown(f"### Blocked Country Transactions ({len(blocked_df)} found)")
                        st.dataframe(blocked_df, use_container_width=True, hide_index=True)
                    else:
                        st.success("No transactions from blocked countries detected")
                else:
                    st.success("No transactions from blocked countries detected")

            elif analysis_key == "secure_3d_analysis":
                st.markdown("### 3D Secure Summary")
                if "summary" in analysis_data:
                    summary_text = analysis_data["summary"]
                    st.write(summary_text)
                if "high_value_non_3d" in analysis_data:
                    non3d_df = pd.DataFrame(analysis_data["high_value_non_3d"]) if isinstance(analysis_data["high_value_non_3d"], list) else analysis_data["high_value_non_3d"]
                    if len(non3d_df) > 0:
                        st.markdown(f"### High-Value Non-3D Transactions ({len(non3d_df)} found)")
                        st.dataframe(non3d_df.head(20), use_container_width=True, hide_index=True)

            else:
                st.json({k: v for k, v in analysis_data.items() if not isinstance(v, pd.DataFrame)})

        except Exception as e:
            st.warning(f"An error occurred rendering this analysis tab: {e}")

st.divider()

# Export options
st.markdown("### 📥 Export Options")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📋 Copy Analysis Summary", use_container_width=True):
        summary_dict = {
            "total_alerts": len(cases_df) if len(cases_df) > 0 else 0,
            "critical_alerts": len(cases_df[cases_df["severity"] == "critical"]) if len(cases_df) > 0 else 0,
            "high_alerts": len(cases_df[cases_df["severity"] == "high"]) if len(cases_df) > 0 else 0,
            "medium_alerts": len(cases_df[cases_df["severity"] == "medium"]) if len(cases_df) > 0 else 0,
        }
        summary_text = build_written_report(results, summary_dict, session_id=sid)
        st.text_area("Summary", summary_text, height=300)

with col2:
    if st.button("💾 Download Analysis JSON", use_container_width=True):
        json_str = json.dumps(results, default=str, indent=2)
        st.download_button(
            label="Save JSON",
            data=json_str,
            file_name=f"analysis_{sid[:8]}.json",
            mime="application/json"
        )

with col3:
    if st.button("📊 View Raw Results", use_container_width=True):
        st.json(results)

