"""
pages/5_History.py – Browse past upload sessions, replay reports.
"""
import streamlit as st
import pandas as pd
import json
import ast
from sqlalchemy import create_engine, text

from core.database import get_all_sessions, get_session, get_cases, DATABASE_URL
from core.reports  import build_excel_report
from core.analyzer import EnhancedFraudDetectionAnalyzer
from styles import apply_base_styles, render_sidebar

st.set_page_config(page_title="History – Sentinel", page_icon="", layout="wide")

apply_base_styles()

st.markdown("# Upload History")
st.markdown("Every analysis session is stored permanently. Click any session to review findings and download reports.")
st.divider()

# ── Load sessions ─────────────────────────────────────────────────
try:
    sessions = get_all_sessions()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.stop()

if len(sessions) == 0:
    st.info("No sessions yet. Go to **📤 Upload Data** to run your first analysis.")
    st.stop()

st.markdown(f"**{len(sessions)} sessions** in the database")

# Summary table
disp = sessions.copy()
disp["uploaded_at"] = pd.to_datetime(disp["uploaded_at"]).dt.strftime("%Y-%m-%d %H:%M")
disp["total_rows"]  = disp["card_rows"] + disp["apm_rows"]
disp["status_icon"] = disp["status"].map({"completed":"✅","processing":"⏳","failed":"❌"})
disp["total_alerts"] = disp["total_alerts"].fillna(0).astype(int)
disp["payout_risk_usd"] = pd.to_numeric(disp["payout_risk_usd"], errors="coerce").fillna(0)

disp_show = disp[["uploaded_at","card_file","apm_file","total_rows","total_alerts","payout_risk_usd","status_icon","session_id"]].copy()
disp_show.columns = ["Uploaded At","Card File","APM File","Total Rows","Alerts","Payout Risk $","Status","Session ID"]
disp_show["Payout Risk $"] = disp_show["Payout Risk $"].apply(lambda x: f"${float(x):,.2f}")
disp_show["Session ID"] = disp_show["Session ID"].astype(str)

st.dataframe(disp_show, use_container_width=True, hide_index=True)

# Custom table with 'View Results' button
for idx, row in disp_show.iterrows():
    cols = st.columns([2,2,2,1,1,1,1,1])
    cols[0].write(row["Uploaded At"])
    cols[1].write(row["Card File"])
    cols[2].write(row["APM File"])
    cols[3].write(f"{row['Total Rows']:,}")
    cols[4].write(f"{row['Alerts']:,}")
    cols[5].write(row["Payout Risk $"])
    cols[6].write(row["Status"])
    if cols[7].button("View Results", key=f"view_{row['Session ID']}"):
        st.experimental_set_query_params(session_id=row["Session ID"])
        st.success(f"Go to the Analysis Results page to view this session.")
# ── Display date range for analysis ──────────────────────────────
st.divider()
st.markdown("### 📅 Analysis Date Ranges")
st.caption("Shows the transaction date period for each session's data")

for _, row in sessions.iterrows():
    try:
        session_results = get_analysis_results(str(row['session_id']))
        metadata = session_results.get("_metadata", {})
        date_range = metadata.get("date_range", {})
        
        col1, col2, col3 = st.columns([1.5, 2, 1])
        with col1:
            st.write(f"**{row['uploaded_at'][:10]}**")
        with col2:
            if date_range:
                all_dates = []
                for source, dr in date_range.items():
                    if dr.get("start"):
                        all_dates.append(dr["start"])
                    if dr.get("end"):
                        all_dates.append(dr["end"])
                if all_dates:
                    min_date = min(all_dates)
                    max_date = max(all_dates)
                    st.write(f"📊 {min_date} → {max_date}")
            else:
                st.write("No date data available")
        with col3:
            st.write(f"{int(row['total_rows']):,} rows")
    except Exception:
        pass

st.divider()

session_labels = [
    f"{row['uploaded_at']} | {row['card_file']} + {row['apm_file']} | {int(row['card_rows']+row['apm_rows']):,} rows"
    for _, row in sessions.iterrows()
]
selected_label = st.selectbox("Select a session to inspect", session_labels)
selected_idx   = session_labels.index(selected_label)
selected_row   = sessions.iloc[selected_idx]
sid = str(selected_row["session_id"])

st.markdown(f"**Session ID:** `{sid}`")

# Display date range from analysis metadata
try:
    session_results = get_analysis_results(sid)
    metadata = session_results.get("_metadata", {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            try:
                metadata = ast.literal_eval(metadata)
            except:
                metadata = {}
    date_range = metadata.get("date_range", {})
    
    if date_range:
        st.divider()
        st.markdown("#### 📅 Analysis Date Coverage")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if date_range.get("Card"):
                st.write(f"**💳 Card TM**")
                st.write(f"{date_range['Card']['start']} to {date_range['Card']['end']}")
        
        with col2:
            if date_range.get("APM"):
                st.write(f"**📱 APM TM**")
                st.write(f"{date_range['APM']['start']} to {date_range['APM']['end']}")
        
        with col3:
            all_dates = []
            for source, dr in date_range.items():
                if dr.get("start"):
                    all_dates.append(dr["start"])
                if dr.get("end"):
                    all_dates.append(dr["end"])
            if all_dates:
                min_date = min(all_dates)
                max_date = max(all_dates)
                st.write(f"**Overall Period**")
                st.write(f"{min_date} to {max_date}")
except Exception:
    pass

# Summary
summary_raw = selected_row.get("summary") or {}
if isinstance(summary_raw, str):
    try:
        summary_raw = json.loads(summary_raw)
    except json.JSONDecodeError:
        try:
            summary_raw = ast.literal_eval(summary_raw)
        except:
            summary_raw = {}

if summary_raw:
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Alerts",    summary_raw.get("total_alerts",0))
    c2.metric("🔴 Critical",     summary_raw.get("critical_alerts",0))
    c3.metric("Payout-Only Phones", summary_raw.get("payout_phones",0))
    c4.metric("Payout-Only Emails", summary_raw.get("payout_emails",0))

    c5,c6,c7,c8 = st.columns(4)
    c5.metric("Sanctions Hits",  summary_raw.get("blocked_txns",0))
    c6.metric("Velocity Violations", int(summary_raw.get("velocity_daily",0))+int(summary_raw.get("velocity_hourly",0)))
    c7.metric("Fraud Networks",  summary_raw.get("fraud_networks",0))
    c8.metric("💰 Payout Risk $", f"${float(summary_raw.get('payout_risk_usd',0)):,.2f}")

# Cases for this session
st.divider()
st.markdown("#### 🚨 Fraud Cases from this Session")
try:
    session_cases = get_cases(session_id=sid)
    if len(session_cases):
        st.dataframe(session_cases, use_container_width=True, hide_index=True, height=300)
    else:
        st.info("No cases recorded for this session.")
except Exception as e:
    st.warning(f"Could not load cases: {e}")

# Transactions
st.divider()
st.markdown("#### 💳 Transactions from this Session")
try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    tx_df = pd.read_sql("""
        SELECT txid, data_source, email, card_no, phone,
               amount_usd, currency, tx_status, tx_type,
               country, merchant, tx_date
        FROM transactions
        WHERE session_id = %(sid)s
        ORDER BY tx_date DESC
        LIMIT 1000
    """, engine, params={"sid": sid})
    if len(tx_df):
        st.info(f"Showing up to 1,000 of this session's transactions")
        st.dataframe(tx_df, use_container_width=True, hide_index=True, height=300)
        # Download transactions
        st.download_button(
            "⬇️  Export Transactions CSV",
            data=tx_df.to_csv(index=False).encode("utf-8"),
            file_name=f"transactions_{sid[:8]}.csv",
            mime="text/csv",
        )
    else:
        st.info("No transactions indexed for this session.")
except Exception as e:
    st.warning(f"Could not load transactions: {e}")

# Download report (regenerate from stored cases)
st.divider()
st.markdown("#### 📥  Download Report")
if st.button("🔄  Generate Excel Report for this Session", use_container_width=True):
    with st.spinner("Building report…"):
        try:
            # We build a minimal analyzer shell with empty results to satisfy the report builder
            # and pass the cases df directly
            from core.analyzer import EnhancedFraudDetectionAnalyzer
            dummy = EnhancedFraudDetectionAnalyzer()
            session_cases_for_report = get_cases(session_id=sid)
            excel_bytes = build_excel_report(dummy, sid, session_cases_for_report)
            st.download_button(
                label="⬇️  Download Excel Report",
                data=excel_bytes,
                file_name=f"sentinel_report_{sid[:8]}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
                key="history_download",
            )
        except Exception as e:
            msg = str(e)
            if "openpyxl" in msg.lower():
                st.error(
                    "Excel export requires openpyxl. Install with: "
                    "`python -m pip install openpyxl==3.1.2`"
                )
            else:
                st.error(f"Report generation failed: {e}")
