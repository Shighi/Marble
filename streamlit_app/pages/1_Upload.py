"""
pages/1_Upload.py – File upload, analysis pipeline, result storage.
"""
import io
import streamlit as st
import pandas as pd

from core.analyzer  import EnhancedFraudDetectionAnalyzer, BUILTIN_BLOCKED_COUNTRIES
from core.database  import (create_session, update_session, bulk_insert_transactions,
                             bulk_insert_cases, save_analysis_results)
from core.reports   import build_excel_report
from core.written_report import build_written_report
from core.fx_rates  import get_fx_map, get_rate_info
from styles import apply_base_styles, render_sidebar

st.set_page_config(page_title="Upload Data – Sentinel", page_icon="", layout="wide")

# Apply shared styles
apply_base_styles()

st.markdown("# Upload Data")
st.markdown("Drop your weekly CSV files below. The engine will normalise currencies, run all 12 forensic rules, and store everything in the database.")
st.divider()

# ── Live FX rate status banner ────────────────────────────────────
fx_info = get_rate_info()
if fx_info["source"] == "live":
    st.success(
        f"🟢 **Live FX rates** loaded — ECB/Frankfurter data for **{fx_info['rate_date']}** "
        f"({fx_info['currency_count']} currencies). Refreshed at {fx_info['fetched_at']}."
    )
else:
    st.warning(
        "🟡 **Fallback FX rates** in use (network unavailable). Amounts will still be converted "
        "using the last known rates. They will auto-update on the next successful connection."
    )

# ── Blocked countries info ────────────────────────────────────────
with st.expander(f"🌍  Sanctioned countries ({len(BUILTIN_BLOCKED_COUNTRIES)} built-in — click to view)", expanded=False):
    cols = st.columns(3)
    for i, country in enumerate(sorted(set(BUILTIN_BLOCKED_COUNTRIES))):
        cols[i % 3].markdown(f"• {country.title()}")
    st.caption("Upload a **Blocked Countries CSV** below to add extra countries on top of this built-in list.")

st.divider()

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    render_sidebar()
    st.divider()
    st.markdown("**Active Rules**")
    for rule in [
        "Sanctions Match",
        "Velocity: >7 txns in 24h (Phone/Email/Card)",
        "Payout-Only: >=2 days",
        "3DS Required: no approved non-3DS",
    ]:
        st.markdown(f'<span class="rule-badge">✅ {rule}</span>', unsafe_allow_html=True)

# ── File uploaders ────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
with col1:
    card_file = st.file_uploader("💳  Card TM CSV", type=["csv"], key="card_upload",
                                  help="Card transaction management file")
with col2:
    apm_file  = st.file_uploader("📱  APM TM CSV",  type=["csv"], key="apm_upload",
                                  help="Alternative payment method file")
with col3:
    blocked_file = st.file_uploader("🌍  Blocked Countries CSV", type=["csv"], key="blocked_upload",
                                     help="List of sanctioned countries")

# ── CSV reader ────────────────────────────────────────────────────
def read_csv(file) -> pd.DataFrame | None:
    if file is None:
        return None
    content = file.read()
    for enc in ["utf-8","latin-1","cp1252"]:
        try:
            return pd.read_csv(io.BytesIO(content), encoding=enc, low_memory=False)
        except Exception:
            pass
    return None

# ── Preview ───────────────────────────────────────────────────────
if card_file or apm_file:
    st.markdown("### 👁  Data Preview & Date Range")
    
    # Show date ranges if available
    if card_file:
        card_file.seek(0)
        df_card = read_csv(card_file)
        if df_card is not None and "Created Date (Server TZ)" in df_card.columns:
            df_card["Created Date (Server TZ)"] = pd.to_datetime(df_card["Created Date (Server TZ)"], errors="coerce")
            card_dates = df_card["Created Date (Server TZ)"].dropna()
            if len(card_dates) > 0:
                st.info(f"💳 **Card TM:** {card_dates.min().date()} to {card_dates.max().date()} ({len(card_dates):,} rows with dates)")
    
    if apm_file:
        apm_file.seek(0)
        df_apm = read_csv(apm_file)
        if df_apm is not None and "Created Date (Server TZ)" in df_apm.columns:
            df_apm["Created Date (Server TZ)"] = pd.to_datetime(df_apm["Created Date (Server TZ)"], errors="coerce")
            apm_dates = df_apm["Created Date (Server TZ)"].dropna()
            if len(apm_dates) > 0:
                st.info(f"📱 **APM TM:** {apm_dates.min().date()} to {apm_dates.max().date()} ({len(apm_dates):,} rows with dates)")
    
    st.divider()
    st.markdown("### 📊 Data Preview")
    tab1, tab2 = st.tabs(["💳 Card TM", "📱 APM TM"])
    with tab1:
        if card_file:
            card_file.seek(0)
            df_prev = read_csv(card_file)
            if df_prev is not None:
                st.info(f"**{len(df_prev):,} rows × {len(df_prev.columns)} columns**")
                st.dataframe(df_prev.head(10), use_container_width=True, hide_index=True)
            card_file.seek(0)
    with tab2:
        if apm_file:
            apm_file.seek(0)
            df_prev = read_csv(apm_file)
            if df_prev is not None:
                st.info(f"**{len(df_prev):,} rows × {len(df_prev.columns)} columns**")
                st.dataframe(df_prev.head(10), use_container_width=True, hide_index=True)
            apm_file.seek(0)

st.divider()

# ── Run Analysis ──────────────────────────────────────────────────
run_btn = st.button("🚀  Run Full Forensic Analysis", type="primary",
                    disabled=(card_file is None and apm_file is None),
                    use_container_width=True)

if run_btn:
    # Load data
    with st.spinner("Reading files…"):
        card_file.seek(0) if card_file else None
        apm_file.seek(0)  if apm_file  else None
        blocked_file.seek(0) if blocked_file else None

        card_df    = read_csv(card_file)    if card_file    else None
        apm_df     = read_csv(apm_file)     if apm_file     else None
        blocked_df = read_csv(blocked_file) if blocked_file else None

    if card_df is None and apm_df is None:
        st.error("❌ Could not read any uploaded files. Check encoding.")
        st.stop()

    # Create DB session
    session_id = create_session(
        card_file.name  if card_file  else "",
        apm_file.name   if apm_file   else "",
        len(card_df)    if card_df is not None else 0,
        len(apm_df)     if apm_df  is not None else 0,
    )
    st.info(f"📋 Session ID: `{session_id}`")

    # Progress bar
    progress = st.progress(0, text="Initialising analyser…")

    analyzer = EnhancedFraudDetectionAnalyzer()
    progress.progress(10, text="Loading data into engine…")
    analyzer.load_from_dataframes(card_df, apm_df, blocked_df)

    progress.progress(25, text="Running forensic analyses…")
    results = analyzer.run_all()
    progress.progress(70, text="Saving transactions to database…")

    # Store transactions
    if analyzer.card_df is not None:
        bulk_insert_transactions(session_id, analyzer.card_df, "CARD")
    if analyzer.apm_df is not None:
        bulk_insert_transactions(session_id, analyzer.apm_df, "APM")

    progress.progress(80, text="Generating fraud cases…")
    cases = analyzer.build_cases()
    bulk_insert_cases(session_id, cases)

    progress.progress(90, text="Saving analysis results…")
    save_analysis_results(session_id, results)

    summary = analyzer.get_summary()
    update_session(session_id, "completed", summary)
    progress.progress(100, text="✅ Analysis complete!")

    st.balloons()
    st.success(f"✅  Analysis complete! **{len(cases)} fraud cases** created.")

    # ── Results summary ───────────────────────────────────────────
    st.markdown("### 📊 Analysis Results")
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("🔴 Critical Alerts",    summary.get("critical_alerts",0))
    c2.metric("📱 Payout-Only Phones", summary.get("payout_phones",0))
    c3.metric("📧 Payout-Only Emails", summary.get("payout_emails",0))
    c4.metric("🌍 Sanctions Hits",     summary.get("blocked_txns",0))
    c5.metric("⚡ Velocity Violations",summary.get("velocity_daily",0)+summary.get("velocity_hourly",0))

    st.metric("💰 Total At-Risk (USD)", f"${summary.get('payout_risk_usd',0):,.2f}")

    # ── Log output ────────────────────────────────────────────────
    with st.expander("📋 Analysis Log"):
        for level, msg in analyzer.log:
            icon = {"success":"✅","warning":"⚠️","error":"❌","info":"ℹ️"}.get(level,"•")
            st.markdown(f"{icon} {msg}")

    # Written text report (Colab-style narrative)
    st.divider()
    st.markdown("### 📝 Written Report")
    written_report = build_written_report(results, summary, session_id=session_id)
    with st.expander("Preview Written Report", expanded=False):
        st.text_area("Report Text", written_report, height=360, key="written_report_preview")
    st.download_button(
        "⬇️ Download Written Report (.txt)",
        data=written_report.encode("utf-8"),
        file_name=f"sentinel_written_report_{session_id[:8]}.txt",
        mime="text/plain",
        use_container_width=True,
    )

    # ── Download report ───────────────────────────────────────────
    st.divider()
    st.markdown("### 📥  Download Report")
    try:
        cases_df_for_report = pd.DataFrame([{
            "Case Ref":    c.get("entity_type",""),
            "Type":        c.get("alert_type",""),
            "Severity":    c.get("severity",""),
            "Entity":      c.get("entity_value",""),
            "Amount USD":  c.get("amount_usd",0),
            "Tx Count":    c.get("tx_count",0),
        } for c in cases])
        excel_bytes = build_excel_report(analyzer, session_id, cases_df_for_report)
        st.download_button(
            label="⬇️  Download Excel Report (.xlsx)",
            data=excel_bytes,
            file_name=f"sentinel_report_{session_id[:8]}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )
    except Exception as e:
        msg = str(e)
        if "openpyxl" in msg.lower():
            st.warning(
                "Excel export requires openpyxl. Install with: "
                "`python -m pip install openpyxl==3.1.2`"
            )
        else:
            st.warning(f"Report generation failed: {e}")

    st.markdown("---")
    st.markdown("➡️ Head to **📊 Dashboard** or **🚨 Cases** to review findings.")
    st.session_state["latest_session"] = session_id
