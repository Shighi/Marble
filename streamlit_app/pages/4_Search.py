"""
pages/4_Search.py – Search transactions and build entity risk profiles.
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

from core.database import search_transactions, DATABASE_URL
from styles import apply_base_styles

st.set_page_config(page_title="Search – Sentinel", page_icon="", layout="wide")
apply_base_styles()

st.markdown("# Entity Search")
st.markdown("Search across all historical uploads by email, phone, card, or transaction ID.")
st.divider()

# ── Search controls ───────────────────────────────────────────────
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    query = st.text_input("Search Query", placeholder="e.g. john@example.com  |  254724274410  |  462845XXXXXX1032")
with col2:
    search_by = st.selectbox("Search Field", ["Email","Phone","Card No","Txid"])
with col3:
    st.markdown("<br>", unsafe_allow_html=True)
    search_btn = st.button("🔍 Search", type="primary", use_container_width=True)

if not query and not search_btn:
    st.info("Enter a search term above and click **Search**.")
    st.stop()

if query:
    with st.spinner("Searching…"):
        try:
            results = search_transactions(query.strip(), search_by)
        except Exception as e:
            st.error(f"Search failed: {e}")
            st.stop()

    if len(results) == 0:
        st.warning(f"No results found for `{query}` in **{search_by}**.")
        st.stop()

    st.success(f"Found **{len(results):,}** transactions for `{query}`")

    # ── Risk profile ──────────────────────────────────────────────
    st.divider()
    st.markdown("### 📋 Entity Risk Profile")

    total_usd  = float(results["amount_usd"].sum())
    approved   = results[results["tx_status"]=="approved"]
    declined   = results[results["tx_status"]=="declined"]
    total_payout   = float(results[results["tx_type"].str.contains("payout|withdrawal|refund", case=False, na=False)]["amount_usd"].sum())
    total_payin    = float(results[results["tx_type"].str.contains("sale|payment|deposit", case=False, na=False)]["amount_usd"].sum())
    unique_cards   = results["card_no"].nunique() if "card_no" in results.columns else 0
    unique_emails  = results["email"].nunique()  if "email"   in results.columns else 0

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Total Transactions",  f"{len(results):,}")
    c2.metric("✅ Approved",         f"{len(approved):,}")
    c3.metric("❌ Declined",         f"{len(declined):,}")
    c4.metric("💰 Total Volume $",   f"${total_usd:,.2f}")
    c5.metric("Approval Rate",       f"{len(approved)/len(results)*100:.1f}%" if len(results) else "—")

    if total_payout > 0 or total_payin > 0:
        c6,c7,c8 = st.columns(3)
        c6.metric("📤 Payout Total $",  f"${total_payout:,.2f}")
        c7.metric("📥 Payin Total $",   f"${total_payin:,.2f}")
        net = total_payout - total_payin
        c8.metric("Net Payout $", f"${net:,.2f}", delta=f"{'⚠️ Risk' if net > 0 else 'OK'}")

    # ── Linked cases ──────────────────────────────────────────────
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        linked_cases = pd.read_sql("""
            SELECT case_ref, alert_type, severity, status, amount_usd
            FROM fraud_cases
            WHERE entity_value ILIKE %(q)s
            ORDER BY created_at DESC
        """, engine, params={"q": f"%{query}%"})
        if len(linked_cases):
            st.divider()
            st.markdown(f"### 🚨 {len(linked_cases)} Linked Fraud Cases")
            st.dataframe(linked_cases, use_container_width=True, hide_index=True)
    except Exception:
        pass

    # ── Transaction table ─────────────────────────────────────────
    st.divider()
    st.markdown("### 📑 All Transactions")

    # Colour mapping helper
    def color_status(val):
        colors = {"approved":"color: #27AE60", "declined":"color: #E63946", "filtered":"color: #8B949E"}
        return colors.get(str(val).lower(), "")

    disp = results.copy()
    disp["amount_usd"] = disp["amount_usd"].apply(lambda x: f"${float(x or 0):,.2f}")
    disp.columns = [c.replace("_"," ").title() for c in disp.columns]

    st.dataframe(
        disp,
        use_container_width=True,
        hide_index=True,
        height=400,
    )

    # ── Charts ────────────────────────────────────────────────────
    if not PLOTLY_AVAILABLE:
        st.warning("Plotly is not installed. Charts are disabled. Install with: `python -m pip install plotly==5.20.0`")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### Transaction Type Breakdown")
        if PLOTLY_AVAILABLE and "tx_type" in results.columns:
            type_agg = results["tx_type"].value_counts().reset_index()
            type_agg.columns = ["type","count"]
            fig = px.pie(type_agg, names="type", values="count",
                         template="plotly_dark", hole=0.4)
            fig.update_layout(plot_bgcolor="#0D1117", paper_bgcolor="#0D1117", margin=dict(t=20,b=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown("#### Volume Over Time")
        if PLOTLY_AVAILABLE and "tx_date" in results.columns:
            results["date"] = pd.to_datetime(results["tx_date"]).dt.date
            daily = results.groupby("date")["amount_usd"].sum().reset_index()
            if len(daily) > 1:
                fig2 = px.bar(daily, x="date", y="amount_usd",
                              labels={"date":"Date","amount_usd":"USD"},
                              template="plotly_dark", color_discrete_sequence=["#E63946"])
                fig2.update_layout(plot_bgcolor="#0D1117", paper_bgcolor="#0D1117", margin=dict(t=20,b=20))
                st.plotly_chart(fig2, use_container_width=True)

    # ── Download ──────────────────────────────────────────────────
    st.divider()
    st.download_button(
        "⬇️ Export Results to CSV",
        data=results.to_csv(index=False).encode("utf-8"),
        file_name=f"search_{query[:20]}.csv",
        mime="text/csv",
        use_container_width=True,
    )
