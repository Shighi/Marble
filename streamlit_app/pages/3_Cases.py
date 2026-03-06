"""
pages/3_Cases.py – Review, filter, and manage fraud cases.
"""
import streamlit as st
import pandas as pd

from core.database import (
    get_cases, update_case_status, get_case_stats, get_all_sessions,
    add_to_whitelist, is_whitelisted, get_whitelist, remove_from_whitelist
)
from styles import apply_base_styles

st.set_page_config(page_title="Cases – Sentinel", page_icon="", layout="wide")
apply_base_styles()
st.divider()
# Filters
st.markdown("**Filters**")
status_filter = st.selectbox("Status", ["all","open","confirmed_fraud","false_positive","under_investigation"])
sev_filter    = st.multiselect("Severity", ["critical","high","medium","low"],
                               default=["critical","high","medium","low"])
type_filter   = st.multiselect("Alert Type",
                               ["payout_only","sanctions","velocity","3ds_anomaly","recurring","timing","bin","card","phone","email","merchant","3d_secure"],
                               default=[])

# Whitelist stats
st.divider()
st.markdown("**Whitelist**")
try:
    whitelist_df = get_whitelist()
    st.metric("Whitelisted Entities", len(whitelist_df) if whitelist_df is not None else 0)
    if st.button("View Whitelist", use_container_width=True):
        st.session_state.show_whitelist = True
except Exception:
    pass

st.markdown("# Case Management")
st.divider()

# KPIs
try:
    stats = get_case_stats()
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Total Cases",       f"{stats.get('total',0):,}")
    c2.metric("🔴 Open",           f"{stats.get('open',0):,}")
    c3.metric("✅ Confirmed Fraud", f"{stats.get('confirmed',0):,}")
    c4.metric("🔬 Investigating",  f"{stats.get('investigating',0):,}")
    c5.metric("💰 At-Risk $",      f"${float(stats.get('at_risk_usd',0)):,.0f}")
except Exception as e:
    st.warning(f"DB error: {e}")
    st.stop()

st.divider()

# Load cases
try:
    cases_df = get_cases(status=status_filter, exclude_whitelisted=False)
except Exception as e:
    st.error(f"Could not load cases: {e}")
    st.stop()

if len(cases_df) == 0:
    st.info("🎉 No cases match the current filters. Upload data to generate alerts.")
    st.stop()

# Apply client-side filters
if sev_filter:
    cases_df = cases_df[cases_df["severity"].isin(sev_filter)]
if type_filter:
    cases_df = cases_df[cases_df["alert_type"].isin(type_filter)]

# Search bar
search = st.text_input("🔍 Search by entity (email, phone, card…)", placeholder="e.g. digitalsolutionbs@gmail.com")
if search:
    cases_df = cases_df[cases_df["entity_value"].str.contains(search, case=False, na=False)]

st.markdown(f"**{len(cases_df):,} cases** matching filters")

# ── Bulk Actions Section ──────────────────────────────────────────
st.markdown("### ⚡ Bulk Case Actions")
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("Mark as Confirmed Fraud", use_container_width=True, key="bulk_confirmed"):
        st.session_state.bulk_action = "confirmed_fraud"

with col2:
    if st.button("Mark as False Positive", use_container_width=True, key="bulk_false"):
        st.session_state.bulk_action = "false_positive"

with col3:
    if st.button("Mark Under Investigation", use_container_width=True, key="bulk_investigating"):
        st.session_state.bulk_action = "under_investigation"

with col4:
    if st.button("Add to Whitelist", use_container_width=True, key="bulk_whitelist"):
        st.session_state.bulk_action = "whitelist"

# ── Table with colour-coded severity and checkboxes ────────────────
SEV_ICONS = {"critical":"🔴","high":"🟠","medium":"🟡","low":"🟢"}
TYPE_ICONS = {"payout_only":"💸","sanctions":"🌍","velocity":"⚡","3ds_anomaly":"🔐","recurring":"🔄","timing":"🕐"}

display_df = cases_df[[
    "case_ref","alert_type","severity","entity_type","entity_value",
    "amount_usd","tx_count","status","created_at"
]].copy()
display_df["severity"]   = display_df["severity"].apply(lambda s: f'{SEV_ICONS.get(s,"•")} {s.upper()}')
display_df["alert_type"] = display_df["alert_type"].apply(lambda t: f'{TYPE_ICONS.get(t,"•")} {t.replace("_"," ").title()}')
display_df["amount_usd"] = display_df["amount_usd"].apply(lambda x: f"${float(x or 0):,.2f}")
display_df.columns = ["Case Ref","Alert Type","Severity","Entity","Value","Amount","# Txns","Status","Created"]
display_df = display_df.reset_index(drop=True)
display_df = display_df.reset_index(drop=True)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Case Ref":   st.column_config.TextColumn(width="small"),
        "Alert Type": st.column_config.TextColumn(width="medium"),
        "Value":      st.column_config.TextColumn(width="large"),
        "Amount":     st.column_config.TextColumn(width="small"),
    },
    height=400,
)

# ── Handle Bulk Actions ───────────────────────────────────────────
if "bulk_action" in st.session_state and st.session_state.bulk_action:
    action = st.session_state.bulk_action
    
    st.divider()
    st.markdown(f"### 📋 Bulk Action: {action.replace('_', ' ').title()}")
    
    # Get selected rows from user
    selected_indices = st.multiselect(
        "Select cases to apply action:",
        options=display_df.index,
        format_func=lambda i: f"{display_df.iloc[i]['Case Ref']} — {display_df.iloc[i]['Value']} ({display_df.iloc[i]['Amount']})"
    )
    
    if selected_indices:
        selected_cases = cases_df.iloc[selected_indices]
        st.write(f"**{len(selected_cases)} cases selected**")
        
        if action == "whitelist":
            st.markdown("**Whitelist Reason:**")
            reason = st.text_area("Why are these entities safe?", placeholder="e.g., VIP customer, known merchant partner", key="whitelist_reason")
            whitelister = st.text_input("Your Name", placeholder="Your name for audit trail", key="whitelister")
            
            if st.button(f"✅ Whitelist {len(selected_cases)} Entities", type="primary", use_container_width=True):
                success_count = 0
                for _, case in selected_cases.iterrows():
                    try:
                        add_to_whitelist(
                            entity_value=case["entity_value"],
                            entity_type=case["entity_type"],
                            reason=reason or "N/A",
                            whitelisted_by=whitelister or "System"
                        )
                        success_count += 1
                    except Exception as e:
                        st.error(f"Failed to whitelist {case['entity_value']}: {e}")
                
                st.success(f"✅ {success_count}/{len(selected_cases)} entities whitelisted!")
                st.session_state.bulk_action = None
                st.cache_data.clear()
                st.rerun()
        
        else:
            reviewer = st.text_input("Reviewer Name", placeholder="Your name", key="bulk_reviewer")
            notes = st.text_area("Bulk Action Notes", placeholder="Reason for bulk status change", key="bulk_notes", height=60)
            
            if st.button(f"💾 Update {len(selected_cases)} Cases", type="primary", use_container_width=True):
                success_count = 0
                for _, case in selected_cases.iterrows():
                    try:
                        update_case_status(
                            case_ref=case["case_ref"],
                            new_status=action,
                            notes=notes or "Bulk action",
                            reviewer=reviewer or "System"
                        )
                        success_count += 1
                    except Exception as e:
                        st.error(f"Failed to update {case['case_ref']}: {e}")
                
                st.success(f"✅ {success_count}/{len(selected_cases)} cases updated to `{action}`!")
                st.session_state.bulk_action = None
                st.cache_data.clear()
                st.rerun()

# ── Update case status (Individual) ──────────────────────────────
st.divider()
st.markdown("### ✏️  Update Single Case Status")

with st.form("update_case_form", border=False):
    col1, col2 = st.columns(2)
    case_ref = col1.selectbox("Select Case", cases_df["case_ref"].tolist())
    new_status = col2.selectbox("New Status", [
        "open","confirmed_fraud","false_positive","under_investigation"
    ])
    reviewer = st.text_input("Reviewer Name", placeholder="Your name")
    notes    = st.text_area("Notes / Reason", placeholder="Add investigation notes here…", height=80)
    submitted = st.form_submit_button("💾  Save Update", type="primary", use_container_width=True)

    if submitted and case_ref:
        try:
            update_case_status(case_ref, new_status, notes, reviewer or "Risk Officer")
            st.success(f"✅ Case **{case_ref}** updated to `{new_status}`")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Failed to update case: {e}")

# ── Whitelist View ──────────────────────────────────────────────
if st.session_state.get("show_whitelist"):
    st.divider()
    st.markdown("### 📋 Current Whitelist")
    
    try:
        whitelist_df = get_whitelist()
        if whitelist_df is not None and len(whitelist_df) > 0:
            st.dataframe(whitelist_df, use_container_width=True, hide_index=True)
            
            # Remove from whitelist
            st.markdown("**Remove from Whitelist:**")
            col1, col2 = st.columns([3, 1])
            entity_to_remove = col1.selectbox(
                "Select entity:",
                whitelist_df["entity_value"].tolist(),
                key="remove_entity"
            )
            if col2.button("❌ Remove", use_container_width=True, key="remove_btn"):
                try:
                    remove_from_whitelist(entity_to_remove)
                    st.success(f"✅ Removed {entity_to_remove} from whitelist")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to remove: {e}")
        else:
            st.info("No whitelisted entities yet.")
    except Exception as e:
        st.error(f"Could not load whitelist: {e}")

# ── Bulk download ─────────────────────────────────────────────────
st.divider()
st.markdown("### 📥  Export Cases")
csv_data = cases_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️  Export to CSV",
    data=csv_data,
    file_name="sentinel_cases.csv",
    mime="text/csv",
    use_container_width=True,
)


