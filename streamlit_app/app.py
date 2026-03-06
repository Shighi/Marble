"""
Sentinel home / landing page.
"""
import streamlit as st

from core.database import get_case_stats, get_transaction_count
from styles import apply_base_styles


st.set_page_config(
    page_title="Sentinel - AML Fraud Detection",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_base_styles()

st.markdown("# Sentinel - AML & Fraud Detection")
st.divider()

try:
    stats = get_case_stats()
    tx_count = get_transaction_count()

    kpis = [
        ("Transactions Indexed", f"{tx_count:,}"),
        ("Total Cases", f"{stats.get('total', 0):,}"),
        ("Open", f"{stats.get('open', 0):,}"),
        ("Confirmed Fraud", f"{stats.get('confirmed', 0):,}"),
        ("At-Risk (USD)", f"${float(stats.get('at_risk_usd', 0)):,.0f}"),
    ]

    kpi_cards = "".join(
        [
            (
                "<div class='kpi-card'>"
                f"<div class='kpi-label'>{label}</div>"
                f"<div class='kpi-value'>{value}</div>"
                "</div>"
            )
            for label, value in kpis
        ]
    )
    st.markdown(f"<div class='kpi-grid'>{kpi_cards}</div>", unsafe_allow_html=True)
except Exception as exc:
    st.warning(f"Could not connect to database. Start Docker first.\n`{exc}`")

st.divider()

feature_cards = [
    (
        "Upload & Analyse",
        "Drag-drop your Card TM and APM CSV files. The engine normalises currencies, "
        "checks sanctions, and runs 12 forensic analyses in seconds.",
    ),
    (
        "Case Management",
        "Every flagged entity becomes a trackable case. Mark as Confirmed Fraud, "
        "False Positive, or Under Investigation with audit notes.",
    ),
    (
        "Search & History",
        "Instantly search any email, phone, card, or transaction ID across historical "
        "uploads with a full audit trail.",
    ),
    (
        "Live Dashboard",
        "Interactive charts for payout risk, BIN performance, merchant approval ratios, "
        "velocity violations, and fraud network graphs.",
    ),
    (
        "Downloadable Reports",
        "Export every analysis as a formatted multi-sheet Excel workbook with executive "
        "summary, case list, and raw flagged data.",
    ),
    (
        "Stateful & Persistent",
        "All data lives in PostgreSQL, so repeat offenders are detected across weekly "
        "uploads automatically.",
    ),
]

feature_html = "".join(
    [
        (
            "<div class='info-card'>"
            f"<h3>{title}</h3>"
            f"<p>{description}</p>"
            "</div>"
        )
        for title, description in feature_cards
    ]
)
st.markdown(f"<div class='feature-grid'>{feature_html}</div>", unsafe_allow_html=True)

st.divider()
st.markdown("**Quick Start ->** Use the **Upload Data** page to begin your first analysis.")
