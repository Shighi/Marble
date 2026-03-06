"""
Shared Streamlit CSS styles for all pages.
Import this module and call apply_styles() to apply consistent styling.
"""
import streamlit as st

def apply_base_styles():
    """Apply base dark theme styles used across all pages."""
    st.markdown("""
<style>
/* Force dark paint from first frame */
:root {
    color-scheme: dark;
    --bg-primary: #0D1117;
    --bg-secondary: #161B22;
    --bg-tertiary: #21262D;
    --text-primary: #E6EDF3;
    --text-secondary: #C9D1D9;
}

/* ── Main background ── */
html, body, #root {
    background: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}
body { background: var(--bg-primary); color: var(--text-primary); }
[data-testid="stApp"] {
    background: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}
[data-testid="stAppViewContainer"] {
    background: var(--bg-primary) !important;
    padding: 0 !important;
}
[data-testid="stMain"] {
    background: var(--bg-primary) !important;
    padding: clamp(1rem, 2vw, 2rem) !important;
}
[data-testid="stHeader"],
[data-testid="stToolbar"] {
    background: var(--bg-primary) !important;
}

/* ── Headings ── */
h1, h2, h3, h4, h5, h6 { color: #E6EDF3 !important; }
h1 { font-size: 2.5rem !important; font-weight: 700 !important; margin-bottom: 16px !important; }
h2 { font-size: 2rem !important; font-weight: 700 !important; margin-bottom: 12px !important; }
h3 { font-size: 1.5rem !important; font-weight: 600 !important; margin-bottom: 10px !important; }
p { color: #C9D1D9 !important; }
[data-testid="stMarkdownContainer"] {
    color: #C9D1D9 !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: var(--bg-secondary) !important;
    border-right: 1px solid #30363D !important;
    min-width: clamp(220px, 20vw, 280px) !important;
    max-width: clamp(220px, 20vw, 280px) !important;
    z-index: 100;
    box-shadow: 2px 0 8px rgba(0, 0, 0, 0.08) !important;
}
[data-testid="stSidebar"] > div {
    background: var(--bg-secondary) !important;
}
[data-testid="stSidebarNav"] {
    padding: 12px 0 !important;
}
[data-testid="stSidebarNav"] a {
    color: #C9D1D9 !important;
    padding: 12px 20px !important;
    border-radius: 0 !important;
    margin: 0 !important;
    font-size: 0.95rem !important;
}
[data-testid="stSidebarNav"] a:hover {
    color: #D32F2F !important;
    background: #21262D !important;
}
[data-testid="stSidebarNav"] a[aria-current="page"] {
    color: #D32F2F !important;
    background: #24161A !important;
    border-left: 4px solid #D32F2F !important;
    padding-left: 16px !important;
}

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: var(--bg-secondary);
    border: 1px solid #30363D;
    border-radius: 8px;
    padding: 14px 18px;
    overflow-wrap: anywhere;
    word-break: break-word;
}
[data-testid="stMetricLabel"] { color: #8B949E !important; font-size: 0.78rem !important; }
[data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 700 !important; color: #E6EDF3 !important; }

/* ── DataFrames ── */
[data-testid="stDataFrame"] thead tr th {
    background: var(--bg-tertiary) !important;
    color: #E6EDF3 !important;
    font-weight: 600 !important;
}

/* ── Buttons ── */
.stButton > button {
    border-radius: 6px;
    font-weight: 600;
    transition: all 0.2s;
    background-color: #D32F2F !important;
    color: white !important;
}
.stButton > button:hover { 
    background-color: #B71C1C !important;
    box-shadow: 0 4px 12px rgba(211, 47, 47, 0.3); 
}

/* ── Inputs and controls ── */
[data-baseweb="input"] > div,
[data-baseweb="select"] > div,
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stTextArea"] textarea {
    background: var(--bg-secondary) !important;
    color: #E6EDF3 !important;
    border-color: #30363D !important;
}
[data-baseweb="select"] * {
    color: #E6EDF3 !important;
}
[role="listbox"] {
    background: var(--bg-secondary) !important;
    color: #E6EDF3 !important;
}

/* ── Data editors / tables ── */
[data-testid="stDataFrame"] {
    background: var(--bg-secondary) !important;
    border: 1px solid #30363D !important;
}
[data-testid="stDataFrame"] tbody tr td {
    background: var(--bg-primary) !important;
    color: #E6EDF3 !important;
}

/* ── Tabs ── */
button[data-baseweb="tab"] {
    font-weight: 600;
    font-size: 0.9rem;
    color: #C9D1D9 !important;
    background: var(--bg-secondary) !important;
}

/* ── Severity badges ── */
.badge-critical { background:#D32F2F; color:#fff; padding:4px 10px; border-radius:4px; font-size:0.75rem; font-weight:700; }
.badge-high     { background:#F57C00; color:#fff; padding:4px 10px; border-radius:4px; font-size:0.75rem; font-weight:700; }
.badge-medium   { background:#FBC02D; color:#000; padding:4px 10px; border-radius:4px; font-size:0.75rem; font-weight:700; }
.badge-low      { background:#388E3C; color:#fff; padding:4px 10px; border-radius:4px; font-size:0.75rem; font-weight:700; }

/* ── Info cards ── */
.info-card {
    background: var(--bg-secondary);
    border: 1px solid #30363D;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 16px;
}
.info-card h3 { margin: 0 0 8px 0; color: #E6EDF3; font-weight: 600; }
.info-card p  { margin: 0; color: #8B949E; font-size: 0.9rem; }

/* ── Upload zone ── */
.upload-zone { 
    border: 2px dashed #D32F2F; 
    border-radius: 8px; 
    padding: 40px; 
    text-align: center; 
    background: var(--bg-secondary); 
    margin-bottom: 20px; 
}

/* ── Rule badges ── */
.rule-badge { 
    display: inline-block; 
    background: var(--bg-tertiary); 
    color: #E6EDF3; 
    padding: 4px 10px; 
    border-radius: 4px; 
    margin: 4px; 
    font-size: 0.82rem; 
    font-weight: 500;
}

/* ── Risk cards ── */
.risk-card { 
    background: var(--bg-secondary); 
    border-left: 4px solid #D32F2F; 
    border-radius: 4px; 
    padding: 12px 16px; 
    margin: 8px 0; 
    border: 1px solid #30363D;
}

/* ── Session cards ── */
.session-card { 
    background: var(--bg-secondary); 
    border: 1px solid #30363D; 
    border-radius: 8px; 
    padding: 16px; 
    margin: 6px 0; 
}
.session-card:hover { border-color: #D32F2F; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }

/* Align Streamlit skeleton with dark UI to avoid flash contrast */
[data-testid="stSkeleton"] {
    background: transparent !important;
}
[data-testid="stSkeleton"] * {
    background: var(--bg-tertiary) !important;
    border-radius: 8px !important;
}

/* Responsive grids for home page sections */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
    margin: 0 0 8px 0;
}
.kpi-card {
    background: var(--bg-secondary);
    border: 1px solid #30363D;
    border-radius: 10px;
    padding: 14px 16px;
    min-height: 96px;
}
.kpi-label {
    color: #8B949E;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    margin-bottom: 6px;
}
.kpi-value {
    color: #E6EDF3;
    font-size: clamp(1.2rem, 2.2vw, 2rem);
    line-height: 1.2;
    font-weight: 700;
    overflow-wrap: anywhere;
    word-break: break-word;
}
.feature-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 12px;
}

@media (max-width: 1100px) {
    [data-testid="stSidebar"] {
        min-width: 220px !important;
        max-width: 220px !important;
    }
}
</style>
""", unsafe_allow_html=True)


def render_sidebar():
    """Render sidebar content (filters, stats). Navigation is auto-generated by Streamlit."""
    pass  # Streamlit auto-generates navigation now
