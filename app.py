"""
app.py — Entrada principal de GeoSales Analytics.
Streamlit Community Cloud ejecuta este archivo automáticamente.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import streamlit as st

st.set_page_config(
    page_title="GeoSales Analytics",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.session import init_session
from components.sidebar       import render_sidebar
from components.tab_upload    import render_upload
from components.tab_mapping   import render_mapping
from components.tab_filter    import render_filter
from components.tab_normalize import render_normalize
from components.tab_geocode   import render_geocode
from components.tab_cluster   import render_cluster
from components.tab_maps      import render_maps
from components.tab_export    import render_export

# ── Inicializar estado de sesión ──────────────────────────────────────────────
init_session()

# ── CSS global ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }

/* Header */
.app-header {
    background: linear-gradient(135deg, #0d2137 0%, #1a1f35 100%);
    border-left: 5px solid #00c9a7;
    border-radius: 10px;
    padding: 1.2rem 1.8rem;
    margin-bottom: 1.2rem;
}
.app-header h1 { color: #00c9a7; font-size: 1.7rem; font-weight: 700; margin: 0; }
.app-header p  { color: #a8b4c8; margin: 0.2rem 0 0; font-size: 0.88rem; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 4px; }
.stTabs [data-baseweb="tab"] {
    background: #1a1f2e;
    border-radius: 8px 8px 0 0;
    font-weight: 600;
    font-size: 0.82rem;
}
.stTabs [aria-selected="true"] {
    background: #00c9a7 !important;
    color: #0f1117 !important;
}

/* Botones primarios */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #00c9a7, #0096c7);
    color: #fff;
    font-weight: 600;
    border: none;
    border-radius: 8px;
}
.stButton > button[kind="primary"]:hover { opacity: 0.88; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #0f1117;
    border-right: 1px solid #1a1f2e;
}
</style>
""", unsafe_allow_html=True)

# ── Banner de la app ──────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
  <h1>🗺️ GeoSales Analytics</h1>
  <p>Normalización · Geocodificación Google Maps · Clusterización · Visualización geoespacial de ventas</p>
</div>
""", unsafe_allow_html=True)

# ── Panel lateral ─────────────────────────────────────────────────────────────
render_sidebar()

# ── Pestañas principales ──────────────────────────────────────────────────────
tabs = st.tabs([
    "📂 Carga",
    "🔗 Mapeo",
    "📅 Filtro",
    "✏️ Normalizar",
    "📍 Geocodificar",
    "🔵 Clusterizar",
    "🗺️ Mapas",
    "💾 Exportar",
])

with tabs[0]: render_upload()
with tabs[1]: render_mapping()
with tabs[2]: render_filter()
with tabs[3]: render_normalize()
with tabs[4]: render_geocode()
with tabs[5]: render_cluster()
with tabs[6]: render_maps()
with tabs[7]: render_export()
