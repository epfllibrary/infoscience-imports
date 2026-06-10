"""Infoscience Import Pipeline — Supervision UI.

Launch:
    streamlit run app.py
    ./run_ui.sh          # also starts the background scheduler
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import env_loader
ACTIVE_ENV = env_loader.load_env()

from ui.auth import current_user, get_allowed_pages, login_wall, logout
from ui.constants import (
    PRIMARY, C_BLACK, C_BLUE, C_DARK, C_GRAY_100, C_GRAY_600,
    C_GREEN, C_RED, C_RED_DARK, C_YELLOW, SECONDARY, SOURCES,
)
from ui.helpers import get_db, mi
from ui.run_state import read_active_run

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Infoscience Imports",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Authentication ────────────────────────────────────────────────────────────
_username, _role = login_wall()

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(
    f"""<style>:root {{
    --primary:   {PRIMARY};
    --secondary:    {SECONDARY};
    --green:    {C_GREEN};
    --yellow:   {C_YELLOW};
    --red:      {C_RED};
    --red-dark: {C_RED_DARK};
    --dark:     {C_DARK};
    --black:    {C_BLACK};
    --gray-600: {C_GRAY_600};
    --gray-100: {C_GRAY_100};
    --blue:     {C_BLUE};
}}</style>""",
    unsafe_allow_html=True,
)
st.markdown(
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
    'family=Inter:wght@400;500;600;700'
    '&family=Roboto+Mono:wght@400;500'
    '&family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200'
    '&display=block" />',
    unsafe_allow_html=True,
)
st.markdown(
    f"<style>{(ROOT / 'ui' / 'styles.css').read_text()}</style>",
    unsafe_allow_html=True,
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
_ENV_STYLE = {
    "dev":  ("background:#dff0c8;color:#3a5a10", "DEV"),
    "test": ("background:#fdefd5;color:#7a4400", "TEST"),
    "prod": ("background:#ffd5d5;color:#7a0000", "PROD ⚠️"),
}
_NAV_ICONS = {
    "Tableau de bord": "dashboard",
    "Lancer un run":   "rocket_launch",
    "Programmation":   "schedule",
    "Publications":    "article",
    "Statistiques":    "bar_chart",
    "Configuration":   "settings",
    "Aide":            "menu_book",
}

with st.sidebar:
    st.markdown(
        f'<div style="font-size:1.15rem;font-weight:700;color:#C8D0E0;'
        f'display:flex;align-items:center;gap:6px;margin-bottom:2px">'
        f'{mi("cloud_sync","ms-neutral")} Infoscience Imports</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    _style, _label = _ENV_STYLE.get(ACTIVE_ENV, _ENV_STYLE["dev"])
    st.markdown(
        f'<div style="{_style};border-radius:6px;padding:5px 12px;'
        f'text-align:center;font-weight:700;font-size:0.85rem;'
        f'letter-spacing:.06em;margin-bottom:6px;">{_label}</div>',
        unsafe_allow_html=True,
    )
    _new_env = st.selectbox(
        "Environnement",
        options=list(env_loader.ENVIRONMENTS),
        index=list(env_loader.ENVIRONMENTS).index(ACTIVE_ENV),
        key="env_selector",
        help="Charge le fichier .env correspondant et isole la base de données.",
    )
    if _new_env != ACTIVE_ENV:
        env_loader.set_active_env(_new_env)
        env_loader.load_env(_new_env)
        st.cache_resource.clear()
        st.rerun()
    if ACTIVE_ENV == "prod":
        st.warning("Connecté à la **production** — les actions sont réelles.")

    st.markdown("---")
    # Consume session-state redirects set by dialogs (which can't write query_params reliably)
    _redirect = st.session_state.pop("_redirect_page", None)
    if _redirect:
        st.query_params["page"] = _redirect

    _allowed = get_allowed_pages(_role)
    _qp      = st.query_params.get("page", _allowed[0] if _allowed else "")
    page     = _qp if _qp in _allowed else (_allowed[0] if _allowed else "")

    _nav_html = '<nav class="sidebar-nav">'
    for _p in _allowed:
        _cls  = "nav-item active" if _p == page else "nav-item"
        _href = f"?page={_p.replace(' ', '+')}"
        _nav_html += (
            f'<a class="{_cls}" href="{_href}" target="_self">'
            f'<span class="ms ms-neutral">{_NAV_ICONS.get(_p, "circle")}</span>'
            f'<span>{_p}</span></a>'
        )
    _nav_html += "</nav>"
    st.markdown(_nav_html, unsafe_allow_html=True)

    st.markdown("---")
    if st.button("Rafraîchir", help="Recharge les données depuis la base",
                 icon=":material/refresh:"):
        st.cache_resource.clear()
        st.rerun()

    st.markdown("---")
    _, _dname, _ = current_user()
    _role_label = {"admin": "Admin", "curator": "Curator", "reporting": "Reporting"}.get(_role, _role)
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:7px;margin-bottom:4px">'
        f'<span class="ms ms-neutral" style="font-size:17px">person</span>'
        f'<span style="color:#C8D0E0;font-size:0.88rem;font-weight:600">'
        f'{_dname or _username}</span></div>'
        f'<div style="color:#667085;font-size:0.76rem;padding-left:24px">'
        f'{_role_label}</div>',
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    if st.button("Déconnexion", icon=":material/logout:"):
        logout()
    st.markdown(
        "<div style='color:#4a5568;font-size:0.72rem;margin-top:8px'>"
        "Infoscience · EPFL Library</div>",
        unsafe_allow_html=True,
    )

# ── Active run banner ─────────────────────────────────────────────────────────
_active = read_active_run()
if _active:
    _run_env = _active.get("env", "?")
    st.warning(
        f"⏳ **Run en cours** [{_run_env.upper()}] — `{_active['run_id']}` "
        f"(sources : {_active['sources']}, démarré : {_active['started_at'][:19].replace('T',' ')})  "
        f"→ Allez sur **Lancer un run** pour suivre la progression.",
        icon=None,
    )

# ── Page router ───────────────────────────────────────────────────────────────
db = get_db()

if page == "Tableau de bord":
    from ui.pages.dashboard import render
    render(db)

elif page == "Lancer un run":
    from ui.pages.run_launcher import render
    render(active_env=ACTIVE_ENV, root=ROOT, sources=SOURCES)

elif page == "Programmation":
    from ui.pages.scheduling import render
    render(active_env=ACTIVE_ENV, root=ROOT, sources=SOURCES, username=_username)

elif page == "Publications":
    from ui.pages.publications import render
    render(db, role=_role)

elif page == "Statistiques":
    from ui.pages.statistics import render
    render(db)

elif page == "Configuration":
    from ui.pages.configuration import render
    render(db, active_env=ACTIVE_ENV)

elif page == "Aide":
    from ui.pages.help import render
    render(root=ROOT)
