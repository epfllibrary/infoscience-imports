"""Infoscience Import Pipeline — Interface de supervision et de pilotage.

Lancement :
    streamlit run app.py

Configuration :
    Les variables d'environnement sont lues depuis le fichier .env à la racine
    du projet (même convention que le pipeline CLI).
"""

from __future__ import annotations

import html as _html
import os
import re
import sys
import subprocess
import time
import threading
import queue
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
# ── path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import env_loader
ACTIVE_ENV = env_loader.load_env()   # loads .env.{active_env} at startup

from config import default_queries
from db.pipeline_db import PipelineDB
from ui.auth import login_wall, logout, get_allowed_pages, current_user
from ui.run_state import (
    write_active_run,
    try_acquire_run_lock,
    read_active_run,
    clear_active_run,
    kill_active_run,
    get_state_file,
)

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Infoscience Imports",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── authentication (must run before any other rendering) ──────────────────────
_username, _role = login_wall()

# ── constants ─────────────────────────────────────────────────────────────────
SOURCES = ["scopus", "crossref", "openalex", "wos", "epo", "zenodo"]

# EPFL Infoscience colour palette
CANARD      = "#007480"   # info / main accent
LEMAN       = "#00A79F"   # lighter teal
C_GREEN     = "#92C642"   # success
C_YELLOW    = "#ec9433"   # warning
C_RED       = "#FF0000"   # danger
C_RED_DARK  = "#B51F1F"   # danger dark
C_DARK      = "#43515f"   # dark panels
C_BLACK     = "#212121"   # body text
C_GRAY_600  = "#707070"   # secondary text
C_GRAY_100  = "#E6E6E6"   # light backgrounds
C_BLUE      = "#2b4e72"   # deduplicated/info

# Alias kept for existing f-string references throughout the file
EPFL_TEAL = CANARD
EPFL_RED  = C_RED

STATUS_COLORS = {
    "imported":  C_GREEN,
    "rejected":  C_RED,
    "running":   C_YELLOW,
    "completed": CANARD,
    "failed":    C_RED_DARK,
}

# ── CSS ───────────────────────────────────────────────────────────────────────
# ── CSS ───────────────────────────────────────────────────────────────────────
# Step 1 — inject colour tokens as CSS custom properties so styles.css can
#           reference them via var() without any Python f-string coupling.
st.markdown(
    f"""<style>:root {{
    --canard:   {CANARD};
    --leman:    {LEMAN};
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
# Step 2 — load the external stylesheet (pure CSS, no Python templating).
st.markdown(
    f"<style>{(ROOT / 'ui' / 'styles.css').read_text()}</style>",
    unsafe_allow_html=True,
)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _slugify(name: str) -> str:
    """Convert a run name to a URL-safe slug for inclusion in run IDs."""
    slug = re.sub(r"[^\w\s-]", "", name.lower().strip())
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:30]


def _make_run_id(name: str = "") -> str:
    """Build a run ID: timestamp[_slug] where slug is derived from name."""
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    slug = _slugify(name) if name.strip() else ""
    return f"{ts}_{slug}" if slug else ts


# ── DB helper ─────────────────────────────────────────────────────────────────
# PipelineDB n'ouvre aucune connexion persistante : chaque opération ouvre,
# exécute et ferme sa propre connexion. Le cache de l'instance est inoffensif.
@st.cache_resource
def get_db() -> PipelineDB:
    return PipelineDB(read_only=True)


def metric_card(label: str, value, sub: str = "") -> str:
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {"<div class='metric-sub'>" + sub + "</div>" if sub else ""}
    </div>"""


def badge(status: str) -> str:
    return f'<span class="badge badge-{status}">{status}</span>'


# ── Sidebar navigation ────────────────────────────────────────────────────────
_ENV_STYLE = {
    "dev":  ("background:#dff0c8;color:#3a5a10", "DEV"),
    "test": ("background:#fdefd5;color:#7a4400", "TEST"),
    "prod": ("background:#ffd5d5;color:#7a0000", "PROD ⚠️"),
}

with st.sidebar:
    st.markdown("## 📚 Infoscience Imports")

    # ── Environment selector ──────────────────────────────────────────────
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

    # ── Navigation (pages filtrées par rôle) ─────────────────────────────
    st.markdown("---")
    _allowed = get_allowed_pages(_role)
    page = st.radio(
        "Navigation",
        _allowed,
        label_visibility="collapsed",
    )
    st.markdown("---")
    if st.button("🔄 Rafraîchir", help="Recharge les données depuis la base"):
        st.cache_resource.clear()
        st.rerun()

    # ── User info + logout ────────────────────────────────────────────────
    st.markdown("---")
    _, _dname, _ = current_user()
    _role_label = {"admin": "Admin", "reporting": "Reporting"}.get(_role, _role)
    st.markdown(
        f"<small style='color:#8898aa'>👤 {_dname or _username}"
        f"<br><span style='color:#607080'>{_role_label}</span></small>",
        unsafe_allow_html=True,
    )
    if st.button("🚪 Déconnexion"):
        logout()
    st.markdown(
        "<small style='color:#607080'>Infoscience · EPFL Library</small>",
        unsafe_allow_html=True,
    )

db = get_db()

# ── Bannière globale : run en cours ───────────────────────────────────────────
_active = read_active_run()
if _active:
    _run_env = _active.get("env", "?")
    st.warning(
        f"⏳ **Run en cours** [{_run_env.upper()}] — `{_active['run_id']}` "
        f"(sources : {_active['sources']}, démarré : {_active['started_at'][:19].replace('T',' ')})  "
        f"→ Allez sur **🚀 Lancer un run** pour suivre la progression.",
        icon=None,
    )

# ==============================================================================
# PAGE 1 — TABLEAU DE BORD
# ==============================================================================
if page == "🏠 Tableau de bord":
    st.markdown("# 🏠 Tableau de bord")

    # ── Run filter (scope all charts below) ────────────────────────────────
    _all_runs = db.get_runs(limit=200)
    _run_opts = ["Tous les runs"] + (
        _all_runs["run_id"].tolist() if not _all_runs.empty else []
    )
    _dash_run = st.selectbox(
        "Périmètre", _run_opts,
        help="Filtrer toutes les visualisations sur un run spécifique.",
        key="dash_run_filter",
    )
    _dash_run_id = None if _dash_run == "Tous les runs" else _dash_run

    # ── Top metrics ────────────────────────────────────────────────────────
    summary = db.get_summary_stats()
    runs_df = db.get_runs(limit=5)

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.markdown(metric_card("Runs", summary["total_runs"]), unsafe_allow_html=True)
    with col2:
        st.markdown(
            metric_card("Importées", summary["total_imported"]), unsafe_allow_html=True
        )
    with col3:
        st.markdown(
            metric_card("Dédoublonnées", summary["total_deduped"]),
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            metric_card("Rejetées", summary["total_rejected"]), unsafe_allow_html=True
        )
    with col5:
        st.markdown(
            metric_card("Auteurs EPFL", summary["total_authors"]),
            unsafe_allow_html=True,
        )
    with col6:
        st.markdown(
            metric_card("Unités", summary["total_units"]), unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Trend chart ────────────────────────────────────────────────────────
    st.markdown(
        '<div class="section-header">Tendance 30 jours</div>', unsafe_allow_html=True
    )
    trend_df = db.get_trend(days=30)
    if not trend_df.empty:
        fig = px.bar(
            trend_df,
            x="day",
            y="count",
            color="status",
            color_discrete_map={"imported": EPFL_TEAL, "rejected": "#e74c3c"},
            barmode="stack",
            labels={"day": "Date", "count": "Publications", "status": "Statut"},
            height=280,
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, width="stretch")
    else:
        st.info(
            "Aucune donnée disponible. Lancez un premier run pour voir les tendances."
        )

    # ── Recent runs ────────────────────────────────────────────────────────
    st.markdown(
        '<div class="section-header">Runs récents</div>', unsafe_allow_html=True
    )
    if not runs_df.empty:
        display = runs_df.copy()
        display["status"] = display["status"].apply(lambda s: badge(s))
        display["dry_run"] = display["dry_run"].apply(lambda v: "✓" if v else "")
        display["duration"] = display["duration_s"].apply(
            lambda s: f"{int(s)//60}m {int(s)%60}s" if pd.notna(s) and s else "—"
        )
        st.write(
            display[
                ["run_id", "started_at", "duration", "sources", "status", "dry_run"]
            ]
            .rename(
                columns={
                    "run_id": "Run",
                    "started_at": "Démarré",
                    "duration": "Durée",
                    "sources": "Sources",
                    "status": "Statut",
                    "dry_run": "Dry-run",
                }
            )
            .to_html(escape=False, index=False),
            unsafe_allow_html=True,
        )
    else:
        st.info("Aucun run enregistré.")

    # ── Sources breakdown ──────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div class="section-header">Répartition par source (tous runs)</div>',
        unsafe_allow_html=True,
    )
    src_df = db.get_sources_breakdown()
    if not src_df.empty:
        src_df = src_df[src_df["source"] != "__total__"]
        fig2 = go.Figure(
            data=[
                go.Bar(
                    name="Collectés",
                    x=src_df["source"],
                    y=src_df["harvested"],
                    marker_color="#b0c4de",
                ),
                go.Bar(
                    name="Importés",
                    x=src_df["source"],
                    y=src_df["loaded"],
                    marker_color=EPFL_TEAL,
                ),
                go.Bar(
                    name="Rejetés",
                    x=src_df["source"],
                    y=src_df["rejected"],
                    marker_color="#e74c3c",
                ),
            ]
        )
        fig2.update_layout(
            barmode="group",
            height=260,
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="white",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig2, width="stretch")

    # ── Analyse des contenus importés ──────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div class="section-header">Analyse des contenus importés</div>',
        unsafe_allow_html=True,
    )

    _ca, _cb = st.columns(2)

    # — Par type de document —
    with _ca:
        st.markdown("**Par type de document**")
        _type_df = db.get_pubs_by_type(_dash_run_id)
        if not _type_df.empty:
            _type_df = _type_df[_type_df["type"].notna()].head(15)
            _fig_type = px.bar(
                _type_df, x="count", y="type", orientation="h",
                color_discrete_sequence=[CANARD],
                labels={"count": "Publications", "type": "Type"},
                height=320,
            )
            _fig_type.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(_fig_type, width="stretch")
        else:
            st.caption("Aucune donnée.")

    # — Statut Open Access —
    with _cb:
        st.markdown("**Statut Open Access**")
        _oa_df = db.get_pubs_by_oa_status(_dash_run_id)
        if not _oa_df.empty:
            _OA_COLORS = {
                "OA + PDF":    C_GREEN,
                "OA sans PDF": LEMAN,
                "OA non-libre": C_YELLOW,
                "Non-OA":      C_GRAY_600,
                "Non défini":  C_GRAY_100,
            }
            _fig_oa = px.pie(
                _oa_df, names="oa_category", values="count",
                color="oa_category",
                color_discrete_map=_OA_COLORS,
                hole=0.42, height=320,
            )
            _fig_oa.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.1),
                showlegend=True,
            )
            _fig_oa.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig_oa, width="stretch")
        else:
            st.caption("Aucune donnée.")

    # ── Publications par année & proportion PDF ─────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    _ya, _yb = st.columns(2)

    # — Par année de publication —
    with _ya:
        st.markdown("**Par année de publication**")
        _year_df = db.get_pubs_by_year(_dash_run_id)
        if not _year_df.empty:
            _fig_yr = px.bar(
                _year_df, x="year", y="count",
                color_discrete_sequence=[CANARD],
                labels={"year": "Année", "count": "Publications"},
                height=280,
            )
            _fig_yr.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
            )
            st.plotly_chart(_fig_yr, width="stretch")
        else:
            st.caption("Aucune donnée.")

    # — Proportion PDF récupéré —
    with _yb:
        st.markdown("**Proportion avec PDF récupéré**")
        _pdf = db.get_pdf_stats(_dash_run_id)
        if _pdf["total"] > 0:
            _pdf_data = pd.DataFrame({
                "label": ["PDF récupéré", "Sans PDF"],
                "count": [_pdf["with_pdf"], _pdf["total"] - _pdf["with_pdf"]],
            })
            _fig_pdf = px.pie(
                _pdf_data, names="label", values="count",
                color="label",
                color_discrete_map={
                    "PDF récupéré": C_GREEN,
                    "Sans PDF":     C_GRAY_100,
                },
                hole=0.42, height=280,
            )
            _fig_pdf.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.1),
            )
            _fig_pdf.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig_pdf, width="stretch")

            # Summary sous le donut
            pct = round(100 * _pdf["with_pdf"] / _pdf["total"]) if _pdf["total"] else 0
            st.caption(
                f"{_pdf['with_pdf']} PDF sur {_pdf['total']} publications importées ({pct} %)"
            )
        else:
            st.caption("Aucune donnée.")

    # ── Top unités et journaux ──────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    _ua, _ub = st.columns(2)

    # — Top unités —
    with _ua:
        st.markdown("**Top unités EPFL**")
        _unit_df = db.get_pubs_by_unit(_dash_run_id, limit=15)
        if not _unit_df.empty:
            _fig_unit = px.bar(
                _unit_df, x="count", y="acronym", orientation="h",
                color_discrete_sequence=[C_BLUE],
                labels={"count": "Publications", "acronym": "Unité"},
                height=360,
            )
            _fig_unit.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(_fig_unit, width="stretch")
        else:
            st.caption("Aucune donnée d'unité disponible.")

    # — Top journaux —
    with _ub:
        st.markdown("**Top journaux**")
        _jour_df = db.get_pubs_by_journal(_dash_run_id, limit=15)
        if not _jour_df.empty:
            _fig_jour = px.bar(
                _jour_df, x="count", y="journal", orientation="h",
                color_discrete_sequence=[CANARD],
                labels={"count": "Publications", "journal": "Journal"},
                height=360,
            )
            _fig_jour.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(_fig_jour, width="stretch")
        else:
            st.caption(
                "Aucun journal disponible. "
                "Les titres de journaux sont alimentés lors des prochains runs."
            )


# ==============================================================================
# PAGE 2 — LANCER UN RUN
# ==============================================================================
elif page == "🚀 Lancer un run":
    st.markdown("# 🚀 Lancer un run")

    active = read_active_run()

    # ── Vue "run en cours" ────────────────────────────────────────────────
    if active:
        log_file = Path(active.get("log_file", ""))
        run_id = active["run_id"]

        st.success(f"⏳ Run **{run_id}** en cours…")
        col1, col2 = st.columns([3, 1])
        with col1:
            st.caption(f"Commande : `{active.get('cmd', '')}`")
        with col2:
            if st.button("⛔ Arrêter le run", type="secondary"):
                if kill_active_run():
                    st.warning("Signal d'arrêt envoyé au processus.")
                    time.sleep(1)
                    st.cache_resource.clear()
                    st.rerun()

        st.markdown(
            '<div class="section-header">Logs en direct</div>', unsafe_allow_html=True
        )
        log_box = st.empty()
        info_box = st.empty()

        while True:
            current = read_active_run()
            if log_file.exists():
                lines = log_file.read_text(
                    encoding="utf-8", errors="replace"
                ).splitlines()
                tail = "\n".join(lines[-300:])
            else:
                tail = "(log non encore disponible…)"
            log_box.markdown(
                f'<div class="log-console">{_html.escape(tail)}</div>',
                unsafe_allow_html=True,
            )
            if current is None:
                info_box.empty()
                break
            info_box.caption("Actualisation dans 2 secondes…")
            time.sleep(2)

        st.cache_resource.clear()
        db2 = get_db()
        runs_df = db2.get_runs(limit=5)
        matching = (
            runs_df[runs_df["run_id"] == run_id] if not runs_df.empty else runs_df
        )
        if not matching.empty:
            status = matching.iloc[0]["status"]
            if status == "completed":
                st.success(
                    "✅ Run terminé avec succès. Consultez les pages Publications et Statistiques."
                )
                st.balloons()
            else:
                st.error(f"❌ Run terminé avec statut : {status}")
        else:
            st.info("Run terminé.")

    # ── Vue "formulaire de lancement" ─────────────────────────────────────
    else:
        st.markdown("Configure les paramètres et lance le pipeline.")

        # ── Fenêtre temporelle — hors formulaire ──────────────────────────
        # Les widgets dans st.form() ne déclenchent pas de rerun : le changement
        # de mode n'aurait aucun effet visible. Hors du formulaire, chaque
        # interaction relance le script et affiche immédiatement les bons contrôles.
        st.markdown(
            '<div class="section-header">Fenêtre temporelle</div>',
            unsafe_allow_html=True,
        )
        col1, col2, col3 = st.columns(3)
        with col1:
            window_mode = st.radio(
                "Mode", ["Fenêtre glissante", "Dates fixes"], horizontal=True
            )
        with col2:
            if window_mode == "Fenêtre glissante":
                window_days = st.number_input(
                    "Jours", min_value=1, max_value=365, value=15
                )
            else:
                start_date_input = st.date_input(
                    "Date de début", value=date.today() - timedelta(days=14)
                )
        with col3:
            if window_mode != "Fenêtre glissante":
                end_date_input = st.date_input("Date de fin", value=date.today())

        with st.form("run_form"):
            st.markdown(
                '<div class="section-header">Nom du run (optionnel)</div>',
                unsafe_allow_html=True,
            )
            run_name_input = st.text_input(
                "Nom",
                placeholder="ex : tests-scopus-janvier",
                help="Inclus dans l'identifiant du run pour faciliter l'identification. "
                     "Laissez vide pour utiliser uniquement la date.",
            )

            st.markdown(
                '<div class="section-header">Sources</div>', unsafe_allow_html=True
            )
            selected_sources = st.multiselect(
                "Sources à inclure",
                options=SOURCES,
                default=SOURCES,
                help="Laissez vide pour utiliser toutes les sources.",
            )

            st.markdown(
                '<div class="section-header">Requêtes (optionnel)</div>',
                unsafe_allow_html=True,
            )
            with st.expander("Personnaliser les requêtes par source", expanded=False):
                st.caption(
                    "Laissez vide pour utiliser les requêtes par défaut de `config.py`. "
                    "Seules les sources sélectionnées ci-dessus sont affichées."
                )
                query_fields: dict = {}
                active = selected_sources or SOURCES
                q_cols = st.columns(2)
                for i, src in enumerate(active):
                    with q_cols[i % 2]:
                        query_fields[src] = st.text_area(
                            src.upper(),
                            value="",
                            placeholder=default_queries.get(src, ""),
                            height=88,
                            key=f"query_{src}",
                        )

            st.markdown(
                '<div class="section-header">Identifiants auteurs (optionnel)</div>',
                unsafe_allow_html=True,
            )
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                scopus_ids = st.text_area(
                    "Scopus Author IDs",
                    placeholder="7004212771\n57201854951",
                    height=80,
                )
            with col_b:
                wos_ids = st.text_area(
                    "WoS ResearcherIDs", placeholder="A-1234-2010", height=80
                )
            with col_c:
                orcid_ids = st.text_area(
                    "ORCID iDs", placeholder="0000-0002-1825-0097", height=80
                )

            st.markdown(
                '<div class="section-header">Options</div>', unsafe_allow_html=True
            )
            col_o1, col_o2, col_o3 = st.columns(3)
            with col_o1:
                dry_run = st.checkbox("Dry-run (sans import DSpace)", value=False)
            with col_o2:
                no_email = st.checkbox("Désactiver l'envoi d'e-mail", value=True)
            with col_o3:
                verbose = st.checkbox("Verbose (-vv)", value=False)

            submitted = st.form_submit_button("▶ Lancer le pipeline", width="stretch")

        if submitted:
            run_id = _make_run_id(run_name_input)
            log_file = ROOT / "logs" / f"run_{run_id}.log"
            log_file.parent.mkdir(parents=True, exist_ok=True)

            cmd = [sys.executable, str(ROOT / "data_pipeline" / "main.py")]
            if window_mode == "Fenêtre glissante":
                cmd += ["--window-days", str(window_days)]
            else:
                cmd += [
                    "--start-date",
                    str(start_date_input),
                    "--end-date",
                    str(end_date_input),
                ]
            if selected_sources:
                cmd += ["--sources", ",".join(selected_sources)]
            cmd += ["--env", ACTIVE_ENV]
            cmd += ["--run-id", run_id]
            for src, qval in query_fields.items():
                if qval.strip():
                    cmd += [f"--query-{src}", qval.strip()]
            if scopus_ids.strip():
                cmd += ["--scopus-ids", scopus_ids.strip().replace("\n", ",")]
            if wos_ids.strip():
                cmd += ["--wos-ids", wos_ids.strip().replace("\n", ",")]
            if orcid_ids.strip():
                cmd += ["--orcid-ids", orcid_ids.strip().replace("\n", ",")]
            if dry_run:
                cmd.append("--dry-run")
            if no_email:
                cmd.append("--no-email")
            if verbose:
                cmd.append("-vv")

            st.code(" ".join(cmd), language="bash")

            import json as _json
            from datetime import datetime as _dt

            log_fh = open(log_file, "w", encoding="utf-8")

            # Tentative d'acquisition atomique du verrou
            acquired = try_acquire_run_lock(
                run_id=run_id,
                pid=0,
                sources=selected_sources or SOURCES,
                dry_run=dry_run,
                log_file=str(log_file),
                cmd=cmd,
            )
            if not acquired:
                log_fh.close()
                log_file.unlink(missing_ok=True)
                st.error(
                    "⛔ Un run est déjà en cours (lancé par un autre utilisateur). "
                    "Attendez sa fin avant d'en démarrer un nouveau."
                )
                st.stop()

            # Verrou acquis — démarrer le subprocess puis mettre à jour le PID réel
            proc = subprocess.Popen(
                cmd,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                cwd=str(ROOT),
                env={**os.environ},
            )
            get_state_file().write_text(
                _json.dumps(
                    {
                        "run_id":     run_id,
                        "pid":        proc.pid,
                        "env":        ACTIVE_ENV,
                        "started_at": _dt.now().isoformat(),
                        "sources":    selected_sources or SOURCES,
                        "dry_run":    dry_run,
                        "log_file":   str(log_file),
                        "cmd":        " ".join(cmd),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            log_fh.close()
            st.rerun()
# ==============================================================================
# PAGE 3 — PROGRAMMATION
# ==============================================================================
elif page == "⏰ Programmation":
    import uuid as _uuid
    import json as _json_sched
    import tempfile as _tmp
    from apscheduler.triggers.cron import CronTrigger as _CT
    from datetime import timezone as _tz

    st.markdown("# ⏰ Programmation des runs")

    _SCHED_FILE = ROOT / "data" / "schedules.json"

    def _load_sched() -> list[dict]:
        if not _SCHED_FILE.exists():
            return []
        try:
            return _json_sched.loads(_SCHED_FILE.read_text(encoding="utf-8")).get("schedules", [])
        except Exception:
            return []

    def _save_sched(schedules: list[dict]) -> None:
        _SCHED_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = _json_sched.dumps({"schedules": schedules}, indent=2, ensure_ascii=False)
        _t = Path(
            _tmp.mktemp(dir=_SCHED_FILE.parent, suffix=".tmp")
        )
        _t.write_text(payload, encoding="utf-8")
        _t.replace(_SCHED_FILE)

    def _next_run_str(cron_expr: str) -> str:
        try:
            trig = _CT.from_crontab(cron_expr.strip(), timezone="Europe/Zurich")
            nxt  = trig.get_next_fire_time(None, datetime.now(_tz.utc))
            return nxt.strftime("%Y-%m-%d %H:%M") if nxt else "—"
        except Exception:
            return "⚠ expression invalide"

    _CRON_PRESETS: dict[str, str] = {
        "Quotidien à 06:00":          "0 6 * * *",
        "Quotidien à 22:00":          "0 22 * * *",
        "Hebdomadaire (lun. 06:00)":  "0 6 * * 1",
        "Bi-hebdomadaire (lun.+jeu.)":"0 6 * * 1,4",
        "Toutes les 6 heures":        "0 */6 * * *",
        "Mensuel (1er du mois 06:00)":"0 6 1 * *",
        "Personnalisé…":              "",
    }
    _STATUS_ICON = {
        "completed": "✅", "running": "⏳", "failed": "❌", None: "—",
    }

    _schedules = _load_sched()

    # ── Scheduler process status ──────────────────────────────────────────
    _sched_log = ROOT / "logs" / "scheduler.log"
    _is_sched_alive = False
    try:
        import psutil as _ps
        for _proc in _ps.process_iter(["pid", "cmdline"]):
            if _proc.info["cmdline"] and "scheduler.py" in " ".join(_proc.info["cmdline"]):
                _is_sched_alive = True
                break
    except ImportError:
        pass  # psutil optional — skip liveness check

    if _is_sched_alive:
        st.success("🟢 Scheduler en cours d'exécution", icon=None)
    else:
        st.info(
            "⚪ Scheduler non détecté. Lancez l'UI via `./run_ui.sh` pour activer "
            "l'exécution automatique des schedules.",
            icon=None,
        )

    # ── Existing schedules ────────────────────────────────────────────────
    if _schedules:
        st.markdown(
            '<div class="section-header">Schedules configurés</div>',
            unsafe_allow_html=True,
        )
        for _s in _schedules:
            _sid = _s["id"]
            _env_badge_style = {
                "dev":  "background:#dff0c8;color:#3a5a10",
                "test": "background:#fdefd5;color:#7a4400",
                "prod": "background:#ffd5d5;color:#7a0000",
            }.get(_s.get("env", "dev"), "")
            _last_status = _s.get("last_run_status")
            _last_icon   = _STATUS_ICON.get(_last_status, "—")

            with st.container(border=True):
                _ca, _cb, _cc, _cd = st.columns([4, 3, 3, 2])
                with _ca:
                    st.markdown(
                        f"**{_s.get('name', _sid)}**  "
                        f"<span style='{_env_badge_style};border-radius:4px;"
                        f"padding:1px 7px;font-size:.78rem;font-weight:700'>"
                        f"{_s.get('env','dev').upper()}</span>",
                        unsafe_allow_html=True,
                    )
                    st.caption(
                        f"Sources : {', '.join(_s.get('sources') or [])}  |  "
                        f"Fenêtre : {_s.get('window_days',15)} j  |  "
                        f"{'Dry-run  |  ' if _s.get('dry_run') else ''}"
                        f"Cron : `{_s.get('cron','')}`"
                    )
                with _cb:
                    st.markdown(
                        f"**Prochain run**  \n{_next_run_str(_s.get('cron',''))}"
                    )
                with _cc:
                    _last_at = (_s.get("last_run_at") or "—")[:16].replace("T", " ")
                    st.markdown(
                        f"**Dernier run**  \n{_last_icon} {_last_at}"
                        + (f"  \n`{_s.get('last_run_id','')}`" if _s.get("last_run_id") else "")
                    )
                with _cd:
                    _new_enabled = st.toggle(
                        "Actif", value=bool(_s.get("enabled")),
                        key=f"tog_{_sid}",
                    )
                    if _new_enabled != bool(_s.get("enabled")):
                        for _x in _schedules:
                            if _x["id"] == _sid:
                                _x["enabled"] = _new_enabled
                        _save_sched(_schedules)
                        st.rerun()

                    _btn1, _btn2 = st.columns(2)
                    with _btn1:
                        if st.button("▶ Now", key=f"run_{_sid}",
                                     help="Lance ce run immédiatement",
                                     use_container_width=True):
                            _now_id = _make_run_id(_s.get("name", ""))
                            _now_log = ROOT / "logs" / f"run_{_now_id}.log"
                            _now_log.parent.mkdir(parents=True, exist_ok=True)
                            _now_cmd = [sys.executable,
                                        str(ROOT / "data_pipeline" / "main.py"),
                                        "--window-days", str(_s.get("window_days", 15)),
                                        "--env", _s.get("env", "dev"),
                                        "--run-id", _now_id]
                            if _s.get("sources"):
                                _now_cmd += ["--sources", ",".join(_s["sources"])]
                            if _s.get("dry_run"):
                                _now_cmd.append("--dry-run")
                            if _s.get("no_email", True):
                                _now_cmd.append("--no-email")
                            _acquired = try_acquire_run_lock(
                                run_id=_now_id, pid=0,
                                sources=_s.get("sources") or SOURCES,
                                dry_run=bool(_s.get("dry_run")),
                                log_file=str(_now_log), cmd=_now_cmd,
                            )
                            if not _acquired:
                                st.error("⛔ Un run est déjà en cours.")
                            else:
                                _p = subprocess.Popen(
                                    _now_cmd,
                                    stdout=open(_now_log, "w"),
                                    stderr=subprocess.STDOUT,
                                    cwd=str(ROOT),
                                    env={**os.environ, "APP_ENV": _s.get("env","dev")},
                                )
                                get_state_file().write_text(
                                    _json_sched.dumps({
                                        "run_id": _now_id, "pid": _p.pid,
                                        "env": _s.get("env","dev"),
                                        "started_at": datetime.now().isoformat(),
                                        "sources": _s.get("sources") or SOURCES,
                                        "dry_run": bool(_s.get("dry_run")),
                                        "log_file": str(_now_log),
                                        "cmd": " ".join(_now_cmd),
                                    }, indent=2), encoding="utf-8",
                                )
                                st.success(f"Run `{_now_id}` démarré.")
                                time.sleep(0.5)
                                st.rerun()

                    with _btn2:
                        if st.button("🗑", key=f"del_{_sid}",
                                     help="Supprimer ce schedule",
                                     use_container_width=True):
                            _save_sched([x for x in _schedules if x["id"] != _sid])
                            st.rerun()
    else:
        st.info("Aucun schedule configuré. Créez-en un ci-dessous.")

    # ── Add new schedule ──────────────────────────────────────────────────
    # The frequency preset selector lives OUTSIDE the form so that selecting
    # a different preset triggers a rerun and updates the cron text input.
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("➕ Nouveau schedule", expanded=not _schedules):
        # ── Preset selector (outside form → triggers rerun on change) ────
        _preset_choice = st.selectbox(
            "Fréquence",
            list(_CRON_PRESETS.keys()),
            key="sched_preset",
            help="Sélectionnez un preset ou choisissez 'Personnalisé…' pour "
                 "saisir une expression cron manuelle.",
        )
        _cron_default = _CRON_PRESETS[_preset_choice]

        # Show a live preview of the next execution
        if _cron_default:
            st.caption(f"Prochain déclenchement : **{_next_run_str(_cron_default)}**")

        with st.form("new_schedule_form", clear_on_submit=True):
            _fn1, _fn2 = st.columns(2)
            with _fn1:
                _new_name = st.text_input("Nom", placeholder="Daily main run")
            with _fn2:
                _new_env  = st.selectbox("Environnement",
                                         list(env_loader.ENVIRONMENTS),
                                         index=list(env_loader.ENVIRONMENTS).index(ACTIVE_ENV))

            _new_sources = st.multiselect(
                "Sources", SOURCES, default=["scopus", "crossref", "openalex"],
            )

            _new_window = st.number_input(
                "Fenêtre glissante (jours)", min_value=1, max_value=365, value=20
            )

            # Cron expression — pre-filled from the preset; editable for
            # custom schedules or minor tweaks (e.g. change hour only).
            _new_cron = st.text_input(
                "Expression cron",
                value=_cron_default,
                placeholder="0 6 * * *",
                help="Format : minute heure jour_mois mois jour_semaine  "
                     "(ex : 0 6 * * 1  = chaque lundi à 06:00)",
            )

            _fo1, _fo2 = st.columns(2)
            with _fo1:
                _new_dry   = st.checkbox("Dry-run (sans import DSpace)", value=False)
            with _fo2:
                _new_email = st.checkbox("Désactiver l'envoi d'e-mail", value=True)

            _submitted_sched = st.form_submit_button(
                "Créer le schedule", use_container_width=True
            )

        if _submitted_sched:
            _err = []
            if not _new_name.strip():
                _err.append("Le nom est obligatoire.")
            if not _new_cron.strip():
                _err.append("L'expression cron est obligatoire.")
            else:
                try:
                    _CT.from_crontab(_new_cron.strip())
                except Exception:
                    _err.append(f"Expression cron invalide : `{_new_cron}`")
            if _err:
                for _e in _err:
                    st.error(_e)
            else:
                _new_sched = {
                    "id":              str(_uuid.uuid4()),
                    "name":            _new_name.strip(),
                    "enabled":         True,
                    "sources":         _new_sources or SOURCES,
                    "window_days":     int(_new_window),
                    "cron":            _new_cron.strip(),
                    "env":             _new_env,
                    "dry_run":         _new_dry,
                    "no_email":        _new_email,
                    "created_by":      _username,
                    "created_at":      datetime.now().isoformat(),
                    "last_run_at":     None,
                    "last_run_id":     None,
                    "last_run_status": None,
                }
                _schedules.append(_new_sched)
                _save_sched(_schedules)
                st.success(
                    f"Schedule **{_new_name}** créé. Prochain run : "
                    f"{_next_run_str(_new_cron.strip())}"
                )
                time.sleep(0.5)
                st.rerun()

    # ── Scheduler log tail ────────────────────────────────────────────────
    if _sched_log.exists():
        with st.expander("📋 Logs du scheduler (50 dernières lignes)"):
            _log_lines = _sched_log.read_text(encoding="utf-8", errors="replace").splitlines()
            st.code("\n".join(_log_lines[-50:]), language=None)


# ==============================================================================
# PAGE 3 — PUBLICATIONS
# ==============================================================================
elif page == "📋 Publications":
    st.markdown("# 📋 Publications")

    db_r = get_db()

    # ── Filtres ──────────────────────────────────────────────────────────
    with st.expander("🔍 Filtres", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            runs_df = db_r.get_runs(limit=50)
            run_opts = ["Tous"] + (
                runs_df["run_id"].tolist() if not runs_df.empty else []
            )
            sel_run = st.selectbox("Run", run_opts)

            all_types = db_r.get_distinct_dc_types()
            sel_type = st.selectbox("Type de document", ["Tous"] + all_types)

        with c2:
            status_opts = [
                "Tous",
                "workflow",
                "workspace",
                "deduplicated",
                "rejected",
                "error",
            ]
            sel_status = st.selectbox("Statut", status_opts)

            all_sources = db_r.get_distinct_sources()
            sel_source = st.selectbox("Source", ["Toutes"] + all_sources)

        with c3:
            all_units = db_r.get_distinct_units()
            sel_unit = st.selectbox("Unité", ["Toutes"] + all_units)
            sciper_q = st.text_input(
                "SCIPER ou nom auteur EPFL", placeholder="123456 ou Dupont"
            )

        search_q = st.text_input("Recherche titre / DOI", placeholder="deep learning…")

        cf1, cf2, cf3, cf4 = st.columns(4)
        with cf1:
            sel_oa = st.selectbox(
                "Statut OA",
                ["Tous", "OA", "Non-OA", "Non-libre", "Non défini"],
                help="Filtre sur le statut Open Access (Unpaywall).",
            )
        with cf2:
            sel_pdf = st.selectbox(
                "PDF récupéré",
                ["Tous", "Avec PDF", "Sans PDF"],
                help="Filtre sur la présence d'un PDF en accès libre.",
            )
        with cf3:
            all_licences = db_r.get_distinct_licences()
            sel_licence = st.selectbox(
                "Licence",
                ["Toutes"] + all_licences,
                help="Filtre sur la licence Unpaywall (cc-by, elsevier-specific…).",
            )
        with cf4:
            sel_epfl = st.selectbox(
                "Statut auteurs EPFL",
                ["Tous", "⚠️ Statut faible", "✅ Statut fort"],
                help="Filtre sur le statut des auteurs EPFL reconciliés.\n"
                     "Faible : tous les auteurs sont hôtes, externes ou étudiants.\n"
                     "Fort : au moins un auteur permanent.",
            )

    # Résoudre sciper_q : si c'est un nom, chercher les scipers correspondants
    resolved_sciper = None
    if sciper_q.strip():
        if sciper_q.strip().isdigit():
            resolved_sciper = sciper_q.strip()
        else:
            matches = db_r.get_epfl_authors(name_search=sciper_q.strip(), limit=10)
            if not matches.empty:
                options = [
                    f"{r['sciper']} — {r['full_name']}" for _, r in matches.iterrows()
                ]
                chosen = st.selectbox("Auteur EPFL trouvé :", options)
                resolved_sciper = chosen.split(" — ")[0] if chosen else None
            else:
                st.caption("Aucun auteur EPFL trouvé pour cette recherche.")

    # ── Helper constants ──────────────────────────────────────────────────
    _WEAK_STATUSES = frozenset({"hôte", "hors epfl", "étudiant"})
    _WEAK_PERSONNEL_POSITIONS = frozenset({
        "academic guest", "consultant", "engineer", "external employee",
        "external student", "guest", "guest phd student", "lecturer",
        "postdoctoral researcher", "visiting professor",
    })
    _NON_OPEN_LICENSES = frozenset({"elsevier-specific", "publisher-specific-oa", "implied-oa"})

    def _is_weak(status, position):
        s = (status or "").strip().lower()
        p = (position or "").strip().lower()
        if not s or s in _WEAK_STATUSES:
            return True
        return s == "personnel" and (not p or p in _WEAK_PERSONNEL_POSITIONS)

    # ── Filter kwargs ─────────────────────────────────────────────────────
    _has_pdf_val = True if sel_pdf == "Avec PDF" else (False if sel_pdf == "Sans PDF" else None)
    _epfl_strength_val = (
        "weak"   if sel_epfl == "⚠️ Statut faible" else
        "strong" if sel_epfl == "✅ Statut fort"   else None
    )
    _filter_kwargs = dict(
        run_id=None if sel_run == "Tous" else sel_run,
        status=None if sel_status == "Tous" else sel_status,
        source=None if sel_source == "Toutes" else sel_source,
        dc_type=None if sel_type == "Tous" else sel_type,
        sciper=resolved_sciper or None,
        unit_acronym=None if sel_unit == "Toutes" else sel_unit,
        search=search_q.strip() or None,
        has_pdf=_has_pdf_val,
        oa_filter=None if sel_oa == "Tous" else sel_oa,
        licence=None if sel_licence == "Toutes" else sel_licence,
        epfl_strength=_epfl_strength_val,
    )

    _filter_sig = str(sorted(_filter_kwargs.items()))
    if "pub_page" not in st.session_state:
        st.session_state["pub_page"] = 1
    if st.session_state.get("_pub_filter_sig") != _filter_sig:
        st.session_state["_pub_filter_sig"] = _filter_sig
        st.session_state["pub_page"] = 1

    # ── Enrichment helpers ────────────────────────────────────────────────
    # Dicts keyed by (run_id, row_id) — works correctly across multiple runs.

    def _build_authors_dict(authors_df):
        out_authors, out_weak = {}, {}
        if authors_df.empty:
            return out_authors, out_weak
        for key_vals, grp in authors_df.groupby(["run_id", "row_id"]):
            key = tuple(key_vals)
            parts, is_all_weak = [], True
            for _, r in grp.iterrows():
                name   = r.get("full_name") or r.get("sciper") or "?"
                st_val = str(r.get("epfl_status") or "").strip() if pd.notna(r.get("epfl_status")) else ""
                pos    = str(r.get("epfl_position") or "").strip() if pd.notna(r.get("epfl_position")) else ""
                hint   = " / ".join(x for x in [st_val, pos] if x)
                # ✓ prefix = reconciled with SCIPER
                parts.append("✓ " + name + (f" ({hint})" if hint else ""))
                if not _is_weak(st_val, pos):
                    is_all_weak = False
            out_authors[key] = "; ".join(parts)
            out_weak[key]    = is_all_weak and len(parts) > 0
        return out_authors, out_weak

    def _build_units_dict(units_df):
        out = {}
        if units_df.empty:
            return out
        for key_vals, grp in units_df.groupby(["run_id", "row_id"]):
            key = tuple(key_vals)
            parts = []
            for _, r in grp.iterrows():
                a = r.get("acronym") or ""
                t = r.get("unit_type") or ""
                parts.append(a + (f" ({t})" if t else ""))
            out[key] = ", ".join(parts)
        return out

    def _build_detected_dict(det_df):
        out = {}
        if det_df.empty:
            return out
        for key_vals, grp in det_df.groupby(["run_id", "row_id"]):
            out[tuple(key_vals)] = "; ".join(grp["author_name"].dropna().tolist())
        return out

    # ── Count + pagination ────────────────────────────────────────────────
    import math as _math

    _total = db_r.count_publications(**_filter_kwargs)

    _pc1, _pc2, _pc3 = st.columns([2, 2, 6])
    with _pc1:
        _page_size = st.selectbox(
            "Lignes / page", [25, 50, 100, 200], index=1, key="pub_page_size"
        )
    _total_pages = max(1, _math.ceil(_total / _page_size))
    with _pc2:
        _page = st.number_input(
            f"Page (/{_total_pages})", min_value=1, max_value=_total_pages,
            key="pub_page", step=1,
        )
    with _pc3:
        st.markdown(
            f"<div style='padding-top:28px;color:{C_GRAY_600};font-size:0.88rem'>"
            f"<b>{_total}</b> publication(s) — page {_page}/{_total_pages}</div>",
            unsafe_allow_html=True,
        )

    _offset = (_page - 1) * _page_size
    pub_df = db_r.get_publications(**_filter_kwargs, limit=_page_size, offset=_offset)

    # ── Enrichment fetch (always post-pagination) ─────────────────────────
    _run_authors: dict = {}
    _run_units:   dict = {}
    _run_weak:    dict = {}
    _run_detected: dict = {}
    _has_enrichment = False

    if not pub_df.empty:
        if sel_run != "Tous":
            # Fetch entire run enrichment — efficient single query per table.
            _a_df = db_r.get_pub_authors_for_run(sel_run)
            if not _a_df.empty:
                _a_df.insert(0, "run_id", sel_run)
            _run_authors, _run_weak = _build_authors_dict(_a_df)

            _u_df = db_r.get_pub_units_for_run(sel_run)
            if not _u_df.empty:
                _u_df.insert(0, "run_id", sel_run)
            _run_units = _build_units_dict(_u_df)

            _d_df = db_r.get_detected_authors_for_run(sel_run)
            if not _d_df.empty:
                _d_df.insert(0, "run_id", sel_run)
            _run_detected = _build_detected_dict(_d_df)
        else:
            # "Tous les runs": fetch enrichment only for the current page's rows.
            _pairs = list(zip(pub_df["run_id"], pub_df["row_id"]))
            _run_authors, _run_weak = _build_authors_dict(
                db_r.get_pub_authors_for_rows(_pairs))
            _run_units = _build_units_dict(
                db_r.get_pub_units_for_rows(_pairs))
            _run_detected = _build_detected_dict(
                db_r.get_detected_authors_for_rows(_pairs))
        _has_enrichment = True

    # ── Quick metrics ─────────────────────────────────────────────────────
    STATUS_LABELS = {
        "workflow": "En workflow", "workspace": "En workspace",
        "deduplicated": "Dédoublonnées", "rejected": "Rejetées", "error": "Erreurs",
    }
    _m = {s: db_r.count_publications(**{**_filter_kwargs, "status": s})
          for s in STATUS_LABELS}
    m_cols = st.columns(5)
    for col, (stat, label) in zip(m_cols, STATUS_LABELS.items()):
        col.metric(label, _m[stat])

    # ── Build display DataFrame ───────────────────────────────────────────
    if not pub_df.empty:
        ds_base = os.getenv("DS_API_ENDPOINT", "").replace("/server/api", "")
        d = pub_df.copy()

        # OA — plain text for datatable (no HTML)
        def _oa_text(r):
            is_oa = None  if pd.isna(r.get("upw_is_oa"))    else bool(r.get("upw_is_oa"))
            _lic  = str(r.get("upw_license") or "").lower().strip()
            if is_oa is None:   return "—"
            if not is_oa:       return "Non-OA"
            if _lic in _NON_OPEN_LICENSES: return "Non-libre"
            return "OA"

        def _lic_text(lic):
            l = str(lic or "").lower().strip()
            if l.startswith("cc-"):              return l.upper()
            if l in ("public-domain", "pd"):     return "Public Domain"
            return ""

        d["OA"]      = d.apply(_oa_text, axis=1)
        d["Licence"] = d["upw_license"].apply(_lic_text)
        d["PDF"]     = d["upw_valid_pdf"].apply(
            lambda v: False if pd.isna(v) else bool(v)
        )

        # Links — LinkColumn needs the full URL as cell value
        d["doi_url"] = d["doi"].apply(
            lambda x: f"https://doi.org/{x}" if pd.notna(x) and str(x).startswith("10.") else None
        )
        d["ws_url"] = d["workspace_id"].apply(
            lambda w: f"{ds_base}/workspaceitems/{int(float(w))}/edit"
                      if pd.notna(w) and w != "" else None
        )
        d["wf_url"] = d["workflow_id"].apply(
            lambda w: f"{ds_base}/workflowitems/{int(float(w))}/edit"
                      if pd.notna(w) and w != "" else None
        )

        # Enrichment columns — keyed by (run_id, row_id) in all modes
        if _has_enrichment:
            def _epfl_authors_cell(r):
                key = (r["run_id"], r["row_id"])
                # ~ = detected (EPFL affiliation found, no SCIPER match)
                # ✓ = reconciled (matched to SCIPER via EPFL People API / DSpace)
                raw_detected = _run_detected.get(key, "")
                unreconciled = "; ".join(
                    f"~ {n}" for n in raw_detected.split("; ") if n
                ) if raw_detected else ""
                reconciled = _run_authors.get(key, "")
                parts = [p for p in (unreconciled, reconciled) if p]
                return "; ".join(parts)

            d["Auteurs EPFL"] = d.apply(_epfl_authors_cell, axis=1)
            d["Unités"] = d.apply(lambda r: _run_units.get((r["run_id"], r["row_id"]), ""), axis=1)
            d["⚠️"]     = d.apply(lambda r: _run_weak.get((r["run_id"], r["row_id"]), False), axis=1)
        else:
            d["Auteurs EPFL"] = ""
            d["Unités"]       = ""
            d["⚠️"]           = False

        # Select and order columns
        _cols = (
            (["run_id"] if sel_run == "Tous" else [])
            + ["pub_year", "title", "source", "dc_type", "status",
               "OA", "Licence", "PDF", "⚠️",
               "Auteurs EPFL", "Unités",
               "doi_url", "ws_url", "wf_url", "error_msg"]
        )
        _cols = [c for c in _cols if c in d.columns]

        st.dataframe(
            d[_cols],
            width="stretch",
            hide_index=True,
            column_config={
                "run_id":      st.column_config.TextColumn("Run",     width="small"),
                "pub_year":    st.column_config.TextColumn("Année",   width="small"),
                "title":       st.column_config.TextColumn("Titre",   width="large"),
                "source":      st.column_config.TextColumn("Source",  width="small"),
                "dc_type":     st.column_config.TextColumn("Type",    width="medium"),
                "status":      st.column_config.TextColumn("Statut",  width="small"),
                "OA":          st.column_config.TextColumn("OA",      width="small"),
                "Licence":     st.column_config.TextColumn("Licence", width="medium"),
                "PDF":         st.column_config.CheckboxColumn("PDF ✓", width="small"),
                "⚠️":          st.column_config.CheckboxColumn("⚠️ Statut faible", width="small"),
                "Auteurs EPFL": st.column_config.TextColumn("Auteurs EPFL", width="large"),
                "Unités":      st.column_config.TextColumn("Unités",  width="medium"),
                "doi_url":     st.column_config.LinkColumn("DOI",       width="medium",
                                   display_text=r"https://doi\.org/(.+)"),
                "ws_url":      st.column_config.LinkColumn("Workspace", width="small",
                                   display_text=r".*/workspaceitems/(\d+)/edit"),
                "wf_url":      st.column_config.LinkColumn("Workflow",  width="small",
                                   display_text=r".*/workflowitems/(\d+)/edit"),
                "error_msg":   st.column_config.TextColumn("Erreur",  width="medium"),
            },
        )

        # ── Downloads ─────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        _dl_cols = st.columns(3)

        with _dl_cols[0]:
            # CSV export uses all matching rows, not just current page
            _full_df = db_r.get_publications(**_filter_kwargs, limit=10_000, offset=0)
            st.download_button(
                "⬇ Publications CSV",
                data=_full_df.to_csv(index=False).encode("utf-8"),
                file_name=f"publications_{sel_run}_{date.today()}.csv",
                mime="text/csv",
            )

        with _dl_cols[1]:
            if sel_run != "Tous":
                ax_df = db_r.get_pub_authors_for_run(sel_run)
                if not ax_df.empty:
                    st.download_button(
                        "⬇ Publications × Auteurs CSV",
                        data=ax_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"pub_authors_{sel_run}_{date.today()}.csv",
                        mime="text/csv",
                    )

        with _dl_cols[2]:
            if sel_run != "Tous":
                run_dir = ROOT / "data" / sel_run
                _reports = list(run_dir.glob("*Report*.xlsx")) if run_dir.exists() else []
                if _reports:
                    with open(_reports[0], "rb") as f:
                        st.download_button(
                            "⬇ Rapport Excel du run",
                            data=f.read(),
                            file_name=_reports[0].name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                else:
                    st.caption("Aucun rapport Excel disponible.")

    else:
        st.info("Aucune publication correspondant aux filtres.")


# ==============================================================================
# PAGE 4 — STATISTIQUES
# ==============================================================================
elif page == "📊 Statistiques":
    st.markdown("# 📊 Statistiques")

    db_r = get_db()
    runs_df = db_r.get_runs(limit=50)
    if runs_df.empty:
        st.info(
            "Aucun run enregistré. Lancez un premier run pour voir les statistiques."
        )
        st.stop()

    # ── Run selector shared across all tabs ───────────────────────────────
    _run_opts_stat = ["Tous les runs"] + runs_df["run_id"].tolist()
    _sel_stat_run = st.selectbox(
        "Périmètre", _run_opts_stat, key="stat_run_filter",
        help="Filtre appliqué à tous les onglets.",
    )
    _stat_run_id = None if _sel_stat_run == "Tous les runs" else _sel_stat_run

    tab_run, tab_authors, tab_units = st.tabs(
        ["📈 Par run", "👤 Auteurs EPFL", "🏛 Unités"]
    )

    # ── Tab 1 : statistiques par run ──────────────────────────────────────
    with tab_run:
        stats_df = db_r.get_run_stats(_stat_run_id) if _stat_run_id else pd.DataFrame()
        pub_df   = db_r.get_publications(run_id=_stat_run_id, limit=5000)

        # ── Row 1 : source funnel + publication status ────────────────────
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(
                '<div class="section-header">Entonnoir par source</div>',
                unsafe_allow_html=True,
            )
            src = (
                stats_df[stats_df["source"] != "__total__"]
                if not stats_df.empty else stats_df
            )
            if not src.empty:
                fig = go.Figure()
                for col_n, color, label in [
                    ("harvested", "#b0c4de", "Collectés"),
                    ("loaded",    EPFL_TEAL,  "Importés"),
                    ("rejected",  "#e74c3c",   "Rejetés"),
                ]:
                    fig.add_trace(go.Bar(
                        name=label, x=src["source"], y=src[col_n],
                        marker_color=color,
                    ))
                fig.update_layout(
                    barmode="group", height=280,
                    margin=dict(l=0, r=0, t=10, b=0), plot_bgcolor="white",
                )
                st.plotly_chart(fig, width="stretch")
            else:
                st.caption("Sélectionnez un run spécifique ou lancez un run.")

        with c2:
            st.markdown(
                '<div class="section-header">Statut des publications</div>',
                unsafe_allow_html=True,
            )
            if not pub_df.empty:
                _sc = pub_df["status"].value_counts().reset_index()
                _sc.columns = ["status", "count"]
                fig2 = px.pie(
                    _sc, names="status", values="count", color="status",
                    color_discrete_map={
                        "workflow":    EPFL_TEAL,  "workspace":   "#f39c12",
                        "deduplicated":"#3498db",  "rejected":    "#e74c3c",
                        "error":       "#95a5a6",
                    },
                    hole=0.42, height=280,
                )
                fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0))
                fig2.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig2, width="stretch")
            else:
                st.caption("Aucune donnée.")

        # ── Row 2 : document types + OA status ───────────────────────────
        c3, c4 = st.columns(2)
        with c3:
            st.markdown(
                '<div class="section-header">Types de documents importés</div>',
                unsafe_allow_html=True,
            )
            _type_df = db_r.get_pubs_by_type(_stat_run_id)
            if not _type_df.empty:
                _type_df = _type_df[_type_df["type"].notna()].head(15)
                fig3 = px.bar(
                    _type_df, x="count", y="type", orientation="h",
                    color_discrete_sequence=[EPFL_TEAL],
                    labels={"count": "Publications", "type": "Type"},
                    height=320,
                )
                fig3.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig3, width="stretch")
            else:
                st.caption("Aucune donnée.")

        with c4:
            st.markdown(
                '<div class="section-header">Statut Open Access</div>',
                unsafe_allow_html=True,
            )
            _oa_df = db_r.get_pubs_by_oa_status(_stat_run_id)
            if not _oa_df.empty:
                _OA_COLORS = {
                    "OA + PDF":     C_GREEN,  "OA sans PDF": LEMAN,
                    "OA non-libre": C_YELLOW, "Non-OA":      C_GRAY_600,
                    "Non défini":   C_GRAY_100,
                }
                fig4 = px.pie(
                    _oa_df, names="oa_category", values="count",
                    color="oa_category", color_discrete_map=_OA_COLORS,
                    hole=0.42, height=320,
                )
                fig4.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0),
                    legend=dict(orientation="h", yanchor="top", y=-0.1),
                )
                fig4.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig4, width="stretch")
            else:
                st.caption("Aucune donnée.")

        # ── Row 3 : year distribution + PDF proportion ────────────────────
        c5, c6 = st.columns(2)
        with c5:
            st.markdown(
                '<div class="section-header">Par année de publication</div>',
                unsafe_allow_html=True,
            )
            _year_df = db_r.get_pubs_by_year(_stat_run_id)
            if not _year_df.empty:
                fig5 = px.bar(
                    _year_df, x="year", y="count",
                    color_discrete_sequence=[CANARD],
                    labels={"year": "Année", "count": "Publications"},
                    height=280,
                )
                fig5.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                )
                st.plotly_chart(fig5, width="stretch")
            else:
                st.caption("Aucune donnée.")

        with c6:
            st.markdown(
                '<div class="section-header">Proportion avec PDF récupéré</div>',
                unsafe_allow_html=True,
            )
            _pdf = db_r.get_pdf_stats(_stat_run_id)
            if _pdf["total"] > 0:
                _pdf_data = pd.DataFrame({
                    "label": ["PDF récupéré", "Sans PDF"],
                    "count": [_pdf["with_pdf"], _pdf["total"] - _pdf["with_pdf"]],
                })
                fig6 = px.pie(
                    _pdf_data, names="label", values="count", color="label",
                    color_discrete_map={"PDF récupéré": C_GREEN, "Sans PDF": C_GRAY_100},
                    hole=0.42, height=280,
                )
                fig6.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0),
                    legend=dict(orientation="h", yanchor="top", y=-0.1),
                )
                fig6.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig6, width="stretch")
                _pct = round(100 * _pdf["with_pdf"] / _pdf["total"]) if _pdf["total"] else 0
                st.caption(
                    f"{_pdf['with_pdf']} PDF sur {_pdf['total']} publications ({_pct} %)"
                )
            else:
                st.caption("Aucune donnée.")

        # ── Row 4 : top journals (full width) ────────────────────────────
        st.markdown(
            '<div class="section-header">Top journaux</div>', unsafe_allow_html=True
        )
        _jour_df = db_r.get_pubs_by_journal(_stat_run_id, limit=15)
        if not _jour_df.empty:
            fig7 = px.bar(
                _jour_df, x="count", y="journal", orientation="h",
                color_discrete_sequence=[CANARD],
                labels={"count": "Publications", "journal": "Journal"},
                height=360,
            )
            fig7.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig7, width="stretch")
        else:
            st.caption("Aucun journal disponible.")

        # ── Source stats table ────────────────────────────────────────────
        if not stats_df.empty:
            st.markdown(
                '<div class="section-header">Détail par source</div>',
                unsafe_allow_html=True,
            )
            st.dataframe(
                stats_df.rename(columns={
                    "source": "Source", "harvested": "Collectés",
                    "deduplicated": "Dédoublonnés", "loaded": "Importés",
                    "rejected": "Rejetés",
                }),
                width="stretch", hide_index=True,
            )

    # ── Tab 2 : auteurs EPFL ──────────────────────────────────────────────
    with tab_authors:
        # ── Top EPFL authors chart ────────────────────────────────────────
        st.markdown(
            '<div class="section-header">Top auteurs EPFL</div>',
            unsafe_allow_html=True,
        )
        _top_auth_df = db_r.get_top_epfl_authors(run_id=_stat_run_id, limit=20)
        if not _top_auth_df.empty:
            _fig_auth = px.bar(
                _top_auth_df, x="pub_count", y="full_name", orientation="h",
                color="main_unit",
                labels={
                    "pub_count": "Publications", "full_name": "Auteur",
                    "main_unit": "Unité",
                },
                height=max(320, len(_top_auth_df) * 28),
            )
            _fig_auth.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02,
                            xanchor="right", x=1),
            )
            st.plotly_chart(_fig_auth, width="stretch")
        else:
            st.caption("Aucun auteur EPFL réconcilié disponible.")

        # ── Author search + table ─────────────────────────────────────────
        st.markdown(
            '<div class="section-header">Auteurs EPFL réconciliés</div>',
            unsafe_allow_html=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            author_search = st.text_input("Rechercher par nom")
        with c2:
            all_units_list = db_r.get_distinct_units()
            filter_unit = st.selectbox(
                "Filtrer par unité", ["Toutes"] + all_units_list,
                key="author_unit_filter",
            )

        authors_df = db_r.get_epfl_authors(
            name_search=author_search.strip() or None,
            unit=filter_unit if filter_unit != "Toutes" else None,
            limit=500,
        )
        st.markdown(f"**{len(authors_df)} auteur(s)**")
        if not authors_df.empty:
            st.dataframe(
                authors_df.rename(columns={
                    "sciper": "SCIPER", "full_name": "Nom",
                    "first_name": "Prénom", "last_name": "Nom de famille",
                    "orcid": "ORCID", "epfl_orcid": "ORCID EPFL",
                    "scopus_id": "Scopus ID", "wos_id": "WoS ID",
                    "openalex_id": "OpenAlex ID", "epfl_status": "Statut",
                    "epfl_position": "Poste", "main_unit": "Unité",
                    "dspace_uuid": "UUID DSpace", "last_seen": "Vu le",
                }),
                width="stretch", hide_index=True,
            )
            st.download_button(
                "⬇ Auteurs CSV",
                data=authors_df.to_csv(index=False).encode("utf-8"),
                file_name=f"epfl_authors_{date.today()}.csv",
                mime="text/csv",
            )
        else:
            st.info(
                "Aucun auteur EPFL dans la base. Lancez un run pour alimenter la table."
            )

    # ── Tab 3 : unités ────────────────────────────────────────────────────
    with tab_units:
        st.markdown(
            '<div class="section-header">Unités EPFL</div>', unsafe_allow_html=True
        )
        units_df = db_r.get_units()
        st.markdown(f"**{len(units_df)} unité(s)**")
        if not units_df.empty:
            _fig_u = px.bar(
                units_df.head(20), x="pub_count", y="acronym", orientation="h",
                color_discrete_sequence=[EPFL_TEAL],
                labels={"pub_count": "Publications", "acronym": "Unité"},
                height=400,
            )
            _fig_u.update_layout(
                margin=dict(l=0, r=0, t=10, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(_fig_u, width="stretch")
            st.dataframe(
                units_df.rename(columns={
                    "acronym": "Acronyme", "name_fr": "Nom",
                    "unit_type": "Type", "epfl_unit_id": "ID EPFL",
                    "author_count": "Auteurs", "pub_count": "Publications",
                }),
                width="stretch", hide_index=True,
            )
        else:
            st.info("Aucune unité dans la base.")


# ==============================================================================
# PAGE 5 — CONFIGURATION
# ==============================================================================
elif page == "⚙️ Configuration":
    st.markdown("# ⚙️ Configuration")
    st.markdown("Variables d'environnement et état des connexions.")

    # ── Env vars status ────────────────────────────────────────────────────
    st.markdown(
        '<div class="section-header">Variables d\'environnement</div>',
        unsafe_allow_html=True,
    )

    env_vars = {
        "DS_API_ENDPOINT": ("DSpace REST API URL", True),
        "DS_API_TOKEN": ("DSpace REST API static token", True),
        "DS_ACCESS_TOKEN": ("DSpace session cookie token (alt. auth)", False),
        "API_EPFL_USER": ("EPFL People API user", False),
        "API_EPFL_PWD": ("EPFL People API password", False),
        "SCOPUS_API_KEY": ("Scopus API key", False),
        "SCOPUS_INST_TOKEN": ("Scopus Inst. token", False),
        "WOS_TOKEN": ("WoS API token", False),
        "EPO_OPS_KEY": ("EPO OPS key", False),
        "EPO_OPS_SECRET": ("EPO OPS secret", False),
        "OPENALEX_API_KEY": ("OpenAlex API key", False),
        "OPENALEX_DATA_VERSION": ("OpenAlex data version (default: 2)", False),
        "ZENODO_API_KEY": ("Zenodo API key", False),
        "ORCID_API_TOKEN": ("ORCID API token", False),
        "ELS_API_KEY": ("Elsevier API key (Unpaywall PDF)", False),
        "CONTACT_API_EMAIL": ("E-mail polite pool APIs", False),
        "USER_AGENT": ("HTTP User-Agent header", False),
        "RECIPIENT_EMAIL": ("E-mail rapport", False),
        "SENDER_EMAIL": ("E-mail expéditeur", False),
        "SMTP_SERVER": ("Serveur SMTP", False),
    }

    rows = []
    for var, (desc, required) in env_vars.items():
        val = os.getenv(var)
        set_icon = "✅" if val else ("🔴" if required else "⚪")
        masked = (
            ("*" * 8 + val[-4:]) if val and len(val) > 4 else ("***" if val else "—")
        )
        rows.append(
            {
                "Variable": var,
                "Description": desc,
                "Requis": "●" if required else "",
                "Valeur": masked,
                "État": set_icon,
            }
        )

    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ── DuckDB info ─────────────────────────────────────────────────────────
    st.markdown(
        '<div class="section-header">Base de données DuckDB</div>',
        unsafe_allow_html=True,
    )
    db_path = db.db_path
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Chemin", str(db_path))
    with col2:
        size_mb = db_path.stat().st_size / 1024 / 1024 if db_path.exists() else 0
        st.metric("Taille", f"{size_mb:.2f} MB")

    # ── Quick .env template ─────────────────────────────────────────────────
    st.markdown('<div class="section-header">Modèle .env</div>', unsafe_allow_html=True)
    st.code(
        """# Infoscience Import Pipeline — Variables d'environnement
# Copier ce fichier en .env à la racine du projet

# ── DSpace REST API (requis) ──────────────────────────────────────────────────
DS_API_ENDPOINT=https://<domain>/server/api
DS_API_TOKEN=<static_token>
# DS_ACCESS_TOKEN=<session_cookie_token>  # alternative auth après login

# ── EPFL People API ───────────────────────────────────────────────────────────
API_EPFL_USER=<username>
API_EPFL_PWD=<password>

# ── Scopus (Elsevier) ─────────────────────────────────────────────────────────
SCOPUS_API_KEY=<key>
SCOPUS_INST_TOKEN=<institutional_token>
ELS_API_KEY=<elsevier_key>  # PDF retrieval via Unpaywall

# ── Web of Science ────────────────────────────────────────────────────────────
WOS_TOKEN=<token>

# ── EPO Open Patent Services ──────────────────────────────────────────────────
EPO_OPS_KEY=<key>
EPO_OPS_SECRET=<secret>

# ── OpenAlex ──────────────────────────────────────────────────────────────────
OPENALEX_API_KEY=<key>
# OPENALEX_DATA_VERSION=2  # version de l'API OpenAlex (défaut : 2)

# ── Zenodo ────────────────────────────────────────────────────────────────────
ZENODO_API_KEY=<key>

# ── ORCID ─────────────────────────────────────────────────────────────────────
ORCID_API_TOKEN=<token>

# ── Polite pool (Crossref, Unpaywall, OpenAlex) ───────────────────────────────
CONTACT_API_EMAIL=<your_email>
# USER_AGENT=EPFL-Infoscience-imports/1.0 (mailto:<your_email>)

# ── Rapport e-mail (optionnel) ────────────────────────────────────────────────
RECIPIENT_EMAIL=<recipient>
SENDER_EMAIL=<sender>
SMTP_SERVER=<smtp_host>
""",
        language="bash",
    )
