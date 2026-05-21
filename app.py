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
from utils import make_run_id
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

# Design system: Datadog aesthetic (DESIGN.md)
CANARD      = "#632CA6"   # Datadog purple   — primary accent
LEMAN       = "#7F56D9"   # Datadog purple bright — secondary accent
C_GREEN     = "#16A34A"   # success
C_YELLOW    = "#D97706"   # warning
C_RED       = "#DC2626"   # danger
C_RED_DARK  = "#991B1B"   # danger dark
C_DARK      = "#101828"   # Datadog black    — dark surfaces
C_BLACK     = "#1D2939"   # Datadog ink      — body text
C_GRAY_600  = "#667085"   # Datadog muted    — secondary text
C_GRAY_100  = "#E4E7EC"   # Datadog border   — light backgrounds
C_BLUE      = "#3B82F6"   # info / deduplicated

# Aliases kept for existing references throughout the file
EPFL_TEAL = CANARD
EPFL_RED  = C_RED

STATUS_COLORS = {
    "imported":  C_GREEN,
    "rejected":  C_RED,
    "running":   C_YELLOW,
    "completed": CANARD,
    "failed":    C_RED_DARK,
    "killed":    C_RED_DARK,
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
# Step 2 — inject Google Fonts via <link> (more reliable than CSS @import in
#           Streamlit, which can be stripped or deferred unexpectedly).
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
# Step 3 — load the external stylesheet (pure CSS, no Python templating).
st.markdown(
    f"<style>{(ROOT / 'ui' / 'styles.css').read_text()}</style>",
    unsafe_allow_html=True,
)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_run_id(name: str = "") -> str:
    return make_run_id(name)


def mi(name: str, extra_class: str = "") -> str:
    """Return a Material Symbols Outlined icon span."""
    cls = f"ms {extra_class}".strip()
    return f'<span class="{cls}">{name}</span>'


def page_title(icon: str, label: str) -> None:
    """Render a page title with a Material Symbols icon."""
    st.markdown(
        f'<h1 class="page-title">{mi(icon)}{label}</h1>',
        unsafe_allow_html=True,
    )


def sh(icon: str, label: str) -> str:
    """Return a section-header div with a Material Symbols icon."""
    return f'<div class="section-header">{mi(icon)}{label}</div>'


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


_SOURCE_TAGS: dict = {
    "scopus":            ("pub-src--scopus",    "Scopus"),
    "wos":               ("pub-src--wos",       "WoS"),
    "crossref":          ("pub-src--crossref",  "Crossref"),
    "openalex+crossref": ("pub-src--openalex",  "OpenAlex"),
    "openalex":          ("pub-src--openalex",  "OpenAlex"),
    "zenodo":            ("pub-src--zenodo",    "Zenodo"),
    "epo":               ("pub-src--epo",       "EPO"),
    "datacite":          ("pub-src--datacite",  "DataCite"),
}
_WEAK_STATUSES_TABLE: frozenset = frozenset({
    "", "student", "phd student", "master student",
    "administrative staff", "technical staff",
    "extern", "alumni", "unknown",
})
_RAW_META_SECTIONS: list = [
    ("Identifiants",        ["doi", "pmid", "bookDOI", "internal_id"]),
    ("Titre & type",        ["title", "doctype", "dc.type", "pubyear", "issueDate"]),
    ("Revue / Article",     ["journalTitle", "journalISSN", "journalVolume", "issue",
                              "startingPage", "endingPage", "artno", "publisher", "publisherPlace"]),
    ("Livre / Série",       ["bookTitle", "seriesTitle", "bookISBN", "seriesISSN",
                              "bookDOI", "bookPart", "editors", "corporateAuthor", "seriesVolume"]),
    ("Mots-clés",           ["keywords"]),
    ("Résumé",              ["abstract"]),
    ("Open Access",         ["upw_is_oa", "upw_oa_status", "upw_license", "upw_version",
                              "upw_host", "upw_pdf_urls", "journal_is_oa", "journal_is_in_doaj"]),
    ("Conférence",          ["conference_info"]),
    ("Financement",         ["fundings_info"]),
    ("Collection DSpace",   ["ifs3_collection", "dc.type_authority"]),
]
_DB_META_SECTIONS: list = [
    ("Identifiants",   ["doi", "internal_id", "dspace_item_uuid"]),
    ("Bibliographique",["title", "dc_type", "pub_year", "source", "status"]),
    ("Open Access",    ["upw_is_oa", "upw_oa_status", "upw_license", "upw_valid_pdf"]),
    ("Pipeline",       ["run_id", "workspace_id", "workflow_id", "loaded_at"]),
    ("Compteurs",      ["seen_count", "infoscience_dedup_count"]),
]
_DEDUP_LABELS: dict = {
    "supersedes_preprint":         "Preprint existant",
    "cross_type_doi":              "DOI cross-type",
    "dataset_in_other_collection": "Dataset avec publication liée",
    "published_version_exists":    "Version publiée existante",
}


def _render_pub_html_table(
    d: "pd.DataFrame",
    cols: list,
) -> str:
    """Render publications as an HTML table. Modals handled separately via st.dialog."""
    import pandas as _pd

    def _e(v) -> str:
        if v is None or (isinstance(v, float) and _pd.isna(v)):
            return ""
        return _html.escape(str(v).strip())

    def _trunc(v, n=90) -> str:
        s = _e(v)
        full = _html.escape(str(v).strip()) if v else ""
        return f'<span title="{full}">{s[:n]}…</span>' if len(s) > n else s

    def _notnull(v) -> bool:
        return v is not None and not (isinstance(v, float) and _pd.isna(v)) and str(v).strip() != ""

    def _actions(row) -> str:
        parts = []
        for col, label, css in [
            ("item_url", "View",  "pub-action--view"),
            ("ws_url",   "Edit",  "pub-action--edit"),
            ("wf_url",   "Claim", "pub-action--claim"),
        ]:
            u = row.get(col)
            if _notnull(u):
                parts.append(f'<a href="{_e(u)}" target="_blank" class="pub-action {css}">{label}</a>')
        return '<div class="pub-actions">' + "".join(parts) + "</div>" if parts else '<span class="pub-dash">—</span>'

    def _src_tag(source) -> str:
        s = str(source or "").lower().strip()
        css, label = _SOURCE_TAGS.get(s, ("pub-src--default", source or "?"))
        return f'<span class="pub-src-tag {css}">{_html.escape(label)}</span>'

    def _type_tag(dc_type) -> str:
        if not _notnull(dc_type):
            return ""
        parts = str(dc_type).split("::")
        label = parts[-1].strip() if len(parts) > 1 else parts[0].strip()
        return f'<span class="pub-type-tag" title="{_e(dc_type)}">{_html.escape(label[:30])}</span>'

    rows_html = []
    for _, row in d.iterrows():
        run_td = f'<td class="pub-td pub-td--sm">{_e(row.get("run_id"))}</td>' if "run_id" in cols else ""
        st_raw = str(row.get("status", "") or "").lower()
        pdf_tag = '<span class="pub-pdf-tag">PDF</span>' if row.get("PDF") else ""
        is_weak = bool(row.get("⚠️"))
        warn_ic = ' <span class="pub-warn-ic" title="Statut EPFL faible">⚠️</span>' if is_weak else ""
        doi_u, src_u = row.get("doi_url"), row.get("src_url")
        lk = ""
        if _notnull(doi_u): lk += f'<a href="{_e(doi_u)}" target="_blank" class="pub-link">DOI</a> '
        if _notnull(src_u): lk += f'<a href="{_e(src_u)}" target="_blank" class="pub-link">src</a>'
        flag_note = _DEDUP_LABELS.get(str(row.get("dedup_note") or ""), "")
        flag_td = f'<td class="pub-td pub-td--flag"><span class="pub-flag-lbl">{_html.escape(flag_note)}</span></td>' if flag_note else '<td class="pub-td pub-td--flag"></td>'

        rows_html.append(f"""<tr>
<td class="pub-td pub-td--actions">{_actions(row)}</td>
{run_td}
<td class="pub-td pub-td--year">{_e(row.get("pub_year"))}</td>
<td class="pub-td pub-td--title">
  <div class="pub-title-tags">{_src_tag(row.get("source"))}{badge(st_raw) if st_raw else ""}{_type_tag(row.get("dc_type"))}</div>
  <span class="pub-title">{_trunc(row.get("title"), 110)}</span>
</td>
<td class="pub-td pub-td--oa">
  <span class="pub-oa-val">{_e(row.get("OA"))}</span>
  <span class="pub-lic-val">{_trunc(row.get("Licence"), 22)}</span>
  {pdf_tag}
</td>
<td class="pub-td pub-td--authors">
  <div class="pub-auth-top">{_trunc(row.get("Auteurs EPFL"), 65)}{warn_ic}</div>
  <span class="pub-units-val">{_e(row.get("Unités"))}</span>
</td>
<td class="pub-td pub-td--links">{lk}</td>
{flag_td}
</tr>""")

    run_th = '<th class="pub-th">Run</th>' if "run_id" in cols else ""
    return f"""<div class="pub-table-wrapper"><table class="pub-table">
<thead><tr>
  <th class="pub-th">Actions</th>{run_th}
  <th class="pub-th">Année</th>
  <th class="pub-th pub-th--wide">Titre</th>
  <th class="pub-th">OA</th>
  <th class="pub-th">Auteurs EPFL</th>
  <th class="pub-th">Liens</th>
  <th class="pub-th">🚩</th>
</tr></thead>
<tbody>{"".join(rows_html)}</tbody>
</table></div>"""


# ── Native Streamlit dialogs for per-publication details ──────────────────────

@st.dialog("📋 Métadonnées collectées", width="large")
def _pub_meta_dialog(row_data: dict, raw_meta: dict):
    import json as _j
    st.markdown(f"**{row_data.get('title', '')}**")
    st.caption(f"{row_data.get('source', '')} · {row_data.get('pub_year', '')}")
    st.divider()
    sections = _RAW_META_SECTIONS if raw_meta else _DB_META_SECTIONS
    source = raw_meta if raw_meta else row_data
    if not raw_meta:
        st.info("Métadonnées complètes disponibles à partir des prochains runs.")
    for sec_name, keys in sections:
        items = [(k, str(source[k])) for k in keys if k in source and source.get(k) and str(source[k]).strip() not in ("", "nan", "None")]
        if not items:
            continue
        st.markdown(f'<div class="pmm-section">{sec_name}</div>', unsafe_allow_html=True)
        for k, v in items:
            if len(v) > 200:
                st.text_area(k, v, height=110, key=f"_mta_{k}", disabled=True, label_visibility="visible")
            else:
                col1, col2 = st.columns([1, 3])
                col1.markdown(f"`{k}`")
                col2.markdown(v)


@st.dialog("👤 Auteurs EPFL", width="large")
def _pub_authors_dialog(title: str, authors: list):
    st.markdown(f"**{title}**")
    st.divider()
    if not authors:
        st.info("Aucun auteur EPFL réconcilié pour cette publication.")
        return
    for i, a in enumerate(authors):
        sciper = a.get("sciper", "")
        name   = a.get("name") or sciper or "?"
        weak   = a.get("weak", False)
        with st.container(border=True):
            hd_col, btn_col = st.columns([4, 1])
            with hd_col:
                if weak:
                    st.warning(f"⚠️ **{name}** — Statut faible")
                else:
                    st.markdown(f"**{name}**")
            with btn_col:
                if sciper:
                    st.link_button("EPFL People", f"https://people.epfl.ch/{sciper}", use_container_width=True)
            c1, c2, c3 = st.columns(3)
            c1.markdown(f"**Statut** {a.get('epfl_status') or '—'}")
            c2.markdown(f"**Position** {a.get('epfl_position') or '—'}")
            c3.markdown(f"**Unité** {a.get('main_unit') or '—'}")
            orcid = a.get("orcid", "")
            if orcid:
                st.markdown(f"**ORCID** [{orcid}](https://orcid.org/{orcid})")


@st.dialog("🚩 Doublon Infoscience", width="large")
def _pub_flagged_dialog(title: str, flagged_raw: str, dedup_note: str, ds_base: str):
    import json as _j
    st.markdown(f"**{title}**")
    label = _DEDUP_LABELS.get(dedup_note, dedup_note or "Signalé")
    st.error(f"**{label}**")
    st.divider()
    try:
        items = _j.loads(flagged_raw)
    except Exception:
        items = []
    if not isinstance(items, list):
        items = [items]
    for item in items:
        uuid = item.get("uuid", "")
        doi  = item.get("doi", "")
        dct  = item.get("dc_type", "")
        with st.container(border=True):
            if uuid:
                st.markdown(f"**UUID** [{uuid}]({ds_base}/items/{uuid})")
            if doi:
                st.markdown(f"**DOI** [{doi}](https://doi.org/{doi})")
            if dct:
                st.markdown(f"**Type** `{dct}`")


def _render_pub_component(
    d: "pd.DataFrame",
    cols: list,
    authors_by_row: dict,
    ds_base: str,
) -> None:
    """Render publications as a self-contained HTML component with working modals.

    Uses st.components.v1.html() which renders in a real iframe — scripts execute,
    <dialog> modals work natively, CSS is fully isolated.
    """
    import json as _jj
    import pandas as _pd
    import base64 as _b64

    def _e(v) -> str:
        if v is None or (isinstance(v, float) and _pd.isna(v)): return ""
        return _html.escape(str(v).strip())

    def _t(v, n=90) -> str:
        s = _e(v)
        full = _html.escape(str(v or ""))
        return f'<span title="{full}">{s[:n]}…</span>' if len(s) > n else s

    def _nn(v) -> bool:
        return v is not None and not (isinstance(v, float) and _pd.isna(v)) and str(v).strip() not in ("", "nan", "None")

    # ── Source tag ────────────────────────────────────────────────────────
    _SRC_CSS = {
        "scopus": "s-scopus", "wos": "s-wos", "crossref": "s-crossref",
        "openalex+crossref": "s-openalex", "openalex": "s-openalex",
        "zenodo": "s-zenodo", "epo": "s-epo", "datacite": "s-datacite",
    }
    _SRC_LBL = {
        "scopus":"Scopus","wos":"WoS","crossref":"Crossref",
        "openalex+crossref":"OpenAlex","openalex":"OpenAlex",
        "zenodo":"Zenodo","epo":"EPO","datacite":"DataCite",
    }

    def _src_tag(src, url=None):
        s = str(src or "").lower().strip()
        label = _html.escape(_SRC_LBL.get(s, src or "?"))
        css = _SRC_CSS.get(s, "s-def")
        if url:
            return f'<a href="{_html.escape(str(url))}" target="_blank" class="src {css}" style="text-decoration:none">{label}</a>'
        return f'<span class="src {css}">{label}</span>'

    def _status_badge(st_raw):
        s = str(st_raw or "").lower().strip()
        if not s: return ""
        return f'<span class="badge st-{s}">{_html.escape(s)}</span>'

    def _type_tag(dc):
        if not _nn(dc): return ""
        parts = str(dc).split("::")
        lbl = parts[-1].strip() if len(parts) > 1 else parts[0].strip()
        return f'<span class="ttype" title="{_e(dc)}">{_html.escape(lbl[:28])}</span>'

    def _yr(v) -> str:
        """Display year as integer — strips the .0 from float-like values."""
        if not _nn(v): return ""
        try: return str(int(float(str(v).strip())))
        except (ValueError, OverflowError): return _e(v)

    def _action_btns(row):
        parts = []
        for col, lbl, cls in [("item_url","View","av"),("ws_url","Edit","ae"),("wf_url","Claim","ac")]:
            u = row.get(col)
            if _nn(u): parts.append(f'<a href="{_e(u)}" target="_blank" class="ab {cls}">{lbl}</a>')
        return '<div class="abl">' + "".join(parts) + "</div>" if parts else '<span class="dash">—</span>'

    # ── Modal content builders ────────────────────────────────────────────
    def _meta_content(row):
        rm = row.get("raw_metadata")
        meta, sections = {}, _DB_META_SECTIONS
        if _nn(rm):
            try: meta = _jj.loads(str(rm)); sections = _RAW_META_SECTIONS
            except Exception: pass
        src = meta if meta else row
        html_parts = [f'<p class="m-ttl">{_t(row.get("title"), 100)}</p>']
        if not meta:
            html_parts.append('<p class="m-info">Métadonnées complètes disponibles à partir des prochains runs.</p>')
        for sec, keys in sections:
            items = [(k, str(src[k])) for k in keys if k in src and _nn(src.get(k))]
            if not items: continue
            html_parts.append(f'<div class="m-sec">{_html.escape(sec)}</div>')
            for k, v in items:
                if len(v) > 200:
                    html_parts.append(f'<div class="m-row m-row-w"><span class="m-key">{_html.escape(k)}</span><pre class="m-pre">{_html.escape(v)}</pre></div>')
                else:
                    html_parts.append(f'<div class="m-row"><span class="m-key">{_html.escape(k)}</span><span class="m-val">{_e(v)}</span></div>')
        return "".join(html_parts)

    def _authors_content(row, authors):
        def _v(x): return _e(x) if x else ""  # clean display value, empty if falsy

        parts = [f'<p class="m-ttl">{_t(row.get("title"), 100)}</p>']
        if not authors:
            parts.append('<p class="m-info">Aucun auteur EPFL réconcilié.</p>')
            return "".join(parts)
        for a in authors:
            sciper = _v(a.get("sciper"))
            orcid  = _v(a.get("orcid"))
            name   = _v(a.get("name")) or sciper or "?"
            status = _v(a.get("epfl_status"))
            pos    = _v(a.get("epfl_position"))
            unit   = _v(a.get("main_unit"))
            weak   = a.get("weak", False)
            sc_lk  = f'<a href="https://people.epfl.ch/{sciper}" target="_blank">{sciper}</a>' if sciper else "—"
            or_lk  = f'<a href="https://orcid.org/{orcid}" target="_blank">{orcid}</a>' if orcid else "—"
            weak_badge = '<span class="pma-wb">⚠️ Statut faible</span>' if weak else ""
            # Status displayed with warning colour if weak
            status_html = (
                f'<span class="pma-st-weak">{status or "—"}</span>' if weak
                else (status or "—")
            )
            parts.append(
                f'<div class="pma-card{"  pma-weak" if weak else ""}">'
                f'<div class="pma-name">{name} {weak_badge}</div>'
                f'<div class="pma-meta">'
                f'<span><b>SCIPER</b> {sc_lk}</span>'
                f'<span><b>Statut</b> {status_html}</span>'
                f'<span><b>Position</b> {pos or "—"}</span>'
                f'<span><b>Unité</b> {unit or "—"}</span>'
                f'<span><b>ORCID</b> {or_lk}</span>'
                f'</div></div>'
            )
        return '<div class="pma-list">' + "".join(parts) + "</div>"

    def _flagged_content(row):
        raw = row.get("flagged_publication"); note = str(row.get("dedup_note") or "")
        label = _DEDUP_LABELS.get(note, note or "Signalé")
        parts = [f'<p class="m-ttl">{_t(row.get("title"), 100)}</p><p class="m-note">{_e(label)}</p>']
        try: items = _jj.loads(str(raw))
        except Exception: items = []
        if not isinstance(items, list): items = [items]
        for it in items:
            uuid = _e(it.get("uuid")); doi = _e(it.get("doi")); dct = _e(it.get("dc_type"))
            ul = f'<a href="{_e(ds_base)}/items/{uuid}" target="_blank">{uuid}</a>' if uuid else "—"
            dl = f'<a href="https://doi.org/{doi}" target="_blank">{doi}</a>' if doi else "—"
            parts.append(f'<div class="pmf-card"><div><b>UUID</b> {ul}</div><div><b>DOI</b> {dl}</div><div><b>Type</b> <code>{dct}</code></div></div>')
        return '<div class="pmf-list">' + "".join(parts) + "</div>"

    # ── Build dialogs + rows ──────────────────────────────────────────────
    has_run = "run_id" in cols
    dialogs, trows = [], []

    for idx, row in enumerate(d.to_dict("records")):
        auths = authors_by_row.get(str(row.get("row_id") or ""), [])
        has_flag = _nn(row.get("flagged_publication"))

        # Dialogs for this row
        for mid, title, content, disabled in [
            (f"pm{idx}", "📋 Métadonnées collectées", _meta_content(row), False),
            (f"pa{idx}", "👤 Auteurs EPFL",           _authors_content(row, auths), not auths),
            (f"pf{idx}", "🚩 Doublon Infoscience",    _flagged_content(row) if has_flag else "", not has_flag),
        ]:
            if not disabled:
                dialogs.append(
                    f'<dialog id="{mid}">'
                    f'<div class="mbox">'
                    f'<div class="mhd"><span>{title}</span><button data-close="{mid}" class="mx">✕</button></div>'
                    f'<div class="mbd">{content}</div>'
                    f'</div></dialog>'
                )

        # Table row
        run_td = f'<td class="c-run">{_e(row.get("run_id"))}</td>' if has_run else ""
        pdf_tag = '<span class="pdf-tag">PDF</span>' if row.get("PDF") else ""
        warn_ic = ' <span title="Statut faible">⚠️</span>' if row.get("⚠️") else ""
        doi_u = row.get("doi_url"); src_u = row.get("src_url")
        doi_val = str(row.get("doi") or "").strip()
        lks = ""
        if _nn(doi_u) and doi_val:
            doi_display = doi_val[:32] + "…" if len(doi_val) > 32 else doi_val
            lks = (
                f'<div class="doi-row">'
                f'<a href="{_e(doi_u)}" target="_blank" class="doi-lk" title="{_html.escape(doi_val)}">{_html.escape(doi_display)}</a>'
                f'<button class="copy-btn" data-copy="{_html.escape(doi_val)}" title="Copier le DOI">⎘</button>'
                f'</div>'
            )
        flag_note = _DEDUP_LABELS.get(str(row.get("dedup_note") or ""), "")

        trows.append(f"""<tr>
<td class="c-act">{_action_btns(row)}</td>
{run_td}
<td class="c-yr">{_yr(row.get("pub_year"))}</td>
<td class="c-ttl">
  <div class="ttags">{_src_tag(row.get("source"), src_u if _nn(src_u) else None)}{_status_badge(row.get("status"))}{_type_tag(row.get("dc_type"))}</div>
  <span class="ttl">{_t(row.get("title"), 105)}</span>
</td>
<td class="c-oa">
  <span class="oa-v">{_e(row.get("OA"))}</span>
  <span class="lic-v">{_t(row.get("Licence"), 20)}</span>
  {pdf_tag}
</td>
<td class="c-auth">
  <div class="auth-n">{_t(row.get("Auteurs EPFL"), 60)}{warn_ic}</div>
  <span class="auth-u">{_e(row.get("Unités"))}</span>
</td>
<td class="c-lk">{lks}</td>
<td class="c-btn"><button data-modal="pm{idx}" class="mbtn">📋</button></td>
<td class="c-btn">{'<button data-modal="pa'+str(idx)+'" class="mbtn">👤</button>' if auths else '<button class="mbtn" disabled>👤</button>'}</td>
<td class="c-btn">{'<button data-modal="pf'+str(idx)+f'" class="mbtn flag-btn">🚩</button>' if has_flag else '<button class="mbtn" disabled>🚩</button>'}</td>
</tr>""")

    run_th = "<th>Run</th>" if has_run else ""

    # ── CSS ───────────────────────────────────────────────────────────────
    CSS = """
*,*::before,*::after{box-sizing:border-box}
*{margin:0;padding:0}
html,body{font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:13px;background:#fff;color:#1D2939;-webkit-font-smoothing:antialiased}
a{color:#632CA6;text-decoration:none}a:hover{text-decoration:underline}

/* Table */
.wrap{border-radius:12px;border:1px solid #E4E7EC;box-shadow:0 2px 8px rgba(16,24,40,.05);overflow:hidden}
table{width:100%;border-collapse:collapse}
th{background:#F9FAFB;color:#667085;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;padding:9px 10px;text-align:left;border-bottom:2px solid #E4E7EC;white-space:nowrap}
td{padding:8px 10px;vertical-align:middle;border-bottom:1px solid #F2F4F7;line-height:1.4}
tr:last-child td{border-bottom:none}
tr:hover td{background:#F8F9FC}
.c-act{width:82px}.c-run{width:90px;font-size:11px;color:#667085}
.c-yr{width:46px;font-weight:600;font-size:13px;white-space:nowrap}
.c-ttl{min-width:210px}.c-oa{width:118px}.c-auth{width:168px}
.c-lk{width:140px}.c-btn{width:30px;text-align:center;padding:6px 3px}
.doi-row{display:flex;align-items:center;gap:4px;margin-bottom:3px}
.doi-lk{font-family:'SF Mono','Roboto Mono',monospace;font-size:10.5px;color:#632CA6;word-break:break-all;flex:1;min-width:0}
.src-lk{font-weight:500;font-size:11px;color:#632CA6}
.copy-btn{flex-shrink:0;background:none;border:1px solid #E4E7EC;border-radius:4px;padding:1px 5px;font-size:12px;cursor:pointer;color:#667085;transition:background .1s,border-color .1s,color .1s;line-height:1.4}
.copy-btn:hover{background:#F0F4FF;border-color:#C4B5FD;color:#632CA6}
.copy-btn.copied{background:#DCFCE7;border-color:#86EFAC;color:#15803D}

/* Source tags */
.src{display:inline-block;padding:1px 6px;border-radius:4px;font-size:9.5px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;vertical-align:middle}
.s-scopus{background:#DBEAFE;color:#1E40AF}.s-wos{background:#EDE9FE;color:#5B21B6}
.s-crossref{background:#CCFBF1;color:#0F766E}.s-openalex{background:#DCFCE7;color:#15803D}
.s-zenodo{background:#FED7AA;color:#9A3412}.s-epo{background:#F3F4F6;color:#374151}
.s-datacite{background:#F3E8FF;color:#7E22CE}.s-def{background:#F1F5F9;color:#64748B}

/* Status badges */
.badge{display:inline-block;padding:1px 7px;border-radius:999px;font-size:10px;font-weight:600;vertical-align:middle}
.st-workflow{background:#EDE9FE;color:#6D28D9}.st-workspace{background:#FEF9C3;color:#854D0E}
.st-deduplicated{background:#DBEAFE;color:#1D4ED8}.st-rejected{background:#FEE2E2;color:#B91C1C}
.st-error{background:#FEE2E2;color:#B91C1C}

/* Type tag */
.ttype{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;background:#F1F5F9;color:#475569;max-width:150px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;vertical-align:middle}

/* Title */
.ttags{display:flex;flex-wrap:wrap;gap:3px;margin-bottom:4px;align-items:center}
.ttl{font-weight:500;color:#101828;font-size:12.5px;line-height:1.4}

/* Action buttons */
.abl{display:flex;flex-direction:column;gap:3px}
.dash{color:#D0D5DD}
.ab{display:block;padding:3px 8px;border-radius:5px;font-size:10.5px;font-weight:600;text-decoration:none!important;text-align:center;border:1px solid;transition:filter .1s,transform .1s;line-height:1.4}
.ab:hover{filter:brightness(.88);transform:translateY(-1px);text-decoration:none!important}
.av{background:#DCFCE7;color:#15803D!important;border-color:#86EFAC}
.ae{background:#FEF9C3;color:#854D0E!important;border-color:#FDE68A}
.ac{background:#EDE9FE;color:#6D28D9!important;border-color:#C4B5FD}

/* OA */
.oa-v{display:block;font-weight:600;color:#16A34A;font-size:11px}
.lic-v{display:block;color:#667085;font-size:11px}
.pdf-tag{display:inline-block;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:700;background:#DCFCE7;color:#15803D;margin-top:2px}

/* Authors */
.auth-n{font-size:11.5px;line-height:1.4;margin-bottom:1px}
.auth-u{font-size:10.5px;color:#98A2B3}

/* Links */
td.c-lk a{color:#632CA6;font-weight:500;font-size:11px;margin-right:4px}

/* Modal trigger buttons */
.mbtn{background:#F9FAFB;border:1px solid #E4E7EC;border-radius:5px;padding:3px 6px;font-size:13px;cursor:pointer;color:#374151;line-height:1;transition:background .1s,border-color .1s;display:block;width:100%}
.mbtn:hover{background:#F0F4FF;border-color:#C4B5FD}
.mbtn:disabled{opacity:.3;cursor:not-allowed}
.flag-btn:hover{background:#FFF5F5;border-color:#FECACA}

/* Modals */
dialog{border:none;border-radius:16px;padding:0;max-width:640px;width:90vw;max-height:80vh;box-shadow:0 24px 64px rgba(16,24,40,.22);overflow:hidden;position:fixed;top:24px;left:50%;transform:translateX(-50%);margin:0}
dialog::backdrop{background:rgba(16,24,40,.5);backdrop-filter:blur(3px);position:fixed;inset:0}
.mbox{display:flex;flex-direction:column;max-height:80vh}
.mhd{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #E4E7EC;font-weight:700;font-size:13.5px;color:#101828;background:#FAFAFA;flex-shrink:0}
.mx{background:none;border:none;font-size:15px;color:#667085;cursor:pointer;padding:2px 6px;border-radius:4px;line-height:1}
.mx:hover{background:#F3F4F6}
.mbd{overflow-y:auto;padding:18px;flex:1}
.m-ttl{font-weight:600;color:#101828;margin:0 0 12px;font-size:13px;line-height:1.4}
.m-note{color:#B91C1C;font-weight:500;font-size:12px;margin:0 0 12px}
.m-info{color:#667085;font-size:12px;font-style:italic;padding:6px 0}
.m-sec{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#667085;margin:14px 0 5px;padding-bottom:4px;border-bottom:1px solid #F2F4F7}
.m-sec:first-of-type{margin-top:0}
.m-row{display:flex;gap:10px;padding:4px 0;border-bottom:1px solid #FAFAFA;font-size:11.5px}
.m-row-w{flex-direction:column;gap:3px}
.m-key{color:#667085;min-width:120px;flex-shrink:0;font-size:11px}
.m-val{color:#1D2939;word-break:break-all}
.m-pre{margin:0;white-space:pre-wrap;word-break:break-word;background:#F8FAFC;border:1px solid #E4E7EC;border-radius:5px;padding:7px 9px;font-size:11px;font-family:inherit;max-height:150px;overflow-y:auto;line-height:1.5;color:#1D2939}
.pma-list{display:flex;flex-direction:column;gap:8px}
.pma-card{border:1px solid #E4E7EC;border-radius:8px;padding:10px 12px;background:#FAFAFA}
.pma-weak{border-color:#FDE68A;background:#FFFBEB}
.pma-name{font-weight:600;font-size:13px;color:#101828;margin-bottom:6px}
.pma-meta{display:grid;grid-template-columns:1fr 1fr;gap:3px 12px;font-size:11px}
.pma-meta span{color:#475569}.pma-meta b{color:#101828;margin-right:3px}
.pma-wb{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700;background:#FEF3C7;color:#92400E;margin-left:5px;vertical-align:middle}
.pma-st-weak{color:#92400E;font-weight:600}
.pmf-list{display:flex;flex-direction:column;gap:8px}
.pmf-card{border:1px solid #FECACA;border-radius:8px;padding:10px 12px;background:#FFF5F5;font-size:12px;display:flex;flex-direction:column;gap:5px}
.pmf-card b{color:#101828;margin-right:3px}
code{background:#F1F5F9;padding:1px 5px;border-radius:3px;font-size:10.5px;color:#475569;font-family:inherit}
"""

    # ── JS (runs in real iframe — works!) ─────────────────────────────────
    JS = """
document.querySelectorAll('[data-copy]').forEach(b=>{
  b.addEventListener('click',e=>{
    e.stopPropagation();
    const txt=b.dataset.copy;
    (navigator.clipboard?.writeText(txt)||Promise.reject()).then(()=>{
      b.classList.add('copied');b.textContent='✓';
      setTimeout(()=>{b.classList.remove('copied');b.textContent='⎘';},1600);
    }).catch(()=>{
      const ta=document.createElement('textarea');ta.value=txt;
      document.body.appendChild(ta);ta.select();document.execCommand('copy');
      document.body.removeChild(ta);
      b.classList.add('copied');b.textContent='✓';
      setTimeout(()=>{b.classList.remove('copied');b.textContent='⎘';},1600);
    });
  });
});
document.querySelectorAll('[data-modal]').forEach(b=>{
  b.addEventListener('click',e=>{
    e.stopPropagation();
    document.getElementById(b.dataset.modal)?.showModal();
  });
});
document.querySelectorAll('[data-close]').forEach(b=>{
  b.addEventListener('click',()=>document.getElementById(b.dataset.close)?.close());
});
document.querySelectorAll('dialog').forEach(d=>{
  d.addEventListener('click',e=>{if(e.target===d)d.close();});
});
"""

    html_doc = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head><body>
{"".join(dialogs)}
<div class="wrap"><table>
<thead><tr>
<th>Actions</th>{run_th}<th>Année</th><th style="min-width:220px">Titre</th>
<th>OA / Licence</th><th>Auteurs EPFL</th><th>DOI</th>
<th title="Métadonnées">📋</th><th title="Auteurs EPFL">👤</th><th title="Doublon">🚩</th>
</tr></thead>
<tbody>{"".join(trows)}</tbody>
</table></div>
<script>{JS}</script>
</body></html>"""

    _b64_src = "data:text/html;base64," + _b64.b64encode(html_doc.encode("utf-8")).decode("ascii")
    st.iframe(_b64_src, height=max(320, len(d) * 66 + 100))


# ── Sidebar navigation ────────────────────────────────────────────────────────
_ENV_STYLE = {
    "dev":  ("background:#dff0c8;color:#3a5a10", "DEV"),
    "test": ("background:#fdefd5;color:#7a4400", "TEST"),
    "prod": ("background:#ffd5d5;color:#7a0000", "PROD ⚠️"),
}

with st.sidebar:
    st.markdown(
        f'<div style="font-size:1.15rem;font-weight:700;color:#C8D0E0;'
        f'display:flex;align-items:center;gap:6px;margin-bottom:2px">'
        f'{mi("cloud_sync","ms-neutral")} Infoscience Imports</div>',
        unsafe_allow_html=True,
    )

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

    # ── Navigation ────────────────────────────────────────────────────────
    _NAV_ICONS = {
        "Tableau de bord": "dashboard",
        "Lancer un run":   "rocket_launch",
        "Programmation":   "schedule",
        "Publications":    "article",
        "Statistiques":    "bar_chart",
        "Configuration":   "settings",
    }
    st.markdown("---")
    _allowed = get_allowed_pages(_role)
    # Active page driven by query params — falls back to first allowed page.
    _default_page = _allowed[0] if _allowed else ""
    _qp = st.query_params.get("page", _default_page)
    page = _qp if _qp in _allowed else _default_page

    _nav_html = '<nav class="sidebar-nav">'
    for _p in _allowed:
        _ico  = _NAV_ICONS.get(_p, "circle")
        _cls  = "nav-item active" if _p == page else "nav-item"
        _href = f"?page={_p.replace(' ', '+')}"
        _nav_html += (
            f'<a class="{_cls}" href="{_href}" target="_self">'
            f'<span class="ms ms-neutral">{_ico}</span>'
            f'<span>{_p}</span></a>'
        )
    _nav_html += "</nav>"
    st.markdown(_nav_html, unsafe_allow_html=True)

    st.markdown("---")
    if st.button("Rafraîchir", help="Recharge les données depuis la base",
                 icon=":material/refresh:"):
        st.cache_resource.clear()
        st.rerun()

    # ── User info + logout ────────────────────────────────────────────────
    st.markdown("---")
    _, _dname, _ = current_user()
    _role_label = {"admin": "Admin", "reporting": "Reporting"}.get(_role, _role)
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

db = get_db()

# ── Bannière globale : run en cours ───────────────────────────────────────────
_active = read_active_run()
if _active:
    _run_env = _active.get("env", "?")
    st.warning(
        f"⏳ **Run en cours** [{_run_env.upper()}] — `{_active['run_id']}` "
        f"(sources : {_active['sources']}, démarré : {_active['started_at'][:19].replace('T',' ')})  "
        f"→ Allez sur **Lancer un run** pour suivre la progression.",
        icon=None,
    )

# ==============================================================================
# PAGE 1 — TABLEAU DE BORD
# ==============================================================================
if page == "Tableau de bord":
    page_title("dashboard", "Tableau de bord")

    db_d     = get_db()
    _kpis    = db_d.get_dashboard_kpis(months=12)
    _runs_df = db_d.get_runs(limit=20)

    # ── KPI tiles (last 12 months) ─────────────────────────────────────────
    def _fmt_dur(s):
        if s is None or pd.isna(s) or s <= 0:
            return "—"
        s = int(s)
        return f"{s//3600}h {(s%3600)//60}m {s%60}s" if s >= 3600 else f"{s//60}m {s%60}s"

    _kc1, _kc2, _kc3, _kc4, _kc5 = st.columns(5)
    with _kc1:
        st.markdown(metric_card("Runs (12 mois)", _kpis["total_runs"]), unsafe_allow_html=True)
    with _kc2:
        st.markdown(
            metric_card("Importés (12 mois)", f"{_kpis['total_imported']:,}"),
            unsafe_allow_html=True)
    with _kc3:
        st.markdown(
            metric_card("Rejetés (12 mois)", f"{_kpis['total_rejected']:,}"),
            unsafe_allow_html=True)
    with _kc4:
        st.markdown(
            metric_card("Taux de succès", f"{_kpis['success_rate']} %",
                        f"{_kpis['completed']} / {_kpis['total_runs']} complétés"),
            unsafe_allow_html=True)
    with _kc5:
        st.markdown(
            metric_card("Durée moyenne / run", _fmt_dur(_kpis["avg_duration_s"])),
            unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Monthly imports trend ──────────────────────────────────────────────
    st.markdown(
        sh("trending_up", "Importés par mois (12 derniers mois)"),
        unsafe_allow_html=True)
    _month_df = db_d.get_imported_by_month(months=12)
    if not _month_df.empty:
        _fig_month = px.bar(
            _month_df, x="month", y="count",
            color_discrete_sequence=[CANARD],
            labels={"month": "Mois", "count": "Publications importées"},
            height=220,
        )
        _fig_month.update_layout(
            margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
            xaxis=dict(
                tickformat="%b %Y",
                tickangle=-30,
                dtick="M1",
            ),
        )
        st.plotly_chart(_fig_month, width="stretch")
    else:
        st.caption("Aucune donnée pour les 12 derniers mois.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Per-run status breakdown + global status donut ─────────────────────
    _da, _db_col = st.columns([3, 2])

    with _da:
        st.markdown(
            sh("bar_chart", "Publications par run (20 derniers)"),
            unsafe_allow_html=True)
        _spr_df = db_d.get_pubs_status_per_run(limit=20)
        if not _spr_df.empty:
            _SPR_COLORS = {
                "workflow":    CANARD,    "workspace":    LEMAN,
                "deduplicated": C_BLUE,  "rejected":     C_RED,
                "error":        C_GRAY_600,
            }
            _fig_spr = px.bar(
                _spr_df, x="run_id", y="count", color="status",
                color_discrete_map=_SPR_COLORS,
                labels={"run_id": "Run", "count": "Publications", "status": "Statut"},
                barmode="stack", height=340,
            )
            _fig_spr.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                xaxis=dict(tickangle=-40, tickfont=dict(size=9)),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(_fig_spr, width="stretch")
        else:
            st.caption("Aucun run enregistré.")

    with _db_col:
        st.markdown(
            sh("donut_large", "Distribution globale"),
            unsafe_allow_html=True)
        _gs_df = db_d.get_pubs_by_status()
        if not _gs_df.empty:
            _fig_gs = px.pie(
                _gs_df, names="status", values="count", color="status",
                color_discrete_map={
                    "workflow":    CANARD,   "workspace":   LEMAN,
                    "deduplicated": C_BLUE,  "rejected":    C_RED,
                    "error":        C_GRAY_600,
                },
                hole=0.45, height=340,
            )
            _fig_gs.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            _fig_gs.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig_gs, width="stretch")
        else:
            st.caption("Aucune donnée.")

    # ── Recent runs table ──────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(sh("history", "Runs récents"), unsafe_allow_html=True)
    if not _runs_df.empty:
        def _fmt_dt(v):
            return str(v)[:19].replace("T", " ") if pd.notna(v) and v is not None else "—"

        _disp = _runs_df.copy()
        _disp["Démarré"] = _disp["started_at"].apply(_fmt_dt)
        _disp["Terminé"] = _disp["ended_at"].apply(_fmt_dt)
        _disp["Durée"]   = _disp["duration_s"].apply(_fmt_dur)
        _disp["Statut"]  = _disp["status"].apply(lambda s: badge(s))
        _disp["DR"]      = _disp["dry_run"].apply(lambda v: "✓" if v else "")
        _disp["Sources"] = _disp["sources"].apply(lambda v: v or "—")
        st.write(
            _disp[["run_id", "Démarré", "Terminé", "Durée", "Sources", "Statut", "DR"]]
            .rename(columns={"run_id": "Run", "DR": "Dry-run"})
            .to_html(escape=False, index=False),
            unsafe_allow_html=True,
        )
    else:
        st.info("Aucun run enregistré.")

    # ── Importés par source × type de document (stacked bar) ─────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        sh("stacked_bar_chart", "Importés par source et type de document"),
        unsafe_allow_html=True)

    _src_type_df = db_d.get_pubs_by_source_and_type()

    if not _src_type_df.empty:
        _top8 = (
            _src_type_df.groupby("dc_type")["count"].sum()
            .nlargest(8).index.tolist()
        )
        _st = _src_type_df.copy()
        _st["dc_type"] = _st["dc_type"].apply(
            lambda t: t if t in _top8 else "Autre"
        )
        _st = _st.groupby(["source", "dc_type"], as_index=False)["count"].sum()

        _fig_st = px.bar(
            _st, x="source", y="count", color="dc_type",
            barmode="stack", height=360,
            color_discrete_sequence=px.colors.qualitative.Plotly,
            labels={"source": "Source", "count": "Publications importées", "dc_type": "Type"},
        )
        _fig_st.update_layout(
            margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(_fig_st, width="stretch")
    else:
        st.caption("Aucune donnée.")


# ==============================================================================
# PAGE 2 — LANCER UN RUN
# ==============================================================================
elif page == "Lancer un run":
    page_title("play_circle", "Lancer un run")

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
                    # Mark the run as killed immediately — the subprocess may not
                    # reach finish_run() if it is killed before the SIGTERM handler fires.
                    try:
                        _db_kill = PipelineDB()
                        _db_kill.finish_run(run_id, status="killed")
                        _db_kill.close()
                    except Exception:
                        pass
                    st.warning("Signal d'arrêt envoyé au processus.")
                    time.sleep(1)
                    st.cache_resource.clear()
                    st.rerun()

        st.markdown(
            sh("terminal", "Logs en direct"), unsafe_allow_html=True
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
            sh("date_range", "Fenêtre temporelle"),
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
                sh("label", "Nom du run (optionnel)"),
                unsafe_allow_html=True,
            )
            run_name_input = st.text_input(
                "Nom",
                placeholder="ex : tests-scopus-janvier",
                help="Inclus dans l'identifiant du run pour faciliter l'identification. "
                     "Laissez vide pour utiliser uniquement la date.",
            )

            st.markdown(
                sh("hub", "Sources"), unsafe_allow_html=True
            )
            selected_sources = st.multiselect(
                "Sources à inclure",
                options=SOURCES,
                default=SOURCES,
                help="Laissez vide pour utiliser toutes les sources.",
            )

            st.markdown(
                sh("search", "Requêtes (optionnel)"),
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
                sh("person_search", "Identifiants auteurs (optionnel)"),
                unsafe_allow_html=True,
            )
            col_a, col_b = st.columns(2)
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
            col_c, col_d = st.columns(2)
            with col_c:
                orcid_ids = st.text_area(
                    "ORCID iDs", placeholder="0000-0002-1825-0097", height=80
                )
            with col_d:
                openalex_ids = st.text_area(
                    "OpenAlex Author IDs", placeholder="A5023888391", height=80
                )

            st.markdown(
                sh("tune", "Options"), unsafe_allow_html=True
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
            if openalex_ids.strip():
                cmd += ["--openalex-ids", openalex_ids.strip().replace("\n", ",")]
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
elif page == "Programmation":
    import uuid as _uuid
    import json as _json_sched
    import tempfile as _tmp
    from apscheduler.triggers.cron import CronTrigger as _CT
    from datetime import timezone as _tz

    page_title("schedule", "Programmation des runs")

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
        "completed": "✅", "running": "⏳", "failed": "❌", "killed": "🛑", None: "—",
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
            sh("event_repeat", "Schedules configurés"),
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
        with st.expander("Logs du scheduler (50 dernières lignes)"):
            _log_lines = _sched_log.read_text(encoding="utf-8", errors="replace").splitlines()
            st.code("\n".join(_log_lines[-50:]), language=None)


# ==============================================================================
# PAGE 3 — PUBLICATIONS
# ==============================================================================
elif page == "Publications":
    page_title("article", "Publications")

    db_r = get_db()

    # ── Filtres ──────────────────────────────────────────────────────────
    _PUB_FILTER_KEYS = {
        "pf_run": [], "pf_type": [], "pf_status": [], "pf_source": [],
        "pf_unit": [], "pf_sciper": "", "pf_search": "",
        "pf_oa": "Tous", "pf_pdf": "Tous", "pf_licence": [], "pf_epfl": "Tous",
        "pf_dedup_note": "Tous",
    }

    def _reset_pub_filters():
        for k, v in _PUB_FILTER_KEYS.items():
            st.session_state[k] = v
        st.session_state["pub_page"] = 1

    with st.expander("🔍 Filtres", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            runs_df = db_r.get_runs(limit=50)
            run_opts = runs_df["run_id"].tolist() if not runs_df.empty else []
            sel_run = st.multiselect("Run", run_opts, key="pf_run")

            all_types = db_r.get_distinct_dc_types()
            sel_type = st.multiselect("Type de document", all_types, key="pf_type")

        with c2:
            sel_status = st.multiselect(
                "Statut",
                ["workflow", "workspace", "deduplicated", "rejected", "error"],
                key="pf_status",
            )

            all_sources = db_r.get_distinct_sources()
            sel_source = st.multiselect("Source", all_sources, key="pf_source")

        with c3:
            all_units = db_r.get_distinct_units()
            sel_unit = st.multiselect("Unité", all_units, key="pf_unit")
            sciper_q = st.text_input(
                "SCIPER ou nom auteur EPFL", placeholder="123456 ou Dupont",
                key="pf_sciper",
            )

        search_q = st.text_input(
            "Recherche titre / DOI", placeholder="deep learning…", key="pf_search",
        )

        cf1, cf2, cf3, cf4, cf5, cf6 = st.columns([2, 2, 2, 2, 2, 1])
        with cf1:
            sel_oa = st.selectbox(
                "Statut OA",
                ["Tous", "OA", "Non-OA", "Non-libre", "Non défini"],
                help="Filtre sur le statut Open Access (Unpaywall).",
                key="pf_oa",
            )
        with cf2:
            sel_pdf = st.selectbox(
                "PDF récupéré",
                ["Tous", "Avec PDF", "Sans PDF"],
                help="Filtre sur la présence d'un PDF en accès libre.",
                key="pf_pdf",
            )
        with cf3:
            all_licences = db_r.get_distinct_licences()
            sel_licence = st.multiselect(
                "Licence",
                all_licences,
                help="Filtre sur la licence Unpaywall (cc-by, elsevier-specific…).",
                key="pf_licence",
            )
        with cf4:
            sel_epfl = st.selectbox(
                "Statut auteurs EPFL",
                ["Tous", "⚠️ Statut faible", "✅ Statut fort"],
                help="Filtre sur le statut des auteurs EPFL reconciliés.\n"
                     "Faible : tous les auteurs sont hôtes, externes ou étudiants.\n"
                     "Fort : au moins un auteur permanent.",
                key="pf_epfl",
            )
        with cf5:
            sel_dedup_note = st.selectbox(
                "Signalement dedup",
                ["Tous", "🚩 Flaggés", "supersedes_preprint", "cross_type_doi"],
                help="Filtre sur les publications signalées lors de la déduplication Infoscience.\n"
                     "supersedes_preprint : version publiée importée, preprint déjà dans Infoscience.\n"
                     "cross_type_doi : même DOI qu'un preprint existant.",
                key="pf_dedup_note",
            )
        with cf6:
            st.markdown("<div style='padding-top:24px'>", unsafe_allow_html=True)
            st.button("↺ Reset", on_click=_reset_pub_filters,
                      use_container_width=True, help="Réinitialiser tous les filtres")
            st.markdown("</div>", unsafe_allow_html=True)

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
    _dedup_note_val = (
        None              if sel_dedup_note == "Tous" else
        "__flagged__"     if sel_dedup_note == "🚩 Flaggés" else
        sel_dedup_note
    )
    _filter_kwargs = dict(
        run_id=sel_run or None,
        status=sel_status or None,
        source=sel_source or None,
        dc_type=sel_type or None,
        sciper=resolved_sciper or None,
        unit_acronym=sel_unit or None,
        search=search_q.strip() or None,
        has_pdf=_has_pdf_val,
        oa_filter=None if sel_oa == "Tous" else sel_oa,
        licence=sel_licence or None,
        epfl_strength=_epfl_strength_val,
        dedup_note=_dedup_note_val,
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

    _enrichment_authors_df = pd.DataFrame()
    if not pub_df.empty:
        if len(sel_run) == 1:
            # Single run selected — fetch entire run enrichment (efficient single query per table).
            _single_run = sel_run[0]
            _a_df = db_r.get_pub_authors_for_run(_single_run)
            if not _a_df.empty:
                _a_df.insert(0, "run_id", _single_run)
            _enrichment_authors_df = _a_df
            _run_authors, _run_weak = _build_authors_dict(_a_df)

            _u_df = db_r.get_pub_units_for_run(_single_run)
            if not _u_df.empty:
                _u_df.insert(0, "run_id", _single_run)
            _run_units = _build_units_dict(_u_df)

            _d_df = db_r.get_detected_authors_for_run(_single_run)
            if not _d_df.empty:
                _d_df.insert(0, "run_id", _single_run)
            _run_detected = _build_detected_dict(_d_df)
        else:
            # "Tous les runs": fetch enrichment only for the current page's rows.
            _pairs = list(zip(pub_df["run_id"], pub_df["row_id"]))
            _auth_df = db_r.get_pub_authors_for_rows(_pairs)
            _enrichment_authors_df = _auth_df
            _run_authors, _run_weak = _build_authors_dict(_auth_df)
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
        def _source_api_url(source, internal_id, doi):
            iid = str(internal_id).strip() if pd.notna(internal_id) and internal_id else None
            d_  = str(doi).strip() if pd.notna(doi) and doi else None
            if source == "crossref":
                key = iid or d_
                return f"https://api.crossref.org/works/{key}" if key else None
            if source == "openalex+crossref":
                key = iid or d_
                return f"https://api.openalex.org/works/https://doi.org/{key}" if key else None
            if source == "scopus":
                if iid:
                    return f"https://www.scopus.com/record/display.uri?eid={iid}&origin=resultslist"
                if d_:
                    _doi_enc = d_.replace("/", "%2F")
                    return f"https://www.scopus.com/results/results.url?s=DOI%28{_doi_enc}%29&origin=searchbasic"
                return None
            if source == "wos":
                return f"https://www.webofscience.com/wos/woscc/full-record/{iid}" if iid else None
            if source == "zenodo":
                if iid and iid.isdigit():
                    return f"https://zenodo.org/api/records/{iid}"
                if d_:
                    m = re.search(r"zenodo\.(\d+)", d_)
                    if m:
                        return f"https://zenodo.org/api/records/{m.group(1)}"
                return None
            if source == "epo":
                return f"https://ops.epo.org/rest-services/published-data/publication/docdb/{iid}/biblio" if iid else None
            if source == "datacite":
                key = iid or d_
                return f"https://api.datacite.org/dois/{key}" if key else None
            # Fallback: CrossRef for any DOI-bearing record
            if d_:
                return f"https://api.crossref.org/works/{d_}"
            return None

        d["src_url"] = d.apply(
            lambda r: _source_api_url(r.get("source"), r.get("internal_id"), r.get("doi")),
            axis=1,
        )

        d["doi_url"] = d["doi"].apply(
            lambda x: f"https://doi.org/{x}" if pd.notna(x) and str(x).startswith("10.") else None
        )
        # ws_url is only valid while the item has not yet been submitted to workflow
        d["ws_url"] = d.apply(
            lambda r: f"{ds_base}/workspaceitems/{int(float(r['workspace_id']))}/edit"
                      if pd.notna(r.get("workspace_id")) and r.get("workspace_id") != ""
                      and (pd.isna(r.get("workflow_id")) or r.get("workflow_id") == "")
                      else None,
            axis=1,
        )
        d["wf_url"] = d["workflow_id"].apply(
            lambda w: f"{ds_base}/admin/workflow?spc.page=1&query=search.uniqueid:XmlWorkflowItem-{int(float(w))}"
                      if pd.notna(w) and w != "" else None
        )
        d["item_url"] = d["dspace_item_uuid"].apply(
            lambda u: f"{ds_base}/items/{u}"
                      if pd.notna(u) and u != "" else None
        ) if "dspace_item_uuid" in d.columns else None

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
            (["run_id"] if not sel_run else [])
            + ["pub_year", "title", "source", "dc_type", "status",
               "OA", "Licence", "PDF", "⚠️",
               "Auteurs EPFL", "Unités",
               "seen_count", "infoscience_dedup_count",
               "src_url", "doi_url", "item_url", "ws_url", "wf_url", "error_msg",
               "dedup_note", "flagged_publication"]
        )
        _cols = [c for c in _cols if c in d.columns]

        # Build per-row author dicts for the authors modal
        def _clean_str(v) -> str:
            """Return empty string for None, NaN, 'nan', 'None', 'null'."""
            if v is None:
                return ""
            if isinstance(v, float) and pd.isna(v):
                return ""
            try:
                if pd.isna(v):
                    return ""
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            return "" if s.lower() in ("nan", "none", "null", "na", "<na>", "nat") else s

        _authors_by_row: dict = {}
        if not _enrichment_authors_df.empty:
            for _, _ar in _enrichment_authors_df.iterrows():
                _rk = str(_ar.get("row_id", ""))
                if not _rk:
                    continue
                _st  = _clean_str(_ar.get("epfl_status"))
                _pos = _clean_str(_ar.get("epfl_position"))
                _weak = _is_weak(_st, _pos)
                _authors_by_row.setdefault(_rk, []).append({
                    "name":          _clean_str(_ar.get("full_name")) or _clean_str(_ar.get("sciper")) or "?",
                    "sciper":        _clean_str(_ar.get("sciper")),
                    "orcid":         _clean_str(_ar.get("orcid")),
                    "epfl_status":   _clean_str(_ar.get("epfl_status")),
                    "epfl_position": _clean_str(_ar.get("epfl_position")),
                    "main_unit":     _clean_str(_ar.get("main_unit")),
                    "weak":          _weak,
                })

        _render_pub_component(d, _cols, _authors_by_row, ds_base)

        # ── Downloads ─────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        _dl_cols = st.columns(3)

        with _dl_cols[0]:
            # CSV export uses all matching rows, not just current page
            _full_df = db_r.get_publications(**_filter_kwargs, limit=10_000, offset=0)
            _run_label = "-".join(sel_run) if sel_run else "all"
            st.download_button(
                "⬇ Publications CSV",
                data=_full_df.to_csv(index=False).encode("utf-8"),
                file_name=f"publications_{_run_label}_{date.today()}.csv",
                mime="text/csv",
            )

        with _dl_cols[1]:
            if len(sel_run) == 1:
                ax_df = db_r.get_pub_authors_for_run(sel_run[0])
                if not ax_df.empty:
                    st.download_button(
                        "⬇ Publications × Auteurs CSV",
                        data=ax_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"pub_authors_{sel_run[0]}_{date.today()}.csv",
                        mime="text/csv",
                    )

        with _dl_cols[2]:
            if len(sel_run) == 1:
                run_dir = ROOT / "data" / sel_run[0]
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
elif page == "Statistiques":
    page_title("bar_chart", "Statistiques")

    db_r = get_db()
    _stat_runs_df = db_r.get_runs(limit=100)
    if _stat_runs_df.empty:
        st.info("Aucun run enregistré. Lancez un premier run pour voir les statistiques.")
        st.stop()

    # ── Run selector — drives all charts below ─────────────────────────────
    _run_opts_stat = ["Tous les runs"] + _stat_runs_df["run_id"].tolist()
    _sel_stat_run = st.selectbox(
        "Périmètre", _run_opts_stat, key="stat_run_filter",
        help="Sélectionnez un run pour explorer ses données en détail, "
             "ou gardez « Tous les runs » pour une vue agrégée.",
    )
    _stat_run_id = None if _sel_stat_run == "Tous les runs" else _sel_stat_run

    tab_overview, tab_pubs, tab_people = st.tabs(
        ["Vue d'ensemble", "Publications", "Auteurs & Unités"]
    )

    # ── Tab 1 : Vue d'ensemble ────────────────────────────────────────────
    with tab_overview:
        _s1, _s2 = st.columns(2)

        # Source funnel — uses get_sources_breakdown which supports run_id=None
        with _s1:
            st.markdown(
                sh("filter_alt", "Entonnoir par source"),
                unsafe_allow_html=True)
            _src_stat = db_r.get_sources_breakdown(run_id=_stat_run_id)
            _src_stat = _src_stat[_src_stat["source"] != "__total__"] if not _src_stat.empty else _src_stat
            if not _src_stat.empty:
                _fig_f = go.Figure()
                for _col, _clr, _lbl in [
                    ("harvested", "#b0c4de", "Collectés"),
                    ("loaded",    CANARD,    "Importés"),
                    ("rejected",  C_RED,     "Rejetés"),
                ]:
                    _fig_f.add_trace(go.Bar(name=_lbl, x=_src_stat["source"],
                                            y=_src_stat[_col], marker_color=_clr))
                _fig_f.update_layout(
                    barmode="group", height=300,
                    margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(_fig_f, width="stretch")
            else:
                st.caption("Aucune donnée de source disponible.")

        # Status distribution — uses get_pubs_by_status (no row limit)
        with _s2:
            st.markdown(
                sh("donut_large", "Distribution des statuts"),
                unsafe_allow_html=True)
            _st_df = db_r.get_pubs_by_status(_stat_run_id)
            if not _st_df.empty:
                _fig_st = px.pie(
                    _st_df, names="status", values="count", color="status",
                    color_discrete_map={
                        "workflow":    CANARD,  "workspace":   LEMAN,
                        "deduplicated": C_BLUE, "rejected":    C_RED,
                        "error":        C_GRAY_600,
                    },
                    hole=0.42, height=300,
                )
                _fig_st.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0),
                    legend=dict(orientation="h", yanchor="top", y=-0.08),
                )
                _fig_st.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(_fig_st, width="stretch")
            else:
                st.caption("Aucune donnée.")

        _s3, _s4 = st.columns(2)

        with _s3:
            st.markdown(
                sh("lock_open", "Statut Open Access"),
                unsafe_allow_html=True)
            _oa_df = db_r.get_pubs_by_oa_status(_stat_run_id)
            if not _oa_df.empty:
                _OA_COLORS_S = {
                    "OA + PDF":    C_GREEN,   "OA sans PDF":  LEMAN,
                    "OA non-libre": C_YELLOW, "Non-OA":       C_GRAY_600,
                    "Non défini":  C_GRAY_100,
                }
                _fig_oa_s = px.pie(
                    _oa_df, names="oa_category", values="count",
                    color="oa_category", color_discrete_map=_OA_COLORS_S,
                    hole=0.42, height=300,
                )
                _fig_oa_s.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0),
                    legend=dict(orientation="h", yanchor="top", y=-0.08),
                )
                _fig_oa_s.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(_fig_oa_s, width="stretch")
            else:
                st.caption("Aucune donnée.")

        with _s4:
            st.markdown(
                sh("picture_as_pdf", "Proportion avec PDF récupéré"),
                unsafe_allow_html=True)
            _pdf_s = db_r.get_pdf_stats(_stat_run_id)
            if _pdf_s["total"] > 0:
                _pdf_data_s = pd.DataFrame({
                    "label": ["PDF récupéré", "Sans PDF"],
                    "count": [_pdf_s["with_pdf"], _pdf_s["total"] - _pdf_s["with_pdf"]],
                })
                _fig_pdf_s = px.pie(
                    _pdf_data_s, names="label", values="count", color="label",
                    color_discrete_map={"PDF récupéré": C_GREEN, "Sans PDF": C_GRAY_100},
                    hole=0.42, height=300,
                )
                _fig_pdf_s.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0),
                    legend=dict(orientation="h", yanchor="top", y=-0.08),
                )
                _fig_pdf_s.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(_fig_pdf_s, width="stretch")
                _pct_s = round(100 * _pdf_s["with_pdf"] / _pdf_s["total"]) if _pdf_s["total"] else 0
                st.caption(
                    f"{_pdf_s['with_pdf']} PDF sur {_pdf_s['total']} publications importées ({_pct_s} %)"
                )
            else:
                st.caption("Aucune donnée.")

        # Per-source detail table (only meaningful for a specific run)
        if _stat_run_id:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(
                sh("table_chart", "Détail par source"),
                unsafe_allow_html=True)
            _detail_df = db_r.get_run_stats(_stat_run_id)
            if not _detail_df.empty:
                st.dataframe(
                    _detail_df.rename(columns={
                        "source": "Source", "harvested": "Collectés",
                        "deduplicated": "Dédoublonnés", "loaded": "Importés",
                        "rejected": "Rejetés",
                    }),
                    width="stretch", hide_index=True,
                )

    # ── Tab 2 : Publications ──────────────────────────────────────────────
    with tab_pubs:
        _p1, _p2 = st.columns(2)

        with _p1:
            st.markdown(
                sh("category", "Types de documents importés"),
                unsafe_allow_html=True)
            _type_stat = db_r.get_pubs_by_type(_stat_run_id)
            if not _type_stat.empty:
                _type_stat = _type_stat[_type_stat["type"].notna()].head(15)
                _fig_t = px.bar(
                    _type_stat, x="count", y="type", orientation="h",
                    color_discrete_sequence=[CANARD],
                    labels={"count": "Publications", "type": "Type"},
                    height=max(280, len(_type_stat) * 22),
                )
                _fig_t.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(_fig_t, width="stretch")
            else:
                st.caption("Aucune donnée.")

        with _p2:
            st.markdown(
                sh("calendar_today", "Par année de publication"),
                unsafe_allow_html=True)
            _year_stat = db_r.get_pubs_by_year(_stat_run_id)
            if not _year_stat.empty:
                _fig_yr = px.bar(
                    _year_stat, x="year", y="count",
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

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            sh("newspaper", "Top journaux"), unsafe_allow_html=True)
        _jour_stat = db_r.get_pubs_by_journal(_stat_run_id, limit=20)
        if not _jour_stat.empty:
            _fig_j = px.bar(
                _jour_stat, x="count", y="journal", orientation="h",
                color_discrete_sequence=[CANARD],
                labels={"count": "Publications", "journal": "Journal"},
                height=max(300, len(_jour_stat) * 22),
            )
            _fig_j.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(_fig_j, width="stretch")
        else:
            st.caption("Aucun journal disponible.")

    # ── Tab 3 : Auteurs & Unités ──────────────────────────────────────────
    with tab_people:
        _col_auth, _col_units = st.columns([3, 2])

        # ── Left column: authors ──────────────────────────────────────────
        with _col_auth:
            st.markdown(
                sh("people", "Top auteurs EPFL"),
                unsafe_allow_html=True)
            _top_auth = db_r.get_top_epfl_authors(run_id=_stat_run_id, limit=20)
            if not _top_auth.empty:
                # Single colour — unit shown in hover to avoid legend explosion
                _fig_auth = px.bar(
                    _top_auth, x="pub_count", y="full_name", orientation="h",
                    color_discrete_sequence=[CANARD],
                    custom_data=["main_unit", "sciper"],
                    labels={"pub_count": "Publications", "full_name": "Auteur"},
                    height=max(300, len(_top_auth) * 26),
                )
                _fig_auth.update_traces(
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "Publications : %{x}<br>"
                        "Unité : %{customdata[0]}<br>"
                        "SCIPER : %{customdata[1]}"
                        "<extra></extra>"
                    )
                )
                _fig_auth.update_layout(
                    margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                    yaxis=dict(autorange="reversed"),
                    showlegend=False,
                )
                st.plotly_chart(_fig_auth, width="stretch")
            else:
                st.caption("Aucun auteur EPFL réconcilié disponible.")

            # Author search + table
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(
                sh("manage_search", "Recherche auteurs"),
                unsafe_allow_html=True)
            _pa1, _pa2 = st.columns(2)
            with _pa1:
                _author_search = st.text_input("Nom", key="stat_author_search")
            with _pa2:
                _all_units_stat = db_r.get_distinct_units()
                _filter_unit = st.selectbox(
                    "Unité", ["Toutes"] + _all_units_stat,
                    key="stat_author_unit",
                )
            _authors_df = db_r.get_epfl_authors(
                name_search=_author_search.strip() or None,
                unit=_filter_unit if _filter_unit != "Toutes" else None,
                limit=500,
            )
            st.caption(f"{len(_authors_df)} auteur(s)")
            if not _authors_df.empty:
                st.dataframe(
                    _authors_df.rename(columns={
                        "sciper": "SCIPER", "full_name": "Nom",
                        "first_name": "Prénom", "last_name": "Famille",
                        "orcid": "ORCID", "epfl_orcid": "ORCID EPFL",
                        "scopus_id": "Scopus", "wos_id": "WoS",
                        "openalex_id": "OpenAlex", "epfl_status": "Statut",
                        "epfl_position": "Poste", "main_unit": "Unité",
                        "dspace_uuid": "UUID DSpace", "last_seen": "Vu le",
                    }),
                    width="stretch", hide_index=True,
                    height=380,
                )
                st.download_button(
                    "⬇ Auteurs CSV",
                    data=_authors_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"epfl_authors_{date.today()}.csv",
                    mime="text/csv",
                )
            else:
                st.info("Aucun auteur EPFL dans la base.")

        # ── Right column: units panel ─────────────────────────────────────
        with _col_units:
            st.markdown(
                sh("account_balance", "Unités EPFL"),
                unsafe_allow_html=True)

            _unit_chart = db_r.get_pubs_by_unit(_stat_run_id, limit=30)
            if not _unit_chart.empty:
                _fig_u = px.bar(
                    _unit_chart, x="count", y="acronym", orientation="h",
                    color_discrete_sequence=[C_BLUE],
                    labels={"count": "Publications", "acronym": ""},
                    height=max(340, len(_unit_chart) * 22),
                )
                _fig_u.update_layout(
                    margin=dict(l=0, r=8, t=4, b=0), plot_bgcolor="white",
                    yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
                    xaxis=dict(title_font=dict(size=11)),
                    showlegend=False,
                )
                _fig_u.update_traces(
                    hovertemplate="<b>%{y}</b> — %{x} publications<extra></extra>"
                )
                st.plotly_chart(_fig_u, width="stretch")
            else:
                st.caption("Aucune donnée d'unité disponible.")

            # Compact units table
            st.markdown("<br>", unsafe_allow_html=True)
            _units_tbl = db_r.get_units()
            if not _units_tbl.empty:
                st.caption(f"{len(_units_tbl)} unités au total")
                st.dataframe(
                    _units_tbl[["acronym", "name_fr", "unit_type",
                                "author_count", "pub_count"]]
                    .rename(columns={
                        "acronym": "Acr.", "name_fr": "Nom",
                        "unit_type": "Type",
                        "author_count": "Auteurs", "pub_count": "Pub.",
                    }),
                    width="stretch", hide_index=True,
                    height=340,
                )
            else:
                st.info("Aucune unité dans la base.")


# ==============================================================================
# PAGE 5 — CONFIGURATION
# ==============================================================================
elif page == "Configuration":
    page_title("settings", "Configuration")
    st.markdown("Variables d'environnement et état des connexions.")

    # ── Env vars status ────────────────────────────────────────────────────
    st.markdown(
        sh("key", "Variables d'environnement"),
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
        sh("storage", "Base de données DuckDB"),
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
    st.markdown(sh("code", "Modèle .env"), unsafe_allow_html=True)
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
