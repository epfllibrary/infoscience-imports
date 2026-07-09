"""OA Monitor page — launch OA enrichment runs and browse NOAM export results."""

from __future__ import annotations

import html as _html
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.helpers import page_title, sh
from oa_monitor.oa_classifier import COAR_VERSION_MAP
from oa_monitor.type_mapping import (
    duckdb_authority_in_clause,
    label_for_authority,
    resolve_noam_type,
    solr_all_noam_types_clause,
    solr_types_authority_clause,
)

# ── Constants ─────────────────────────────────────────────────────────────────

_STATE_FILE_TPL = "data/oa_monitor_{env}.json"
_DEFAULT_OUTPUT_DIR = "data/noam"
_DEFAULT_WORK_DIR = "data/oa_work"

# Solr types_authority clauses per NOAM resource type — driven by the COAR
# authority code (language-independent), not dc.type's free-text value.
# See oa_monitor/type_mapping.py.
_TYPES_JOURNAL_ARTICLE = solr_types_authority_clause("Journal article")
_TYPES_BOOK = solr_types_authority_clause("Book")
_TYPES_BOOK_PART = solr_types_authority_clause("Book part")
_TYPES_CONFERENCE_PAPER = solr_types_authority_clause("Conference paper")
_TYPES_ALL_NOAM = solr_all_noam_types_clause()

# SQL IN clause for the enriched table — mirrors resolve_noam_type() in
# oa_monitor/type_mapping.py. Conference poster, conference presentation and
# untyped items are excluded per NOAM.
_NOAM_TYPES_SQL = duckdb_authority_in_clause()

_QUERY_PRESETS: dict[str, str] = {
    "Toutes les publications":          "",
    "Types NOAM uniquement":            _TYPES_ALL_NOAM,
    "Journal article (NOAM)":           _TYPES_JOURNAL_ARTICLE,
    "Book (NOAM)":                      _TYPES_BOOK,
    "Book part (NOAM)":                 _TYPES_BOOK_PART,
    "Conference paper (NOAM)":          _TYPES_CONFERENCE_PAPER,
    "Metadata-only uniquement":         'datacite.rights:"metadata-only"',
    "Open access uniquement":           'datacite.rights:"open access"',
}

# Shared SQL fragments used to build gap conditions.
_GAP_BASE = (
    f"{_NOAM_TYPES_SQL}"
    " AND existing_access_level ILIKE '%metadata%'"
    " AND oa_category_basic = 'Open'"
)
_CC_FILTER = "(resolved_license LIKE 'cc%' OR resolved_license = 'public-domain')"
_VERSIONED = "resolved_version IN ('publishedVersion', 'acceptedVersion')"

# Gap-detection SQL fragments keyed by human label.
# All gaps are scoped to NOAM resource types — conference posters, presentations, and
# untyped items are excluded so they never generate spurious gap flags.
_GAP_PRESETS: dict[str, str] = {
    "(tous)":                                   "",
    "⚡ Gap OA : metadata-only + OA enrichi (tous niveaux)":
        f"{_GAP_BASE}",
    "⚡ Gap 1/4 — metadata-only + Diamond":
        f"{_GAP_BASE} AND oa_category_advanced = 'Diamond'",
    "⚡ Gap 2/4 — metadata-only + Gold CC versioned":
        f"{_GAP_BASE} AND oa_category_advanced = 'Gold'"
        f" AND {_CC_FILTER} AND {_VERSIONED}",
    "⚡ Gap 3/4 — metadata-only + Hybrid CC versioned":
        f"{_GAP_BASE} AND oa_category_advanced = 'Hybrid'"
        f" AND {_CC_FILTER} AND {_VERSIONED}",
    "⚡ Gap 4/4 — metadata-only + Green":
        f"{_GAP_BASE} AND oa_category_advanced = 'Green'",
    "? Incohérence : open access + Fermé enrichi":
        f"{_NOAM_TYPES_SQL}"
        " AND existing_access_level ILIKE '%open%' AND NOT existing_access_level ILIKE '%metadata%'"
        " AND oa_category_basic = 'Closed'"
        " AND resolved_version IN ('publishedVersion', 'acceptedVersion')",
    "📋 Open access IFS sans licence":
        f"{_NOAM_TYPES_SQL}"
        " AND existing_access_level ILIKE '%open%'"
        " AND NOT existing_access_level ILIKE '%metadata%'"
        " AND (existing_license IS NULL OR existing_license = '')",
    "⌛ Embargo + OA Gold enrichi":
        f"{_NOAM_TYPES_SQL}"
        " AND existing_access_level ILIKE '%embargo%' AND oa_category_advanced ILIKE '%Gold%'",
}

# Collectability gaps — based on raw OpenAlex oa_status and Unpaywall upw_is_oa.
# These are independent of NOAM classification and apply to all publication types:
# the goal is to identify metadata-only items that have OA fulltext available to collect.
#
# best_oa_is_oa / upw_is_oa are stored as VARCHAR in the enriched table (values may be
# 'true', 'false', or '' for missing).  TRY_CAST … IS TRUE maps '' and NULL → FALSE
# so the comparison never raises a ConversionException.
_COLLECT_BASE = "existing_access_level ILIKE '%metadata%'"
_OA_IS_TRUE = "TRY_CAST(best_oa_is_oa AS BOOLEAN) IS TRUE"
_OA_IS_NOT_TRUE = "NOT TRY_CAST(best_oa_is_oa AS BOOLEAN) IS TRUE"
_UPW_IS_TRUE = "TRY_CAST(upw_is_oa AS BOOLEAN) IS TRUE"

_COLLECT_GAP_PRESETS: dict[str, str] = {
    "(aucun)": "",
    "⬇️ Collectables — tous signaux OA":
        f"{_COLLECT_BASE} AND ({_OA_IS_TRUE} OR {_UPW_IS_TRUE})",
    "⬇️ Collectables [OA-1] OpenAlex Gold / Diamond":
        f"{_COLLECT_BASE} AND oa_status IN ('gold', 'diamond') AND {_OA_IS_TRUE}",
    "⬇️ Collectables [OA-2] OpenAlex Hybrid":
        f"{_COLLECT_BASE} AND oa_status = 'hybrid' AND {_OA_IS_TRUE}",
    "⬇️ Collectables [OA-3] OpenAlex Green":
        f"{_COLLECT_BASE} AND oa_status = 'green' AND {_OA_IS_TRUE}",
    "⬇️ Collectables [UPW] Unpaywall OA (sans match OpenAlex)":
        f"{_COLLECT_BASE} AND {_UPW_IS_TRUE} AND ({_OA_IS_NOT_TRUE})",
}


# OA category colour palette (shared by barometer charts).
_OA_CAT_COLORS: dict[str, str] = {
    "Diamond": "#818CF8",
    "Gold":    "#F59E0B",
    "Hybrid":  "#3B82F6",
    "Green":   "#10B981",
    "Bronze":  "#F97316",
    "Closed":  "#9CA3AF",
}
_OA_CAT_ORDER = ["Diamond", "Gold", "Hybrid", "Green", "Bronze", "Closed"]

# ── State helpers ─────────────────────────────────────────────────────────────

def _state_file(env: str) -> Path:
    return Path(_STATE_FILE_TPL.format(env=env))


def _read_active(env: str) -> dict | None:
    sf = _state_file(env)
    if not sf.exists():
        return None
    try:
        state = json.loads(sf.read_text(encoding="utf-8"))
        pid = state.get("pid")
        if pid:
            try:
                os.kill(int(pid), 0)
                return state
            except (ProcessLookupError, PermissionError):
                sf.unlink(missing_ok=True)
                return None
    except Exception:
        return None


def _write_state(env: str, state: dict) -> None:
    sf = _state_file(env)
    sf.parent.mkdir(parents=True, exist_ok=True)
    sf.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _clear_state(env: str) -> None:
    _state_file(env).unlink(missing_ok=True)


# ── HTML badge helpers ────────────────────────────────────────────────────────

_BB = "font-size:0.70rem;padding:2px 7px;border-radius:4px;font-weight:500;white-space:nowrap;"
_CHIP = "font-size:0.68rem;padding:1px 6px;border-radius:3px;background:#F9FAFB;border:1px solid #E5E7EB;color:#374151;white-space:nowrap;"


_IFS_BASE = "https://infoscience.epfl.ch/handle"


def _ifs_url(handle) -> str | None:
    if not _nn(handle):
        return None
    return f"{_IFS_BASE}/{str(handle).strip()}"


def _nn(v) -> bool:
    return v is not None and str(v).strip().lower() not in ("nan", "none", "<na>", "")


def _sv(row: dict, k: str, default: str = "") -> str:
    v = row.get(k)
    return str(v).strip() if _nn(v) else default


def _bool_label(v) -> str:
    """Render a True/False-ish value (incl. OpenAlex's stringified bool) as Oui/Non."""
    if not _nn(v):
        return ""
    s = str(v).strip().lower()
    if s in ("true", "1"):
        return "Oui"
    if s in ("false", "0"):
        return "Non"
    return ""


def _ifs_access_badge(val) -> str:
    if not _nn(val):
        return ""
    v = str(val).strip()
    vl = v.lower()
    if "open" in vl and "metadata" not in vl:
        css = f"{_BB}background:#D1FAE5;color:#065F46;border:1px solid #A7F3D0"
    elif "restrict" in vl:
        css = f"{_BB}background:#FEF3C7;color:#92400E;border:1px solid #FDE68A"
    elif "embargo" in vl:
        css = f"{_BB}background:#FEF3C7;color:#B45309;border:1px solid #FDE68A"
    elif "metadata" in vl:
        css = f"{_BB}background:#F3F4F6;color:#6B7280;border:1px solid #D1D5DB"
    elif "closed" in vl:
        css = f"{_BB}background:#FEE2E2;color:#991B1B;border:1px solid #FECACA"
    else:
        css = f"{_BB}background:#F9FAFB;color:#374151;border:1px solid #E5E7EB"
    return f'<span style="{css}">{_html.escape(v[:35])}</span>'


def _oa_cat_badge(advanced: str, basic: str) -> str:
    if not _nn(advanced):
        return ""
    a = str(advanced).strip()
    al = a.lower()
    if "gold" in al:
        css = f"{_BB}background:#FFFBEB;color:#92400E;border:1px solid #F59E0B"
    elif "green" in al:
        css = f"{_BB}background:#ECFDF5;color:#065F46;border:1px solid #34D399"
    elif "hybrid" in al:
        css = f"{_BB}background:#EFF6FF;color:#1E40AF;border:1px solid #93C5FD"
    elif "bronze" in al:
        css = f"{_BB}background:#FFF7ED;color:#9A3412;border:1px solid #FDBA74"
    elif "closed" in al or str(basic).strip().lower() == "closed":
        css = f"{_BB}background:#F3F4F6;color:#6B7280;border:1px solid #D1D5DB"
    else:
        css = f"{_BB}background:#F9FAFB;color:#374151;border:1px solid #E5E7EB"
    return f'<span style="{css}">{_html.escape(a[:40])}</span>'


def _chip_span(val, color: str = "#374151") -> str:
    if not _nn(val):
        return ""
    v = str(val).strip()
    # Shorten COAR version URIs
    if "coar" in v.lower() and "/" in v:
        _MAP = {
            "c_970fb48d4fbd8a85": "published",
            "c_ab4af688f83e57aa": "accepted",
            "c_71e4c1898caa6e32": "submitted",
        }
        code = v.rstrip("/").rsplit("/", 1)[-1]
        v = _MAP.get(code, code)
    return f'<span style="{_CHIP}color:{color}">{_html.escape(v[:30])}</span>'


def _gap_badge(existing_access: str | None, oa_basic: str | None) -> str:
    ea = (existing_access or "").lower()
    ob = (oa_basic or "").lower()
    if "metadata" in ea and ob == "open":
        return (
            '<span style="font-size:0.68rem;padding:2px 7px;border-radius:4px;'
            'font-weight:600;background:#FEF2F2;color:#DC2626;border:1px solid #FCA5A5">'
            '⚡ Gap OA</span>'
        )
    if "open" in ea and "metadata" not in ea and ob == "closed":
        return (
            '<span style="font-size:0.68rem;padding:2px 7px;border-radius:4px;'
            'font-weight:600;background:#FFFBEB;color:#92400E;border:1px solid #F59E0B">'
            '? Incohérence</span>'
        )
    return ""


def _dc_type_chip(dc_type: str | None) -> str:
    if not _nn(dc_type):
        return ""
    parts = str(dc_type).split("::")
    label = (parts[-1] if len(parts) > 1 else parts[0]).strip()[:28]
    return (
        f'<span style="font-size:0.68rem;padding:1px 6px;border-radius:3px;'
        f'background:#EFF6FF;color:#1E40AF;border:1px solid #BFDBFE">'
        f'{_html.escape(label)}</span>'
    )


def _fulltext_indicator(existing_access: str | None) -> tuple[str, str]:
    """Return (icon_text, color) indicating whether a fulltext exists in Infoscience."""
    ea = (existing_access or "").lower()
    if "open" in ea and "metadata" not in ea:
        return "✓ Document public", "#065F46"
    if "restrict" in ea:
        return "⚠ Document restreint", "#92400E"
    if "embargo" in ea:
        return "⌛ Sous embargo", "#B45309"
    if "metadata" in ea:
        return "✗ Pas de document", "#6B7280"
    return "", "#9CA3AF"


def _build_oa_row_html(row: dict) -> str:
    """Build rich HTML for a single enriched row (Infoscience vs OA enriched side-by-side)."""
    year = _sv(row, "pubyear", "—")
    title = _sv(row, "title", "—")
    doi = _sv(row, "doi")
    publishedin = _sv(row, "publishedin")

    # Infoscience metadata
    ifs_access = row.get("existing_access_level")
    ifs_license = _sv(row, "existing_license")
    ifs_version = _sv(row, "existing_version") or _sv(row, "legacy_version")
    ft_text, ft_color = _fulltext_indicator(ifs_access)

    # Enriched metadata
    oa_adv = _sv(row, "oa_category_advanced")
    oa_basic = _sv(row, "oa_category_basic")
    best_ver = _sv(row, "best_oa_version") or _sv(row, "resolved_version")
    best_lic = _sv(row, "best_oa_license") or _sv(row, "resolved_license")
    _DASH = '<span style="font-size:0.68rem;color:#9CA3AF">—</span>'

    # Assembled badge groups
    ifs_badges = " ".join(filter(None, [
        _ifs_access_badge(ifs_access),
        _chip_span(ifs_version) if ifs_version else "",
        _chip_span(ifs_license) if ifs_license else "",
    ]))
    enrich_badges = " ".join(filter(None, [
        _oa_cat_badge(oa_adv, oa_basic),
        _chip_span(best_ver) if best_ver else "",
        _chip_span(best_lic) if best_lic else "",
    ]))

    gap = _gap_badge(str(ifs_access) if ifs_access else None, oa_basic)
    type_chip = _dc_type_chip(row.get("dc_type"))

    # Footer
    handle = _sv(row, "handle")
    ifs_link = _ifs_url(handle)
    _lnk = "font-size:0.68rem;text-decoration:none;font-family:monospace"
    doi_html = (
        f'<a href="https://doi.org/{_html.escape(doi)}" target="_blank" '
        f'style="{_lnk};color:#6B7280">doi:{_html.escape(doi[:50])}</a>'
    ) if doi else ""
    ifs_html = (
        f'<a href="{ifs_link}" target="_blank" rel="noopener noreferrer" '
        f'style="{_lnk};color:#3B82F6">'
        f'Infoscience:{_html.escape(handle[:30])}</a>'
    ) if ifs_link else ""
    pub_html = (
        f'<span style="font-size:0.68rem;color:#9CA3AF">{_html.escape(publishedin[:55])}</span>'
    ) if publishedin else ""
    footer_parts = [p for p in [ifs_html, doi_html, pub_html] if p]
    sep = '<span style="color:#D1D5DB;font-size:0.68rem"> · </span>'
    footer_html = sep.join(footer_parts)

    ft_html = (
        f'<span style="font-size:0.65rem;color:{ft_color};margin-top:3px;display:block">'
        f'{ft_text}</span>'
    ) if ft_text else ""

    return (
        f'<div style="padding:6px 2px">'
        # Header: year · type · gap indicator
        f'<div style="display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-bottom:3px">'
        f'<span style="font-size:0.73rem;color:#9CA3AF;font-weight:500;min-width:2.5rem">'
        f'{_html.escape(year)}</span>'
        f'{type_chip}{gap}'
        f'</div>'
        # Title
        f'<div style="font-size:0.87rem;font-weight:600;color:#111827;margin-bottom:5px;line-height:1.3">'
        f'{_html.escape(title[:150])}{"…" if len(title) > 150 else ""}'
        f'</div>'
        # Two-column: Infoscience | Enrichi
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:4px">'
        # Infoscience column
        f'<div style="background:#F9FAFB;border-radius:5px;padding:5px 8px;border:1px solid #E5E7EB">'
        f'<div style="font-size:0.60rem;color:#9CA3AF;font-weight:700;margin-bottom:4px;'
        f'text-transform:uppercase;letter-spacing:0.05em">📚 Infoscience</div>'
        f'<div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center">'
        f'{ifs_badges if ifs_badges else _DASH}'
        f'</div>'
        f'{ft_html}'
        f'</div>'
        # Enriched column
        f'<div style="background:#F0FDF4;border-radius:5px;padding:5px 8px;border:1px solid #D1FAE5">'
        f'<div style="font-size:0.60rem;color:#9CA3AF;font-weight:700;margin-bottom:4px;'
        f'text-transform:uppercase;letter-spacing:0.05em">✨ Enrichi (OA)</div>'
        f'<div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center">'
        f'{enrich_badges if enrich_badges else _DASH}'
        f'</div>'
        f'</div>'
        f'</div>'
        # Footer: DOI · journal
        f'<div style="margin-top:2px">{footer_html}</div>'
        f'</div>'
    )


# ── OA detail modal ───────────────────────────────────────────────────────────

@st.dialog("Détails OA", width="large")
def _oa_detail_modal(row: dict) -> None:
    """Show all OA metadata fields for a single enriched item."""
    title = _sv(row, "title", "—")
    doi = _sv(row, "doi")
    handle = _sv(row, "handle")

    st.markdown(
        f'<div style="font-size:1rem;font-weight:700;color:#111827;margin-bottom:6px">'
        f'{_html.escape(title[:130])}</div>',
        unsafe_allow_html=True,
    )
    ifs_link = _ifs_url(handle)
    link_parts = []
    if ifs_link:
        label = f"↗ Infoscience `{handle}`" if handle else "↗ Infoscience"
        link_parts.append(f"[{label}]({ifs_link})")
    if doi:
        link_parts.append(f"[DOI: {doi}](https://doi.org/{doi})")
    if link_parts:
        st.markdown("  ·  ".join(link_parts))

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            '<div style="font-size:0.78rem;font-weight:700;color:#374151;margin-bottom:6px">'
            '📚 INFOSCIENCE</div>',
            unsafe_allow_html=True,
        )
        ifs_access = _sv(row, "existing_access_level")
        ft_text, ft_color = _fulltext_indicator(ifs_access or "")
        if ft_text:
            st.markdown(
                f'<span style="font-size:0.82rem;color:{ft_color};font-weight:600">'
                f'{ft_text}</span>',
                unsafe_allow_html=True,
            )
        for label, key in [
            ("Accès", "existing_access_level"),
            ("Licence", "existing_license"),
            ("Version", "existing_version"),
            ("Version (legacy)", "legacy_version"),
            ("Embargo", "embargo"),
            ("Type document", "dc_type"),
            ("Année publication", "pubyear"),
            ("Publié dans", "publishedin"),
            ("ISSN", "issn"),
            ("ISBN", "isbn"),
            ("UUID", "infoscience_uuid"),
        ]:
            v = _sv(row, key)
            if v:
                st.markdown(f"**{label}:** `{v}`")

    with col2:
        st.markdown(
            '<div style="font-size:0.78rem;font-weight:700;color:#374151;margin-bottom:6px">'
            '✨ ENRICHI (OA)</div>',
            unsafe_allow_html=True,
        )
        for label, key in [
            ("OA basique", "oa_category_basic"),
            ("OA avancé", "oa_category_advanced"),
            ("OpenAlex OA status", "oa_status"),
            ("Version retenue", "best_oa_version"),
            ("Licence retenue", "best_oa_license"),
            ("Version résolue", "resolved_version"),
            ("Licence résolue", "resolved_license"),
            ("Hébergeur OA", "best_oa_host_org"),
            ("Source primaire", "primary_source_type"),
            ("Dépôt OA ?", "oa_any_repository_has_fulltext"),
            ("UPW OA status", "upw_oa_status"),
            ("UPW licence", "upw_license"),
            ("UPW version", "upw_version"),
        ]:
            v = _sv(row, key)
            if v:
                st.markdown(f"**{label}:** `{v}`")

        # DOAJ — a first-class Gold signal (NOAM: "as defined by ... DOAJ"),
        # shown from both sources for traceability of the Gold decision.
        doaj_openalex = _bool_label(row.get("best_oa_is_in_doaj"))
        doaj_upw = _bool_label(row.get("upw_journal_is_in_doaj"))
        if doaj_openalex:
            st.markdown(f"**Dans DOAJ (OpenAlex):** `{doaj_openalex}`")
        if doaj_upw:
            st.markdown(f"**Dans DOAJ (Unpaywall):** `{doaj_upw}`")

        best_pdf = _sv(row, "best_oa_pdf_url")
        if best_pdf:
            label = best_pdf[:70] + "…" if len(best_pdf) > 70 else best_pdf
            st.markdown(f"**PDF URL:** [{label}]({best_pdf})")


# ── Raw data Excel export ─────────────────────────────────────────────────────

_EXCEL_MAX_CELL = 32767


def _truncate_str_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Clip every string column to Excel's 32767-character cell limit."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(
            lambda v: v[:_EXCEL_MAX_CELL] if isinstance(v, str) else v
        )
    return df


def _build_raw_excel(con, year_from: int, year_to: int) -> bytes:
    """Build a multi-sheet Excel with harvest, enriched, and gap analysis data."""
    import io  # pylint: disable=import-outside-toplevel

    available = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='enriched'"
    ).fetchall()}
    hav_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='harvest'"
    ).fetchall()}

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # Sheet 1: harvest (raw DSpace metadata)
        if hav_cols:
            df_h = con.execute(
                f"SELECT * EXCLUDE (_year_from, _year_to) FROM harvest "
                f"WHERE _year_from={year_from} AND _year_to={year_to}"
            ).df()
            if _nn_df(df_h, "handle"):
                df_h.insert(0, "infoscience_url", df_h["handle"].apply(_ifs_url))
            _truncate_str_cols(df_h).to_excel(writer, sheet_name="Harvest", index=False)

        # Sheet 2: enriched (all OA metadata)
        sel = [c for c in _OA_BROWSER_COLS if c in available]
        df_e = con.execute(
            f"SELECT {', '.join(sel)} FROM enriched "
            f"WHERE _year_from={year_from} AND _year_to={year_to} "
            f"ORDER BY pubyear DESC, title"
        ).df()
        if "handle" in df_e.columns:
            df_e.insert(0, "infoscience_url", df_e["handle"].apply(_ifs_url))
        _truncate_str_cols(df_e).to_excel(writer, sheet_name="Enriched", index=False)

        # Sheet 3: gap analysis — one row per item × gap type
        gap_frames: list[pd.DataFrame] = []
        for glabel, gsql in _GAP_PRESETS.items():
            if not gsql:
                continue
            df_g = con.execute(
                f"SELECT {', '.join(sel)} FROM enriched "
                f"WHERE _year_from={year_from} AND _year_to={year_to} AND ({gsql}) "
                f"ORDER BY pubyear DESC, title"
            ).df()
            if df_g.empty:
                continue
            df_g.insert(0, "gap_type", glabel)
            if "handle" in df_g.columns:
                df_g.insert(1, "infoscience_url", df_g["handle"].apply(_ifs_url))
            gap_frames.append(df_g)

        if gap_frames:
            _truncate_str_cols(pd.concat(gap_frames, ignore_index=True)).to_excel(
                writer, sheet_name="Gaps", index=False
            )

    buf.seek(0)
    return buf.read()


def _nn_df(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns


# ── OA Barometer ──────────────────────────────────────────────────────────────

def _render_barometer(root: Path, active_env: str) -> None:
    """Charts and KPIs derived from enriched data — OA barometer."""
    con = _open_db(root, active_env)
    if con is None:
        st.info("Aucune donnée enrichie disponible. Lancez d'abord un enrichissement OA.")
        return
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if "enriched" not in tables:
            st.info("Table enriched absente — lancez l'étape `enrich`.")
            return

        all_ranges = con.execute(
            "SELECT DISTINCT _year_from, _year_to FROM enriched ORDER BY 1, 2"
        ).fetchall()
        if not all_ranges:
            st.info("Aucune donnée enrichie.")
            return

        range_labels = [f"{yf}–{yt}" for yf, yt in all_ranges]
        sel = st.selectbox(
            "Plage d'années",
            range_labels,
            index=len(range_labels) - 1,
            key="baro_range",
        )
        sel_yf, sel_yt = all_ranges[range_labels.index(sel)]
        w = f"_year_from={sel_yf} AND _year_to={sel_yt}"

        available = {r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='enriched'"
        ).fetchall()}

        # ── KPIs globaux ──────────────────────────────────────────────────
        st.markdown(sh("bar_chart", "Vue globale"), unsafe_allow_html=True)

        total = con.execute(f"SELECT COUNT(*) FROM enriched WHERE {w}").fetchone()[0]
        open_n = con.execute(
            f"SELECT COUNT(*) FROM enriched WHERE {w} AND oa_category_basic = 'Open'"
        ).fetchone()[0]
        closed_n = total - open_n
        gaps_n = con.execute(
            f"SELECT COUNT(*) FROM enriched WHERE {w} AND ({_GAP_BASE})"
        ).fetchone()[0]
        has_collect_cols = "best_oa_is_oa" in available and "upw_is_oa" in available
        if has_collect_cols:
            collectable_n = con.execute(
                f"SELECT COUNT(*) FROM enriched WHERE {w}"
                f" AND ({_COLLECT_BASE})"
                f" AND ({_OA_IS_TRUE} OR {_UPW_IS_TRUE})"
            ).fetchone()[0]
        else:
            collectable_n = 0

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.metric("Publications", f"{total:,}")
        with c2:
            st.metric(
                "Open Access", f"{open_n:,}",
                delta=f"{open_n / total * 100:.0f} %" if total else None,
            )
        with c3:
            st.metric(
                "Fermé", f"{closed_n:,}",
                delta=f"-{closed_n / total * 100:.0f} %" if total else None,
                delta_color="inverse",
            )
        with c4:
            st.metric(
                "Gaps NOAM ⚡", f"{gaps_n:,}",
                help="Metadata-only dans Infoscience + OA enrichi (types NOAM)",
            )
        with c5:
            st.metric(
                "Collectables ⬇️", f"{collectable_n:,}",
                help="Metadata-only avec signal OA OpenAlex ou Unpaywall",
            )

        # ── Répartition OA avancée + taux OA par année ───────────────────
        st.divider()
        col_left, col_right = st.columns([2, 3])

        with col_left:
            st.markdown("**Répartition OA avancée**")
            df_cat = con.execute(
                f"SELECT oa_category_advanced AS Catégorie, COUNT(*) AS N"
                f" FROM enriched WHERE {w} AND oa_category_advanced IS NOT NULL"
                f" GROUP BY 1 ORDER BY N DESC"
            ).df()
            if not df_cat.empty:
                st.bar_chart(
                    df_cat.set_index("Catégorie"),
                    color="#3B82F6",
                    horizontal=True,
                    height=280,
                )

        with col_right:
            st.markdown("**Taux Open Access par année de publication (%)**")
            df_yr = con.execute(
                f"SELECT TRY_CAST(pubyear AS INTEGER) AS Année,"
                f" COUNT(*) AS Total,"
                f" SUM(CASE WHEN oa_category_basic = 'Open' THEN 1 ELSE 0 END) AS Open_n"
                f" FROM enriched WHERE {w} AND TRY_CAST(pubyear AS INTEGER) IS NOT NULL"
                f" GROUP BY 1 ORDER BY 1"
            ).df()
            if not df_yr.empty:
                df_yr["% OA"] = (df_yr["Open_n"] / df_yr["Total"] * 100).round(1)
                st.line_chart(
                    df_yr.set_index("Année")[["% OA"]],
                    color="#10B981",
                    height=280,
                )

        # ── Évolution par catégorie OA avancée ───────────────────────────
        st.divider()
        st.markdown("**Évolution des catégories OA avancées par année de publication**")
        df_adv = con.execute(
            f"SELECT TRY_CAST(pubyear AS INTEGER) AS Année,"
            f" oa_category_advanced AS Catégorie, COUNT(*) AS N"
            f" FROM enriched WHERE {w}"
            f" AND TRY_CAST(pubyear AS INTEGER) IS NOT NULL"
            f" AND oa_category_advanced IS NOT NULL"
            f" GROUP BY 1, 2 ORDER BY 1"
        ).df()
        if not df_adv.empty:
            df_adv_pivot = df_adv.pivot_table(
                index="Année", columns="Catégorie", values="N", fill_value=0,
            )
            ordered = [c for c in _OA_CAT_ORDER if c in df_adv_pivot.columns]
            others = [c for c in df_adv_pivot.columns if c not in _OA_CAT_ORDER]
            df_adv_pivot = df_adv_pivot[ordered + others]
            colors = [_OA_CAT_COLORS.get(c, "#6B7280") for c in df_adv_pivot.columns]
            st.bar_chart(df_adv_pivot, color=colors, height=320)

        # ── Collectabilité ────────────────────────────────────────────────
        if has_collect_cols and "oa_status" in available:
            st.divider()
            st.markdown(
                "**Collectabilité OA — items metadata-only avec signal OA natif**"
            )
            col_m, col_c = st.columns([1, 2])
            cb = _COLLECT_BASE

            with col_m:
                cg = con.execute(
                    f"SELECT COUNT(*) FROM enriched WHERE {w} AND ({cb})"
                    f" AND oa_status IN ('gold','diamond') AND {_OA_IS_TRUE}"
                ).fetchone()[0]
                ch = con.execute(
                    f"SELECT COUNT(*) FROM enriched WHERE {w} AND ({cb})"
                    f" AND oa_status = 'hybrid' AND {_OA_IS_TRUE}"
                ).fetchone()[0]
                cgr = con.execute(
                    f"SELECT COUNT(*) FROM enriched WHERE {w} AND ({cb})"
                    f" AND oa_status = 'green' AND {_OA_IS_TRUE}"
                ).fetchone()[0]
                cu = con.execute(
                    f"SELECT COUNT(*) FROM enriched WHERE {w} AND ({cb})"
                    f" AND {_UPW_IS_TRUE} AND ({_OA_IS_NOT_TRUE})"
                ).fetchone()[0]
                st.metric("Gold / Diamond", f"{cg:,}",
                          help="OpenAlex gold ou diamond, OA confirmé")
                st.metric("Hybrid", f"{ch:,}",
                          help="OpenAlex hybrid, OA confirmé")
                st.metric("Green", f"{cgr:,}",
                          help="OpenAlex green, OA confirmé")
                st.metric("Unpaywall seul", f"{cu:,}",
                          help="OA Unpaywall sans signal OpenAlex")

            with col_c:
                df_coll = con.execute(
                    f"SELECT COALESCE(oa_status, 'inconnu') AS Source, COUNT(*) AS N"
                    f" FROM enriched WHERE {w} AND ({cb})"
                    f" AND ({_OA_IS_TRUE} OR {_UPW_IS_TRUE})"
                    f" GROUP BY 1 ORDER BY N DESC"
                ).df()
                if not df_coll.empty:
                    _src_colors = {
                        "gold": "#F59E0B", "diamond": "#818CF8",
                        "hybrid": "#3B82F6", "green": "#10B981",
                        "bronze": "#F97316", "closed": "#9CA3AF",
                        "inconnu": "#D1D5DB",
                    }
                    _src_order = [
                        "gold", "diamond", "hybrid", "green",
                        "bronze", "closed", "inconnu",
                    ]
                    # Pivot: one column per oa_status → color list matches column count
                    df_coll_wide = df_coll.set_index("Source")["N"].to_frame().T
                    ordered = [c for c in _src_order if c in df_coll_wide.columns]
                    others = [c for c in df_coll_wide.columns if c not in _src_order]
                    df_coll_wide = df_coll_wide[ordered + others]
                    coll_colors = [
                        _src_colors.get(c, "#6B7280") for c in df_coll_wide.columns
                    ]
                    st.bar_chart(
                        df_coll_wide,
                        color=coll_colors,
                        horizontal=True,
                        height=160,
                    )

        # ── OA par type NOAM ──────────────────────────────────────────────
        st.divider()
        st.markdown("**Taux OA par type de ressource NOAM**")
        # Grouped by dc_type_authority (COAR code, language-independent) rather
        # than dc_type's free-text value — French- and English-labeled items
        # of the same underlying type must land in the same bar.
        df_type = con.execute(
            f"SELECT dc_type_authority, oa_category_basic AS OA, COUNT(*) AS N"
            f" FROM enriched WHERE {w} AND ({_NOAM_TYPES_SQL})"
            f" GROUP BY 1, 2"
        ).df()
        if not df_type.empty:
            df_type["Type"] = df_type["dc_type_authority"].apply(resolve_noam_type)
            df_type = df_type.groupby(["Type", "OA"], as_index=False)["N"].sum()
            df_type_pivot = df_type.pivot_table(
                index="Type", columns="OA", values="N", fill_value=0,
            )
            oa_cols = [c for c in ["Open", "Closed"] if c in df_type_pivot.columns]
            df_type_pivot = df_type_pivot[oa_cols]
            type_colors = [
                "#10B981" if c == "Open" else "#9CA3AF"
                for c in df_type_pivot.columns
            ]
            st.bar_chart(df_type_pivot, color=type_colors, horizontal=True, height=320)

        # ── Distribution des licences OA ──────────────────────────────────
        if "resolved_license" in available:
            st.divider()
            st.markdown("**Distribution des licences (publications Open Access)**")
            df_lic = con.execute(
                f"SELECT resolved_license AS Licence, COUNT(*) AS N"
                f" FROM enriched WHERE {w} AND oa_category_basic = 'Open'"
                f" AND resolved_license IS NOT NULL AND resolved_license != ''"
                f" GROUP BY 1 ORDER BY N DESC LIMIT 15"
            ).df()
            if not df_lic.empty:
                st.bar_chart(
                    df_lic.set_index("Licence"),
                    color="#10B981",
                    horizontal=True,
                    height=max(260, len(df_lic) * 32),
                )

    finally:
        con.close()


# ── Page entry point ──────────────────────────────────────────────────────────

def render(
    active_env: str = "dev",
    root: Path = Path("."),
    role: str = "reporting",
) -> None:
    page_title("monitoring", "OA Monitor")

    active = _read_active(active_env)

    tab_run, tab_baro, tab_results = st.tabs(["▶ Lancer", "📊 Baromètre", "📂 Résultats"])

    with tab_run:
        if role not in ("admin", "curator"):
            st.info("Vous avez accès en lecture seule. Contactez un admin pour lancer un enrichissement.")
        elif active:
            _render_running(active, active_env)
        else:
            _render_form(active_env, root)

    with tab_baro:
        _render_barometer(root, active_env)

    with tab_results:
        _render_results(root, active_env=active_env)


# ── Running view ──────────────────────────────────────────────────────────────

def _render_running(active: dict, env: str) -> None:
    log_file = Path(active.get("log_file", ""))

    st.success(f"⏳ Enrichissement OA en cours… (PID {active.get('pid')})")
    st.caption(f"Commande : `{active.get('cmd', '')}`")

    col_stop, _ = st.columns([1, 4])
    with col_stop:
        if st.button("⛔ Arrêter", type="secondary"):
            pid = active.get("pid")
            if pid:
                try:
                    os.kill(int(pid), 15)
                    time.sleep(1)
                except Exception:
                    pass
            _clear_state(env)
            st.warning("Signal d'arrêt envoyé.")
            time.sleep(1)
            st.rerun()

    st.markdown(sh("terminal", "Logs en direct"), unsafe_allow_html=True)
    log_box = st.empty()
    info_box = st.empty()

    while True:
        current = _read_active(env)
        if log_file.exists():
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
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

    # Final log snapshot
    if log_file.exists():
        lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
        log_box.markdown(
            f'<div class="log-console">{_html.escape(chr(10).join(lines[-300:]))}</div>',
            unsafe_allow_html=True,
        )

    step_done = active.get("step", "all")
    yf = active.get("year_from") or "?"
    yt = active.get("year_to") or "?"
    _NEXT: dict[str, str] = {
        "harvest":     f"→ Sélectionnez l'étape **enrich** avec les mêmes années ({yf}–{yt}) et relancez.",
        "enrich":      "→ Sélectionnez l'étape **export-noam** pour générer les fichiers NOAM.",
        "export-noam": "→ Les fichiers NOAM sont disponibles dans l'onglet **📂 Résultats**.",
        "all":         "→ Pipeline complet terminé. Consultez l'onglet **📂 Résultats**.",
        "inspect":     "→ Consultez les logs ci-dessus pour le résumé de la DB.",
        "consolidate-rap": (
            "→ Sélectionnez l'étape **rap-gaps** pour croiser avec Infoscience "
            "(après un `harvest`+`enrich` sur les années couvertes), ou consultez "
            "directement la section **Suivi R&P** dans l'onglet **📂 Résultats**."
        ),
        "rap-gaps": "→ Consultez la section **Suivi R&P** dans l'onglet **📂 Résultats**.",
    }
    st.success(f"✅ Étape **{step_done}** terminée !")
    st.info(_NEXT.get(step_done, "→ Consultez l'onglet **📂 Résultats**."))
    time.sleep(3)
    st.rerun()


# ── Launch form ───────────────────────────────────────────────────────────────

_STEP_HELP = (
    "**all** — harvest + enrichissement + export (pipeline complet)\n\n"
    "**harvest** — extraction DSpace → DB::harvest\n\n"
    "**enrich** — DB::harvest → OpenAlex + Unpaywall + classification → DB::enriched\n\n"
    "**export-noam** — DB::enriched → CSV + Excel NOAM\n\n"
    "**inspect** — résumé des données disponibles dans la DB\n\n"
    "**consolidate-rap** — consolide les suivis R&P (data/apc) → DB::rap_tracking\n\n"
    "**rap-gaps** — croise rap_tracking × enriched (toutes années) → DB::rap_gap_analysis"
)

# Steps operating on a fixed Infoscience year range (require Période/Périmètre).
_YEAR_RANGE_STEPS = ("all", "harvest", "enrich", "export-noam", "inspect")


def _render_form(active_env: str, root: Path) -> None:
    st.markdown(
        "Configure et lance l'enrichissement OA sur les publications Infoscience existantes, "
        "ou la consolidation du suivi R&P (APC)."
    )

    # ── Étape — outside form: its value drives which fields below are shown ──
    st.markdown(sh("route", "Étape"), unsafe_allow_html=True)
    step = st.selectbox(
        "Étape",
        options=["all", "harvest", "enrich", "export-noam", "inspect",
                 "consolidate-rap", "rap-gaps"],
        index=0,
        help=_STEP_HELP,
        key="oa_monitor_step",
    )
    needs_year_range = step in _YEAR_RANGE_STEPS

    year_from = year_to = None
    extra_filter = ""
    apc_root = "data/apc"

    if needs_year_range:
        # ── Période — outside form for immediate rerun ────────────────────────
        st.markdown(sh("date_range", "Période"), unsafe_allow_html=True)
        col_y1, col_y2 = st.columns(2)
        current_year = datetime.today().year
        with col_y1:
            year_from = st.number_input(
                "Année de début", min_value=2000, max_value=current_year,
                value=current_year - 1, step=1,
            )
        with col_y2:
            year_to = st.number_input(
                "Année de fin", min_value=2000, max_value=current_year,
                value=current_year, step=1,
            )

        if year_from > year_to:
            st.error("L'année de début doit être ≤ l'année de fin.")
            return

        # ── Périmètre — preset + Solr override ───────────────────────────────
        st.markdown(sh("filter_alt", "Périmètre"), unsafe_allow_html=True)
        preset_label = st.selectbox(
            "Sélection rapide",
            options=list(_QUERY_PRESETS.keys()),
            index=0,
            help="Filtre pré-configuré appliqué à la requête DSpace.",
        )
        preset_filter = _QUERY_PRESETS[preset_label]

        with st.expander("Filtre Solr avancé", expanded=bool(preset_filter)):
            extra_filter = st.text_input(
                "Filtre additionnel (ajouté à la requête de base)",
                value=preset_filter,
                placeholder='ex : dc.type:"text::journal::journal article"',
                help=(
                    "Syntaxe Solr/Lucene. Ajouté avec AND à la requête de base. "
                    "Modifiez ici pour affiner au-delà des présélections."
                ),
            )
    elif step == "consolidate-rap":
        st.markdown(sh("folder_open", "Source"), unsafe_allow_html=True)
        apc_root = st.text_input(
            "Répertoire des suivis R&P",
            value="data/apc",
            help="Arborescence publisher/contrat/*.xlsx à consolider (voir oa_monitor/rap_consolidator.py).",
        )
    else:  # rap-gaps — no extra input, joins existing rap_tracking × enriched

        st.info(
            "Croise `rap_tracking` (toutes les années) avec `enriched` (toutes les plages "
            "déjà harvestées/enrichies). Lancez `consolidate-rap` puis `harvest`+`enrich` "
            "au préalable si ce n'est pas déjà fait."
        )

    # ── Formulaire ────────────────────────────────────────────────────────────
    with st.form("oa_monitor_form"):
        if needs_year_range:
            st.markdown(sh("tune", "Options"), unsafe_allow_html=True)
            col_o2, col_o3 = st.columns(2)
            with col_o2:
                no_unpaywall = st.checkbox(
                    "Sans Unpaywall",
                    value=False,
                    help="Ignore l'enrichissement Unpaywall (plus rapide, moins complet).",
                )
            with col_o3:
                verbose = st.checkbox("Verbose (-vv)", value=False)
        else:
            no_unpaywall = False
            verbose = st.checkbox("Verbose (-vv)", value=False)

        st.markdown(sh("folder_open", "Répertoires"), unsafe_allow_html=True)
        if needs_year_range:
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                work_dir = st.text_input(
                    "Dossier de travail (checkpoints DuckDB)",
                    value=_DEFAULT_WORK_DIR,
                    help="Contient oa_monitor_{env}.duckdb — tables harvest et enriched pour enchaîner les étapes.",
                )
            with col_d2:
                output_dir = st.text_input(
                    "Dossier NOAM (export final)",
                    value=_DEFAULT_OUTPUT_DIR,
                    help="Chemin où seront écrits les CSV et l'Excel NOAM.",
                )
        else:
            work_dir = st.text_input(
                "Dossier de travail (checkpoints DuckDB)",
                value=_DEFAULT_WORK_DIR,
                help="Contient oa_monitor_{env}.duckdb — tables rap_tracking et rap_gap_analysis.",
            )
            output_dir = _DEFAULT_OUTPUT_DIR

        if needs_year_range:
            with st.expander("Paramètres avancés", expanded=False):
                col_b1, col_b2, col_b3 = st.columns(3)
                with col_b1:
                    batch_size = st.number_input(
                        "Taille de batch OpenAlex", min_value=10, max_value=200,
                        value=100, step=10,
                        help="Nombre de DOIs par requête OpenAlex. 100 recommandé avec token premium.",
                    )
                with col_b2:
                    upw_workers = st.number_input(
                        "Workers Unpaywall", min_value=1, max_value=20,
                        value=5, step=1,
                        help="Nombre de requêtes Unpaywall parallèles.",
                    )
                with col_b3:
                    max_items = st.number_input(
                        "Limite harvest (0 = illimité)",
                        min_value=0, max_value=100_000, value=0, step=100,
                        help=(
                            "Cap le nombre d'items harvestés. "
                            "0 = pas de limite (production). "
                            "Utile pour tester sur un sous-ensemble."
                        ),
                    )
        else:
            batch_size, upw_workers, max_items = 100, 5, 0

        submitted = st.form_submit_button("▶ Lancer", type="primary")

    if not submitted:
        return

    # ── Build command ─────────────────────────────────────────────────────────
    run_id = f"oa_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{run_id}.log"

    cmd = [
        sys.executable,
        str(root / "oa_monitor" / "run_oa_enricher.py"),
        "--env",      active_env,
        "--step",     step,
        "--work-dir", work_dir,
    ]
    if step == "consolidate-rap":
        cmd += ["--apc-root", apc_root]
    elif needs_year_range:
        cmd += [
            "--year-from",   str(int(year_from)),
            "--year-to",     str(int(year_to)),
            "--output-dir",  output_dir,
            "--batch-size",  str(int(batch_size)),
            "--upw-workers", str(int(upw_workers)),
        ]
        if extra_filter.strip():
            cmd += ["--extra-filter", extra_filter.strip()]
        if int(max_items) > 0:
            cmd += ["--max-items", str(int(max_items))]
        if no_unpaywall:
            cmd.append("--no-unpaywall")
    # rap-gaps needs no further arguments.
    if verbose:
        cmd.append("-vv")

    st.code(" ".join(cmd), language="bash")

    log_fh = open(log_file, "w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        cwd=str(root),
        env={**os.environ},
    )
    log_fh.close()

    _write_state(active_env, {
        "pid":        proc.pid,
        "run_id":     run_id,
        "started_at": datetime.now().isoformat(),
        "log_file":   str(log_file),
        "cmd":        " ".join(cmd),
        "year_from":  int(year_from) if year_from is not None else None,
        "year_to":    int(year_to) if year_to is not None else None,
        "step":       step,
    })

    # Reap the child process when it exits so the zombie doesn't fool _read_active
    # (os.kill(zombie, 0) succeeds on macOS — proc.wait() is the reliable check).
    threading.Thread(
        target=lambda: (proc.wait(), _clear_state(active_env)),
        daemon=True,
    ).start()

    st.rerun()


# ── DuckDB connection helper ──────────────────────────────────────────────────

def _open_db(root: Path, env: str):
    """Return a read-only DuckDB connection to the OA Monitor DB, or None if absent."""
    try:
        import duckdb  # pylint: disable=import-outside-toplevel
    except ImportError:
        return None
    db = root / _DEFAULT_WORK_DIR / f"oa_monitor_{env}.duckdb"
    if not db.exists():
        return None
    try:
        return duckdb.connect(str(db), read_only=True)
    except Exception:
        return None


# ── Results view ──────────────────────────────────────────────────────────────

def _render_results(root: Path, active_env: str = "dev") -> None:
    # Execute a pending purge BEFORE opening the read-only connection — DuckDB
    # forbids opening a write connection while a read-only one is already open.
    pending = st.session_state.pop("_pending_purge", None)
    if pending:
        db_path_str = pending.get("db_path", "")
        yf, yt = pending.get("yf"), pending.get("yt")
        parts: list[str] = []
        if db_path_str and Path(db_path_str).exists():
            with st.spinner("Suppression des données DuckDB…"):
                totals = _purge_oa_data(Path(db_path_str), yf, yt)
            harvest_n = totals.get("harvest", 0)
            enriched_n = totals.get("enriched", 0)
            parts.append(f"{harvest_n:,} lignes harvest + {enriched_n:,} lignes enriched")
        if pending.get("delete_noam") and pending.get("noam_dir"):
            with st.spinner("Suppression des exports NOAM…"):
                noam_totals = _purge_noam_files(Path(pending["noam_dir"]), yf, yt)
            csv_n = noam_totals.get("csv", 0)
            excel_n = noam_totals.get("excel", 0)
            parts.append(f"{csv_n} CSV + {excel_n} Excel NOAM")
        if parts:
            st.success("Suppression terminée : " + " · ".join(parts) + ".")

    pending_rap = st.session_state.pop("_pending_rap_purge", None)
    if pending_rap:
        db_path_str = pending_rap.get("db_path", "")
        if db_path_str and Path(db_path_str).exists():
            with st.spinner("Suppression des tables R&P…"):
                totals = _purge_rap_data(Path(db_path_str))
            rap_n = totals.get("rap_tracking", 0)
            gap_n = totals.get("rap_gap_analysis", 0)
            st.success(
                f"Suppression terminée : {rap_n:,} lignes rap_tracking + "
                f"{gap_n:,} lignes rap_gap_analysis. Les fichiers source dans "
                f"data/apc/ n'ont pas été touchés."
            )

    con = _open_db(root, active_env)

    st.markdown(sh("storage", "Base OA Monitor"), unsafe_allow_html=True)

    if con is None:
        st.info(
            "Aucune donnée trouvée. Lancez **▶ Lancer** avec l'étape `harvest` pour commencer."
        )
    else:
        try:
            _render_db(con, root, active_env)
            st.divider()
            _render_rap_section(con, root, active_env)
        finally:
            con.close()

    # ── Exports NOAM ─────────────────────────────────────────────────────────
    noam_dir = root / _DEFAULT_OUTPUT_DIR
    xlsx_files = sorted(noam_dir.glob("*_NOAM_repository_survey_*.xlsx"), reverse=True) \
        if noam_dir.exists() else []
    csv_files = sorted(noam_dir.glob("*_repo-data_dataset_epfl_*.csv"), reverse=True) \
        if noam_dir.exists() else []

    if not xlsx_files and not csv_files:
        return

    st.markdown(sh("download", "Exports NOAM"), unsafe_allow_html=True)

    if xlsx_files:
        st.markdown("**Excel NOAM**")
        for f in xlsx_files[:5]:
            col_name, col_dl = st.columns([5, 1])
            with col_name:
                st.caption(f.name)
            with col_dl:
                st.download_button(
                    "⬇",
                    data=f.read_bytes(),
                    file_name=f.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_xlsx_{f.name}",
                )

    if csv_files:
        st.markdown("**CSV par année**")
        for f in csv_files[:20]:
            col_name, col_dl = st.columns([5, 1])
            with col_name:
                st.caption(f.name)
            with col_dl:
                st.download_button(
                    "⬇",
                    data=f.read_bytes(),
                    file_name=f.name,
                    mime="text/csv",
                    key=f"dl_csv_{f.name}",
                )


def _purge_oa_data(db_path: Path, year_from: int | None, year_to: int | None) -> dict[str, int]:
    """Delete harvest + enriched rows.

    If year_from/year_to are None → delete everything.
    Returns {table: rows_deleted}.
    """
    import duckdb  # pylint: disable=import-outside-toplevel
    totals: dict[str, int] = {}
    con = duckdb.connect(str(db_path))
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in ("harvest", "enriched"):
            if table not in tables:
                totals[table] = 0
                continue
            if year_from is None:
                deleted = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                con.execute(f"DELETE FROM {table}")
            else:
                deleted = con.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE _year_from = {year_from} AND _year_to = {year_to}"
                ).fetchone()[0]
                con.execute(
                    f"DELETE FROM {table} "
                    f"WHERE _year_from = {year_from} AND _year_to = {year_to}"
                )
            totals[table] = deleted
    finally:
        con.close()
    return totals


def _purge_rap_data(db_path: Path) -> dict[str, int]:
    """Drop rap_tracking + rap_gap_analysis. Never touches data/apc source files.

    Both tables are dropped outright (not just emptied) so a subsequent
    consolidation clearly regenerates the table rather than silently
    operating on stale/empty rows. Returns {table: rows_deleted}.
    """
    import duckdb  # pylint: disable=import-outside-toplevel
    totals: dict[str, int] = {}
    con = duckdb.connect(str(db_path))
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in ("rap_tracking", "rap_gap_analysis"):
            if table not in tables:
                totals[table] = 0
                continue
            deleted = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(f"DROP TABLE {table}")
            totals[table] = deleted
    finally:
        con.close()
    return totals


def _purge_noam_files(
    noam_dir: Path,
    year_from: int | None,
    year_to: int | None,
) -> dict[str, int]:
    """Delete NOAM export files for the given year range (or all if year_from is None).

    CSV naming: {year}_repo-data_dataset_epfl_{date}.csv  (one file per year)
    Excel naming: {yf}_{yt}_NOAM_repository_survey_{date}.xlsx  (one per export run)

    Returns {"csv": n, "excel": n}.
    """
    if not noam_dir.exists():
        return {"csv": 0, "excel": 0}

    csv_n = 0
    excel_n = 0

    if year_from is None:
        for f in noam_dir.glob("*_repo-data_dataset_epfl_*.csv"):
            f.unlink()
            csv_n += 1
        for f in noam_dir.glob("*_NOAM_repository_survey_*.xlsx"):
            f.unlink()
            excel_n += 1
    else:
        valid_years = set(range(year_from, year_to + 1))
        for f in noam_dir.glob("*_repo-data_dataset_epfl_*.csv"):
            try:
                if int(f.name.split("_")[0]) in valid_years:
                    f.unlink()
                    csv_n += 1
            except (ValueError, IndexError):
                pass
        for f in noam_dir.glob(f"{year_from}_{year_to}_NOAM_repository_survey_*.xlsx"):
            f.unlink()
            excel_n += 1

    return {"csv": csv_n, "excel": excel_n}


def _count_noam_files(
    noam_dir: Path,
    year_from: int | None,
    year_to: int | None,
) -> tuple[int, int]:
    """Count NOAM files that would be deleted (csv_count, excel_count)."""
    if not noam_dir.exists():
        return 0, 0
    if year_from is None:
        csv_n = sum(1 for _ in noam_dir.glob("*_repo-data_dataset_epfl_*.csv"))
        excel_n = sum(1 for _ in noam_dir.glob("*_NOAM_repository_survey_*.xlsx"))
    else:
        valid_years = set(range(year_from, year_to + 1))
        csv_n = sum(
            1 for f in noam_dir.glob("*_repo-data_dataset_epfl_*.csv")
            if _try_year(f.name) in valid_years
        )
        excel_n = sum(
            1 for _ in noam_dir.glob(f"{year_from}_{year_to}_NOAM_repository_survey_*.xlsx")
        )
    return csv_n, excel_n


def _try_year(filename: str) -> int | None:
    try:
        return int(filename.split("_")[0])
    except (ValueError, IndexError):
        return None


def _render_purge(
    con,
    db_path: Path,
    noam_dir: Path,
    sel_yf: int,
    sel_yt: int,
    all_ranges: list[tuple[int, int]],
) -> None:
    """Render the data purge UI section (inside an expander).

    Uses the already-open read-only `con` for count queries.  The actual
    deletion is deferred to the next Streamlit rerun via session_state so
    that `con` is closed before a write connection is opened.
    """
    st.warning(
        "⚠️ Cette action supprime définitivement les données de la base OA Monitor. "
        "Elle est irréversible.",
        icon=None,
    )

    scope = st.radio(
        "Portée de la suppression",
        options=[f"Plage sélectionnée ({sel_yf}–{sel_yt})", "Toutes les données"],
        key="purge_scope",
        horizontal=True,
    )
    purge_all = scope == "Toutes les données"

    yf = None if purge_all else sel_yf
    yt = None if purge_all else sel_yt

    if purge_all:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        total_rows = sum(
            con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("harvest", "enriched") if t in tables
        )
        st.caption(
            f"Supprimera **toutes les données** : {len(all_ranges)} plage(s) d'années, "
            f"≈ {total_rows:,} lignes au total (harvest + enriched)."
        )
    else:
        st.caption(
            f"Supprimera les données pour **{sel_yf}–{sel_yt}** "
            "dans les tables harvest et enriched."
        )

    # NOAM export files option
    csv_count, excel_count = _count_noam_files(noam_dir, yf, yt)
    noam_label = (
        f"Supprimer aussi les exports NOAM ({csv_count} CSV, {excel_count} Excel)"
        if (csv_count or excel_count)
        else "Supprimer aussi les exports NOAM (aucun fichier trouvé)"
    )
    delete_noam = st.checkbox(
        noam_label,
        key="purge_noam",
        value=False,
        disabled=(csv_count == 0 and excel_count == 0),
    )

    confirmed = st.checkbox(
        "Je confirme la suppression irréversible",
        key="purge_confirm",
        value=False,
    )
    purge_btn = st.button(
        "🗑 Supprimer",
        disabled=not confirmed,
        type="primary",
        key="purge_execute",
    )

    if purge_btn and confirmed:
        # Defer execution: the read-only `con` must be closed before a write
        # connection can be opened on the same DuckDB file.
        st.session_state["_pending_purge"] = {
            "db_path": str(db_path),
            "yf": yf,
            "yt": yt,
            "delete_noam": delete_noam,
            "noam_dir": str(noam_dir),
        }
        st.rerun()


def _render_db(con, root: Path, active_env: str) -> None:
    """Render the DB monitoring section given an open read-only DuckDB connection."""
    db_path = root / _DEFAULT_WORK_DIR / f"oa_monitor_{active_env}.duckdb"
    size_kb = db_path.stat().st_size // 1024
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}

    # ── Available year ranges ─────────────────────────────────────────────────
    all_ranges: list[tuple[int, int]] = []
    for tbl in ("harvest", "enriched"):
        if tbl in tables:
            rows = con.execute(
                f"SELECT DISTINCT _year_from, _year_to FROM {tbl} ORDER BY 1, 2"
            ).fetchall()
            for yf, yt in rows:
                if (yf, yt) not in all_ranges:
                    all_ranges.append((yf, yt))

    if not all_ranges:
        st.caption(f"DB présente ({size_kb:,} KB) mais aucune donnée encore chargée.")
        return

    st.caption(f"DB : `{db_path.name}`  —  {size_kb:,} KB")

    range_labels = [f"{yf}–{yt}" for yf, yt in all_ranges]
    sel_label = st.selectbox(
        "Plage d'années",
        options=range_labels,
        index=len(range_labels) - 1,
        key="oa_db_range",
    )
    sel_yf, sel_yt = all_ranges[range_labels.index(sel_label)]

    # ── Status metrics ────────────────────────────────────────────────────────
    col_h, col_e, _ = st.columns([1, 1, 3])
    for tbl, col, label in [("harvest", col_h, "Harvest"), ("enriched", col_e, "Enriched")]:
        with col:
            if tbl in tables:
                n = con.execute(
                    f"SELECT COUNT(*) FROM {tbl} WHERE _year_from={sel_yf} AND _year_to={sel_yt}"
                ).fetchone()[0]
                st.metric(label, f"{n:,}" if n else "—", delta="✓" if n else None)
            else:
                st.metric(label, "—")

    if "enriched" not in tables:
        st.info(
            "Le harvest est disponible. "
            "Retournez dans **▶ Lancer**, sélectionnez l'étape `enrich` avec les mêmes années, et lancez."
        )
        return

    n_enriched = con.execute(
        f"SELECT COUNT(*) FROM enriched WHERE _year_from={sel_yf} AND _year_to={sel_yt}"
    ).fetchone()[0]
    if n_enriched == 0:
        st.info(
            "Aucune donnée enrichie pour cette plage. "
            "Retournez dans **▶ Lancer** et sélectionnez l'étape `enrich`."
        )
        return

    # ── OA breakdown ──────────────────────────────────────────────────────────
    st.markdown("**Répartition OA**")
    breakdown_rows = con.execute(
        f"SELECT oa_category_advanced, oa_category_basic, COUNT(*) AS n "
        f"FROM enriched WHERE _year_from={sel_yf} AND _year_to={sel_yt} "
        f"GROUP BY 1, 2 ORDER BY n DESC"
    ).df()

    open_n = int(breakdown_rows.loc[
        breakdown_rows["oa_category_basic"] == "Open", "n"
    ].sum())
    closed_n = n_enriched - open_n

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total", f"{n_enriched:,}")
    with c2:
        st.metric("Open Access", f"{open_n:,}",
                  delta=f"{open_n/n_enriched*100:.0f}%" if n_enriched else None)
    with c3:
        st.metric("Fermé", f"{closed_n:,}",
                  delta=f"-{closed_n/n_enriched*100:.0f}%" if n_enriched else None,
                  delta_color="inverse")

    disp = breakdown_rows[["oa_category_advanced", "n"]].rename(
        columns={"oa_category_advanced": "Catégorie OA avancée", "n": "N"}
    )
    st.dataframe(disp, width="stretch", hide_index=True)

    # ── Export données brutes ─────────────────────────────────────────────────
    # st.download_button's data= is evaluated on every rerun even when not
    # clicked. _build_raw_excel() writes ~3 full sheets via openpyxl (measured:
    # 13+ seconds on a 22k-row production range, dominated by openpyxl's
    # per-cell writes) — computing it unconditionally made every single filter
    # click / page turn on this tab pay that cost. Compute once on explicit
    # request instead, cached in session_state per (env, year range).
    col_dl, _ = st.columns([2, 5])
    with col_dl:
        fname = f"oa_raw_{active_env}_{sel_yf}-{sel_yt}.xlsx"
        cache_key = f"_raw_excel_bytes_{active_env}_{sel_yf}_{sel_yt}"
        if st.button("📄 Préparer l'export Excel brut", key=f"raw_excel_prepare_{sel_yf}_{sel_yt}"):
            try:
                with st.spinner("Génération de l'export Excel (peut prendre plusieurs secondes)…"):
                    st.session_state[cache_key] = _build_raw_excel(con, sel_yf, sel_yt)
            except Exception as exc:
                st.caption(f"Export indisponible : {exc}")
        raw_bytes = st.session_state.get(cache_key)
        if raw_bytes:
            st.download_button(
                label="⬇ Télécharger l'export brut (Excel)",
                data=raw_bytes,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_raw_{sel_yf}_{sel_yt}",
                help="Feuilles : Harvest · Enriched · Gaps (toutes les incohérences détectées)",
            )

    # ── Purge données ─────────────────────────────────────────────────────────
    noam_dir = root / _DEFAULT_OUTPUT_DIR
    with st.expander("🗑 Vider les données", expanded=False):
        _render_purge(con, db_path, noam_dir, sel_yf, sel_yt, all_ranges)

    # ── Data browser ──────────────────────────────────────────────────────────
    with st.expander("Parcourir les données enrichies", expanded=False):
        _render_oa_browser(con, sel_yf, sel_yt)


# ── Rich OA data browser ──────────────────────────────────────────────────────

_OA_BROWSER_COLS = [
    "infoscience_uuid", "handle", "doi", "title", "pubyear", "dc_type",
    "existing_access_level", "existing_version", "legacy_version",
    "existing_license", "embargo", "journal", "publishedin", "series", "issn", "isbn",
    "oa_status", "oa_is_oa", "oa_any_repository_has_fulltext",
    "best_oa_is_in_doaj",
    "best_oa_version", "best_oa_license", "best_oa_pdf_url", "best_oa_host_org",
    "primary_source_type",
    "upw_is_oa", "upw_oa_status", "upw_license", "upw_version",
    "upw_journal_is_oa", "upw_journal_is_in_doaj",
    "resolved_version", "resolved_license",
    "oa_category_basic", "oa_category_advanced",
]


_GAP_COLORS: dict[str, tuple[str, str]] = {
    "⚡ Gap OA : metadata-only + OA enrichi":          ("#FEF2F2", "#DC2626"),
    "? Incohérence : open access + Fermé enrichi":     ("#FFFBEB", "#92400E"),
    "📋 Open access IFS sans licence":                  ("#EFF6FF", "#1D4ED8"),
    "⌛ Embargo + OA Gold enrichi":                    ("#F5F3FF", "#6D28D9"),
}

_TABLE_COLS: list[tuple[str, str]] = [
    ("pubyear",               "Année"),
    ("title",                 "Titre"),
    ("existing_access_level", "Accès IFS"),
    ("existing_license",      "Licence IFS"),
    ("existing_version",      "Version IFS"),
    ("embargo",               "Embargo"),
    ("oa_category_advanced",  "OA enrichi"),
    ("resolved_version",      "Version OA"),
    ("resolved_license",      "Licence OA"),
    ("publishedin",           "Revue"),
    ("doi",                   "DOI"),
    ("handle",                "Handle"),
]


def _render_oa_browser(con, year_from: int, year_to: int) -> None:
    """Rich filterable OA browser with Infoscience vs enriched metadata side-by-side."""

    available = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='enriched'"
    ).fetchall()}

    base_where = f"_year_from={year_from} AND _year_to={year_to}"

    # ── Gap KPI tiles ─────────────────────────────────────────────────────────
    # One conditional-aggregation query instead of one COUNT(*) round trip per
    # preset — same result, a single scan of `enriched`.
    gap_items = [(glabel, gsql) for glabel, gsql in _GAP_PRESETS.items() if gsql]
    gap_counts: dict[str, int] = {}
    if gap_items:
        select_parts = ", ".join(
            f"SUM(CASE WHEN ({gsql}) THEN 1 ELSE 0 END) AS c{i}"
            for i, (_, gsql) in enumerate(gap_items)
        )
        gap_row = con.execute(f"SELECT {select_parts} FROM enriched WHERE {base_where}").fetchone()
        gap_counts = {glabel: int(gap_row[i] or 0) for i, (glabel, _) in enumerate(gap_items)}

    # ── Filter option lists ───────────────────────────────────────────────────
    # One list(DISTINCT ...) aggregation query instead of 7 separate
    # SELECT DISTINCT round trips. MIN/MAX(pubyear) (for the year-range slider
    # bounds) is folded into the same scan rather than a separate round trip.
    # dc_type_authority (not dc_type) drives the "Type de document" filter —
    # dc_type's free-text value is locale-dependent (English/French duplicates
    # for the same COAR type); the authority code is language-independent.
    _FILTER_COLS = [
        "existing_access_level", "oa_category_advanced", "dc_type_authority",
        "existing_license", "existing_version", "resolved_license", "resolved_version",
    ]
    filter_select = ", ".join(
        f"list(DISTINCT {c}) FILTER (WHERE {c} IS NOT NULL) AS {c}"
        for c in _FILTER_COLS
    )
    filter_select += (
        ", MIN(TRY_CAST(pubyear AS INTEGER)) AS pubyear_min, "
        "MAX(TRY_CAST(pubyear AS INTEGER)) AS pubyear_max"
    )
    filter_row = con.execute(f"SELECT {filter_select} FROM enriched WHERE {base_where}").fetchone()
    filter_options = dict(zip(_FILTER_COLS, filter_row[:len(_FILTER_COLS)]))
    for col in _FILTER_COLS:
        filter_options[col] = sorted(filter_options[col] or [])
    py_min_raw, py_max_raw = filter_row[len(_FILTER_COLS):]
    py_min = int(py_min_raw) if py_min_raw is not None else None
    py_max = int(py_max_raw) if py_max_raw is not None else None

    # ── Human-readable option labels for raw code/URI-valued columns ─────────
    # "Type de document" — authority code -> readable COAR type label.
    type_label_to_authority: dict[str, list[str]] = {}
    for _auth in filter_options["dc_type_authority"]:
        _label = label_for_authority(_auth) or _auth
        type_label_to_authority.setdefault(_label, []).append(_auth)
    type_label_opts = sorted(type_label_to_authority)

    # "Version IFS" — COAR version URI -> readable version name.
    version_label_to_raw: dict[str, list[str]] = {}
    for _raw in filter_options["existing_version"]:
        _label = COAR_VERSION_MAP.get(_raw, _raw)
        version_label_to_raw.setdefault(_label, []).append(_raw)
    version_label_opts = sorted(version_label_to_raw)

    any_gap = any(v > 0 for v in gap_counts.values())
    if any_gap:
        st.markdown(
            '<div style="font-size:0.72rem;font-weight:700;color:#6B7280;'
            'text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px">'
            'Écarts détectés</div>',
            unsafe_allow_html=True,
        )
        tile_cols = st.columns(len(gap_counts))
        for col_idx, (glabel, gcount) in enumerate(gap_counts.items()):
            bg, fg = _GAP_COLORS.get(glabel, ("#F9FAFB", "#374151"))
            with tile_cols[col_idx]:
                clicked = st.button(
                    f"{gcount:,}",
                    key=f"gap_tile_{col_idx}",
                    help=f"Filtrer : {glabel}",
                    width="stretch",
                )
                st.markdown(
                    f'<div style="font-size:0.63rem;color:{fg};background:{bg};'
                    f'border-radius:4px;padding:2px 4px;text-align:center;margin-top:2px;'
                    f'line-height:1.3">{_html.escape(glabel)}</div>',
                    unsafe_allow_html=True,
                )
                if clicked:
                    st.session_state["br_gap"] = glabel
                    st.rerun()

    # ── View toggle ───────────────────────────────────────────────────────────
    view_col, _ = st.columns([2, 5])
    with view_col:
        view_mode = st.radio(
            "Vue",
            options=["🃏 Visuel", "📋 Tableau"],
            horizontal=True,
            key="br_view",
            label_visibility="collapsed",
        )

    # ── Filter panel ──────────────────────────────────────────────────────────
    _BR_FILTER_KEYS = [
        "br_pubyear_range", "br_ifs", "br_oa", "br_gap", "br_collect_gap", "br_type",
        "br_lic_ifs", "br_ver_ifs", "br_doi_presence",
        "br_lic_oa", "br_ver_oa", "br_doi", "br_title",
    ]

    with st.container():
        st.markdown('<span class="oabr-filter-anchor"></span>', unsafe_allow_html=True)
        st.markdown(sh("filter_alt", "Filtres"), unsafe_allow_html=True)

        # ── Group 0 — publication year range ─────────────────────────────────
        st.markdown(sh("date_range", "Année de publication"), unsafe_allow_html=True)
        if py_min is not None and py_max is not None and py_min < py_max:
            # Initialize session state before the widget (same pattern as br_gap
            # above) — pre-seed / clamp so a stale range from a previously
            # selected DB partition never falls outside the current bounds.
            cur_py_range = st.session_state.get("br_pubyear_range")
            if (
                not isinstance(cur_py_range, tuple)
                or len(cur_py_range) != 2
                or cur_py_range[0] < py_min
                or cur_py_range[1] > py_max
            ):
                st.session_state["br_pubyear_range"] = (py_min, py_max)
            pubyear_range = st.slider(
                "Intervalle d'années",
                min_value=py_min,
                max_value=py_max,
                key="br_pubyear_range",
            )
        elif py_min is not None:
            st.caption(f"Une seule année de publication disponible : {py_min}")
            pubyear_range = (py_min, py_min)
        else:
            pubyear_range = None

        # ── Group 1 — access & OA gaps ───────────────────────────────────────
        st.markdown(sh("lock_open", "Accès & écarts OA"), unsafe_allow_html=True)
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)

        with col_f1:
            ifs_filter = st.multiselect(
                "Accès Infoscience", filter_options["existing_access_level"],
                key="br_ifs", placeholder="tous…",
            )

        with col_f2:
            oa_filter = st.multiselect(
                "Statut OA enrichi", filter_options["oa_category_advanced"],
                key="br_oa", placeholder="tous…",
            )

        with col_f3:
            gap_opts = list(_GAP_PRESETS.keys())
            # Initialize session state before the widget — passing both `index=` and `key=`
            # when the key already exists in session_state raises a Streamlit warning.
            cur_gap = st.session_state.get("br_gap")
            if cur_gap not in gap_opts:
                st.session_state["br_gap"] = gap_opts[0]
            gap_label = st.selectbox(
                "Écart NOAM",
                options=gap_opts,
                key="br_gap",
                help="Incohérences entre l'accès Infoscience et la classification OA NOAM.",
            )

        with col_f4:
            collect_opts = list(_COLLECT_GAP_PRESETS.keys())
            cur_collect = st.session_state.get("br_collect_gap")
            if cur_collect not in collect_opts:
                st.session_state["br_collect_gap"] = collect_opts[0]
            collect_gap_label = st.selectbox(
                "Collectabilité OA",
                options=collect_opts,
                key="br_collect_gap",
                help=(
                    "Identifie les items metadata-only ayant un signal OA natif "
                    "depuis OpenAlex (oa_status) ou Unpaywall (upw_is_oa), "
                    "indépendamment de la classification NOAM."
                ),
            )

        # ── Group 2 — document metadata ──────────────────────────────────────
        st.markdown(sh("category", "Métadonnées du document"), unsafe_allow_html=True)
        col_g1, col_g2, col_g3, col_g4 = st.columns(4)

        with col_g1:
            # Stale selections from a previous DB partition / code version may
            # no longer be valid options — drop them before the widget renders
            # (Streamlit errors if a session-state default isn't in `options`).
            _cur_type_sel = st.session_state.get("br_type", [])
            if any(v not in type_label_opts for v in _cur_type_sel):
                st.session_state["br_type"] = [v for v in _cur_type_sel if v in type_label_opts]
            type_filter = st.multiselect(
                "Type de document", type_label_opts,
                key="br_type", placeholder="tous…",
                help="Basé sur le code d'autorité COAR — indépendant de la langue d'affichage.",
            )

        with col_g2:
            ifs_lic_filter = st.multiselect(
                "Licence IFS", filter_options["existing_license"],
                key="br_lic_ifs", placeholder="toutes…",
            )

        with col_g3:
            _cur_ver_sel = st.session_state.get("br_ver_ifs", [])
            if any(v not in version_label_opts for v in _cur_ver_sel):
                st.session_state["br_ver_ifs"] = [v for v in _cur_ver_sel if v in version_label_opts]
            ifs_ver_filter = st.multiselect(
                "Version IFS", version_label_opts,
                key="br_ver_ifs", placeholder="toutes…",
            )

        with col_g4:
            doi_presence = st.selectbox(
                "Présence DOI",
                options=["Tous", "Avec DOI", "Sans DOI"],
                key="br_doi_presence",
            )

        # ── Group 3 — enriched OA metadata ───────────────────────────────────
        st.markdown(sh("auto_awesome", "Licence & version OA enrichies"), unsafe_allow_html=True)
        col_h1, col_h2 = st.columns(2)

        with col_h1:
            oa_lic_filter = st.multiselect(
                "Licence OA (enrichie)", filter_options["resolved_license"],
                key="br_lic_oa", placeholder="toutes…",
            )

        with col_h2:
            oa_ver_filter = st.multiselect(
                "Version OA (enrichie)", filter_options["resolved_version"],
                key="br_ver_oa", placeholder="toutes…",
            )

        # ── Group 4 — free-text search + reset ───────────────────────────────
        st.markdown(sh("manage_search", "Recherche libre"), unsafe_allow_html=True)
        col_i1, col_i2, col_i3 = st.columns([3, 3, 2])

        with col_i1:
            doi_filter = st.text_input("DOI contient", key="br_doi", placeholder="10.…")

        with col_i2:
            text_filter = st.text_input("Titre contient", key="br_title", placeholder="mot-clé…")

        with col_i3:
            st.markdown('<div style="padding-top:26px">', unsafe_allow_html=True)
            if st.button(
                "Réinitialiser les filtres", key="btn_reset_oa_browser",
                width="stretch", icon=":material/filter_alt_off:",
            ):
                for _k in _BR_FILTER_KEYS:
                    st.session_state.pop(_k, None)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    # ── Build WHERE clause ────────────────────────────────────────────────────
    def _in_clause(col: str, values: list[str]) -> str:
        escaped = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in values)
        return f"{col} IN ({escaped})"

    conds = [base_where]
    if pubyear_range and pubyear_range != (py_min, py_max):
        # pubyear is stored as VARCHAR — TRY_CAST avoids a BinderException on
        # mixed VARCHAR/INTEGER comparison (and silently drops non-numeric junk).
        conds.append(
            f"TRY_CAST(pubyear AS INTEGER) BETWEEN {pubyear_range[0]} AND {pubyear_range[1]}"
        )
    if ifs_filter:
        conds.append(_in_clause("existing_access_level", ifs_filter))
    if oa_filter:
        conds.append(_in_clause("oa_category_advanced", oa_filter))
    if type_filter:
        # type_filter holds selected human labels — translate back to the
        # underlying COAR authority codes for the actual column filter.
        type_authorities = [
            a for lbl in type_filter for a in type_label_to_authority.get(lbl, [])
        ]
        if type_authorities:
            conds.append(_in_clause("dc_type_authority", type_authorities))
    if ifs_lic_filter:
        conds.append(_in_clause("existing_license", ifs_lic_filter))
    if ifs_ver_filter:
        # ifs_ver_filter holds selected human labels — translate back to the
        # underlying COAR version URIs for the actual column filter.
        version_raws = [
            r for lbl in ifs_ver_filter for r in version_label_to_raw.get(lbl, [])
        ]
        if version_raws:
            conds.append(_in_clause("existing_version", version_raws))
    if oa_lic_filter:
        conds.append(_in_clause("resolved_license", oa_lic_filter))
    if oa_ver_filter:
        conds.append(_in_clause("resolved_version", oa_ver_filter))
    gap_sql = _GAP_PRESETS.get(gap_label, "")
    if gap_sql:
        conds.append(f"({gap_sql})")
    collect_sql = _COLLECT_GAP_PRESETS.get(collect_gap_label, "")
    if collect_sql:
        conds.append(f"({collect_sql})")
    if doi_presence == "Avec DOI":
        conds.append("doi IS NOT NULL AND doi != ''")
    elif doi_presence == "Sans DOI":
        conds.append("(doi IS NULL OR doi = '')")
    if doi_filter.strip():
        safe = doi_filter.strip().replace("'", "''")
        conds.append(f"doi ILIKE '%{safe}%'")
    if text_filter.strip():
        safe = text_filter.strip().replace("'", "''")
        conds.append(f"title ILIKE '%{safe}%'")
    where = " AND ".join(conds)

    total = con.execute(f"SELECT COUNT(*) FROM enriched WHERE {where}").fetchone()[0]
    page_size = 20
    n_pages = max(1, (total + page_size - 1) // page_size)

    if total == 0:
        st.info("Aucun résultat pour ces filtres.")
        return

    # Reset page when filters change
    filter_sig = (
        f"{pubyear_range}|{sorted(ifs_filter)}|{sorted(oa_filter)}|{sorted(type_filter)}"
        f"|{sorted(ifs_lic_filter)}|{sorted(ifs_ver_filter)}"
        f"|{sorted(oa_lic_filter)}|{sorted(oa_ver_filter)}"
        f"|{gap_label}|{collect_gap_label}|{doi_presence}|{doi_filter}|{text_filter}|{view_mode}"
    )
    if st.session_state.get("_oa_br_filter_sig") != filter_sig:
        st.session_state["_oa_br_filter_sig"] = filter_sig
        st.session_state["br_page"] = 1
    elif "br_page" not in st.session_state:
        st.session_state["br_page"] = 1

    col_pg, col_info = st.columns([2, 5])
    with col_pg:
        page = st.number_input(
            "Page", min_value=1, max_value=n_pages,
            step=1, key="br_page",
        )
    with col_info:
        st.caption(f"{total:,} résultats — page {page}/{n_pages}")

    # ── Fetch page ────────────────────────────────────────────────────────────
    sel_cols = [c for c in _OA_BROWSER_COLS if c in available]

    df_page = con.execute(
        f"SELECT {', '.join(sel_cols)} FROM enriched WHERE {where} "
        f"ORDER BY pubyear DESC, title "
        f"LIMIT {page_size} OFFSET {(page - 1) * page_size}"
    ).df()

    # ── TABLE VIEW ────────────────────────────────────────────────────────────
    if view_mode == "📋 Tableau":
        tbl_cols = [(src, lbl) for src, lbl in _TABLE_COLS if src in df_page.columns]
        df_tbl = df_page[[c for c, _ in tbl_cols]].copy()
        df_tbl.columns = [lbl for _, lbl in tbl_cols]

        # Make handle into a clickable URL column
        if "Handle" in df_tbl.columns:
            df_tbl["IFS"] = df_page["handle"].apply(
                lambda h: _ifs_url(h) or ""
            )

        st.dataframe(
            df_tbl,
            width="stretch",
            hide_index=True,
            column_config={
                "IFS": st.column_config.LinkColumn("↗", display_text="Infoscience"),
                "DOI": st.column_config.LinkColumn(
                    "DOI",
                    display_text="↗",
                ),
            },
        )
        return

    # ── VISUAL VIEW ───────────────────────────────────────────────────────────
    st.markdown(
        '<div style="font-size:0.68rem;color:#9CA3AF;margin-bottom:4px">'
        '📚 Infoscience = accès / version / licence actuels dans le dépôt  ·  '
        '✨ Enrichi = OA status résolu via OpenAlex + Unpaywall  ·  '
        '⚡ Gap OA = metadata-only mais OA Gold/Green enrichi  ·  '
        '? Incohérence = open access mais classifié Fermé'
        '</div>',
        unsafe_allow_html=True,
    )

    rows = df_page.to_dict("records")
    with st.container(border=True):
        for idx, row in enumerate(rows):
            col_main, col_btn = st.columns([8.5, 0.7])
            with col_main:
                st.markdown(_build_oa_row_html(row), unsafe_allow_html=True)
            with col_btn:
                if st.button(
                    "",
                    icon=":material/open_in_new:",
                    key=f"oa_det_{idx}_{page}",
                    help="Tous les champs OA",
                    width="stretch",
                ):
                    _oa_detail_modal(row)
            if idx < len(rows) - 1:
                st.markdown(
                    '<hr style="margin:2px 0;border:none;border-top:1px solid #F3F4F6">',
                    unsafe_allow_html=True,
                )


# ── R&P (Read & Publish / APC) tracking section ──────────────────────────────

_RAP_GAP_LABELS: dict[str, str] = {
    "open_with_fulltext": "✅ OA avec fulltext",
    "needs_attention":    "⚠️ À corriger",
    "not_in_infoscience": "❌ Absent d'Infoscience",
    "no_doi":             "⏳ Sans DOI (en cours)",
}

_RAP_GAP_DETAIL_COLS: list[tuple[str, str]] = [
    ("tracking_year",         "Année suivi"),
    ("publisher",             "Éditeur"),
    ("contract_id",           "Contrat"),
    ("Journal",               "Journal"),
    ("Article Title",         "Titre"),
    ("Contact Author",        "Auteur contact"),
    ("DOI",                   "DOI"),
    ("handle",                "Handle Infoscience"),
    ("existing_access_level", "Accès IFS"),
    ("resolved_version",      "Version"),
    ("oa_category_advanced",  "OA enrichi"),
]


def _render_rap_section(con, root: Path, active_env: str) -> None:
    """R&P (Read & Publish) APC tracking — consolidation status + Infoscience gaps."""
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if "rap_tracking" not in tables:
        return  # nothing to show before the first `consolidate-rap` run

    st.markdown(sh("receipt_long", "Suivi R&P (APC)"), unsafe_allow_html=True)

    total = con.execute("SELECT COUNT(*) FROM rap_tracking").fetchone()[0]
    review_n = con.execute(
        "SELECT COUNT(*) FROM rap_tracking WHERE "
        "license_needs_review OR oa_type_needs_review OR article_type_needs_review"
    ).fetchone()[0]
    n_publishers = con.execute("SELECT COUNT(DISTINCT publisher) FROM rap_tracking").fetchone()[0]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Articles suivis (R&P)", f"{total:,}")
    with c2:
        st.metric(
            "À vérifier manuellement ⚠️", f"{review_n:,}",
            help="Valeurs non reconnues (licence / type OA / type d'article) lors de la consolidation.",
        )
    with c3:
        st.metric("Éditeurs", f"{n_publishers}")

    if review_n:
        with st.expander(f"⚠️ {review_n} ligne(s) à vérifier manuellement", expanded=False):
            review_df = con.execute(
                'SELECT source_file, "Journal", "Article Title", "OA type", '
                '"Article Type", "License" FROM rap_tracking WHERE '
                "license_needs_review OR oa_type_needs_review OR article_type_needs_review"
            ).df()
            st.dataframe(review_df, width="stretch", hide_index=True)

    if "rap_gap_analysis" not in tables:
        st.info(
            "Lancez l'étape `rap-gaps` (après `harvest`+`enrich` sur les années "
            "couvertes) pour croiser ce suivi avec Infoscience."
        )
    else:
        st.markdown("**Statut Infoscience**")
        gap_counts = dict(
            con.execute("SELECT gap_status, COUNT(*) FROM rap_gap_analysis GROUP BY 1").fetchall()
        )

        cols = st.columns(4)
        for col, (status, label) in zip(cols, _RAP_GAP_LABELS.items()):
            with col:
                st.metric(label, f"{gap_counts.get(status, 0):,}")

        gap_options = [s for s in _RAP_GAP_LABELS if gap_counts.get(s, 0) > 0]
        if gap_options:
            sel_status = st.selectbox(
                "Filtrer par statut",
                options=gap_options,
                format_func=lambda s: _RAP_GAP_LABELS[s],
                key="rap_gap_filter",
            )
            df_detail = con.execute(
                "SELECT * FROM rap_gap_analysis WHERE gap_status = ? ORDER BY tracking_year DESC",
                [sel_status],
            ).df()
            show_cols = [c for c, _ in _RAP_GAP_DETAIL_COLS if c in df_detail.columns]
            disp = df_detail[show_cols].rename(columns=dict(_RAP_GAP_DETAIL_COLS))
            st.dataframe(disp, width="stretch", hide_index=True)

            # st.download_button's data= is evaluated on every rerun even when
            # not clicked — querying the full table unconditionally here would
            # hold the DuckDB connection open on every page view, needlessly
            # widening the window for a lock conflict with a concurrent CLI
            # run. Compute once on explicit request instead, cached in
            # session_state.
            if st.button("📄 Préparer l'export CSV complet", key="rap_gap_export_prepare"):
                with st.spinner("Génération de l'export…"):
                    st.session_state["_rap_gap_csv_bytes"] = (
                        con.execute("SELECT * FROM rap_gap_analysis")
                        .df().to_csv(index=False).encode("utf-8")
                    )
            csv_bytes = st.session_state.get("_rap_gap_csv_bytes")
            if csv_bytes:
                st.download_button(
                    "⬇ Télécharger l'export CSV complet",
                    data=csv_bytes,
                    file_name="rap_gap_analysis.csv",
                    mime="text/csv",
                    key="dl_rap_gap_analysis",
                )

    db_path = root / _DEFAULT_WORK_DIR / f"oa_monitor_{active_env}.duckdb"
    with st.expander("🗑 Vider les données R&P", expanded=False):
        _render_rap_purge(con, db_path)


def _render_rap_purge(con, db_path: Path) -> None:
    """Purge rap_tracking + rap_gap_analysis — never touches data/apc.

    Deletion is deferred to the next rerun via session_state, same pattern as
    _render_purge(): the read-only `con` must be closed before a write
    connection can be opened on the same DuckDB file.
    """
    rap_n = con.execute("SELECT COUNT(*) FROM rap_tracking").fetchone()[0]
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    gap_n = (
        con.execute("SELECT COUNT(*) FROM rap_gap_analysis").fetchone()[0]
        if "rap_gap_analysis" in tables else 0
    )

    st.warning(
        "⚠️ Supprime les tables consolidées `rap_tracking` et `rap_gap_analysis` "
        "de la base DuckDB. **Les fichiers source dans `data/apc/` ne sont jamais "
        "touchés** — relancez `consolidate-rap` pour tout reconstruire.",
        icon=None,
    )
    st.caption(f"Supprimera {rap_n:,} lignes rap_tracking + {gap_n:,} lignes rap_gap_analysis.")

    confirmed = st.checkbox(
        "Je confirme la suppression",
        key="rap_purge_confirm",
        value=False,
    )
    purge_btn = st.button(
        "🗑 Supprimer les données R&P",
        disabled=not confirmed,
        type="primary",
        key="rap_purge_execute",
    )

    if purge_btn and confirmed:
        st.session_state["_pending_rap_purge"] = {"db_path": str(db_path)}
        st.rerun()
