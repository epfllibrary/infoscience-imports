"""Researcher registry card component.

Each researcher is rendered as a bordered card via st.container(border=True).
Cards are laid out in a 2-column grid by the caller (_render_registry).

Dialog: show_researcher_dialog presents 5 tabs:
  Profil | Unités | Moissonnées | Infoscience | Lacunes
"""

from __future__ import annotations

import html as _html
import json

import streamlit as st

# ── HTML helpers ──────────────────────────────────────────────────────────────

def _esc(v: object) -> str:
    return _html.escape(str(v)) if v is not None else ""


def _nn(v: object) -> bool:
    if v is None:
        return False
    return str(v).strip() not in ("", "nan", "None", "NaT")


def _s(v: object, default: str = "") -> str:
    return str(v).strip() if _nn(v) else default


# ── Material Symbol helper ────────────────────────────────────────────────────

def _ms(name: str, size: int = 13, fill: int = 0) -> str:
    """Inline Material Symbol sized for badge/card use."""
    return (
        f'<span style="font-family:\'Material Symbols Outlined\';'
        f'font-variation-settings:\'FILL\' {fill},\'wght\' 400,\'GRAD\' 0,\'opsz\' 20;'
        f'font-size:{size}px;line-height:1;vertical-align:middle;display:inline-block;'
        f'margin-right:3px;white-space:nowrap;font-feature-settings:\'liga\';'
        f'-webkit-font-smoothing:antialiased;">{name}</span>'
    )


# ── Badge / chip helpers ──────────────────────────────────────────────────────

def _sciper_chip(sciper: str) -> str:
    url = f"https://people.epfl.ch/{_esc(sciper)}"
    return (
        f'<a href="{url}" target="_blank" class="rmcard-sciper" title="Annuaire EPFL">'
        f'{_ms("badge", 11)}{_esc(sciper)}</a>'
    )


def _position_chip(pos: str | None) -> str:
    if not _nn(pos):
        return ""
    return (
        f'<span class="rmcard-pos-chip">'
        f'{_ms("work", 11)}{_esc(_s(pos)[:40])}</span>'
    )


def _active_badge(is_active: object) -> str:
    if not is_active:
        return (
            f'<span class="rmid rmid-inactive">'
            f'{_ms("do_not_disturb_on", 12)}inactif</span>'
        )
    return ""


def _orcid_badge(orcid: str | None, linked: bool = False) -> str:
    """Three states: linked (green+verified), found-not-linked (amber+link), absent (gray)."""
    if _nn(orcid):
        url = f"https://orcid.org/{_esc(_s(orcid))}"
        if linked:
            icon = _ms("verified", fill=1)
            css = "rmid rmid-orcid-linked"
            title = f"ORCID lié EPFL : {_esc(orcid)}"
        else:
            icon = _ms("link", fill=0)
            css = "rmid rmid-orcid-unlinked"
            title = f"ORCID (non lié EPFL) : {_esc(orcid)}"
        return f'<a href="{url}" target="_blank" class="{css}" title="{title}">{icon}ORCID</a>'
    return (
        f'<span class="rmid rmid-missing" title="ORCID non renseigné">'
        f'{_ms("fingerprint")}ORCID</span>'
    )


def _openalex_badge(openalex_id: str | None) -> str:
    if _nn(openalex_id):
        oa = _s(openalex_id)
        url = oa if oa.startswith("http") else f"https://openalex.org/{oa}"
        return (
            f'<a href="{_esc(url)}" target="_blank" class="rmid rmid-openalex" '
            f'title="OpenAlex : {_esc(oa)}">'
            f'{_ms("travel_explore", fill=1)}OpenAlex</a>'
        )
    return (
        f'<span class="rmid rmid-missing" title="OpenAlex non renseigné">'
        f'{_ms("travel_explore")}OpenAlex</span>'
    )


def _scopus_badge(scopus_id: str | None) -> str:
    if not _nn(scopus_id):
        return ""
    url = f"https://www.scopus.com/authid/detail.uri?authorId={_esc(_s(scopus_id))}"
    return (
        f'<a href="{url}" target="_blank" class="rmid rmid-scopus" '
        f'title="Scopus Author ID : {_esc(scopus_id)}">'
        f'{_ms("find_in_page", fill=1)}Scopus</a>'
    )


def _wos_badge(researcher_id: str | None) -> str:
    if not _nn(researcher_id):
        return ""
    url = f"https://publons.com/researcher/{_esc(_s(researcher_id))}"
    return (
        f'<a href="{url}" target="_blank" class="rmid rmid-wos" '
        f'title="WoS ResearcherID : {_esc(researcher_id)}">'
        f'{_ms("import_contacts", fill=1)}WoS</a>'
    )


def _dspace_badge(url: str | None, uuid: str | None) -> str:
    if _nn(url):
        return (
            f'<a href="{_esc(_s(url))}" target="_blank" class="rmid rmid-dspace" '
            f'title="Profil Infoscience">'
            f'{_ms("library_books", fill=1)}Infoscience</a>'
        )
    if _nn(uuid):
        iu = f"https://infoscience.epfl.ch/entities/person/{_esc(_s(uuid))}"
        return (
            f'<a href="{iu}" target="_blank" class="rmid rmid-dspace" '
            f'title="Profil Infoscience">'
            f'{_ms("library_books", fill=1)}Infoscience</a>'
        )
    return (
        f'<span class="rmid rmid-missing" title="Pas de profil Infoscience">'
        f'{_ms("library_books")}Infoscience</span>'
    )


def _unit_chip(unit: str | None, name: str = "") -> str:
    if not _nn(unit):
        return ""
    url = f"https://search.epfl.ch/?filter=unit&acro={_esc(_s(unit))}"
    title_attr = f' title="{_esc(name)}"' if _nn(name) else ""
    return (
        f'<a href="{url}" target="_blank" class="rmcard-unit-chip"{title_attr}>{_esc(_s(unit))}</a>'
    )


def _school_chip(level2: str | None, name: str = "") -> str:
    if not _nn(level2):
        return ""
    url = f"https://search.epfl.ch/?filter=unit&acro={_esc(_s(level2))}"
    title_attr = f' title="{_esc(name)}"' if _nn(name) else ""
    return (
        f'<a href="{url}" target="_blank" class="rmcard-school-chip"{title_attr}>{_esc(_s(level2))}</a>'
    )


# ── Person-publication rendering ─────────────────────────────────────────────

_PP_SRC_LABEL: dict[str, str] = {
    "openalex": "OpenAlex",
    "orcid":    "ORCID",
}
_PP_SRC_CSS: dict[str, str] = {
    "openalex": "ptbl-src-openalex",
    "orcid":    "ptbl-src-orcid",
}

_PP_GAP_CSS: dict[str, str] = {
    "missing_in_infoscience": "ppub-gap-missing",
    "in_infoscience":         "ppub-gap-in-is",
    "superseded_preprint":    "ppub-gap-superseded",
}
_PP_GAP_LABEL: dict[str, str] = {
    "missing_in_infoscience": "Absent IS",
    "in_infoscience":         "Présent IS",
    "superseded_preprint":    "Préprint — publié IS",
}


def _pp_src_badges(sources_json: str | None) -> str:
    try:
        sources = json.loads(sources_json or "[]")
    except Exception:
        sources = []
    parts = []
    for src in sources:
        s = str(src).lower()
        css   = _PP_SRC_CSS.get(s, "ptbl-src-default")
        label = _PP_SRC_LABEL.get(s, s.capitalize())
        parts.append(f'<span class="ptbl-badge {css}">{label}</span>')
    return "".join(parts)


def _pp_type_badge(dc_type: str | None) -> str:
    if not dc_type or str(dc_type).strip() in ("", "None"):
        return ""
    parts = str(dc_type).split("::")
    lbl   = (parts[-1] if len(parts) > 1 else parts[0]).strip()[:28]
    return f'<span class="ptbl-type" title="{_esc(dc_type)}">{_esc(lbl)}</span>'


def _pp_sync_chip(synced: object) -> str:
    if not synced:
        return ""
    return '<span class="ppub-sync">IS ↑</span>'


def _pp_preprint_chip(has_preprint: object) -> str:
    if not has_preprint:
        return ""
    return '<span class="ppub-preprint">⤵ preprint</span>'


def _pp_gap_badge(gap_status: str | None) -> str:
    if not gap_status:
        return ""
    css   = _PP_GAP_CSS.get(gap_status, "ppub-gap-missing")
    label = _PP_GAP_LABEL.get(gap_status, gap_status)
    return f'<span class="{css}">{_esc(label)}</span>'


def _pp_pub_row(pub: dict) -> str:
    """Build one rich HTML row for a person publication."""
    year        = _s(pub.get("pub_year"), "—")
    title       = _s(pub.get("title"), "—")
    doi         = _s(pub.get("doi"))
    journal     = _s(pub.get("journal_title"))
    sources     = pub.get("sources_found")
    dc_type     = pub.get("dc_type")
    synced      = pub.get("orcid_infoscience_synced")
    has_preprint = pub.get("has_preprint_version")

    meta = (
        f'<span class="ptbl-year">{_esc(year)}</span>'
        f'{_pp_src_badges(sources)}'
        f'{_pp_type_badge(dc_type)}'
        f'{_pp_sync_chip(synced)}'
        f'{_pp_preprint_chip(has_preprint)}'
    )

    footer_parts = []
    if journal:
        footer_parts.append(f'<span class="ptbl-auth-inline">{_esc(journal[:60])}</span>')
    if doi:
        doi_url = f"https://doi.org/{doi}"
        footer_parts.append(
            f'<a href="{doi_url}" target="_blank" class="ptbl-doi">'
            f'{_esc(doi[:40])}{"…" if len(doi) > 40 else ""}'
            f'</a>'
        )
    dot    = '<span class="ptbl-sep-dot">·</span>'
    footer = f' {dot} '.join(footer_parts)

    return (
        f'<div class="ptbl-row">'
        f'<div class="ptbl-row-meta">{meta}</div>'
        f'<div class="ptbl-title">{_esc(title[:160])}{"…" if len(title) > 160 else ""}</div>'
        f'<div class="ptbl-row-footer">{footer}</div>'
        f'</div>'
    )


def _pp_is_output_row(out: dict) -> str:
    """Build one rich HTML row for an Infoscience output."""
    year    = _s(out.get("pub_year"), "—")
    title   = _s(out.get("title"), "—")
    doi     = _s(out.get("doi"))
    handle  = _s(out.get("handle"))
    dc_type = out.get("dc_type")

    meta = (
        f'<span class="ptbl-year">{_esc(year)}</span>'
        f'{_pp_type_badge(dc_type)}'
    )

    footer_parts = []
    if doi:
        doi_url = f"https://doi.org/{doi}"
        footer_parts.append(
            f'<a href="{doi_url}" target="_blank" class="ptbl-doi">'
            f'{_esc(doi[:40])}{"…" if len(doi) > 40 else ""}'
            f'</a>'
        )
    if handle:
        is_url = f"https://infoscience.epfl.ch/handle/{handle}"
        footer_parts.append(
            f'<a href="{is_url}" target="_blank" class="ppub-handle">'
            f'hdl:{_esc(handle)}</a>'
        )
    dot    = '<span class="ptbl-sep-dot">·</span>'
    footer = f' {dot} '.join(footer_parts)

    return (
        f'<div class="ptbl-row">'
        f'<div class="ptbl-row-meta">{meta}</div>'
        f'<div class="ptbl-title">{_esc(title[:160])}{"…" if len(title) > 160 else ""}</div>'
        f'<div class="ptbl-row-footer">{footer}</div>'
        f'</div>'
    )


def _pp_gap_row(gap: dict) -> str:
    """Build one rich HTML row for a gap analysis entry."""
    year       = _s(gap.get("pub_year"), "—")
    title      = _s(gap.get("title"), "—")
    doi        = _s(gap.get("doi"))
    dc_type    = gap.get("dc_type")
    gap_status = _s(gap.get("gap_status"))

    meta = (
        f'<span class="ptbl-year">{_esc(year)}</span>'
        f'{_pp_gap_badge(gap_status)}'
        f'{_pp_type_badge(dc_type)}'
    )

    footer_parts = []
    if doi:
        doi_url = f"https://doi.org/{doi}"
        footer_parts.append(
            f'<a href="{doi_url}" target="_blank" class="ptbl-doi">'
            f'{_esc(doi[:40])}{"…" if len(doi) > 40 else ""}'
            f'</a>'
        )
    dot    = '<span class="ptbl-sep-dot">·</span>'
    footer = f' {dot} '.join(footer_parts)

    return (
        f'<div class="ptbl-row">'
        f'<div class="ptbl-row-meta">{meta}</div>'
        f'<div class="ptbl-title">{_esc(title[:160])}{"…" if len(title) > 160 else ""}</div>'
        f'<div class="ptbl-row-footer">{footer}</div>'
        f'</div>'
    )


def _pub_stats_html(harvested: int, missing, infoscience_pubs: int = 0) -> str:
    harvest_val = int(harvested) if harvested else 0
    pubs_css = "rmstat-pubs" + (" rmstat-zero" if harvest_val == 0 else "")
    pubs_chip = (
        f'<span class="{pubs_css}">'
        f'{_ms("article", 11)}'
        f'{harvest_val} pub{"s" if harvest_val != 1 else ""}</span>'
    )
    is_val = int(infoscience_pubs) if infoscience_pubs else 0
    is_chip = (
        f'<span class="rmstat-is-pubs" title="Publications collectées dans Infoscience">'
        f'{_ms("hub", 11)}'
        f'{is_val} IS</span>'
    ) if is_val > 0 else ""
    if missing is None:
        gap_chip = (
            f'<span class="rmstat-unanalyzed">'
            f'{_ms("hourglass_empty", 11)}non analysé</span>'
        )
    elif missing == 0:
        gap_chip = (
            f'<span class="rmstat-ok">'
            f'{_ms("check_circle", 11, fill=1)}OK</span>'
        )
    else:
        gap_chip = (
            f'<span class="rmstat-gaps">'
            f'{_ms("warning", 11, fill=1)}'
            f'{missing} lacune{"s" if missing > 1 else ""}</span>'
        )
    return f'{pubs_chip}{is_chip}{gap_chip}'


# ── Card render ───────────────────────────────────────────────────────────────

def render_researcher_card(
    row: dict,
    role: str,
    on_detail: "callable | None" = None,
) -> None:
    """Render one researcher as a bordered card with Material icon badges."""
    sciper       = _s(row.get("sciper"), "?")
    name         = _s(row.get("full_name"), "—")
    pos          = row.get("epfl_position")
    orcid        = row.get("orcid")
    orcid_linked = bool(row.get("orcid_epfl_linked"))
    scopus_id    = row.get("scopus_author_id")
    oa_id        = row.get("openalex_id")
    rid          = row.get("researcher_id")
    is_active    = bool(row.get("is_active", True))
    unit         = row.get("main_unit")
    level2       = row.get("unit_level_2")
    level3       = row.get("unit_level_3")
    dspace_url   = row.get("infoscience_profile_url")
    dspace_uuid  = row.get("dspace_uuid")
    harvested        = int(row.get("harvested_pubs") or 0)
    infoscience_pubs = int(row.get("infoscience_pubs") or 0)
    missing          = row.get("gaps_missing")
    last_sync        = row.get("last_people_sync")

    sync_str = ""
    if _nn(last_sync):
        s = str(last_sync).strip()
        sync_str = s[:10] if len(s) >= 10 else s

    ids_html = (
        f'<div class="rmcard-ids">'
        f'{_orcid_badge(orcid, orcid_linked)}'
        f'{_openalex_badge(oa_id)}'
        f'{_scopus_badge(scopus_id)}'
        f'{_wos_badge(rid)}'
        f'{_dspace_badge(dspace_url, dspace_uuid)}'
        f'{_active_badge(is_active)}'
        f'</div>'
    )

    unit_name    = _s(row.get("unit_name"))
    lv2_name     = _s(row.get("unit_level_2_name"))
    lv3_name     = _s(row.get("unit_level_3_name"))

    units_html = ""
    if _nn(unit) or _nn(level2):
        chips = []
        if _nn(level2):
            chips.append(_school_chip(level2, lv2_name))
        if _nn(level3) and _s(level3) not in (_s(level2), _s(unit)):
            chips.append(_unit_chip(level3, lv3_name))
        if _nn(unit) and _s(unit) not in (_s(level2), _s(level3)):
            chips.append(_unit_chip(unit, unit_name))
        if chips:
            sep = '<span class="rmcard-unit-sep">›</span>'
            units_html = (
                f'<div class="rmcard-unit-breadcrumb">'
                f'{_ms("corporate_fare", 11)}'
                f'{sep.join(chips)}'
                f'</div>'
            )

    sync_chip = (
        f'<span class="rmcard-sync">'
        f'{_ms("schedule", 11)}{_esc(sync_str)}</span>'
    ) if sync_str else ""

    footer_html = (
        f'<div class="rmcard-footer">'
        f'<div class="rmcard-footer-stats">{_pub_stats_html(harvested, missing, infoscience_pubs)}</div>'
        f'<div class="rmcard-footer-sync">{sync_chip}</div>'
        f'</div>'
    )

    pos_chip = _position_chip(pos)
    pos_row = f'<div class="rmcard-pos-row">{pos_chip}</div>' if pos_chip else ""

    with st.container(border=True):
        name_col, btn_col = st.columns([5, 1])
        with name_col:
            st.markdown(
                f'<div class="rmcard-name-row">'
                f'<span class="rmcard-fullname">{_esc(name)}</span>'
                f'{_sciper_chip(sciper)}'
                f'</div>'
                f'{pos_row}',
                unsafe_allow_html=True,
            )
        with btn_col:
            if on_detail is not None:
                if st.button(
                    "Voir →",
                    key=f"rmcard_{sciper}",
                    help=f"Détails — {name}",
                    width="stretch",
                ):
                    on_detail(row)

        st.markdown(ids_html, unsafe_allow_html=True)
        if units_html:
            st.markdown(units_html, unsafe_allow_html=True)
        st.markdown('<div class="rmcard-divider"></div>', unsafe_allow_html=True)
        st.markdown(footer_html, unsafe_allow_html=True)


# ── Dialog helpers ────────────────────────────────────────────────────────────

def _dlg_id_row(icon: str, label: str, value_html: str, found: bool = True) -> str:
    icon_style = (
        f"font-family:'Material Symbols Outlined';"
        f"font-variation-settings:'FILL' {1 if found else 0},'wght' 400,'GRAD' 0,'opsz' 20;"
        f"font-size:16px;line-height:1;vertical-align:middle;display:inline-block;"
        f"margin-right:6px;white-space:nowrap;font-feature-settings:'liga';"
        f"-webkit-font-smoothing:antialiased;"
        f"color:{'#64748B' if found else '#CBD5E1'};"
    )
    row_class = "rmdlg-id-row" + ("" if found else " rmdlg-id-empty")
    return (
        f'<div class="{row_class}">'
        f'<span style="{icon_style}">{icon}</span>'
        f'<span class="rmdlg-id-label">{label}</span>'
        f'<span class="rmdlg-id-value">{value_html}</span>'
        f'</div>'
    )


def _unit_level_crumb(acronym: str, css_class: str, icon: str, title: str = "") -> str:
    if not _nn(acronym):
        return ""
    people_url = f"https://search.epfl.ch/?filter=unit&acro={_esc(acronym)}"
    ms_style = (
        "font-family:'Material Symbols Outlined';"
        "font-variation-settings:'FILL' 0,'wght' 400,'GRAD' 0,'opsz' 20;"
        "font-size:13px;line-height:1;vertical-align:middle;display:inline-block;"
        "margin-right:3px;white-space:nowrap;font-feature-settings:'liga';"
        "-webkit-font-smoothing:antialiased;"
    )
    title_attr = f' title="{_esc(title)}"' if _nn(title) else ""
    return (
        f'<a href="{people_url}" target="_blank" class="rmunit-crumb {css_class}"{title_attr}>'
        f'<span style="{ms_style}">{icon}</span>'
        f'{_esc(acronym)}</a>'
    )


# ── Detail dialog ─────────────────────────────────────────────────────────────

@st.dialog("Chercheur", width="large")
def show_researcher_dialog(row: dict, db=None) -> None:
    sciper = _s(row.get("sciper"), "?")
    name   = _s(row.get("full_name"), "—")

    st.markdown(
        f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;'
        f'padding-bottom:12px;border-bottom:1px solid #E2E8F0">'
        f'<span style="font-size:1.05rem;font-weight:700;color:#0F172A">{_esc(name)}</span>'
        f'{_sciper_chip(sciper)}'
        f'{_position_chip(row.get("epfl_position"))}'
        f'{_active_badge(bool(row.get("is_active", True)))}'
        f'</div>',
        unsafe_allow_html=True,
    )

    harvested = int(row.get("harvested_pubs") or 0)
    missing   = row.get("gaps_missing")
    in_is     = row.get("gaps_in_infoscience")

    kpi_css = (
        "display:inline-block;min-width:90px;padding:8px 14px;"
        "background:#F8FAFC;border:1px solid #E2E8F0;border-radius:8px;"
        "margin-right:8px;margin-bottom:12px;vertical-align:top"
    )
    val_css = "font-size:1.4rem;font-weight:700;color:#0F172A;line-height:1.1"
    lbl_css = "font-size:0.63rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;margin-bottom:3px"
    kpis = [
        ("Moissonnées", harvested),
        ("Infoscience", in_is if in_is is not None else "—"),
        ("Lacunes", missing if missing is not None else "—"),
    ]
    kpi_html = "".join(
        f'<div style="{kpi_css}"><div style="{lbl_css}">{lbl}</div>'
        f'<div style="{val_css}">{val}</div></div>'
        for lbl, val in kpis
    )
    st.markdown(f'<div style="margin-bottom:4px">{kpi_html}</div>', unsafe_allow_html=True)

    t_profil, t_units, t_moissonnes, t_infoscience, t_lacunes = st.tabs([
        "Profil",
        "Unités",
        f"Moissonnées ({harvested})",
        "Infoscience",
        f"Lacunes ({missing if missing is not None else '?'})",
    ])

    with t_profil:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Identifiants**")

            orcid        = row.get("orcid")
            orcid_linked = bool(row.get("orcid_epfl_linked"))
            if _nn(orcid):
                orcid_url = f"https://orcid.org/{_s(orcid)}"
                linked_badge = (
                    f' <span class="rmdlg-linked-badge">'
                    f'{_ms("verified", 12, fill=1)}lié EPFL</span>'
                    if orcid_linked else
                    f' <span class="rmdlg-unlinked-badge">'
                    f'{_ms("link_off", 12)}non lié</span>'
                )
                orcid_val = f'<a href="{orcid_url}" target="_blank">{_esc(_s(orcid))}</a>{linked_badge}'
                found = True
            else:
                orcid_val = "—"
                found = False
            st.markdown(
                _dlg_id_row("fingerprint", "ORCID", orcid_val, found),
                unsafe_allow_html=True,
            )

            oa_id = row.get("openalex_id")
            if _nn(oa_id):
                oa = _s(oa_id)
                url = oa if oa.startswith("http") else f"https://openalex.org/{oa}"
                oa_val = f'<a href="{url}" target="_blank">{_esc(oa)}</a>'
                found = True
            else:
                oa_val = "—"
                found = False
            st.markdown(
                _dlg_id_row("travel_explore", "OpenAlex", oa_val, found),
                unsafe_allow_html=True,
            )

            sid = row.get("scopus_author_id")
            if _nn(sid):
                url = f"https://www.scopus.com/authid/detail.uri?authorId={_s(sid)}"
                s_val = f'<a href="{url}" target="_blank">{_esc(_s(sid))}</a>'
                found = True
            else:
                s_val = "—"
                found = False
            st.markdown(
                _dlg_id_row("find_in_page", "Scopus", s_val, found),
                unsafe_allow_html=True,
            )

            rid = row.get("researcher_id")
            if _nn(rid):
                url = f"https://publons.com/researcher/{_s(rid)}"
                r_val = f'<a href="{url}" target="_blank">{_esc(_s(rid))}</a>'
                found = True
            else:
                r_val = "—"
                found = False
            st.markdown(
                _dlg_id_row("import_contacts", "WoS ResearcherID", r_val, found),
                unsafe_allow_html=True,
            )

            is_url  = row.get("infoscience_profile_url")
            uuid    = row.get("dspace_uuid")
            if _nn(is_url):
                is_val = f'<a href="{_esc(_s(is_url))}" target="_blank">profil ↗</a>'
                found  = True
            elif _nn(uuid):
                link   = f"https://infoscience.epfl.ch/entities/person/{_s(uuid)}"
                is_val = f'<a href="{_esc(link)}" target="_blank">profil ↗</a>'
                found  = True
            else:
                is_val = "—"
                found  = False
            st.markdown(
                _dlg_id_row("library_books", "Infoscience", is_val, found),
                unsafe_allow_html=True,
            )

            email = row.get("email")
            if _nn(email):
                e_val = (
                    f'<a href="mailto:{_esc(_s(email))}">{_esc(_s(email))}</a>'
                )
                st.markdown(
                    _dlg_id_row("mail", "Email", e_val, True),
                    unsafe_allow_html=True,
                )

            nv_raw = row.get("name_variants")
            if _nn(nv_raw):
                try:
                    import json as _json
                    variants = _json.loads(_s(nv_raw))
                    if variants:
                        nv_html = " · ".join(_esc(str(v)) for v in variants)
                        st.markdown(
                            _dlg_id_row("badge", "Variantes", nv_html, True),
                            unsafe_allow_html=True,
                        )
                except Exception:
                    pass

            oa_nv_raw = row.get("openalex_name_variants")
            if _nn(oa_nv_raw):
                try:
                    import json as _json
                    oa_variants = _json.loads(_s(oa_nv_raw))
                    if oa_variants:
                        oa_nv_html = " · ".join(_esc(str(v)) for v in oa_variants)
                        st.markdown(
                            _dlg_id_row("badge", "Variantes (OpenAlex)", oa_nv_html, True),
                            unsafe_allow_html=True,
                        )
                except Exception:
                    pass

        with c2:
            st.markdown("**Affiliation**")
            main_unit = _s(row.get("main_unit"), "—")
            unit_url  = f"https://search.epfl.ch/?filter=unit&acro={main_unit}" if _nn(row.get("main_unit")) else None
            unit_val  = f'<a href="{unit_url}" target="_blank">{_esc(main_unit)}</a>' if unit_url else main_unit
            st.markdown(
                _dlg_id_row("biotech", "Labo / Unité", unit_val, _nn(row.get("main_unit"))),
                unsafe_allow_html=True,
            )

            level2 = row.get("unit_level_2")
            if _nn(level2):
                lv2_url = f"https://search.epfl.ch/?filter=unit&acro={_s(level2)}"
                lv2_val = f'<a href="{lv2_url}" target="_blank">{_esc(_s(level2))}</a>'
                st.markdown(
                    _dlg_id_row("school", "Faculté / École", lv2_val, True),
                    unsafe_allow_html=True,
                )

            epfl_class = row.get("epfl_class")
            if _nn(epfl_class):
                st.markdown(
                    _dlg_id_row("person", "Classe EPFL", _esc(_s(epfl_class)), True),
                    unsafe_allow_html=True,
                )

            sync_dt = _s(row.get("last_people_sync"), "—")
            sync_val = sync_dt[:10] if len(sync_dt) >= 10 else sync_dt
            st.markdown(
                _dlg_id_row("sync", "Sync People", sync_val, sync_val != "—"),
                unsafe_allow_html=True,
            )

            gap_dt = _s(row.get("last_gap_analysis_at"), "—")
            gap_val = gap_dt[:10] if len(gap_dt) >= 10 else gap_dt
            st.markdown(
                _dlg_id_row("analytics", "Analyse lacunes", gap_val, gap_val != "—"),
                unsafe_allow_html=True,
            )

    with t_units:
        if db is None:
            st.info("Base de données non disponible.")
        else:
            try:
                units = db.get_researcher_units(sciper)
                if not units:
                    st.info(
                        "Aucune donnée d'unité disponible. "
                        "Lancez une synchronisation pour mettre à jour."
                    )
                else:
                    name_map = {
                        _s(u.get("unit_name")): _s(u.get("unit_label"))
                        for u in units
                        if _nn(u.get("unit_name")) and _nn(u.get("unit_label"))
                    }
                    for u in units:
                        _render_unit_card(u, name_map)
                    primary_count = sum(1 for u in units if u.get("is_primary"))
                    st.caption(f"{len(units)} unité(s), dont {primary_count} principale(s)")
            except Exception as exc:
                st.error(f"Erreur : {exc}")

    with t_moissonnes:
        if db is None or harvested == 0:
            st.info("Aucune publication moissonnée pour ce chercheur.")
        else:
            try:
                pubs = db.get_person_publications(sciper)
                if pubs:
                    synced_count = sum(1 for p in pubs if p.get("orcid_infoscience_synced"))
                    preprint_count = sum(1 for p in pubs if p.get("has_preprint_version"))
                    sep = '<hr class="ptbl-sep">'
                    rows_html = sep.join(_pp_pub_row(p) for p in pubs)
                    st.markdown(
                        f'<div class="ppub-list">{rows_html}</div>',
                        unsafe_allow_html=True,
                    )
                    caption_parts = [f"{len(pubs)} publications — OpenAlex / ORCID"]
                    if synced_count:
                        caption_parts.append(f"{synced_count} déjà sync. vers Infoscience")
                    if preprint_count:
                        caption_parts.append(f"{preprint_count} avec version preprint")
                    st.caption(" · ".join(caption_parts))
                else:
                    st.info("Aucune publication moissonnée trouvée.")
            except Exception as exc:
                st.error(f"Erreur : {exc}")

    with t_infoscience:
        if db is None:
            st.info("Base de données non disponible.")
        else:
            try:
                outputs = db.get_person_infoscience_outputs(sciper)
                if outputs:
                    sep = '<hr class="ptbl-sep">'
                    rows_html = sep.join(_pp_is_output_row(o) for o in outputs)
                    st.markdown(
                        f'<div class="ppub-list">{rows_html}</div>',
                        unsafe_allow_html=True,
                    )
                    st.caption(f"{len(outputs)} publications liées dans Infoscience (profil CRIS)")
                else:
                    st.info(
                        "Aucune publication liée dans Infoscience. "
                        "Lancez une analyse de lacunes pour mettre à jour."
                    )
            except Exception as exc:
                st.error(f"Erreur : {exc}")

    with t_lacunes:
        if db is None or missing is None:
            st.info("Aucune analyse de lacunes disponible pour ce chercheur.")
        elif missing == 0:
            st.success("Toutes les publications moissonnées sont présentes dans Infoscience.")
        else:
            try:
                gap_df = db.get_person_gaps_df(
                    sciper=sciper, gap_status="missing_in_infoscience"
                )
                if not gap_df.empty:
                    gap_records = gap_df.to_dict("records")
                    sep = '<hr class="ptbl-sep">'
                    rows_html = sep.join(_pp_gap_row(g) for g in gap_records)
                    st.markdown(
                        f'<div class="ppub-list">{rows_html}</div>',
                        unsafe_allow_html=True,
                    )
                    st.caption(f"{len(gap_df)} publication(s) absente(s) dans Infoscience")
                else:
                    st.success("Aucune lacune détectée.")
            except Exception as exc:
                st.error(f"Erreur : {exc}")


def _render_unit_card(u: dict, name_map: dict | None = None) -> None:
    """Render one unit row as a bordered card with 3-level breadcrumb."""
    uid           = _s(u.get("unit_id"), "")       # numeric internal ID (kept for fallback)
    unit_acronym  = _s(u.get("unit_name"), "")      # short acronym, e.g. "NAL"
    unit_fullname = _s(u.get("unit_label"), "")     # English full name
    unit_type_val = _s(u.get("unit_type"), "")      # e.g. "Laboratory", "Institute"
    lvl2          = _s(u.get("unit_level_2"), "")
    lvl3          = _s(u.get("unit_level_3"), "")
    unit_cf       = _s(u.get("unit_cf"), "")
    position      = _s(u.get("position"), "")
    epfl_class    = _s(u.get("epfl_class"), "")
    is_primary    = bool(u.get("is_primary"))
    valid_from    = _s(u.get("valid_from"), "")[:10]
    valid_to      = _s(u.get("valid_to"), "")[:10]
    nm            = name_map or {}

    display_id = unit_acronym or uid   # prefer acronym; numeric as fallback

    with st.container(border=True):
        hcol, dcol = st.columns([4, 1])
        with hcol:
            primary_html = (
                f' <span class="rmunit-primary-badge">'
                f'{_ms("star", 11, fill=1)}Principale</span>'
            ) if is_primary else ""
            cf_chip = (
                f' <span class="rmunit-cf-chip"'
                f' title="Centre Financier · champ epfl.unit.cf dans Infoscience">'
                f'<span class="rmunit-cf-label">CF</span>{_esc(unit_cf)}</span>'
            ) if unit_cf else ""
            type_chip = (
                f' <span class="rmunit-type-chip">({_esc(unit_type_val)})</span>'
            ) if unit_type_val else ""
            name_html = (
                f'<span class="rmunit-name"> — {_esc(unit_fullname)}</span>'
            ) if unit_fullname else ""
            st.markdown(
                f'<div class="rmunit-header">'
                f'<strong>{_esc(display_id)}</strong>'
                f'{name_html}'
                f'{type_chip}'
                f'{cf_chip}'
                f'{primary_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
            if position or epfl_class:
                pos_html = _esc(position) if position else ""
                class_html = (
                    f' <span class="rmunit-class-chip">{_esc(epfl_class)}</span>'
                    if epfl_class else ""
                )
                st.markdown(
                    f'<div class="rmunit-position-row">'
                    f'{_ms("work", 11)}{pos_html}{class_html}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        with dcol:
            if valid_from or valid_to:
                st.caption(f"{valid_from or '—'} → {valid_to or '∞'}")

        # Breadcrumb: school (lvl2) › institute (lvl3) › lab (display_id)
        crumbs = []
        if lvl2 and lvl2 != display_id:
            crumbs.append(_unit_level_crumb(lvl2, "rmunit-crumb-school", "school", nm.get(lvl2, "")))
        if lvl3 and lvl3 != display_id and lvl3 != lvl2:
            crumbs.append(_unit_level_crumb(lvl3, "rmunit-crumb-institute", "domain", nm.get(lvl3, "")))
        if display_id:
            people_url = f"https://search.epfl.ch/?filter=unit&acro={_esc(display_id)}"
            # epfl.unit.code = unit acronym/identifier (not the CF code)
            is_url = f"https://infoscience.epfl.ch/search?query=epfl.unit.code%3A{_esc(display_id)}"
            ms_style = (
                "font-family:'Material Symbols Outlined';"
                "font-variation-settings:'FILL' 0,'wght' 400,'GRAD' 0,'opsz' 20;"
                "font-size:13px;line-height:1;vertical-align:middle;display:inline-block;"
                "margin-right:3px;white-space:nowrap;font-feature-settings:'liga';"
                "-webkit-font-smoothing:antialiased;"
            )
            lab_title = f' title="{_esc(unit_fullname)}"' if unit_fullname else ""
            crumbs.append(
                f'<a href="{people_url}" target="_blank" class="rmunit-crumb rmunit-crumb-lab"{lab_title}>'
                f'<span style="{ms_style}">biotech</span>'
                f'{_esc(display_id)}</a>'
                f' <a href="{is_url}" target="_blank" class="rmunit-is-link" '
                f'title="Publications dans Infoscience (epfl.unit.code:{_esc(display_id)})">IS ↗</a>'
            )

        if crumbs:
            sep = '<span class="rmunit-crumb-sep">›</span>'
            st.markdown(
                f'<div class="rmunit-breadcrumb">{sep.join(crumbs)}</div>',
                unsafe_allow_html=True,
            )
