"""Publications table — native Streamlit rendering with per-row dialogs.

Layout: 3 outer columns per row  (actions | main content | icon buttons)
- col_act  : HTML <a> links for View/Edit/Claim — no Streamlit widget needed
- col_main : single rich HTML block with year, badges, title, OA/authors/doi footer
- col_btns : nested st.columns for 📋 👤 🚩 [🗑] icon buttons

CSS (:has selector, Chrome105+/FF121+/Safari15.4+) scopes all padding reduction
and button overrides to rows that contain a .ptbl-row element, leaving the rest
of the app unaffected.
"""

from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from ui.constants import (
    DEDUP_LABELS,
    DB_META_SECTIONS,
    RAW_META_SECTIONS,
)


# ── Source / status maps ───────────────────────────────────────────────────────

_SOURCE_LABEL: dict[str, str] = {
    "scopus": "Scopus", "wos": "WoS", "crossref": "Crossref",
    "openalex+crossref": "OpenAlex", "openalex": "OpenAlex",
    "zenodo": "Zenodo", "epo": "EPO", "datacite": "DataCite",
}
_SOURCE_CSS: dict[str, str] = {
    "scopus": "ptbl-src-scopus", "wos": "ptbl-src-wos",
    "crossref": "ptbl-src-crossref", "openalex+crossref": "ptbl-src-openalex",
    "openalex": "ptbl-src-openalex", "zenodo": "ptbl-src-zenodo",
    "epo": "ptbl-src-epo", "datacite": "ptbl-src-datacite",
}
_STATUS_CSS: dict[str, str] = {
    "workflow": "ptbl-st-workflow", "workspace": "ptbl-st-workspace",
    "deduplicated": "ptbl-st-deduplicated", "rejected": "ptbl-st-rejected",
    "error": "ptbl-st-error", "deleted": "ptbl-st-deleted",
}


# ── Value helpers ──────────────────────────────────────────────────────────────

def _nn(v) -> bool:
    return v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip() not in ("", "nan", "None")


def _s(v, default: str = "") -> str:
    return str(v).strip() if _nn(v) else default


# ── HTML fragment builders ─────────────────────────────────────────────────────

def _src_badge(source, src_url=None) -> str:
    s     = _s(source).lower()
    css   = _SOURCE_CSS.get(s, "ptbl-src-default")
    label = _SOURCE_LABEL.get(s, _s(source, "?"))
    span  = f'<span class="ptbl-badge {css}">{label}</span>'
    if src_url:
        return f'<a href="{src_url}" target="_blank" style="text-decoration:none">{span}</a>'
    return span


def _status_badge(status) -> str:
    s   = _s(status).lower()
    css = _STATUS_CSS.get(s, "ptbl-src-default")
    return f'<span class="ptbl-badge {css}">{s}</span>' if s else ""


def _type_badge(dc_type) -> str:
    if not _nn(dc_type):
        return ""
    parts = str(dc_type).split("::")
    lbl   = (parts[-1] if len(parts) > 1 else parts[0]).strip()[:28]
    return f'<span class="ptbl-type" title="{dc_type}">{lbl}</span>'


def _action_links(row: dict) -> str:
    parts = []
    for col, lbl, css in [
        ("item_url", "View",  "ptbl-act-view"),
        ("ws_url",   "Edit",  "ptbl-act-edit"),
        ("wf_url",   "Claim", "ptbl-act-claim"),
    ]:
        u = row.get(col)
        if _nn(u):
            parts.append(f'<a href="{u}" target="_blank" class="ptbl-act {css}">{lbl}</a>')
    return (
        '<div class="ptbl-actions">' + "".join(parts) + "</div>"
        if parts else '<span style="color:#D0D5DD;font-size:11px">—</span>'
    )


def _main_content(row: dict, title: str, has_run: bool, idx: int) -> str:
    """Build the compact 3-line rich HTML block for the main content column."""
    year = _s(row.get("pub_year"), "—")
    src_u   = row.get("src_url") if _nn(row.get("src_url")) else None

    # ── Meta line: year · source badge · status badge · type
    meta = (
        f'<span class="ptbl-year">{year}</span>'
        f'{_src_badge(row.get("source"), src_u)}'
        f'{_status_badge(row.get("status"))}'
        f'{_type_badge(row.get("dc_type"))}'
    )
    if has_run:
        meta += f'<span class="ptbl-run-chip">{_s(row.get("run_id"))}</span>'

    # ── Footer line: OA · lic · PDF · authors · units · doi
    oa      = _s(row.get("OA"), "—")
    lic     = _s(row.get("Licence"))
    pdf     = row.get("PDF")
    auth    = _s(row.get("Auteurs EPFL"))
    warn    = row.get("⚠️")
    units   = _s(row.get("Unités"))
    doi_val = _s(row.get("doi"))
    doi_url = row.get("doi_url")

    fp: list[str] = [f'<span class="ptbl-oa-inline">{oa}</span>']
    if lic:
        fp.append(f'<span class="ptbl-lic-inline">{lic}</span>')
    if pdf:
        fp.append('<span class="ptbl-pdf-inline">PDF</span>')
    if auth:
        warn_ic = ' <span class="ptbl-warn">⚠</span>' if warn else ""
        fp.append(
            f'<span class="ptbl-auth-inline">'
            f'{auth[:70]}{"…" if len(auth) > 70 else ""}{warn_ic}'
            f'</span>'
        )
    if units:
        fp.append(f'<span class="ptbl-units-inline">{units}</span>')
    if doi_val and _nn(doi_url):
        fp.append(
            f'<a href="{doi_url}" target="_blank" class="ptbl-doi">'
            f'{doi_val[:36]}{"…" if len(doi_val) > 36 else ""}'
            f'</a>'
        )

    dot    = '<span class="ptbl-sep-dot">·</span>'
    footer = f' {dot} '.join(fp)

    return (
        f'<div class="ptbl-row">'
        f'<div class="ptbl-row-meta">{meta}</div>'
        f'<div class="ptbl-title">'
        f'{title[:150]}{"…" if len(title) > 150 else ""}'
        f'</div>'
        f'<div class="ptbl-row-footer">{footer}</div>'
        f'</div>'
    )


# ── Dialogs ───────────────────────────────────────────────────────────────────

@st.dialog("📋 Métadonnées collectées", width="large")
def _meta_modal(row: dict) -> None:
    """Show collected metadata for a publication."""
    st.markdown(f"**{_s(row.get('title'), '—')}**")
    st.caption(f"{_s(row.get('source'))} · {_s(row.get('pub_year'))}")
    st.divider()
    rm = row.get("raw_metadata")
    meta: dict = {}
    sections = DB_META_SECTIONS
    if _nn(rm):
        try:
            meta = json.loads(str(rm))
            sections = RAW_META_SECTIONS
        except Exception:
            pass
    src = meta if meta else row
    if not meta:
        st.info("Métadonnées non disponibles pour cet item.")
    for sec_name, keys in sections:
        items = [(k, str(src[k])) for k in keys if k in src and _nn(src.get(k))]
        if not items:
            continue
        st.markdown(f'<div class="ptbl-meta-sec">{sec_name}</div>', unsafe_allow_html=True)
        for k, v in items:
            if len(v) > 200:
                st.text_area(k, v, height=88, key=f"_mta_{k}", disabled=True)
            else:
                c1, c2 = st.columns([1, 3])
                c1.caption(k)
                c2.markdown(v)


@st.dialog("👤 Auteurs EPFL", width="large")
def _authors_modal(title: str, authors: list) -> None:
    """Show reconciled EPFL authors for a publication."""
    st.markdown(f"**{title}**")
    st.divider()
    if not authors:
        st.info("Aucun auteur EPFL réconcilié pour cette publication.")
        return
    for a in authors:
        sciper = a.get("sciper", "")
        name   = a.get("name") or sciper or "?"
        weak   = a.get("weak", False)
        with st.container(border=True):
            hd, btn = st.columns([4, 1])
            with hd:
                if weak:
                    st.warning(f"⚠️ **{name}** — Statut faible")
                else:
                    st.markdown(f"**{name}**")
            with btn:
                if sciper:
                    st.link_button("EPFL People", f"https://people.epfl.ch/{sciper}",
                                   use_container_width=True)
            c1, c2, c3 = st.columns(3)
            c1.markdown(f"**Statut** {a.get('epfl_status') or '—'}")
            c2.markdown(f"**Position** {a.get('epfl_position') or '—'}")
            c3.markdown(f"**Unité** {a.get('main_unit') or '—'}")
            orcid = a.get("orcid", "")
            if orcid:
                st.markdown(f"**ORCID** [{orcid}](https://orcid.org/{orcid})")


@st.dialog("🚩 Doublon Infoscience", width="large")
def _flagged_modal(title: str, flagged_raw: str, dedup_note: str, ds_base: str) -> None:
    """Show flagged duplicate info for curation."""
    st.markdown(f"**{title}**")
    st.error(f"**{DEDUP_LABELS.get(dedup_note, dedup_note or 'Signalé')}**")
    st.divider()
    try:
        items = json.loads(flagged_raw)
    except Exception:
        items = []
    if not isinstance(items, list):
        items = [items]
    for item in items:
        with st.container(border=True):
            uuid_val = item.get("uuid", "")
            doi      = item.get("doi", "")
            dc_type  = item.get("dc_type", "")
            if uuid_val:
                st.markdown(f"**UUID** [{uuid_val}]({ds_base}/items/{uuid_val})")
            if doi:
                st.markdown(f"**DOI** [{doi}](https://doi.org/{doi})")
            if dc_type:
                st.markdown(f"**Type** `{dc_type}`")


@st.dialog("Supprimer un item importé", width="small")
def _delete_modal(
    workspace_id: str,
    workflow_id: str | None,
    item_uuid: str | None,
    title: str,
    db,
) -> None:
    """Confirmation and execution of DSpace item deletion."""
    st.markdown(f"**{title[:100]}**")
    if workflow_id:
        st.info("Item en **workflow** — rejet puis suppression du workspace.",
                icon=":material/info:")
    st.caption(
        f"workspace_id : `{workspace_id}`"
        + (f"  |  workflow_id : `{workflow_id}`" if workflow_id else "")
    )
    st.warning("Cette action est irréversible.", icon=":material/warning:")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Supprimer", type="primary", use_container_width=True,
                     icon=":material/delete_forever:"):
            from clients.dspace_client_wrapper import DSpaceClientWrapper
            with st.spinner("Connexion à DSpace…"):
                try:
                    client = DSpaceClientWrapper()
                except Exception as exc:
                    st.error(f"Impossible de se connecter à DSpace : {exc}")
                    return
            label = "Rejet du workflow puis suppression…" if workflow_id else "Suppression en cours…"
            with st.spinner(label):
                ok, msg = client.delete_item(workspace_id, workflow_id, item_uuid)
            if not ok:
                st.error(msg)
                return
            with st.spinner("Mise à jour de la base…"):
                try:
                    from db.pipeline_db import PipelineDB as _W
                    _W().mark_deleted_by_workspace(workspace_id)
                except Exception as exc:
                    st.error(f"Item supprimé dans DSpace mais erreur DB : {exc}.")
                    return
            st.session_state["_del_toast"] = f"«{title[:60]}» supprimé avec succès."
            st.rerun()
    with col2:
        if st.button("Annuler", use_container_width=True):
            st.rerun()


# ── Column widths ──────────────────────────────────────────────────────────────

_W_ACT   = 0.9   # action links
_W_MAIN  = 7.5   # rich content block
_W_BTN   = 0.38  # each icon button


# ── Public rendering function ─────────────────────────────────────────────────

def render_pub_component(
    d: pd.DataFrame,
    cols: list,
    authors_by_row: dict,
    ds_base: str,
    role: str = "reporting",
    db=None,
) -> None:
    """Render publications as compact native Streamlit rows with per-row dialogs."""
    has_run = "run_id" in cols
    n_btns  = 4 if role == "admin" else 3
    widths  = [_W_ACT, _W_MAIN] + [_W_BTN] * n_btns

    with st.container(border=True):
        # ── Header ────────────────────────────────────────────────────────────
        hdr = st.columns(widths)
        hdr[0].markdown('<div class="ptbl-hdr">Actions</div>', unsafe_allow_html=True)
        hdr[1].markdown('<div class="ptbl-hdr">Publication</div>', unsafe_allow_html=True)
        for i, lbl in enumerate(["Meta", "Aut.", "Signal."] + (["Suppr."] if role == "admin" else [])):
            hdr[2 + i].markdown(
                f'<div class="ptbl-hdr" style="text-align:center">{lbl}</div>',
                unsafe_allow_html=True,
            )
        st.markdown('<hr class="ptbl-sep">', unsafe_allow_html=True)

        # ── Rows ──────────────────────────────────────────────────────────────
        for idx, row in enumerate(d.to_dict("records")):
            auths    = authors_by_row.get(_s(row.get("row_id")), [])
            has_flag = _nn(row.get("flagged_publication"))
            title    = _s(row.get("title"), "—")
            ws_raw   = row.get("workspace_id")
            wf_raw   = row.get("workflow_id")
            uuid_raw = row.get("dspace_item_uuid")

            rc = st.columns(widths)

            # col 0 — action links (HTML only, no Streamlit widget)
            rc[0].markdown(_action_links(row), unsafe_allow_html=True)

            # col 1 — rich content block (carries row class for :has() CSS)
            rc[1].markdown(
                _main_content(row, title, has_run, idx),
                unsafe_allow_html=True,
            )

            # col 2 — metadata
            if rc[2].button("", icon=":material/description:", key=f"meta_{idx}",
                             use_container_width=True, help="Métadonnées"):
                _meta_modal(row)

            # col 3 — authors
            if rc[3].button("", icon=":material/people:", key=f"auth_{idx}",
                             use_container_width=True, help="Auteurs EPFL",
                             disabled=not auths):
                if auths:
                    _authors_modal(title, auths)

            # col 4 — flagged
            if rc[4].button("", icon=":material/flag:", key=f"flag_{idx}",
                             use_container_width=True, help="Doublon Infoscience",
                             disabled=not has_flag):
                if has_flag:
                    _flagged_modal(
                        title,
                        _s(row.get("flagged_publication"), "[]"),
                        _s(row.get("dedup_note")),
                        ds_base,
                    )

            # col 5 — delete (admin only)
            if role == "admin":
                ws_id: str | None = None
                if _nn(ws_raw):
                    try:
                        ws_id = str(int(float(str(ws_raw))))
                    except (ValueError, TypeError):
                        ws_id = _s(ws_raw) or None

                if rc[5].button("", icon=":material/delete:", key=f"del_{idx}",
                                 use_container_width=True, help="Supprimer",
                                 disabled=not ws_id):
                    if ws_id:
                        wf_id: str | None = None
                        if _nn(wf_raw):
                            try:
                                wf_id = str(int(float(str(wf_raw))))
                            except (ValueError, TypeError):
                                wf_id = _s(wf_raw) or None
                        _delete_modal(ws_id, wf_id, _s(uuid_raw) or None, title, db)

            st.markdown('<hr class="ptbl-sep">', unsafe_allow_html=True)
