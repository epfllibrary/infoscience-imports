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

import html as _html
import json

import pandas as pd
import streamlit as st

from ui.constants import (
    DEDUP_LABELS,
    DB_META_SECTIONS,
    RAW_META_SECTIONS,
    INFOSCIENCE_STATUS_LABELS,
    INFOSCIENCE_STATUS_CSS,
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


def _has_no_abstract(row: dict) -> bool:
    rm = row.get("raw_metadata")
    if not _nn(rm):
        return True
    try:
        abst = json.loads(str(rm)).get("abstract")
        return not abst or not str(abst).strip()
    except Exception:
        return True


def _s(v, default: str = "") -> str:
    return str(v).strip() if _nn(v) else default


def _esc(v) -> str:
    return _html.escape(str(v)) if v is not None else ""


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


def _infoscience_badge(status) -> str:
    s = _s(status).lower()
    if not s:
        return ""
    css   = INFOSCIENCE_STATUS_CSS.get(s, "ifs-st-pending")
    label = INFOSCIENCE_STATUS_LABELS.get(s, s)
    return f'<span class="ifs-badge {css}">{label}</span>'


def _quality_badges(row: dict) -> str:
    if _s(row.get("infoscience_status")).lower() != "published":
        return ""
    if row.get("quality_checked_at") is None:
        return ""
    parts = []
    if row.get("quality_abstract_ok") is False:
        parts.append('<span class="pub-quality-warn" title="Résumé absent dans Infoscience">⚑ résumé</span>')
    if row.get("quality_pdf_ok") is False:
        parts.append('<span class="pub-quality-warn" title="PDF OA non trouvé dans Infoscience">⚑ PDF OA</span>')
    return "".join(parts)


def _type_badge(dc_type) -> str:
    if not _nn(dc_type):
        return ""
    parts = str(dc_type).split("::")
    lbl   = (parts[-1] if len(parts) > 1 else parts[0]).strip()[:28]
    return f'<span class="ptbl-type" title="{dc_type}">{lbl}</span>'


def _action_links(row: dict, ds_base: str = "") -> str:
    ifs = _s(row.get("infoscience_status")).lower()
    if ifs == "published":
        handle = row.get("infoscience_handle")
        if _nn(handle) and ds_base:
            href = f"{ds_base}/handle/{handle}"
            return (
                '<div class="ptbl-actions">'
                f'<a href="{href}" target="_blank" class="ptbl-act ptbl-act-public">Public</a>'
                "</div>"
            )
        return '<span style="color:#D0D5DD;font-size:11px">—</span>'

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

    # ── Meta line: year · source badge · status badge · type · infoscience status
    meta = (
        f'<span class="ptbl-year">{year}</span>'
        f'{_src_badge(row.get("source"), src_u)}'
        f'{_status_badge(row.get("status"))}'
        f'{_type_badge(row.get("dc_type"))}'
        f'{_infoscience_badge(row.get("infoscience_status"))}'
        f'{_quality_badges(row)}'
    )
    if _has_no_abstract(row):
        meta += '<span class="ms ptbl-no-abst ptbl-no-abst--missing" title="Résumé manquant">hide_source</span>'
    else:
        meta += '<span class="ms ptbl-no-abst ptbl-no-abst--present" title="Résumé présent">subject</span>'
    if row.get("_all_former_epfl"):
        meta += '<span class="ptbl-badge ptbl-former-epfl" title="Tous les auteurs EPFL sont d\'anciens membres rejetés">Former EPFL</span>'
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

@st.dialog("Métadonnées", width="large")
def _meta_modal(row: dict) -> None:
    """Show collected metadata for a publication."""
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

    parts = [
        f'<div class="modal-header">'
        f'<div class="modal-title">{_esc(_s(row.get("title"), "—"))}</div>'
        f'<div class="modal-subtitle">{_esc(_s(row.get("source")))} · {_esc(_s(row.get("pub_year")))}</div>'
        f'</div>'
    ]
    if not meta:
        parts.append('<p class="modal-empty">Métadonnées brutes non disponibles — colonnes DB affichées.</p>')

    for sec_name, keys in sections:
        items = [(k, str(src[k])) for k in keys if k in src and _nn(src.get(k))]
        if not items:
            continue
        parts.append(f'<div class="modal-sec">{_esc(sec_name)}</div><div class="modal-grid">')
        for k, v in items:
            ev = _esc(v)
            if len(v) > 250:
                parts.append(
                    f'<span class="modal-key">{_esc(k)}</span>'
                    f'<details class="modal-long">'
                    f'<summary>{ev[:100]}…</summary>'
                    f'<span class="modal-val-long">{ev}</span>'
                    f'</details>'
                )
            else:
                parts.append(f'<span class="modal-key">{_esc(k)}</span><span class="modal-val">{ev}</span>')
        parts.append("</div>")

    st.markdown("".join(parts), unsafe_allow_html=True)


@st.dialog("Auteurs EPFL", width="large")
def _authors_modal(title: str, authors: list, ds_base: str = "") -> None:
    """Show reconciled EPFL authors for a publication."""
    if not authors:
        st.info("Aucun auteur EPFL réconcilié pour cette publication.")
        return

    parts = [
        f'<div class="modal-header">'
        f'<div class="modal-title">{_esc(title[:120])}</div>'
        f'</div>'
        f'<div class="modal-author-list">'
    ]
    for a in authors:
        sciper     = a.get("sciper", "")
        dspace_uuid = a.get("dspace_uuid", "")
        name       = _esc(a.get("name") or sciper or "?")
        weak       = a.get("weak", False)
        status     = _esc(a.get("epfl_status") or "")
        pos        = _esc(a.get("epfl_position") or "")
        unit       = _esc(a.get("main_unit") or "")
        orcid      = _esc(a.get("orcid") or "")

        is_former       = a.get("epfl_is_former", False)
        is_rejected     = is_former and not a.get("dspace_link_valid", True)
        is_active       = bool(sciper) and not is_former
        is_unreconciled = not bool(sciper)

        if is_rejected:
            card_cls = "modal-author-card modal-author-former-rejected"
        elif is_former:
            card_cls = "modal-author-card modal-author-former"
        elif is_unreconciled:
            card_cls = "modal-author-card modal-author-unreconciled"
        elif weak:
            card_cls = "modal-author-card modal-author-weak"
        else:
            card_cls = "modal-author-card"

        weak_badge   = '<span class="modal-badge-weak">⚠ Statut faible</span>' if weak and not is_unreconciled else ""
        former_badge = (
            '<span class="modal-badge-former modal-badge-former--rejected">Former EPFL (rejeté)</span>'
            if is_rejected else
            '<span class="modal-badge-former">Former EPFL (toléré)</span>'
            if is_former else ""
        )
        unreconciled_badge = '<span class="modal-badge-unreconciled">Non réconcilié</span>' if is_unreconciled else ""

        if is_rejected:
            ms_icon = '<span class="ms modal-ms-icon modal-ms-former-rejected" title="Ancien membre EPFL — affiliation hors fenêtre (rejetée)">person_off</span>'
        elif is_former:
            ms_icon = '<span class="ms modal-ms-icon modal-ms-former-tolerated" title="Ancien membre EPFL — affiliation tolérée">schedule</span>'
        elif is_active:
            ms_icon = '<span class="ms modal-ms-icon modal-ms-active" title="Membre EPFL actif">verified</span>'
        elif is_unreconciled:
            ms_icon = '<span class="ms modal-ms-icon modal-ms-unreconciled" title="Affiliation EPFL détectée — non réconcilié (SCIPER introuvable)">person_search</span>'
        else:
            ms_icon = ""

        people     = (f'<a href="https://people.epfl.ch/{sciper}" target="_blank" class="modal-author-link">People →</a>'
                      if sciper else "")
        infoscience = (f'<a href="{ds_base}/entities/person/{dspace_uuid}" target="_blank" class="modal-author-link">Infoscience →</a>'
                       if dspace_uuid and ds_base else "")

        sciper_chip = f'<span class="modal-chip modal-chip-sciper">SCIPER {_esc(sciper)}</span>' if sciper else ""
        orcid_chip  = (
            f'<a href="https://orcid.org/{orcid}" target="_blank" class="modal-chip modal-chip-orcid">'
            f'ORCID {orcid}</a>'
            if orcid else ""
        )
        status_chip = f'<span class="modal-chip modal-chip-status">{status}</span>' if status else ""
        pos_chip    = f'<span class="modal-chip modal-chip-pos">{pos}</span>' if pos else ""
        unit_chip   = f'<span class="modal-chip modal-chip-unit">{unit}</span>' if unit else ""
        chips       = status_chip + pos_chip + unit_chip

        parts.append(
            f'<div class="{card_cls}">'
            f'<div class="modal-author-hd">'
            f'{ms_icon}'
            f'<span class="modal-author-name">{name}</span>'
            f'{weak_badge}{former_badge}{unreconciled_badge}{people}{infoscience}'
            f'</div>'
            f'<div class="modal-chips">{chips}{sciper_chip}{orcid_chip}</div>'
            f'</div>'
        )
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


@st.dialog("Doublon Infoscience", width="large")
def _flagged_modal(title: str, flagged_raw: str, dedup_note: str, ds_base: str) -> None:
    """Show flagged duplicate info for curation."""
    label = _esc(DEDUP_LABELS.get(dedup_note, dedup_note or "Signalé"))
    parts = [
        f'<div class="modal-header">'
        f'<div class="modal-title">{_esc(title[:120])}</div>'
        f'<span class="modal-flag-label">{label}</span>'
        f'</div>'
    ]
    try:
        items = json.loads(flagged_raw)
    except Exception:
        items = []
    if not isinstance(items, list):
        items = [items]

    parts.append('<div class="modal-dup-list">')
    for item in items:
        uuid_val = item.get("uuid", "")
        doi      = item.get("doi", "")
        dc_type  = item.get("dc_type", "")
        parts.append('<div class="modal-dup-card"><div class="modal-grid">')
        if uuid_val:
            parts.append(
                f'<span class="modal-key">uuid</span>'
                f'<span class="modal-val">'
                f'<a href="{ds_base}/items/{_esc(uuid_val)}" target="_blank" class="modal-link">{_esc(uuid_val)}</a>'
                f'</span>'
            )
        if doi:
            parts.append(
                f'<span class="modal-key">doi</span>'
                f'<span class="modal-val">'
                f'<a href="https://doi.org/{_esc(doi)}" target="_blank" class="modal-link">{_esc(doi)}</a>'
                f'</span>'
            )
        if dc_type:
            parts.append(
                f'<span class="modal-key">type</span>'
                f'<span class="modal-val"><code class="modal-code">{_esc(dc_type)}</code></span>'
            )
        parts.append("</div></div>")
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


@st.dialog("Supprimer un item importé", width="small")
def _delete_modal(
    workspace_id: str,
    workflow_id: str | None,
    item_uuid: str | None,
    title: str,
    db,
) -> None:
    """Confirmation and execution of DSpace item deletion."""
    info = (
        f'<div class="modal-header">'
        f'<div class="modal-title">{_esc(title[:100])}</div>'
        f'</div>'
        f'<div class="modal-grid" style="margin-bottom:10px">'
        f'<span class="modal-key">workspace</span>'
        f'<span class="modal-val"><code class="modal-code">{_esc(str(workspace_id))}</code></span>'
    )
    if workflow_id:
        info += (
            f'<span class="modal-key">workflow</span>'
            f'<span class="modal-val"><code class="modal-code">{_esc(str(workflow_id))}</code></span>'
        )
    info += "</div>"
    if workflow_id:
        info += '<p class="modal-empty" style="color:#92400E;margin-bottom:6px">Rejet du workflow avant suppression du workspace.</p>'
    st.markdown(info, unsafe_allow_html=True)
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

_W_ACT   = 0.85  # action links
_W_MAIN  = 6.8   # rich content block
_W_BTN   = 0.46  # each icon button

# Header icon for each button column (Material Symbols name → tooltip)
_BTN_HEADER: dict[str, tuple[str, str]] = {
    "Meta":    ("description", "Métadonnées"),
    "Aut.":    ("people",      "Auteurs EPFL"),
    "Signal.": ("flag",        "Doublon Infoscience"),
    "Sync":    ("sync",        "Synchroniser avec Infoscience"),
    "Suppr.":  ("delete",      "Supprimer"),
}


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
    has_run    = "run_id" in cols
    can_sync   = role != "reporting"
    can_delete = role == "admin"
    n_btns     = 3 + int(can_sync) + int(can_delete)
    widths     = [_W_ACT, _W_MAIN] + [_W_BTN] * n_btns

    _sync_col = 5
    _del_col  = 5 + int(can_sync)

    with st.container(border=True):
        # ── Header ────────────────────────────────────────────────────────────
        hdr = st.columns(widths)
        hdr[0].markdown('<div class="ptbl-hdr">Actions</div>', unsafe_allow_html=True)
        hdr[1].markdown('<div class="ptbl-hdr">Publication</div>', unsafe_allow_html=True)
        btn_labels = ["Meta", "Aut.", "Signal."]
        if can_sync:
            btn_labels.append("Sync")
        if can_delete:
            btn_labels.append("Suppr.")
        for i, lbl in enumerate(btn_labels):
            icon, tooltip = _BTN_HEADER.get(lbl, (lbl, lbl))
            hdr[2 + i].markdown(
                f'<div class="ptbl-hdr" style="text-align:center">'
                f'<span class="ms ms-neutral ptbl-hdr-icon" title="{tooltip}">{icon}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
        st.markdown('<hr class="ptbl-sep">', unsafe_allow_html=True)

        # ── Rows ──────────────────────────────────────────────────────────────
        for idx, row in enumerate(d.to_dict("records")):
            auths      = authors_by_row.get(f"{_s(row.get('run_id'))}:{_s(row.get('row_id'))}", [])
            all_former = bool(auths) and all(a.get("epfl_is_former", False) for a in auths)
            row        = {**row, "_all_former_epfl": all_former}
            has_flag   = _nn(row.get("flagged_publication"))
            title      = _s(row.get("title"), "—")
            ws_raw   = row.get("workspace_id")
            wf_raw   = row.get("workflow_id")
            uuid_raw = row.get("dspace_item_uuid")

            # Pre-compute string IDs once — reused by sync and delete buttons
            ws_id: str | None = None
            if _nn(ws_raw):
                try:
                    ws_id = str(int(float(str(ws_raw))))
                except (ValueError, TypeError):
                    ws_id = _s(ws_raw) or None
            wf_id: str | None = None
            if _nn(wf_raw):
                try:
                    wf_id = str(int(float(str(wf_raw))))
                except (ValueError, TypeError):
                    wf_id = _s(wf_raw) or None

            rc = st.columns(widths)

            # col 0 — action links (HTML only, no Streamlit widget)
            rc[0].markdown(_action_links(row, ds_base), unsafe_allow_html=True)

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
                    _authors_modal(title, auths, ds_base)

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

            # col 5 — sync (admin + curator, disabled without DSpace identifiers)
            if can_sync:
                _ifs = _s(row.get("infoscience_status")).lower()
                _has_ids = bool(_nn(uuid_raw) or ws_id or wf_id)
                if rc[_sync_col].button(
                    "", icon=":material/sync:", key=f"sync_{idx}",
                    use_container_width=True,
                    help="Synchroniser le statut et les contrôles qualité avec Infoscience",
                    disabled=not _has_ids or _ifs == "deleted",
                ):
                    st.session_state["_pub_pending_sync"] = {
                        "run_id":           _s(row.get("run_id")),
                        "pub_id":           _s(row.get("pub_id")),
                        "dspace_item_uuid": _s(uuid_raw) or None,
                        "workspace_id":     ws_id,
                        "workflow_id":      wf_id,
                        "upw_license":      _s(row.get("upw_license")) or None,
                    }
                    st.rerun()

            # col 5/6 — delete (admin only, disabled once published)
            if can_delete:
                _is_published = _s(row.get("infoscience_status")).lower() == "published"
                if rc[_del_col].button("", icon=":material/delete:", key=f"del_{idx}",
                                       use_container_width=True, help="Supprimer",
                                       disabled=not ws_id or _is_published):
                    if ws_id:
                        _delete_modal(ws_id, wf_id, _s(uuid_raw) or None, title, db)

            st.markdown('<hr class="ptbl-sep">', unsafe_allow_html=True)
