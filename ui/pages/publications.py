"""Publications page — filterable paginated table with download buttons."""

from __future__ import annotations

import math
import os
from datetime import date

import pandas as pd
import streamlit as st

from db.pipeline_db import PipelineDB
from ui.constants import C_GRAY_600
from ui.helpers import page_title
from ui.pub_helpers import is_weak, oa_text, lic_text, source_api_url
from ui.components.pub_table import render_pub_component


_FILTER_DEFAULTS: dict[str, object] = {
    "pf_run": [], "pf_type": [], "pf_status": [], "pf_source": [],
    "pf_unit": [], "pf_sciper": "", "pf_search": "",
    "pf_oa": "Tous", "pf_pdf": "Tous", "pf_licence": [], "pf_epfl": "Tous",
    "pf_dedup_note": "Tous",
}

_STATUS_LABELS: dict[str, str] = {
    "workflow": "En workflow", "workspace": "En workspace",
    "deduplicated": "Dédoublonnées", "rejected": "Rejetées", "error": "Erreurs",
}


def render(db: PipelineDB) -> None:
    """Render the publications page — filterable paginated table with download buttons."""
    page_title("article", "Publications")

    _render_filters(db)
    _render_table(db)


def _render_filters(db: PipelineDB) -> None:
    def _reset():
        for k, v in _FILTER_DEFAULTS.items():
            st.session_state[k] = v
        st.session_state["pub_page"] = 1

    with st.expander("Filtres", icon=":material/search:", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            runs_df = db.get_runs(limit=50)
            run_opts = runs_df["run_id"].tolist() if not runs_df.empty else []
            st.multiselect("Run", run_opts, key="pf_run")
            st.multiselect("Type de document", db.get_distinct_dc_types(), key="pf_type")
        with c2:
            st.multiselect(
                "Statut",
                ["workflow", "workspace", "deduplicated", "rejected", "error"],
                key="pf_status",
            )
            st.multiselect("Source", db.get_distinct_sources(), key="pf_source")
        with c3:
            st.multiselect("Unité", db.get_distinct_units(), key="pf_unit")
            st.text_input("SCIPER ou nom auteur EPFL", placeholder="123456 ou Dupont", key="pf_sciper")

        st.text_input("Recherche titre / DOI", placeholder="deep learning…", key="pf_search")

        cf1, cf2, cf3, cf4, cf5, cf6 = st.columns([2, 2, 2, 2, 2, 1])
        with cf1:
            st.selectbox(
                "Statut OA", ["Tous", "OA", "Non-OA", "Non-libre", "Non défini"],
                help="Filtre sur le statut Open Access (Unpaywall).", key="pf_oa",
            )
        with cf2:
            st.selectbox(
                "PDF récupéré", ["Tous", "Avec PDF", "Sans PDF"],
                help="Filtre sur la présence d'un PDF en accès libre.", key="pf_pdf",
            )
        with cf3:
            st.multiselect(
                "Licence", db.get_distinct_licences(),
                help="Filtre sur la licence Unpaywall.", key="pf_licence",
            )
        with cf4:
            st.selectbox(
                "Statut auteurs EPFL",
                ["Tous", "⚠️ Statut faible", "✅ Statut fort"],
                help=(
                    "Faible : tous les auteurs sont hôtes, externes ou étudiants.\n"
                    "Fort : au moins un auteur permanent."
                ),
                key="pf_epfl",
            )
        with cf5:
            st.selectbox(
                "Signalement dedup",
                ["Tous", "🚩 Flaggés", "supersedes_preprint", "cross_type_doi"],
                help=(
                    "supersedes_preprint : version publiée importée, preprint déjà dans Infoscience.\n"
                    "cross_type_doi : même DOI qu'un preprint existant."
                ),
                key="pf_dedup_note",
            )
        with cf6:
            st.markdown("<div style='padding-top:24px'>", unsafe_allow_html=True)
            st.button("Reset", icon=":material/refresh:", on_click=_reset,
                      use_container_width=True, help="Réinitialiser tous les filtres")
            st.markdown("</div>", unsafe_allow_html=True)


def _build_filter_kwargs(db: PipelineDB) -> dict:
    sel_run        = st.session_state.get("pf_run", [])
    sel_type       = st.session_state.get("pf_type", [])
    sel_status     = st.session_state.get("pf_status", [])
    sel_source     = st.session_state.get("pf_source", [])
    sel_unit       = st.session_state.get("pf_unit", [])
    sciper_q       = st.session_state.get("pf_sciper", "")
    search_q       = st.session_state.get("pf_search", "")
    sel_oa         = st.session_state.get("pf_oa", "Tous")
    sel_pdf        = st.session_state.get("pf_pdf", "Tous")
    sel_licence    = st.session_state.get("pf_licence", [])
    sel_epfl       = st.session_state.get("pf_epfl", "Tous")
    sel_dedup_note = st.session_state.get("pf_dedup_note", "Tous")

    resolved_sciper = _resolve_sciper(db, sciper_q)

    return dict(
        run_id        = sel_run or None,
        status        = sel_status or None,
        source        = sel_source or None,
        dc_type       = sel_type or None,
        sciper        = resolved_sciper or None,
        unit_acronym  = sel_unit or None,
        search        = search_q.strip() or None,
        has_pdf       = True if sel_pdf == "Avec PDF" else (False if sel_pdf == "Sans PDF" else None),
        oa_filter     = None if sel_oa == "Tous" else sel_oa,
        licence       = sel_licence or None,
        epfl_strength = (
            "weak"   if sel_epfl == "⚠️ Statut faible" else
            "strong" if sel_epfl == "✅ Statut fort"   else None
        ),
        dedup_note    = (
            None          if sel_dedup_note == "Tous"     else
            "__flagged__" if sel_dedup_note == "🚩 Flaggés" else
            sel_dedup_note
        ),
    ), sel_run


def _resolve_sciper(db: PipelineDB, sciper_q: str) -> str | None:
    if not sciper_q.strip():
        return None
    if sciper_q.strip().isdigit():
        return sciper_q.strip()
    matches = db.get_epfl_authors(name_search=sciper_q.strip(), limit=10)
    if not matches.empty:
        options = [f"{r['sciper']} — {r['full_name']}" for _, r in matches.iterrows()]
        chosen = st.selectbox("Auteur EPFL trouvé :", options)
        return chosen.split(" — ")[0] if chosen else None
    st.caption("Aucun auteur EPFL trouvé pour cette recherche.")
    return None


def _render_table(db: PipelineDB) -> None:
    filter_kwargs, sel_run = _build_filter_kwargs(db)

    filter_sig = str(sorted(filter_kwargs.items()))
    if "pub_page" not in st.session_state:
        st.session_state["pub_page"] = 1
    if st.session_state.get("_pub_filter_sig") != filter_sig:
        st.session_state["_pub_filter_sig"] = filter_sig
        st.session_state["pub_page"] = 1

    total = db.count_publications(**filter_kwargs)

    _pc1, _pc2, _pc3 = st.columns([2, 2, 6])
    with _pc1:
        page_size = st.selectbox("Lignes / page", [25, 50, 100, 200], index=1, key="pub_page_size")
    total_pages = max(1, math.ceil(total / page_size))
    with _pc2:
        page_num = st.number_input(
            f"Page (/{total_pages})", min_value=1, max_value=total_pages,
            key="pub_page", step=1,
        )
    with _pc3:
        st.markdown(
            f"<div style='padding-top:28px;color:{C_GRAY_600};font-size:0.88rem'>"
            f"<b>{total}</b> publication(s) — page {page_num}/{total_pages}</div>",
            unsafe_allow_html=True,
        )

    offset  = (page_num - 1) * page_size
    pub_df  = db.get_publications(**filter_kwargs, limit=page_size, offset=offset)

    # ── Quick metrics ─────────────────────────────────────────────────────────
    _m = {s: db.count_publications(**{**filter_kwargs, "status": s}) for s in _STATUS_LABELS}
    _m_flagged = db.count_publications(**{**filter_kwargs, "dedup_note": "__flagged__"})
    m_cols = st.columns(6)
    for col, (stat, label) in zip(m_cols, _STATUS_LABELS.items()):
        col.metric(label, _m[stat])
    m_cols[5].metric("Signalées", _m_flagged, icon=":material/flag:")

    if pub_df.empty:
        st.info("Aucune publication correspondant aux filtres.")
        return

    ds_base = os.getenv("DS_API_ENDPOINT", "").replace("/server/api", "")
    d = _enrich_dataframe(pub_df, db, sel_run, ds_base)

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

    authors_by_row = _build_authors_modal_dict(d, db, sel_run)
    render_pub_component(d, _cols, authors_by_row, ds_base)

    _render_downloads(db, filter_kwargs, sel_run)


def _enrich_dataframe(pub_df: pd.DataFrame, db: PipelineDB, sel_run: list, ds_base: str) -> pd.DataFrame:
    d = pub_df.copy()

    d["OA"]      = d.apply(oa_text, axis=1)
    d["Licence"] = d["upw_license"].apply(lic_text)
    d["PDF"]     = d["upw_valid_pdf"].apply(lambda v: False if pd.isna(v) else bool(v))

    d["src_url"] = d.apply(
        lambda r: source_api_url(r.get("source"), r.get("internal_id"), r.get("doi")), axis=1
    )
    d["doi_url"] = d["doi"].apply(
        lambda x: f"https://doi.org/{x}" if pd.notna(x) and str(x).startswith("10.") else None
    )
    d["ws_url"] = d.apply(
        lambda r: (
            f"{ds_base}/workspaceitems/{int(float(r['workspace_id']))}/edit"
            if pd.notna(r.get("workspace_id")) and r.get("workspace_id") != ""
            and (pd.isna(r.get("workflow_id")) or r.get("workflow_id") == "")
            else None
        ), axis=1,
    )
    d["wf_url"] = d.apply(
        lambda r: (
            f"{ds_base}/mydspace?configuration=workflow&spc.page=1&query={r['dspace_item_uuid']}"
            if pd.notna(r.get("workflow_id")) and r.get("workflow_id") != ""
            and pd.notna(r.get("dspace_item_uuid")) and r.get("dspace_item_uuid") != ""
            else None
        ), axis=1,
    )
    d["item_url"] = (
        d["dspace_item_uuid"].apply(
            lambda u: f"{ds_base}/items/{u}" if pd.notna(u) and u != "" else None
        )
        if "dspace_item_uuid" in d.columns else None
    )

    # Author + unit enrichment
    if len(sel_run) == 1:
        single_run = sel_run[0]
        a_df = db.get_pub_authors_for_run(single_run)
        if not a_df.empty:
            a_df.insert(0, "run_id", single_run)
        u_df = db.get_pub_units_for_run(single_run)
        if not u_df.empty:
            u_df.insert(0, "run_id", single_run)
        det_df = db.get_detected_authors_for_run(single_run)
        if not det_df.empty:
            det_df.insert(0, "run_id", single_run)
    else:
        pairs   = list(zip(pub_df["run_id"], pub_df["row_id"]))
        a_df    = db.get_pub_authors_for_rows(pairs)
        u_df    = db.get_pub_units_for_rows(pairs)
        det_df  = db.get_detected_authors_for_rows(pairs)

    run_authors, run_weak = _build_authors_dict(a_df)
    run_units             = _build_units_dict(u_df)
    run_detected          = _build_detected_dict(det_df)

    def _epfl_authors_cell(r):
        key = (r["run_id"], r["row_id"])
        raw_detected = run_detected.get(key, "")
        unreconciled = "; ".join(f"~ {n}" for n in raw_detected.split("; ") if n) if raw_detected else ""
        reconciled   = run_authors.get(key, "")
        return "; ".join(p for p in (unreconciled, reconciled) if p)

    d["Auteurs EPFL"] = d.apply(_epfl_authors_cell, axis=1)
    d["Unités"]       = d.apply(lambda r: run_units.get((r["run_id"], r["row_id"]), ""), axis=1)
    d["⚠️"]           = d.apply(lambda r: run_weak.get((r["run_id"], r["row_id"]), False), axis=1)

    return d


def _build_authors_modal_dict(d: pd.DataFrame, db: PipelineDB, sel_run: list) -> dict:
    if len(sel_run) == 1:
        auth_df = db.get_pub_authors_for_run(sel_run[0])
    else:
        pairs   = list(zip(d["run_id"], d["row_id"]))
        auth_df = db.get_pub_authors_for_rows(pairs)

    out: dict[str, list] = {}
    for _, ar in auth_df.iterrows():
        rk = _clean_str(ar.get("row_id"))
        if not rk:
            continue
        st_val = _clean_str(ar.get("epfl_status"))
        pos    = _clean_str(ar.get("epfl_position"))
        out.setdefault(rk, []).append({
            "name":          _clean_str(ar.get("full_name")) or _clean_str(ar.get("sciper")) or "?",
            "sciper":        _clean_str(ar.get("sciper")),
            "orcid":         _clean_str(ar.get("orcid")),
            "epfl_status":   st_val,
            "epfl_position": pos,
            "main_unit":     _clean_str(ar.get("main_unit")),
            "weak":          is_weak(st_val, pos),
        })
    return out


def _render_downloads(db: PipelineDB, filter_kwargs: dict, sel_run: list) -> None:
    st.markdown("<br>", unsafe_allow_html=True)
    dl_cols = st.columns(3)
    run_label = "-".join(sel_run) if sel_run else "all"

    with dl_cols[0]:
        full_df = db.get_publications(**filter_kwargs, limit=10_000, offset=0)
        st.download_button(
            "Publications CSV",
            icon=":material/download:",
            data=full_df.to_csv(index=False).encode("utf-8"),
            file_name=f"publications_{run_label}_{date.today()}.csv",
            mime="text/csv",
        )

    with dl_cols[1]:
        if len(sel_run) == 1:
            ax_df = db.get_pub_authors_for_run(sel_run[0])
            if not ax_df.empty:
                st.download_button(
                    "Publications × Auteurs CSV",
                    icon=":material/download:",
                    data=ax_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"pub_authors_{sel_run[0]}_{date.today()}.csv",
                    mime="text/csv",
                )

    with dl_cols[2]:
        if len(sel_run) == 1:
            from pathlib import Path
            _project_root = Path(__file__).resolve().parent.parent.parent
            run_dir  = _project_root / "data" / sel_run[0]
            reports  = list(run_dir.glob("*Report*.xlsx")) if run_dir.exists() else []
            if reports:
                with open(reports[0], "rb") as f:
                    st.download_button(
                        "Rapport Excel du run",
                        icon=":material/download:",
                        data=f.read(),
                        file_name=reports[0].name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            else:
                st.caption("Aucun rapport Excel disponible.")


# ── Pure data helpers ─────────────────────────────────────────────────────────

def _clean_str(v) -> str:
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


def _build_authors_dict(authors_df: pd.DataFrame) -> tuple[dict, dict]:
    out_authors: dict = {}
    out_weak: dict    = {}
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
            parts.append("✓ " + name + (f" ({hint})" if hint else ""))
            if not is_weak(st_val, pos):
                is_all_weak = False
        out_authors[key] = "; ".join(parts)
        out_weak[key]    = is_all_weak and len(parts) > 0
    return out_authors, out_weak


def _build_units_dict(units_df: pd.DataFrame) -> dict:
    out: dict = {}
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


def _build_detected_dict(det_df: pd.DataFrame) -> dict:
    out: dict = {}
    if det_df.empty:
        return out
    for key_vals, grp in det_df.groupby(["run_id", "row_id"]):
        out[tuple(key_vals)] = "; ".join(grp["author_name"].dropna().tolist())
    return out
