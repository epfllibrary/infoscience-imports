"""Researcher Monitor page — registry, gap analysis, sync controls.

URI routing: ?sciper=<SCIPER> opens the dedicated researcher detail page.
No query param → shows the registry + tabs view.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from db.pipeline_db import PipelineDB
from ui.components.researcher_jobs import (
    launch_import_job,
    launch_researcher_job,
    read_active_researcher_job,
    render_researcher_job_form,
    render_researcher_job_running,
)
from ui.components.researcher_table import (
    render_harvested_content,
    render_infoscience_content,
    render_lacunes_tab,
    render_profil_content,
    render_researcher_card,
    render_researcher_header,
    render_units_content,
)
from ui.helpers import db_lock_guard, metric_card, mi, page_title, sh

try:
    from apscheduler.triggers.cron import CronTrigger as _CronTrigger
    _HAS_APSCHEDULER = True
except ImportError:
    _HAS_APSCHEDULER = False

_RESEARCHER_SYNC_JOB_KEY = "researcher_registry_sync"
_STATUS_ICON: dict[str | None, str] = {
    "completed": "✅", "running": "⏳", "failed": "❌", None: "—",
}
_PAGE_SIZE = 25
_CARD_PAGE_SIZE = 16


# ── Schedules.json helpers ────────────────────────────────────────────────────

def _load_system_jobs(sched_file: Path) -> dict:
    if not sched_file.exists():
        return {}
    try:
        return json.loads(sched_file.read_text(encoding="utf-8")).get("system_jobs", {})
    except Exception:
        return {}


def _save_system_job(sched_file: Path, key: str, updates: dict) -> None:
    existing: dict = {}
    if sched_file.exists():
        try:
            existing = json.loads(sched_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    existing.setdefault("system_jobs", {}).setdefault(key, {}).update(updates)
    fd, tmp_path = tempfile.mkstemp(dir=sched_file.parent, suffix=".tmp")
    tmp = Path(tmp_path)
    try:
        os.close(fd)
        tmp.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(sched_file)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ── Filtering helpers ─────────────────────────────────────────────────────────

def _apply_registry_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    view = df.copy()

    if not filters.get("show_inactive", False):
        view = view[view["is_active"] == True]  # noqa: E712

    unit = filters.get("unit", [])
    if unit:
        view = view[view["main_unit"].isin(unit)]

    school = filters.get("school", [])
    if school:
        view = view[view["unit_level_2"].astype(str).str.strip().isin(school)]

    orcid_mode = filters.get("orcid_mode", "Tous")
    _has_orcid = view["orcid"].notna() & (view["orcid"].astype(str).str.strip() != "")
    _orcid_linked = view["orcid_epfl_linked"].fillna(False).astype(bool)
    if orcid_mode == "Avec ORCID":
        view = view[_has_orcid]
    elif orcid_mode == "Sans ORCID":
        view = view[~_has_orcid]
    elif orcid_mode == "ORCID lié EPFL":
        view = view[_orcid_linked]
    elif orcid_mode == "ORCID non lié":
        view = view[_has_orcid & ~_orcid_linked]

    openalex_mode = filters.get("openalex_mode", "Tous")
    _has_oa = view["openalex_id"].notna() & (view["openalex_id"].astype(str).str.strip() != "")
    _oa_in_is = view["openalex_in_infoscience"].fillna(False).astype(bool)
    if openalex_mode == "Avec OpenAlex":
        view = view[_has_oa]
    elif openalex_mode == "Sans OpenAlex":
        view = view[~_has_oa]
    elif openalex_mode == "OpenAlex depuis IS":
        view = view[_oa_in_is]
    elif openalex_mode == "OpenAlex non dans IS":
        view = view[_has_oa & ~_oa_in_is]

    dspace_mode = filters.get("dspace_mode", "Tous")
    if dspace_mode == "Avec profil Infoscience":
        view = view[
            view["dspace_uuid"].notna() & (view["dspace_uuid"].astype(str).str.strip() != "")
        ]
    elif dspace_mode == "Sans profil Infoscience":
        view = view[
            view["dspace_uuid"].isna() | (view["dspace_uuid"].astype(str).str.strip() == "")
        ]

    gap_mode = filters.get("gap_mode", "Tous")
    if gap_mode == "Avec lacunes":
        view = view[view["gaps_missing"].notna() & (view["gaps_missing"] > 0)]
    elif gap_mode == "Sans lacune":
        view = view[view["gaps_missing"].notna() & (view["gaps_missing"] == 0)]
    elif gap_mode == "Non analysés":
        view = view[view["last_gap_analysis_at"].isna()]

    position_filter = filters.get("position_filter", [])
    if position_filter:
        view = view[view["epfl_position"].astype(str).str.strip().isin(position_filter)]

    class_filter = filters.get("class_filter", [])
    if class_filter:
        view = view[view["epfl_class"].astype(str).str.strip().isin(class_filter)]

    search = filters.get("search", "").strip().lower()
    if search:
        mask = (
            view["full_name"].str.lower().str.contains(search, na=False)
            | view["sciper"].astype(str).str.lower().str.contains(search, na=False)
            | view["main_unit"].str.lower().str.contains(search, na=False)
            | view["epfl_position"].str.lower().str.contains(search, na=False)
        )
        view = view[mask]

    return view


# ── Registry helpers ──────────────────────────────────────────────────────────

def _count_filled(series: "pd.Series") -> int:
    return int(series.notna().sum() - (series.astype(str).str.strip() == "").sum())


_REGISTRY_EXPORT_COLS = [
    "sciper", "full_name", "epfl_position", "epfl_class",
    "main_unit", "unit_level_2", "orcid", "scopus_author_id",
    "openalex_id", "dspace_uuid", "last_harvest_at",
    "last_gap_analysis_at", "gaps_missing", "harvested_pubs",
    "infoscience_pubs", "is_active",
]


# ── KPI row helper ────────────────────────────────────────────────────────────

def _kpi_row(items: list[tuple[str, object, str]]) -> None:
    cols = st.columns(len(items))
    for col, (label, value, sub) in zip(cols, items):
        col.markdown(metric_card(label, value, sub), unsafe_allow_html=True)


# ── Main render ───────────────────────────────────────────────────────────────

def render(
    db: PipelineDB,
    active_env: str = "dev",
    root: Path = Path("."),
    role: str = "reporting",
) -> None:
    sciper_param = st.query_params.get("sciper")
    if sciper_param:
        _render_researcher_detail(
            sciper=str(sciper_param),
            db=db,
            role=role,
            root=root,
            active_env=active_env,
        )
        return

    page_title("manage_accounts", "Chercheurs")

    tab_reg, tab_pubs, tab_gaps, tab_sync = st.tabs([
        "Registre",
        "Publications",
        "Lacunes",
        "Synchronisation",
    ])

    with tab_reg:
        db_lock_guard(lambda: _render_registry(db, role=role, root=root, active_env=active_env))

    with tab_pubs:
        db_lock_guard(lambda: _render_publications(db))

    with tab_gaps:
        db_lock_guard(lambda: _render_gaps(db, active_env=active_env, root=root, role=role))

    with tab_sync:
        _render_sync(db, active_env=active_env, root=root, role=role)


# ── Researcher detail page ────────────────────────────────────────────────────

def _render_researcher_detail(
    sciper: str,
    db: PipelineDB,
    role: str,
    root: Path,
    active_env: str,
) -> None:
    """Full detail page for a single researcher, routed via ?sciper=<SCIPER>."""
    if st.button("← Retour au registre", key="btn_back_to_registry", type="secondary"):
        del st.query_params["sciper"]
        st.rerun()

    # ── Fetch researcher row ──────────────────────────────────────────────────
    try:
        reg_df = db.get_researcher_registry_df(active_only=False)
        matches = reg_df[reg_df["sciper"].astype(str) == str(sciper)]
    except Exception as exc:
        st.error(f"Erreur lors de la récupération du registre : {exc}")
        return

    if matches.empty:
        st.error(f"SCIPER **{sciper}** introuvable dans le registre.")
        return

    row = matches.iloc[0].to_dict()
    name = str(row.get("full_name") or "?")

    page_title("person", name)
    render_researcher_header(row)

    # ── KPI row ───────────────────────────────────────────────────────────────
    harvested    = int(row.get("harvested_pubs") or 0)
    infos_pubs   = int(row.get("infoscience_pubs") or 0)
    gaps_missing = row.get("gaps_missing")
    last_harvest = str(row.get("last_harvest_at") or "—")[:10]
    last_gap     = str(row.get("last_gap_analysis_at") or "—")[:10]

    _kpi_row([
        ("Moissonnées", harvested, f"dernier : {last_harvest}"),
        ("Infoscience", infos_pubs, "profil CRIS"),
        ("Lacunes",
         "—" if gaps_missing is None else int(gaps_missing),
         f"analysé le {last_gap}"),
    ])

    st.markdown("---")

    # ── Active job detection ──────────────────────────────────────────────────
    active_job = read_active_researcher_job(root)
    job_is_mine = (
        active_job is not None
        and str(active_job.get("sciper", "")).strip() == str(sciper).strip()
    )

    if job_is_mine:
        render_researcher_job_running(active_job, root)
        return

    # ── Action buttons (admin / non-reporting only) ───────────────────────────
    if role != "reporting":
        _render_detail_actions(sciper, row, db, role, root, active_env, active_job)
        st.markdown("")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    t_profil, t_units, t_moissonnes, t_infoscience, t_lacunes = st.tabs([
        "Profil",
        "Unités",
        f"Moissonnées ({harvested})",
        f"Infoscience ({infos_pubs})",
        f"Lacunes ({int(gaps_missing) if gaps_missing is not None else '—'})",
    ])

    with t_profil:
        render_profil_content(row)

    with t_units:
        render_units_content(sciper, db)

    with t_moissonnes:
        render_harvested_content(sciper, db, harvested)

    with t_infoscience:
        render_infoscience_content(sciper, db)

    with t_lacunes:
        render_lacunes_tab(
            sciper=sciper,
            missing=gaps_missing,
            db=db,
            role=role,
            row=row,
            on_import=(
                (lambda sc, start_year, _row=row, _root=root, _env=active_env, _db=db:
                    launch_import_job(_root, _env, _db, _row, sc, override_start_year=start_year))
                if role != "reporting" else None
            ),
        )


def _render_detail_actions(
    sciper: str,
    row: dict,
    db: PipelineDB,
    role: str,
    root: Path,
    active_env: str,
    active_job: dict | None,
) -> None:
    """Sync / Harvest / Analyze action buttons shown above the tabs on the detail page."""
    if active_job:
        st.warning(
            "⏳ Une tâche est en cours pour un autre chercheur — "
            "actions désactivées le temps qu'elle se termine."
        )
        return

    ac1, ac2, ac3 = st.columns(3)

    with ac1:
        with st.container(border=True):
            st.markdown("**Sync People**")
            st.caption("Rafraîchit les données depuis l'API EPFL People.")
            if st.button(
                "Synchroniser",
                key=f"btn_detail_sync_{sciper}",
                icon=":material/sync:",
                width="stretch",
            ):
                launch_researcher_job(root, active_env, "refresh", sciper=sciper)

    with ac2:
        with st.container(border=True):
            st.markdown("**Moissonner**")
            st.caption("Récupère les publications OpenAlex / ORCID.")
            if st.button(
                "Moissonner",
                key=f"btn_detail_harvest_{sciper}",
                icon=":material/cloud_download:",
                width="stretch",
            ):
                launch_researcher_job(root, active_env, "harvest", sciper=sciper)

    with ac3:
        with st.container(border=True):
            st.markdown("**Analyser les lacunes**")
            st.caption("Compare les moissonnées avec le profil Infoscience.")
            if st.button(
                "Analyser",
                key=f"btn_detail_analyze_{sciper}",
                icon=":material/analytics:",
                width="stretch",
            ):
                launch_researcher_job(root, active_env, "analyze", sciper=sciper)


# ── Tab: Registre ─────────────────────────────────────────────────────────────

def _render_registry(
    db: PipelineDB,
    role: str,
    root: Path = Path("."),
    active_env: str = "dev",
) -> None:
    df = db.get_researcher_registry_df(active_only=False)

    if df.empty:
        st.info("Registre vide — lancez une synchronisation depuis l'onglet **Synchronisation**.")
        return

    # ── KPIs ──────────────────────────────────────────────────────────────────
    active = df[df["is_active"] == True]  # noqa: E712
    n_active = len(active)

    n_orcid    = _count_filled(active["orcid"])
    n_openalex = _count_filled(active.get("openalex_id", pd.Series(dtype=str)))
    n_scopus   = _count_filled(active.get("scopus_author_id", pd.Series(dtype=str)))
    n_dspace   = _count_filled(active.get("dspace_uuid", pd.Series(dtype=str)))
    n_analyzed = int(active["last_gap_analysis_at"].notna().sum())
    n_missing  = int(active["gaps_missing"].fillna(0).sum())

    _kpi_row([
        ("Chercheurs actifs", n_active, f"{len(df)} au total"),
        ("Avec ORCID", n_orcid, f"{n_orcid * 100 // max(n_active, 1)} %"),
        ("Avec OpenAlex", n_openalex, f"{n_openalex * 100 // max(n_active, 1)} %"),
        ("Avec Scopus ID", n_scopus, f"{n_scopus * 100 // max(n_active, 1)} %"),
        ("Profil Infoscience", n_dspace, f"{n_dspace * 100 // max(n_active, 1)} %"),
        ("Total lacunes", n_missing, f"{n_analyzed} analysés"),
    ])

    st.markdown("---")

    # ── Filter panel ──────────────────────────────────────────────────────────
    with st.container():
        st.markdown('<span class="rmfilter-anchor"></span>', unsafe_allow_html=True)

        fr1, fr2, fr3, fr4 = st.columns([3, 2, 2, 1])
        with fr1:
            search = st.text_input(
                "Recherche",
                key="rm_search",
                placeholder="Nom, SCIPER, unité, position…",
            )
        with fr2:
            unit_opts = sorted(
                df["main_unit"].dropna().astype(str).str.strip().unique().tolist()
            )
            unit_filter = st.multiselect("Unité", unit_opts, key="rm_unit_filter")
        with fr3:
            schools_raw = df.get("unit_level_2", pd.Series(dtype=str))
            school_opts = sorted(
                schools_raw.dropna().astype(str).str.strip()
                .replace("", float("nan")).dropna().unique().tolist()
            )
            school_filter = st.multiselect("Faculté / École", school_opts, key="rm_school_filter")
        with fr4:
            show_inactive = st.checkbox(
                "Inactifs", value=False, key="rm_show_inactive"
            )

        fr5, fr6, fr7, fr8, fr9 = st.columns([2, 2, 2, 2, 2])
        with fr5:
            orcid_mode = st.selectbox(
                "ORCID",
                ["Tous", "Avec ORCID", "ORCID lié EPFL", "ORCID non lié", "Sans ORCID"],
                key="rm_orcid_mode",
                help=(
                    "'Lié EPFL' = ORCID officiellement rattaché au compte EPFL People. "
                    "'Non lié' = ORCID connu mais pas encore rattaché au compte EPFL."
                ),
            )
        with fr6:
            openalex_mode = st.selectbox(
                "OpenAlex",
                ["Tous", "Avec OpenAlex", "OpenAlex depuis IS", "OpenAlex non dans IS", "Sans OpenAlex"],
                key="rm_openalex_mode",
                help=(
                    "'Depuis IS' = OpenAlex ID stocké dans le profil Infoscience. "
                    "'Non dans IS' = ID inféré (ORCID/DOI) mais absent du profil Infoscience."
                ),
            )
        with fr7:
            dspace_mode = st.selectbox(
                "Profil Infoscience",
                ["Tous", "Avec profil Infoscience", "Sans profil Infoscience"],
                key="rm_dspace_mode",
            )
        with fr8:
            gap_mode = st.selectbox(
                "Lacunes",
                ["Tous", "Avec lacunes", "Sans lacune", "Non analysés"],
                key="rm_gap_mode",
            )
        with fr9:
            sort_by = st.selectbox(
                "Tri",
                [
                    "Nom A→Z",
                    "Enrollment ↓",
                    "Enrollment ↑",
                    "Publications ↓",
                    "Publications IS ↓",
                    "Lacunes ↓",
                    "% Lacunes ↓",
                    "Dernière analyse ↓",
                    "Dernière moisson ↓",
                ],
                key="rm_sort_by",
            )

        fr10, fr11, _, fr_rst = st.columns([2, 2, 4, 2])
        with fr10:
            position_opts = sorted(
                df["epfl_position"].dropna().astype(str).str.strip()
                .replace("", float("nan")).dropna().unique().tolist()
            )
            position_filter = st.multiselect("Position", position_opts, key="rm_position_filter")
        with fr11:
            class_opts = sorted(
                df.get("epfl_class", pd.Series(dtype=str)).dropna().astype(str).str.strip()
                .replace("", float("nan")).dropna().unique().tolist()
            )
            class_filter = st.multiselect("Classe", class_opts, key="rm_class_filter")
        with fr_rst:
            st.markdown('<div style="padding-top:26px">', unsafe_allow_html=True)
            if st.button(
                "Réinitialiser les filtres", key="btn_reset_rm_filters",
                width="stretch", icon=":material/filter_alt_off:",
            ):
                for _k in [
                    "rm_search", "rm_unit_filter", "rm_school_filter",
                    "rm_show_inactive", "rm_orcid_mode", "rm_openalex_mode",
                    "rm_dspace_mode", "rm_gap_mode", "rm_sort_by",
                    "rm_position_filter", "rm_class_filter",
                ]:
                    st.session_state.pop(_k, None)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    # ── Apply filters ─────────────────────────────────────────────────────────
    view = _apply_registry_filters(df, {
        "show_inactive":   show_inactive,
        "unit":            unit_filter,
        "school":          school_filter,
        "orcid_mode":      orcid_mode,
        "openalex_mode":   openalex_mode,
        "dspace_mode":     dspace_mode,
        "gap_mode":        gap_mode,
        "position_filter": position_filter,
        "class_filter":    class_filter,
        "search":          search,
    })

    # ── Sort ──────────────────────────────────────────────────────────────────
    if sort_by == "Enrollment ↓":
        view = view.sort_values("enrollment_date", ascending=False, na_position="last")
    elif sort_by == "Enrollment ↑":
        view = view.sort_values("enrollment_date", ascending=True, na_position="last")
    elif sort_by == "Publications ↓":
        view = view.sort_values("harvested_pubs", ascending=False)
    elif sort_by == "Publications IS ↓":
        view = view.sort_values("infoscience_pubs", ascending=False)
    elif sort_by == "Lacunes ↓":
        view = view.sort_values("gaps_missing", ascending=False)
    elif sort_by == "% Lacunes ↓":
        total_pubs = view["harvested_pubs"].replace(0, pd.NA)
        view = view.assign(
            _gap_pct=view["gaps_missing"] / total_pubs
        ).sort_values("_gap_pct", ascending=False, na_position="last").drop(columns=["_gap_pct"])
    elif sort_by == "Dernière analyse ↓":
        view = view.sort_values("last_gap_analysis_at", ascending=False, na_position="last")
    elif sort_by == "Dernière moisson ↓":
        view = view.sort_values("last_harvest_at", ascending=False, na_position="last")

    if view.empty:
        st.info("Aucun chercheur ne correspond aux filtres sélectionnés.")
        return

    # ── Pagination ────────────────────────────────────────────────────────────
    total_pages = max(1, math.ceil(len(view) / _CARD_PAGE_SIZE))

    filter_sig = (
        f"{show_inactive}|{','.join(sorted(unit_filter))}|{','.join(sorted(school_filter))}"
        f"|{orcid_mode}|{openalex_mode}|{dspace_mode}|{gap_mode}"
        f"|{','.join(sorted(position_filter))}|{','.join(sorted(class_filter))}"
        f"|{search}|{sort_by}"
    )
    if "rm_page" not in st.session_state:
        st.session_state["rm_page"] = 1
    if st.session_state.get("_rm_filter_sig") != filter_sig:
        st.session_state["_rm_filter_sig"] = filter_sig
        st.session_state["rm_page"] = 1

    pc1, _, pc3, pc4 = st.columns([2, 2, 4, 2])
    with pc1:
        page_num = st.number_input(
            f"Page (/{total_pages})", min_value=1, max_value=total_pages,
            key="rm_page", step=1,
        )
    with pc3:
        st.markdown(
            f'<div style="padding-top:28px;font-size:0.88rem;color:#64748B">'
            f'<b style="color:#0F172A;font-variant-numeric:tabular-nums">{len(view)}</b> '
            f'chercheur(s) &nbsp;·&nbsp; page <b>{page_num}</b> / {total_pages}'
            f'</div>',
            unsafe_allow_html=True,
        )
    with pc4:
        st.markdown('<div style="padding-top:20px">', unsafe_allow_html=True)
        export_df = view[[c for c in _REGISTRY_EXPORT_COLS if c in view.columns]]
        st.download_button(
            "Exporter CSV",
            export_df.to_csv(index=False).encode("utf-8"),
            file_name=f"registre_{datetime.now():%Y%m%d}.csv",
            mime="text/csv",
            icon=":material/download:",
            width="stretch",
        )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Card grid (2 columns) ─────────────────────────────────────────────────
    start   = (page_num - 1) * _CARD_PAGE_SIZE
    page_df = view.iloc[start: start + _CARD_PAGE_SIZE]

    for i in range(0, len(page_df), 2):
        col_left, col_right = st.columns(2)
        for j, col in enumerate((col_left, col_right)):
            idx = i + j
            if idx < len(page_df):
                row = page_df.iloc[idx].to_dict()
                with col:
                    render_researcher_card(row, role=role)


# ── Tab: Publications ─────────────────────────────────────────────────────────

def _render_publications(db: PipelineDB) -> None:
    reg_df = db.get_researcher_registry_df(active_only=False)
    if reg_df.empty:
        st.info("Registre vide — lancez une synchronisation depuis l'onglet **Synchronisation**.")
        return

    sf1, sf2 = st.columns([3, 1])
    with sf1:
        researcher_opts = [
            f"{row['full_name']} ({row['sciper']})" for _, row in reg_df.iterrows()
        ]
        selected = st.selectbox("Chercheur", researcher_opts, key="pub_researcher_sel")
    with sf2:
        view_mode = st.selectbox(
            "Source", ["Moissonnées", "Infoscience", "Lacunes"], key="pub_view_mode"
        )

    sciper = selected.split("(")[-1].rstrip(")") if selected else None
    if not sciper:
        return

    yf1, yf2, yf3, yf4 = st.columns([2, 2, 3, 1])
    with yf1:
        start_y = st.number_input(
            "Depuis", min_value=2000, max_value=2030, value=2018, key="pub_start_year"
        )
    with yf2:
        end_y = st.number_input(
            "Jusqu'à", min_value=2000, max_value=2030,
            value=datetime.now().year, key="pub_end_year",
        )
    with yf3:
        pub_search = st.text_input(
            "Filtrer (titre, DOI, type…)", key="pub_search",
            placeholder="nature, 10.1038, journal-article…",
        )
    with yf4:
        st.markdown('<div style="padding-top:26px">', unsafe_allow_html=True)
        if st.button(
            "", key="btn_reset_pub_filters",
            width="stretch", icon=":material/filter_alt_off:",
            help="Réinitialiser les filtres",
        ):
            for _k in ["pub_view_mode", "pub_start_year", "pub_end_year", "pub_search"]:
                st.session_state.pop(_k, None)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    if view_mode == "Moissonnées":
        pubs = db.get_person_publications(sciper, start_year=int(start_y), end_year=int(end_y))
        if not pubs:
            st.info(
                "Aucune publication moissonnée pour ce chercheur dans cette période. "
                "Lancez une moisson depuis l'onglet **Synchronisation**."
            )
            return
        disp = pd.DataFrame(pubs)[[
            "pub_year", "title", "doi", "dc_type", "journal_title", "sources_found"
        ]].copy()
        disp.columns = ["Année", "Titre", "DOI", "Type", "Journal / Éditeur", "Sources"]
        disp["DOI"] = disp["DOI"].fillna("—")
        if pub_search:
            s = pub_search.lower()
            mask = disp.apply(lambda r: any(s in str(v).lower() for v in r), axis=1)
            disp = disp[mask]
        st.dataframe(
            disp, width="stretch", hide_index=True,
            column_config={
                "Année":  st.column_config.TextColumn(width="small"),
                "Titre":  st.column_config.TextColumn(width="large"),
                "Type":   st.column_config.TextColumn(width="medium"),
                "Sources": st.column_config.TextColumn(width="small"),
            },
        )
        st.caption(f"{len(disp)} publication(s) — OpenAlex / ORCID")
        _download_button(disp, f"pubs_moissonnees_{sciper}")

    elif view_mode == "Infoscience":
        outputs = db.get_person_infoscience_outputs(sciper)
        if not outputs:
            st.info(
                "Aucune publication liée dans Infoscience. "
                "Lancez une analyse de lacunes pour mettre à jour."
            )
            return
        disp = pd.DataFrame(outputs)[[
            "pub_year", "title", "doi", "dc_type", "handle"
        ]].copy()
        disp.columns = ["Année", "Titre", "DOI", "Type", "Handle Infoscience"]
        disp["DOI"] = disp["DOI"].fillna("—")
        for col, bound, op in [("Année", int(start_y), "ge"), ("Année", int(end_y), "le")]:
            disp = disp[disp[col].apply(
                lambda y, b=bound, o=op: (
                    str(y).isdigit()
                    and (int(str(y)) >= b if o == "ge" else int(str(y)) <= b)
                )
            )]
        if pub_search:
            s = pub_search.lower()
            disp = disp[disp.apply(lambda r: any(s in str(v).lower() for v in r), axis=1)]
        st.dataframe(disp, width="stretch", hide_index=True)
        st.caption(f"{len(disp)} publication(s) liée(s) dans Infoscience via profil CRIS")
        _download_button(disp, f"pubs_infoscience_{sciper}")

    else:
        gap_df = db.get_person_gaps_df(
            sciper=sciper, start_year=int(start_y), end_year=int(end_y)
        )
        if gap_df.empty:
            st.info("Aucun résultat d'analyse de lacunes pour ce chercheur.")
            return
        disp = gap_df[["pub_year", "title", "doi", "dc_type", "gap_status"]].copy()
        disp.columns = ["Année", "Titre", "DOI", "Type", "Statut"]
        disp["Statut"] = disp["Statut"].map({
            "missing_in_infoscience": "🔴 Manquante",
            "in_infoscience":         "🟢 Dans Infoscience",
            "superseded_preprint":    "🟡 Préprint — publié dans IS",
        }).fillna(disp["Statut"])
        disp["DOI"] = disp["DOI"].fillna("—")
        if pub_search:
            s = pub_search.lower()
            disp = disp[disp.apply(lambda r: any(s in str(v).lower() for v in r), axis=1)]
        st.dataframe(disp, width="stretch", hide_index=True)
        st.caption(f"{len(disp)} publication(s) dans l'analyse de lacunes")
        _download_button(disp, f"lacunes_{sciper}")


def _download_button(df: pd.DataFrame, stem: str) -> None:
    st.download_button(
        "Télécharger CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{stem}_{datetime.now():%Y%m%d}.csv",
        mime="text/csv",
        icon=":material/download:",
    )


# ── Tab: Lacunes ──────────────────────────────────────────────────────────────

def _render_gaps(db: PipelineDB, active_env: str, root: Path, role: str) -> None:
    stats = db.get_researcher_gap_stats()
    if not stats or stats.get("total_pubs", 0) == 0:
        st.info(
            "Aucune analyse de lacunes disponible. "
            "Lancez une analyse depuis l'onglet **Synchronisation**."
        )
        return

    total      = stats.get("total_pubs", 0)
    missing    = stats.get("total_missing", 0)
    in_is      = stats.get("total_in_infoscience", 0)
    superseded = stats.get("total_superseded_preprint", 0)

    _kpi_row([
        ("Chercheurs analysés",  stats.get("researchers_analyzed", 0), ""),
        ("Publications comparées", total, ""),
        ("Dans Infoscience",     in_is,      f"{in_is * 100 // max(total, 1)} %"),
        ("Préprints (publiés dans IS)",  superseded, f"{superseded * 100 // max(total, 1)} %"),
        ("Lacunes",              missing,    f"{missing * 100 // max(total, 1)} % manquantes"),
    ])

    st.markdown("---")

    _STATUS_OPTIONS = ["missing_in_infoscience", "in_infoscience", "superseded_preprint"]
    _STATUS_LABEL = {
        "missing_in_infoscience": "🔴 Manquantes",
        "in_infoscience":         "🟢 Dans Infoscience",
        "superseded_preprint":    "🟡 Préprint — publié dans IS",
    }

    gf1, gf2, gf3, gf4 = st.columns([2, 2, 2, 3])
    with gf1:
        status_filter = st.multiselect(
            "Statut",
            _STATUS_OPTIONS,
            format_func=lambda x: _STATUS_LABEL.get(x, x),
            key="gap_status_filter",
            help=(
                "**Manquantes** — publication absente d'Infoscience (vraie lacune).  \n"
                "**Dans Infoscience** — publication déjà présente dans IS.  \n"
                "**Préprint — publié dans IS** — préprint récolté pour lequel IS contient "
                "déjà la version publiée : ce n'est pas une lacune."
            ),
        )
    with gf2:
        start_y = st.number_input(
            "Année depuis", min_value=2000, max_value=2030,
            value=2020, key="gap_start_year",
        )
    with gf3:
        end_y = st.number_input(
            "Année jusqu'à", min_value=2000, max_value=2030,
            value=datetime.now().year, key="gap_end_year",
        )
    with gf4:
        gap_search = st.text_input(
            "Chercheur (nom ou SCIPER)", key="gap_researcher_search",
            placeholder="Dupont, 349140…",
        )

    gf5, gf6, gf7, gf_rst = st.columns([3, 3, 2, 1])
    with gf5:
        type_filter = st.multiselect(
            "Type de document", db.get_person_gaps_dc_types(), key="gap_type_filter"
        )
    with gf6:
        unit_filter = st.multiselect("Unité", db.get_person_gaps_units(), key="gap_unit_filter")
    with gf7:
        school_filter = st.multiselect(
            "Faculté / École", db.get_person_gaps_schools(), key="gap_school_filter"
        )
    with gf_rst:
        st.markdown('<div style="padding-top:26px">', unsafe_allow_html=True)
        if st.button(
            "", key="btn_reset_gap_filters",
            width="stretch", icon=":material/filter_alt_off:",
            help="Réinitialiser les filtres",
        ):
            for _k in [
                "gap_status_filter", "gap_start_year", "gap_end_year",
                "gap_researcher_search", "gap_type_filter",
                "gap_unit_filter", "gap_school_filter",
            ]:
                st.session_state.pop(_k, None)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    sciper_filter = None
    if gap_search:
        reg_df = db.get_researcher_registry_df(active_only=False)
        s = gap_search.lower()
        matches = reg_df[
            reg_df["full_name"].str.lower().str.contains(s, na=False)
            | reg_df["sciper"].astype(str).str.lower().str.contains(s, na=False)
        ]
        if len(matches) == 1:
            sciper_filter = matches.iloc[0]["sciper"]
            st.caption(
                f"Chercheur sélectionné : **{matches.iloc[0]['full_name']}** ({sciper_filter})"
            )
        elif len(matches) > 1:
            st.caption(f"{len(matches)} chercheurs correspondent — affinez la recherche")

    df = db.get_person_gaps_df(
        sciper=sciper_filter,
        gap_status=status_filter or None,
        start_year=int(start_y),
        end_year=int(end_y),
        dc_type=type_filter or None,
        main_unit=unit_filter or None,
        unit_level_2=school_filter or None,
    )

    if df.empty:
        st.info("Aucune lacune trouvée avec ces filtres.")
    else:
        display = df[[
            "pub_year", "full_name", "main_unit", "title", "doi", "dc_type", "gap_status",
        ]].copy()
        display.columns = ["Année", "Chercheur", "Unité", "Titre", "DOI", "Type", "Statut"]
        display["Statut"] = display["Statut"].map({
            "missing_in_infoscience": "🔴 Manquante",
            "in_infoscience":         "🟢 Dans Infoscience",
            "superseded_preprint":    "🟡 Préprint — publié dans IS",
        }).fillna(display["Statut"])
        display["Titre"] = display["Titre"].str[:80]
        st.dataframe(display, width="stretch", hide_index=True)
        st.caption(f"{len(df)} publications affichées")
        _download_button(display, f"lacunes_{datetime.now():%Y%m%d}")


# ── Tab: Synchronisation ──────────────────────────────────────────────────────

def _render_sync(db: PipelineDB, active_env: str, root: Path, role: str) -> None:
    sched_file = root / "data" / "schedules.json"
    job = _load_system_jobs(sched_file).get(_RESEARCHER_SYNC_JOB_KEY, {})

    enabled   = job.get("enabled", True)
    last_at   = (job.get("last_run_at") or "—")[:16].replace("T", " ")
    last_icon = _STATUS_ICON.get(job.get("last_run_status"))

    with st.container(border=True):
        ca, cb, cc, cd = st.columns([4, 3, 3, 2])
        with ca:
            st.markdown(
                "**Synchronisation du registre chercheurs**  "
                "<span style='background:#F1F5F9;color:#475569;border-radius:4px;"
                "padding:1px 7px;font-size:.78rem;font-weight:700'>SYSTÈME</span>",
                unsafe_allow_html=True,
            )
            st.caption(
                "Synchronise le registre depuis l'API EPFL People (ORCID, unité, statut) "
                "et offboarde les chercheurs qui ne sont plus actifs.  |  "
                "Planifié : quotidien à **03:00**"
            )
        with cb:
            st.markdown(f"Dernier run : **{last_at}** {last_icon or ''}")
        with cc:
            active_count = len(db.get_researcher_registry_df(active_only=True))
            st.markdown(f"Chercheurs actifs : **{active_count}**")
        with cd:
            if role != "reporting":
                new_enabled = st.toggle(
                    "Actif",
                    value=enabled,
                    key="researcher_sync_enabled",
                    help="Active/désactive la synchronisation automatique quotidienne",
                )
                if new_enabled != enabled:
                    _save_system_job(
                        sched_file, _RESEARCHER_SYNC_JOB_KEY, {"enabled": new_enabled}
                    )
                    st.rerun()

    st.markdown("")

    if role == "reporting":
        return

    st.markdown(sh("sync", "Lancer manuellement"), unsafe_allow_html=True)

    active_job = read_active_researcher_job(root)
    if active_job:
        render_researcher_job_running(active_job, root)
        return

    render_researcher_job_form(active_env, root, db)

    st.markdown("")
    st.markdown(sh("info", "Architecture"), unsafe_allow_html=True)
    st.caption(
        "Les tâches lancées ici s'exécutent en subprocess isolé (le process survit à un "
        "redémarrage de l'UI). La sync automatique nocturne tourne en-process dans le "
        "scheduler daemon — ses logs sont dans `logs/scheduler.log`."
    )
