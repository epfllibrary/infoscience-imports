"""Researcher Monitor page — registry, gap analysis, sync controls."""

from __future__ import annotations

import html as _html
import json
import math
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from db.pipeline_db import PipelineDB
from ui.components.researcher_table import (
    render_researcher_card,
    show_researcher_dialog,
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


# ── Researcher job lock file (live log tracking) ──────────────────────────────

def _job_lock_file(root: Path) -> Path:
    return root / "data" / "researcher_job_active.json"


def _is_pid_running(pid: int) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        waited, _ = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return False
    except (ChildProcessError, OSError):
        pass
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _read_active_researcher_job(root: Path) -> dict | None:
    lock = _job_lock_file(root)
    if not lock.exists():
        return None
    try:
        data = json.loads(lock.read_text(encoding="utf-8"))
    except Exception:
        lock.unlink(missing_ok=True)
        return None
    pid = data.get("pid")
    if pid and _is_pid_running(int(pid)):
        return data
    lock.unlink(missing_ok=True)
    return None


def _write_researcher_job_lock(
    root: Path, action: str, pid: int, log_file: Path, cmd: list[str], env: str,
    sciper: str | None = None,
) -> None:
    lock = _job_lock_file(root)
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(
        json.dumps({
            "action":     action,
            "pid":        pid,
            "log_file":   str(log_file),
            "started_at": datetime.now().isoformat(),
            "env":        env,
            "cmd":        " ".join(cmd),
            "sciper":     sciper,
        }, indent=2),
        encoding="utf-8",
    )


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
    """Count non-null, non-empty, non-whitespace values in a pandas Series."""
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
    """Render metric cards in equal columns. items = [(label, value, sub), …]"""
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

    # ── Filter panel — white elevated container ────────────────────────────────
    with st.container():
        st.markdown('<span class="rmfilter-anchor"></span>', unsafe_allow_html=True)

        # Row 1: search · unit · school · show inactive
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

        # Row 2: ORCID · OpenAlex · Infoscience · Lacunes · Tri
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

        # Row 3: position · class · reset
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
                use_container_width=True, icon=":material/filter_alt_off:",
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
    # "Nom A→Z" preserves the DB default order (last_name, first_name)

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
            use_container_width=True,
        )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Card grid (2 columns) ─────────────────────────────────────────────────
    start = (page_num - 1) * _CARD_PAGE_SIZE
    page_df = view.iloc[start: start + _CARD_PAGE_SIZE]

    for i in range(0, len(page_df), 2):
        col_left, col_right = st.columns(2)
        for j, col in enumerate((col_left, col_right)):
            idx = i + j
            if idx < len(page_df):
                row = page_df.iloc[idx].to_dict()
                with col:
                    render_researcher_card(
                        row,
                        role=role,
                        on_detail=lambda r, _db=db, _root=root, _env=active_env, _role=role: show_researcher_dialog(
                            r,
                            db=_db,
                            role=_role,
                            root=_root,
                            on_sync=lambda sc, __root=_root, __env=_env: _launch_researcher_job(
                                __root, __env, "refresh", sciper=sc
                            ),
                            on_harvest=lambda sc, __root=_root, __env=_env: _launch_researcher_job(
                                __root, __env, "harvest", sciper=sc
                            ),
                            on_analyze=lambda sc, __root=_root, __env=_env: _launch_researcher_job(
                                __root, __env, "analyze", sciper=sc
                            ),
                        ),
                    )


# ── Tab: Publications ─────────────────────────────────────────────────────────

def _render_publications(db: PipelineDB) -> None:
    reg_df = db.get_researcher_registry_df(active_only=False)
    if reg_df.empty:
        st.info("Registre vide — lancez une synchronisation depuis l'onglet **Synchronisation**.")
        return

    # ── Controls ──────────────────────────────────────────────────────────────
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
            use_container_width=True, icon=":material/filter_alt_off:",
            help="Réinitialiser les filtres",
        ):
            for _k in ["pub_view_mode", "pub_start_year", "pub_end_year", "pub_search"]:
                st.session_state.pop(_k, None)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── Moissonnées ───────────────────────────────────────────────────────────
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

    # ── Infoscience ───────────────────────────────────────────────────────────
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
        # Year filter — Infoscience outputs have no server-side year filter
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

    # ── Lacunes ───────────────────────────────────────────────────────────────
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

    # ── Filters ───────────────────────────────────────────────────────────────
    _STATUS_OPTIONS = ["missing_in_infoscience", "in_infoscience", "superseded_preprint"]
    _STATUS_LABEL = {
        "missing_in_infoscience": "🔴 Manquantes",
        "in_infoscience":         "🟢 Dans Infoscience",
        "superseded_preprint":    "🟡 Préprint — publié dans IS",
    }

    # Row 1: status · years · researcher search
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

    # Row 2: type · unit · school · reset
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
            use_container_width=True, icon=":material/filter_alt_off:",
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

    active_job = _read_active_researcher_job(root)
    if active_job:
        _render_researcher_job_running(active_job, root)
        return

    _render_researcher_job_form(active_env, root, db)

    st.markdown("")
    st.markdown(sh("info", "Architecture"), unsafe_allow_html=True)
    st.caption(
        "Les tâches lancées ici s'exécutent en subprocess isolé (le process survit à un "
        "redémarrage de l'UI). La sync automatique nocturne tourne en-process dans le "
        "scheduler daemon — ses logs sont dans `logs/scheduler.log`."
    )


def _render_researcher_job_running(active_job: dict, root: Path) -> None:
    """Live log view for an active researcher job — mirrors run_launcher._render_running."""
    action   = active_job.get("action", "?")
    started  = (active_job.get("started_at") or "")[:16].replace("T", " ")
    log_file = Path(active_job.get("log_file", ""))
    pid      = active_job.get("pid")

    info_col, stop_col = st.columns([6, 2])
    with info_col:
        st.info(f"⏳ Tâche **{action}** en cours depuis {started}…")
    with stop_col:
        if pid and st.button("⛔ Arrêter", key="btn_stop_researcher", type="secondary"):
            try:
                os.kill(int(pid), signal.SIGTERM)
                st.warning("Signal d'arrêt envoyé au processus.")
                time.sleep(1)
                _job_lock_file(root).unlink(missing_ok=True)
                st.rerun()
            except Exception as exc:
                st.error(f"Impossible d'arrêter le processus : {exc}")

    st.markdown(sh("terminal", "Logs en direct"), unsafe_allow_html=True)
    log_box  = st.empty()
    info_box = st.empty()

    while True:
        current = _read_active_researcher_job(root)
        if log_file.exists():
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
            tail  = "\n".join(lines[-300:])
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

    st.success(f"✅ Tâche **{action}** terminée.")
    st.rerun()


def _render_researcher_job_form(active_env: str, root: Path, db: "PipelineDB | None" = None) -> None:
    """Launch buttons for manual registry sync, harvest, and gap analysis."""
    sc1, sc2 = st.columns(2)

    with sc1:
        with st.container(border=True):
            st.markdown("**Sync registre (EPFL People API)**")
            st.caption(
                "Découverte complète depuis l'API EPFL People : "
                "ajoute les nouveaux membres, offboarde les partants."
            )
            no_orcid = st.checkbox(
                "Sans enrichissement ORCID", key="sync_no_orcid",
                help="Plus rapide, ne contacte pas l'API ORCID",
            )
            no_dspace = st.checkbox(
                "Sans enrichissement Infoscience", key="sync_no_dspace",
                help="Ne récupère pas le profil Infoscience (uuid, OpenAlex IS, variantes de noms)",
            )
            no_openalex = st.checkbox(
                "Sans inférence OpenAlex", key="sync_no_openalex",
                help="Ne tente pas d'inférer l'OpenAlex ID par recoupement de DOIs",
            )
            if st.button(
                "Synchroniser le registre", key="btn_sync_registry",
                icon=":material/people:", type="primary",
            ):
                _launch_researcher_job(
                    root, active_env, "sync",
                    no_orcid=no_orcid,
                    no_dspace=no_dspace,
                    no_openalex=no_openalex,
                )

    with sc2:
        with st.container(border=True):
            st.markdown("**Actualiser les données du registre**")
            st.caption(
                "Ré-enrichit les entrées déjà présentes (SCIPER connus) sans découverte "
                "ni offboarding. Utile pour mettre à jour ORCID, profils Infoscience "
                "et identifiants OpenAlex sur le périmètre existant."
            )
            ref_no_orcid = st.checkbox(
                "Sans enrichissement ORCID", key="ref_no_orcid",
                help="Plus rapide, ne contacte pas l'API ORCID",
            )
            ref_no_dspace = st.checkbox(
                "Sans enrichissement Infoscience", key="ref_no_dspace",
            )
            ref_no_openalex = st.checkbox(
                "Sans inférence OpenAlex", key="ref_no_openalex",
            )
            ref_sciper_input = st.text_input(
                "Restreindre à des SCIPERs (optionnel)",
                key="ref_sciper_input",
                placeholder="349140, 120091, …",
                help="Laisser vide pour actualiser tous les chercheurs actifs du registre",
            )
            if st.button(
                "Actualiser", key="btn_refresh_registry",
                icon=":material/refresh:", type="secondary",
            ):
                sciper_arg = ref_sciper_input.strip() or None
                _launch_researcher_job(
                    root, active_env, "refresh",
                    no_orcid=ref_no_orcid,
                    no_dspace=ref_no_dspace,
                    no_openalex=ref_no_openalex,
                    sciper=sciper_arg,
                )

    sc3, sc4 = st.columns(2)
    with sc3:
        with st.container(border=True):
            st.markdown("**Moissonner les publications**")
            st.caption(
                "Récupère les publications depuis OpenAlex et ORCID. "
                "La fenêtre est automatiquement restreinte à la période d'affiliation EPFL "
                "de chaque chercheur (enrollment → offboarding)."
            )
            col_y1, col_y2 = st.columns(2)
            with col_y1:
                start_y = st.number_input(
                    "Depuis (borne max)", min_value=2000, max_value=2030,
                    value=datetime.now().year - 3, key="harvest_start",
                )
            with col_y2:
                end_y = st.number_input(
                    "Jusqu'à (borne max)", min_value=2000, max_value=2030,
                    value=datetime.now().year, key="harvest_end",
                )
            if st.button("Moissonner", key="btn_harvest", icon=":material/cloud_download:", type="primary"):
                _launch_researcher_job(
                    root, active_env, "harvest",
                    start_year=int(start_y), end_year=int(end_y),
                )

    with st.container(border=True):
        st.markdown("**Analyse des lacunes Infoscience**")
        st.caption(
            "Compare les publications moissonnées aux items Infoscience liés à chaque chercheur "
            "via son profil CRIS (relation ``RELATION.Person.researchoutputs``)."
        )
        _ALL_OPT = "Tous les chercheurs avec publications"
        if db is not None:
            reg_df = db.get_researcher_registry_df(active_only=True)
            with_pubs = reg_df[reg_df["harvested_pubs"] > 0]
            analyze_opts = [_ALL_OPT] + [
                f"{row['full_name']} ({row['sciper']})"
                for _, row in with_pubs.iterrows()
            ]
        else:
            analyze_opts = [_ALL_OPT]
        selected_analyze = st.selectbox("Chercheur", analyze_opts, key="analyze_researcher_sel")
        if st.button("Analyser", key="btn_analyze", icon=":material/analytics:", type="primary"):
            sciper_arg = (
                None if selected_analyze == _ALL_OPT
                else selected_analyze.split("(")[-1].rstrip(")")
            )
            _launch_researcher_job(root, active_env, "analyze", sciper=sciper_arg)


def _launch_researcher_job(
    root: Path,
    active_env: str,
    action: str,
    no_orcid: bool = False,
    no_dspace: bool = False,
    no_openalex: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    sciper: str | None = None,
    use_cached: bool = False,
) -> None:
    if _read_active_researcher_job(root):
        st.error("⛔ Une tâche est déjà en cours. Attendez sa fin avant d'en lancer une nouvelle.")
        return

    python = sys.executable
    cmd = [
        python, str(root / "researcher_monitor" / "main.py"),
        "--action", action, "--env", active_env,
    ]
    if no_orcid:
        cmd.append("--no-orcid")
    if no_dspace:
        cmd.append("--no-dspace")
    if no_openalex:
        cmd.append("--no-openalex")
    if start_year:
        cmd += ["--start-year", str(start_year)]
    if end_year:
        cmd += ["--end-year", str(end_year)]
    if sciper:
        cmd += ["--sciper", sciper]
    if use_cached:
        cmd.append("--use-cached")

    log_path = root / "logs" / f"researcher_{action}_{datetime.now():%Y%m%d_%H%M%S}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=open(log_path, "w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        _write_researcher_job_lock(root, action, proc.pid, log_path, cmd, active_env, sciper=sciper)
        st.rerun()
    except Exception as exc:
        st.error(f"Erreur au lancement : {exc}")
