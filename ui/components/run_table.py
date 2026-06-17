"""Runs table component — filterable, paginated, with review tracking.

Layout: 11 columns per row
  Run | Terminé | Durée | Sources | Pipeline | DR | Importés | Suivi | Actions | Voir | ⋯

Suivi   — read-only HTML badge (status display)
Actions — icon-only buttons (person_add / task_alt+lock_open / restart_alt / sync)
Voir    — navigate to publications filtered by this run
⋯       — run detail (all users) + duplicate/re-trigger (admin only)

CSS (:has selector) scopes all padding reduction and button overrides to rows
that contain a .rtbl-row element, leaving the rest of the app unaffected.
"""

from __future__ import annotations

import datetime
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

from data_pipeline.infoscience_status_sync import run_sync as _infoscience_sync
from db.pipeline_db import PipelineDB
from ui.auth import current_user
from ui.constants import RUN_STATUSES, SOURCES
from ui.helpers import badge, fmt_dt, fmt_dur, _make_run_id
from ui.run_state import try_acquire_run_lock

_DS_BASE_URL = os.environ.get("DS_API_ENDPOINT", "").split("/server")[0]

_REVIEW_STATUS_OPTIONS = ["unclaimed", "in_progress", "done"]
_REVIEW_STATUS_LABELS  = {
    "unclaimed":   "Non traité",
    "in_progress": "En cours",
    "done":        "Terminé",
}
_PAGE_SIZE_OPTIONS = [10, 20, 50]

_COLS    = [2.4, 1.4, 0.9, 1.9, 1.1, 0.45, 1.1, 1.7, 0.9, 0.7, 0.9]
_HEADERS = ["Run", "Terminé", "Durée", "Sources", "Pipeline",
            "DR", "Importés", "Suivi", "Actions", "Voir", "Dupliquer"]

_SRC_CSS_KEYS = frozenset({"scopus", "wos", "crossref", "openalex", "zenodo", "epo", "datacite"})


def _mi(icon: str, cls: str = "") -> str:
    c = f"ms {cls}".strip()
    return f'<span class="{c}">{icon}</span>'


def _src_chips_html(sources_str: str) -> str:
    if not sources_str or sources_str == "—":
        return "—"
    return "".join(
        f'<span class="ptbl-badge ptbl-src-'
        f'{s.strip().lower() if s.strip().lower() in _SRC_CSS_KEYS else "default"}">'
        f'{s.strip()}</span>'
        for s in sources_str.split(",") if s.strip()
    )


def _date_str(val) -> str:
    if val is None:
        return "?"
    # pandas Timestamp / datetime objects: strftime avoids the " 00:00:00" suffix
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val)
    # Trim any time component already present in stored strings
    if len(s) > 10 and s[10] in (" ", "T"):
        s = s[:10]
    return "?" if s in ("None", "NaT", "", "nan") else s


# ── Combined run detail + re-trigger dialog ───────────────────────────────────

@st.dialog("Run", width="large")
def _run_dialog(row: dict, role: str, root: Path, active_env: str) -> None:
    from env_loader import ENVIRONMENTS
    from ui.run_state import get_state_file

    import html as _html
    rid         = row.get("run_id", "—")
    sources     = row.get("sources") or "—"
    sources_str = "" if sources == "—" else sources
    ws_s        = _date_str(row.get("window_start"))
    we_s        = _date_str(row.get("window_end"))
    dry         = bool(row.get("dry_run"))
    status      = str(row.get("status") or "—")
    imported    = int(row.get("imported_count") or 0)
    published   = int(row.get("published_count") or 0)
    synced      = int(row.get("synced_count") or 0)
    no_abst     = int(row.get("quality_no_abstract_count") or 0)
    no_pdf      = int(row.get("quality_no_pdf_count") or 0)
    rev_st      = row.get("review_status") or None
    claimed     = row.get("claimed_by") or None
    if isinstance(rev_st, float):
        rev_st = None
    if isinstance(claimed, float):
        claimed = None

    # Parse stored overrides / author IDs
    _qo_raw = row.get("query_overrides")
    try:
        query_overrides = json.loads(_qo_raw) if _qo_raw and not isinstance(_qo_raw, float) else {}
    except (ValueError, TypeError):
        query_overrides = {}

    def _split_ids(key):
        v = row.get(key)
        if not v or (isinstance(v, float)):
            return []
        return [x.strip() for x in str(v).split(",") if x.strip()]

    sc_ids = _split_ids("scopus_ids")
    wo_ids = _split_ids("wos_ids")
    or_ids = _split_ids("orcid_ids")
    oa_ids = _split_ids("openalex_ids")

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f'<div class="run-detail-header">'
        f'<span class="run-detail-id">{rid}</span>'
        f'<span class="badge badge-{status}">{status}</span>'
        + (
            '<span style="font-size:11px;color:#92400E;background:#FEF3C7;'
            'padding:2px 8px;border-radius:999px;font-weight:600;">dry-run</span>'
            if dry else ""
        )
        + '</div>',
        unsafe_allow_html=True,
    )

    # ── Metrics ───────────────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    c1.metric("Durée", fmt_dur(row.get("duration_s")))
    c2.metric("Importés", imported)
    c3.metric("Publiés (sync)", published if synced > 0 else "—")

    st.divider()

    # ── Detail grid ───────────────────────────────────────────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(
            f'<div class="modal-grid">'
            f'<span class="modal-sec" style="grid-column:1/-1">Exécution</span>'
            f'<span class="modal-key">Démarré</span>'
            f'<span class="modal-val">{fmt_dt(row.get("started_at"))}</span>'
            f'<span class="modal-key">Terminé</span>'
            f'<span class="modal-val">{fmt_dt(row.get("ended_at"))}</span>'
            f'<span class="modal-key">Fenêtre</span>'
            f'<span class="modal-val">{ws_s} → {we_s}</span>'
            + (
                f'<span class="modal-key">Suivi</span>'
                f'<span class="modal-val">{rev_st}'
                + (f' — <em>{claimed}</em>' if claimed else "")
                + '</span>'
                if rev_st else ""
            )
            + '</div>',
            unsafe_allow_html=True,
        )
    with col_r:
        st.markdown(
            f'<div class="modal-grid">'
            f'<span class="modal-sec" style="grid-column:1/-1">Sources</span>'
            f'<span class="modal-key" style="align-self:start;padding-top:6px">Sources</span>'
            f'<span class="modal-val">'
            f'<div class="modal-chips" style="flex-wrap:wrap;gap:3px;">'
            f'{_src_chips_html(sources)}'
            f'</div></span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    if no_abst or no_pdf:
        st.markdown(
            '<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;">'
            + (
                f'<span class="pub-quality-warn">'
                f'⚑ {no_abst} résumé{"s" if no_abst > 1 else ""} manquant{"s" if no_abst > 1 else ""}</span>'
                if no_abst else ""
            )
            + (
                f'<span class="pub-quality-warn">'
                f'⚑ {no_pdf} PDF OA manquant{"s" if no_pdf > 1 else ""}</span>'
                if no_pdf else ""
            )
            + '</div>',
            unsafe_allow_html=True,
        )

    # ── Query overrides + author IDs ──────────────────────────────────────────
    _author_id_rows = [
        ("--scopus-ids",   sc_ids),
        ("--wos-ids",      wo_ids),
        ("--orcid-ids",    or_ids),
        ("--openalex-ids", oa_ids),
    ]
    _has_overrides = bool(query_overrides)
    _has_ids       = any(lst for _, lst in _author_id_rows)

    if _has_overrides or _has_ids:
        st.divider()
        ov_col, id_col = st.columns(2)
        with ov_col:
            if _has_overrides:
                rows_html = "".join(
                    f'<span class="modal-key">--query-{_html.escape(src)}</span>'
                    f'<span class="modal-val modal-long">'
                    f'<details><summary>{_html.escape(q[:60])}{"…" if len(q) > 60 else ""}</summary>'
                    f'<span class="modal-val-long">{_html.escape(q)}</span></details></span>'
                    for src, q in sorted(query_overrides.items())
                )
                st.markdown(
                    f'<div class="modal-grid">'
                    f'<span class="modal-sec" style="grid-column:1/-1">Requêtes personnalisées</span>'
                    f'{rows_html}</div>',
                    unsafe_allow_html=True,
                )
        with id_col:
            if _has_ids:
                id_rows_html = "".join(
                    f'<span class="modal-key">{flag}</span>'
                    f'<span class="modal-val">'
                    + "".join(
                        f'<span class="modal-chip modal-chip-sciper">{_html.escape(i)}</span> '
                        for i in ids
                    )
                    + '</span>'
                    for flag, ids in _author_id_rows if ids
                )
                st.markdown(
                    f'<div class="modal-grid">'
                    f'<span class="modal-sec" style="grid-column:1/-1">Identifiants auteurs</span>'
                    f'{id_rows_html}</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── CLI command ───────────────────────────────────────────────────────────
    parts = ["python3 data_pipeline/main.py"]
    if ws_s != "?" and we_s != "?":
        parts += [f"--start-date {ws_s}", f"--end-date {we_s}"]
    if sources and sources != "—":
        parts.append(f"--sources {sources}")
    for src, q in sorted((query_overrides or {}).items()):
        parts.append(f'--query-{src} "{q}"')
    if sc_ids:
        parts.append(f'--scopus-ids {",".join(sc_ids)}')
    if wo_ids:
        parts.append(f'--wos-ids {",".join(wo_ids)}')
    if or_ids:
        parts.append(f'--orcid-ids {",".join(or_ids)}')
    if oa_ids:
        parts.append(f'--openalex-ids {",".join(oa_ids)}')
    if dry:
        parts.append("--dry-run")
    parts.append(f"--run-id {rid}")
    st.code(" \\\n  ".join(parts), language="bash")

    # ── Re-trigger section (admin only) ───────────────────────────────────────
    if role != "admin":
        return

    st.divider()
    st.markdown(
        '<div class="modal-sec" style="margin-bottom:10px;">Re-déclencher</div>',
        unsafe_allow_html=True,
    )

    env_choice = st.radio(
        "Environnement cible",
        list(ENVIRONMENTS),
        index=list(ENVIRONMENTS).index(active_env) if active_env in ENVIRONMENTS else 0,
        horizontal=True,
        format_func=lambda e: {"dev": "🟢 Dev", "test": "🟡 Test", "prod": "🔴 Prod"}.get(e, e),
        key=f"dup_env_{rid}",
    )
    col_o1, col_o2 = st.columns(2)
    with col_o1:
        dry_run = st.checkbox(
            "Dry-run (sans import DSpace)", value=dry, key=f"dup_dry_{rid}",
        )
    with col_o2:
        no_email = st.checkbox(
            "Désactiver l'envoi d'e-mail", value=True, key=f"dup_mail_{rid}",
        )

    if env_choice == "prod" and not dry_run:
        st.warning(
            "⚠️ Lancement en **production** sans dry-run — "
            "les items seront importés dans Infoscience."
        )

    if st.button("▶ Re-déclencher ce run", type="primary", width="stretch",
                 key=f"dup_launch_{rid}"):
        new_run_id = _make_run_id()
        log_file   = root / "logs" / f"run_{new_run_id}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        selected_sources = (
            [s.strip() for s in sources_str.split(",") if s.strip()]
            if sources_str else SOURCES
        )

        cmd = [sys.executable, str(root / "data_pipeline" / "main.py")]
        if ws_s != "?" and we_s != "?":
            cmd += ["--start-date", ws_s, "--end-date", we_s]
        if sources_str:
            cmd += ["--sources", sources_str]
        for _src, _q in sorted((query_overrides or {}).items()):
            cmd += [f"--query-{_src}", _q]
        if sc_ids:
            cmd += ["--scopus-ids", ",".join(sc_ids)]
        if wo_ids:
            cmd += ["--wos-ids", ",".join(wo_ids)]
        if or_ids:
            cmd += ["--orcid-ids", ",".join(or_ids)]
        if oa_ids:
            cmd += ["--openalex-ids", ",".join(oa_ids)]
        cmd += ["--env", env_choice, "--run-id", new_run_id]
        if dry_run:
            cmd.append("--dry-run")
        if no_email:
            cmd.append("--no-email")

        log_fh   = open(log_file, "w", encoding="utf-8")
        acquired = try_acquire_run_lock(
            run_id=new_run_id, pid=0,
            sources=selected_sources,
            dry_run=dry_run,
            log_file=str(log_file),
            cmd=cmd,
        )
        if not acquired:
            log_fh.close()
            log_file.unlink(missing_ok=True)
            st.error("⛔ Un run est déjà en cours. Attendez sa fin avant de re-déclencher.")
            return

        proc = subprocess.Popen(
            cmd, stdout=log_fh, stderr=subprocess.STDOUT,
            cwd=str(root), env={**os.environ},
        )
        get_state_file().write_text(
            json.dumps({
                "run_id":     new_run_id,
                "pid":        proc.pid,
                "env":        env_choice,
                "started_at": datetime.datetime.now().isoformat(),
                "sources":    selected_sources,
                "dry_run":    dry_run,
                "log_file":   str(log_file),
                "cmd":        " ".join(cmd),
            }, indent=2),
            encoding="utf-8",
        )
        log_fh.close()
        st.session_state["_redirect_page"] = "Lancer un run"
        st.rerun()


# ── Main render ────────────────────────────────────────────────────────────────

def render_run_table(
    db: PipelineDB,
    root: Path | None = None,
    active_env: str | None = None,
    default_with_imports: bool = False,
) -> None:
    """Render the filterable, paginated runs table with review tracking."""
    if root is None:
        root = Path(__file__).resolve().parent.parent.parent
    if active_env is None:
        active_env = os.environ.get("APP_ENV", "dev")

    _username, _display_name, _role = current_user()

    # ── Execute any pending mutation BEFORE fetching table data ───────────────
    # Pattern: button click → store action in session_state → st.rerun() →
    # this block fires → DB write → fresh data fetch below.
    _action = st.session_state.pop("_run_pending_action", None)
    if _action:
        PipelineDB(db.db_path).set_run_review_status(
            _action["run_id"], _action["to_status"], _username, _role
        )

    _sync_run_id = st.session_state.pop("_run_pending_sync", None)
    if _sync_run_id:
        with st.spinner(f"Synchronisation Infoscience pour {_sync_run_id}…"):
            _res = _infoscience_sync(db_path=db.db_path, run_id=_sync_run_id)
        _parts = [f"{_res['updated']} mis à jour", f"{_res['checked']} vérifiés"]
        if _res["errors"]:
            _parts.append(f"{_res['errors']} erreurs")
        st.toast(", ".join(_parts) + ".", icon="✅" if not _res["errors"] else "⚠️")

    # ── Filters ───────────────────────────────────────────────────────────────
    if "rf_with_imports" not in st.session_state:
        st.session_state["rf_with_imports"] = default_with_imports

    with st.expander("Filtres", icon=":material/search:", expanded=False):
        _fc1, _fc2, _fc3, _fc4 = st.columns([2, 2, 2, 2])
        with _fc1:
            st.date_input("Démarré après", value=None, key="rf_date_from")
        with _fc2:
            st.date_input("Démarré avant", value=None, key="rf_date_to")
        with _fc3:
            st.multiselect("Statut pipeline", RUN_STATUSES, key="rf_status")
        with _fc4:
            st.multiselect(
                "Suivi", _REVIEW_STATUS_OPTIONS,
                format_func=lambda v: _REVIEW_STATUS_LABELS[v],
                key="rf_review_status")
        _fs1, _fs2, _fs3, _fs4 = st.columns([2, 1.5, 1.5, 1])
        with _fs1:
            st.text_input("Nom du run", placeholder="20250527…", key="rf_search")
        with _fs2:
            _claimer_opts = ["__me__"] + db.get_distinct_claimers()
            st.multiselect(
                "Traité par",
                _claimer_opts,
                format_func=lambda v: "Mes runs" if v == "__me__" else v,
                key="rf_claimed_by")
        with _fs3:
            st.multiselect("Sources", SOURCES, key="rf_sources")
        with _fs4:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Réinitialiser", key="rf_reset", width="stretch"):
                for _k in ("rf_date_from", "rf_date_to", "rf_status",
                           "rf_review_status", "rf_search", "rf_claimed_by",
                           "rf_sources", "rf_with_imports"):
                    st.session_state.pop(_k, None)
                st.session_state["run_page"] = 1
                st.rerun()
        st.toggle(
            "Avec imports uniquement",
            key="rf_with_imports",
            help="Afficher uniquement les runs contenant au moins un item importé (workspace ou workflow)",
        )

    _date_from     = st.session_state.get("rf_date_from") or None
    _date_to       = st.session_state.get("rf_date_to") or None
    _status        = st.session_state.get("rf_status") or None
    _review_status = st.session_state.get("rf_review_status") or None
    _search        = st.session_state.get("rf_search") or None
    _claimed_by_raw = st.session_state.get("rf_claimed_by") or None
    _sources       = st.session_state.get("rf_sources") or None
    _with_imports  = st.session_state.get("rf_with_imports", default_with_imports)
    _claimed_by = (
        [_username if v == "__me__" else v for v in _claimed_by_raw]
        if _claimed_by_raw else None
    )

    _filter_sig = (
        _date_from, _date_to,
        tuple(_status or []), tuple(_review_status or []),
        _search or "", tuple(_claimed_by_raw or []),
        tuple(_sources or []),
        _with_imports,
    )
    if st.session_state.get("_run_filter_sig") != _filter_sig:
        st.session_state["_run_filter_sig"] = _filter_sig
        st.session_state["run_page"] = 1

    # ── Pagination ────────────────────────────────────────────────────────────
    _total = db.count_runs(
        status=_status, date_from=_date_from, date_to=_date_to,
        search=_search, review_status=_review_status, claimed_by=_claimed_by,
        sources=_sources, with_imports=_with_imports)

    _pc1, _pc2, _pc3 = st.columns([2, 2, 5])
    with _pc1:
        _page_size = st.selectbox(
            "Lignes / page", _PAGE_SIZE_OPTIONS, index=0, key="run_page_size")
    _total_pages = max(1, math.ceil(_total / _page_size))
    with _pc2:
        _page_num = st.number_input(
            f"Page (/{_total_pages})", min_value=1, max_value=_total_pages,
            key="run_page", step=1)
    with _pc3:
        st.caption(f"{_total} run(s) au total")

    _offset  = (_page_num - 1) * _page_size
    _runs_df = db.get_runs(
        status=_status, date_from=_date_from, date_to=_date_to,
        search=_search, review_status=_review_status, claimed_by=_claimed_by,
        sources=_sources, with_imports=_with_imports, limit=_page_size, offset=_offset)

    if _runs_df.empty:
        st.info("Aucun run correspondant aux filtres.")
        return

    # ── Header ────────────────────────────────────────────────────────────────
    _h = st.columns(_COLS)
    for _lbl, _col in zip(_HEADERS, _h):
        _col.markdown(f'<div class="rtbl-hdr">{_lbl}</div>', unsafe_allow_html=True)

    # ── Rows ──────────────────────────────────────────────────────────────────
    for _, _row in _runs_df.iterrows():
        st.markdown('<hr class="rtbl-sep">', unsafe_allow_html=True)
        _c   = st.columns(_COLS)
        _rid = _row["run_id"]

        # Detect researcher_import runs by run_type column or run_id prefix
        _run_type = _row.get("run_type") or ""
        _is_researcher_import = (
            _run_type == "researcher_import"
            or str(_rid).startswith("researcher_import_")
        )
        _forced_sciper_val = _row.get("forced_sciper") or ""
        _researcher_badge = ""
        if _is_researcher_import:
            _researcher_label = "Import ciblé"
            if _forced_sciper_val and ":" in str(_forced_sciper_val):
                _researcher_name = str(_forced_sciper_val).split(":", 1)[1].strip()
                _researcher_label = f"Import — {_researcher_name[:25]}"
            _researcher_badge = (
                f'<span class="rtbl-researcher-badge">'
                f'<span style="font-family:\'Material Symbols Outlined\';font-size:10px;'
                f'vertical-align:middle;margin-right:3px;">person_search</span>'
                f'{_researcher_label}</span>'
            )

        # col 0: run ID + invisible CSS scope marker + optional researcher badge
        _c[0].markdown(
            f'<span class="rtbl-row"></span>'
            f'<span class="rtbl-run-id">{_rid}</span>'
            + (f'<br>{_researcher_badge}' if _researcher_badge else ""),
            unsafe_allow_html=True,
        )
        _c[1].markdown(f'<div class="rtbl-cell">{fmt_dt(_row["ended_at"])}</div>',
                       unsafe_allow_html=True)
        _c[2].markdown(f'<div class="rtbl-cell">{fmt_dur(_row["duration_s"])}</div>',
                       unsafe_allow_html=True)
        _c[3].markdown(f'<div class="rtbl-cell">{_row["sources"] or "—"}</div>',
                       unsafe_allow_html=True)
        _c[4].markdown(badge(_row["status"]), unsafe_allow_html=True)
        _c[5].markdown(
            f'<div class="rtbl-cell">'
            f'{_mi("check", "ms-neutral") if _row["dry_run"] else ""}</div>',
            unsafe_allow_html=True,
        )

        # col 6: imported count until first sync, then published/imported ratio
        # + quality warning chips when quality issues are detected
        _imported  = int(_row.get("imported_count")  or 0)
        _published = int(_row.get("published_count") or 0)
        _synced    = int(_row.get("synced_count")    or 0)
        _pub_handle = None if pd.isna(_row.get("published_handle") or None) else _row.get("published_handle")
        _no_abstract = int(_row.get("quality_no_abstract_count") or 0)
        _no_pdf      = int(_row.get("quality_no_pdf_count")      or 0)
        if _synced > 0 and _imported > 0:
            if _published == 1 and _pub_handle and _DS_BASE_URL:
                _href = f"{_DS_BASE_URL}/handle/{_pub_handle}"
                _col6_html = (
                    f'<a href="{_href}" target="_blank" class="rtbl-published-chip rtbl-published-link">'
                    f'{_published}/{_imported}</a>'
                )
            else:
                _col6_html = f'<span class="rtbl-published-chip">{_published}/{_imported}</span>'
        elif _imported:
            _col6_html = f'<span class="rtbl-count-chip">{_imported}</span>'
        else:
            _col6_html = "—"
        _quality_html = ""
        if _no_abstract:
            _quality_html += (
                f'<span class="pub-quality-warn" title="Notices publiées sans résumé">'
                f'⚑ {_no_abstract} résumé{"s" if _no_abstract > 1 else ""}</span>'
            )
        if _no_pdf:
            _quality_html += (
                f'<span class="pub-quality-warn" title="Notices publiées sans PDF OA">'
                f'⚑ {_no_pdf} PDF OA</span>'
            )
        _quality_block = (
            f'<div class="rtbl-quality-wrap">{_quality_html}</div>'
            if _quality_html else ""
        )
        _c[6].markdown(
            f'<div class="rtbl-cell">{_col6_html}{_quality_block}</div>',
            unsafe_allow_html=True,
        )

        # DuckDB 1.5+ returns str dtype for mixed NULL/string columns;
        # NULL rows come back as float NaN (truthy) instead of None.
        # pd.isna() normalises both None and NaN to a Python None.
        _rs_raw  = _row.get("review_status")
        _rs      = None if pd.isna(_rs_raw) else _rs_raw
        _cb_raw  = _row.get("claimed_by")
        _cb      = "" if pd.isna(_cb_raw) else (_cb_raw or "")
        _rts     = _row.get("review_updated_at")
        _can_act = _username == _cb or _role == "admin"
        _done    = _row["status"] == "completed"

        # col 7: review status badge (read-only)
        if not _done:
            _c[7].markdown(
                '<span class="rtbl-cell" style="color:#CBD5E1">—</span>',
                unsafe_allow_html=True)
        elif not _rs:
            _c[7].markdown(
                f'<div class="rtbl-status rtbl-unclaimed">'
                f'{_mi("radio_button_unchecked")}Non pris en charge</div>',
                unsafe_allow_html=True)
        elif _rs == "in_progress":
            _c[7].markdown(
                f'<div class="rtbl-status rtbl-inprogress">'
                f'{_mi("schedule")}En cours</div>'
                f'<span class="rtbl-user">{_cb}</span>'
                f'<span class="rtbl-ts">{fmt_dt(_rts)}</span>',
                unsafe_allow_html=True)
        elif _rs == "done":
            _c[7].markdown(
                f'<div class="rtbl-status rtbl-done">'
                f'{_mi("check_circle")}Traité</div>'
                f'<span class="rtbl-user">{_cb}</span>'
                f'<span class="rtbl-ts">{fmt_dt(_rts)}</span>',
                unsafe_allow_html=True)

        # col 8: action buttons
        with _c[8]:
            if not _done:
                pass
            elif not _rs:
                if st.button("", icon=":material/person_add:",
                             key=f"claim_{_rid}", width="stretch",
                             help="Prendre en charge"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": "in_progress",
                    }
                    st.rerun()
            elif _rs == "in_progress" and _can_act:
                if st.button("", icon=":material/task_alt:",
                             key=f"done_{_rid}", width="stretch",
                             help="Marquer terminé"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": "done",
                    }
                    st.rerun()
                if st.button("", icon=":material/lock_open:",
                             key=f"unclaim_{_rid}", width="stretch",
                             help="Libérer"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": None,
                    }
                    st.rerun()
            elif _rs == "done" and (_role == "admin" or (_role == "curator" and _cb == _username)):
                if st.button("", icon=":material/restart_alt:",
                             key=f"reopen_{_rid}", width="stretch",
                             help="Réouvrir"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": "in_progress",
                    }
                    st.rerun()
                if st.button("", icon=":material/sync:",
                             key=f"sync_{_rid}", width="stretch",
                             help="Synchroniser les statuts Infoscience"):
                    st.session_state["_run_pending_sync"] = _rid
                    st.rerun()

        # col 9: navigate to publications filtered by this run
        with _c[9]:
            if st.button("", icon=":material/visibility:",
                         key=f"pubs_{_rid}", width="stretch",
                         help="Voir les publications"):
                st.session_state["_jump_to_run"] = _rid
                st.query_params["page"] = "Publications"
                st.rerun()

        # col 10: detail + re-trigger (single dialog, re-trigger section admin only)
        with _c[10]:
            _icon = ":material/content_copy:" if _role == "admin" else ":material/info:"
            _help = "Détail + re-déclencher" if _role == "admin" else "Voir le détail du run"
            if st.button("", icon=_icon, key=f"detail_{_rid}",
                         width="stretch", help=_help):
                _run_dialog(_row.to_dict(), _role, root, active_env)
