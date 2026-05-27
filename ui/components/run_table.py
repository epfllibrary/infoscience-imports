"""Runs table component — filterable, paginated, with review tracking.

Layout: 9 columns per row
  Run | Démarré | Terminé | Durée | Sources | Pipeline | DR | Suivi | Actions

Suivi  — read-only HTML badge (status display)
Actions — icon-only buttons (person_add / task_alt+lock_open / restart_alt)

CSS (:has selector) scopes all padding reduction and button overrides to rows
that contain a .rtbl-row element, leaving the rest of the app unaffected.
"""

from __future__ import annotations

import math

import pandas as pd
import streamlit as st

from db.pipeline_db import PipelineDB
from ui.auth import current_user
from ui.constants import RUN_STATUSES
from ui.helpers import badge, fmt_dt, fmt_dur
_REVIEW_STATUS_OPTIONS = ["unclaimed", "in_progress", "done"]
_REVIEW_STATUS_LABELS  = {
    "unclaimed":   "Non pris en charge",
    "in_progress": "En cours",
    "done":        "Terminé",
}
_PAGE_SIZE_OPTIONS = [10, 20, 50]

# col layout: Run | Terminé | Durée | Sources | Pipeline | DR | Importés | Suivi | Actions | Voir
_COLS = [3.0, 1.4, 0.9, 2.0, 1.2, 0.45, 0.8, 1.8, 0.9, 0.7]
_HEADERS = ["Run", "Terminé", "Durée", "Sources", "Pipeline",
            "DR", "Importés", "Suivi", "Actions", "Voir"]


def _mi(icon: str, cls: str = "") -> str:
    c = f"ms {cls}".strip()
    return f'<span class="{c}">{icon}</span>'


def render_run_table(db: PipelineDB) -> None:
    """Render the filterable, paginated runs table with review tracking."""
    _username, _display_name, _role = current_user()

    # ── Execute any pending mutation BEFORE fetching table data ───────────────
    # Pattern: button click → store action in session_state → st.rerun() →
    # this block fires → DB write → fresh data fetch below.
    _action = st.session_state.pop("_run_pending_action", None)
    if _action:
        PipelineDB(db.db_path).set_run_review_status(
            _action["run_id"], _action["to_status"], _username, _role
        )

    # ── Filters ───────────────────────────────────────────────────────────────
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
        _fs1, _fs2, _fs3 = st.columns([2, 1, 1])
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
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Réinitialiser", key="rf_reset", use_container_width=True):
                for _k in ("rf_date_from", "rf_date_to", "rf_status",
                           "rf_review_status", "rf_search", "rf_claimed_by"):
                    st.session_state.pop(_k, None)
                st.session_state["run_page"] = 1
                st.rerun()

    _date_from     = st.session_state.get("rf_date_from") or None
    _date_to       = st.session_state.get("rf_date_to") or None
    _status        = st.session_state.get("rf_status") or None
    _review_status = st.session_state.get("rf_review_status") or None
    _search        = st.session_state.get("rf_search") or None
    _claimed_by_raw = st.session_state.get("rf_claimed_by") or None
    _claimed_by = (
        [_username if v == "__me__" else v for v in _claimed_by_raw]
        if _claimed_by_raw else None
    )

    _filter_sig = (
        _date_from, _date_to,
        tuple(_status or []), tuple(_review_status or []),
        _search or "", tuple(_claimed_by_raw or []),
    )
    if st.session_state.get("_run_filter_sig") != _filter_sig:
        st.session_state["_run_filter_sig"] = _filter_sig
        st.session_state["run_page"] = 1

    # ── Pagination ────────────────────────────────────────────────────────────
    _total = db.count_runs(
        status=_status, date_from=_date_from, date_to=_date_to,
        search=_search, review_status=_review_status, claimed_by=_claimed_by)

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
        limit=_page_size, offset=_offset)

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

        # col 0: run ID + invisible CSS scope marker
        _c[0].markdown(
            f'<span class="rtbl-row"></span>'
            f'<span class="rtbl-run-id">{_rid}</span>',
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

        # col 6: imported count (workflow + workspace)
        _imported = int(_row.get("imported_count") or 0)
        _imp_html = f'<span class="rtbl-count-chip">{_imported}</span>' if _imported else "—"
        _c[6].markdown(f'<div class="rtbl-cell">{_imp_html}</div>', unsafe_allow_html=True)

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
                             key=f"claim_{_rid}", use_container_width=True,
                             help="Prendre en charge"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": "in_progress",
                    }
                    st.rerun()
            elif _rs == "in_progress" and _can_act:
                if st.button("", icon=":material/task_alt:",
                             key=f"done_{_rid}", use_container_width=True,
                             help="Marquer terminé"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": "done",
                    }
                    st.rerun()
                if st.button("", icon=":material/lock_open:",
                             key=f"unclaim_{_rid}", use_container_width=True,
                             help="Libérer"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": None,
                    }
                    st.rerun()
            elif _rs == "done" and _role == "admin":
                if st.button("", icon=":material/restart_alt:",
                             key=f"reopen_{_rid}", use_container_width=True,
                             help="Réouvrir"):
                    st.session_state["_run_pending_action"] = {
                        "run_id": _rid, "to_status": "in_progress",
                    }
                    st.rerun()

        # col 9: navigate to publications filtered by this run
        with _c[9]:
            if st.button("", icon=":material/visibility:",
                         key=f"pubs_{_rid}", use_container_width=True,
                         help="Voir les publications"):
                st.session_state["_jump_to_run"] = _rid
                st.query_params["page"] = "Publications"
                st.rerun()
