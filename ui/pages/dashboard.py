"""Dashboard page — KPI tiles, trends, per-run breakdown."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from db.pipeline_db import PipelineDB
from ui.constants import PRIMARY, C_BLUE, C_GRAY_600, C_RED, SECONDARY
from ui.helpers import fmt_dur, metric_card, page_title, sh, badge


def render(db: PipelineDB) -> None:
    """Render the dashboard page — KPI tiles, trends, and per-run breakdown."""
    page_title("dashboard", "Tableau de bord")

    _kpis    = db.get_dashboard_kpis(months=12)
    _runs_df = db.get_runs(limit=20)

    # ── KPI tiles ─────────────────────────────────────────────────────────────
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
            metric_card("Durée moyenne / run", fmt_dur(_kpis["avg_duration_s"])),
            unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Monthly imports trend ─────────────────────────────────────────────────
    st.markdown(sh("trending_up", "Importés par mois (12 derniers mois)"), unsafe_allow_html=True)
    _month_df = db.get_imported_by_month(months=12)
    if not _month_df.empty:
        _fig = px.bar(
            _month_df, x="month", y="count",
            color_discrete_sequence=[PRIMARY],
            labels={"month": "Mois", "count": "Publications importées"},
            height=220,
        )
        _fig.update_layout(
            margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
            xaxis=dict(tickformat="%b %Y", tickangle=-30, dtick="M1"),
        )
        st.plotly_chart(_fig, width="stretch")
    else:
        st.caption("Aucune donnée pour les 12 derniers mois.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Per-run status breakdown + global status donut ────────────────────────
    _col_a, _col_b = st.columns([3, 2])

    with _col_a:
        st.markdown(sh("bar_chart", "Publications par run (20 derniers)"), unsafe_allow_html=True)
        _spr_df = db.get_pubs_status_per_run(limit=20)
        if not _spr_df.empty:
            _SPR_COLORS = {
                "workflow":     PRIMARY,     "workspace":     SECONDARY,
                "deduplicated": C_BLUE,     "rejected":      C_RED,
                "error":        C_GRAY_600,
            }
            _fig = px.bar(
                _spr_df, x="run_id", y="count", color="status",
                color_discrete_map=_SPR_COLORS,
                labels={"run_id": "Run", "count": "Publications", "status": "Statut"},
                barmode="stack", height=340,
            )
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                xaxis=dict(tickangle=-40, tickfont=dict(size=9)),
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucun run enregistré.")

    with _col_b:
        st.markdown(sh("donut_large", "Distribution globale"), unsafe_allow_html=True)
        _gs_df = db.get_pubs_by_status()
        if not _gs_df.empty:
            _fig = px.pie(
                _gs_df, names="status", values="count", color="status",
                color_discrete_map={
                    "workflow":     PRIMARY,  "workspace":    SECONDARY,
                    "deduplicated": C_BLUE,  "rejected":     C_RED,
                    "error":        C_GRAY_600,
                },
                hole=0.45, height=340,
            )
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            _fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée.")

    # ── Recent runs table ─────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(sh("history", "Runs récents"), unsafe_allow_html=True)
    if not _runs_df.empty:
        from ui.helpers import fmt_dt
        _disp = _runs_df.copy()
        _disp["Démarré"] = _disp["started_at"].apply(fmt_dt)
        _disp["Terminé"] = _disp["ended_at"].apply(fmt_dt)
        _disp["Durée"]   = _disp["duration_s"].apply(fmt_dur)
        _disp["Statut"]  = _disp["status"].apply(badge)
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

    # ── Imports by source × document type ────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        sh("stacked_bar_chart", "Importés par source et type de document"),
        unsafe_allow_html=True)
    _src_type_df = db.get_pubs_by_source_and_type()
    if not _src_type_df.empty:
        _top8 = (
            _src_type_df.groupby("dc_type")["count"].sum()
            .nlargest(8).index.tolist()
        )
        _st = _src_type_df.copy()
        _st["dc_type"] = _st["dc_type"].apply(lambda t: t if t in _top8 else "Autre")
        _st = _st.groupby(["source", "dc_type"], as_index=False)["count"].sum()
        _fig = px.bar(
            _st, x="source", y="count", color="dc_type",
            barmode="stack", height=360,
            color_discrete_sequence=px.colors.qualitative.Plotly,
            labels={"source": "Source", "count": "Publications importées", "dc_type": "Type"},
        )
        _fig.update_layout(
            margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
            legend=dict(orientation="h", yanchor="top", y=-0.08),
        )
        st.plotly_chart(_fig, width="stretch")
    else:
        st.caption("Aucune donnée.")
