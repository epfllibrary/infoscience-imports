"""Statistics page — per-run and global charts for publications, authors, units."""

from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db.pipeline_db import PipelineDB
from ui.constants import PRIMARY, C_BLUE, C_GRAY_100, C_GRAY_600, C_GREEN, C_RED, C_YELLOW, SECONDARY
from ui.helpers import page_title, sh


def render(db: PipelineDB) -> None:
    """Render the statistics page — per-run and global charts for publications, authors, and units."""
    page_title("bar_chart", "Statistiques")

    _runs_df = db.get_runs(limit=100)
    if _runs_df.empty:
        st.info("Aucun run enregistré. Lancez un premier run pour voir les statistiques.")
        st.stop()

    _run_opts = ["Tous les runs"] + _runs_df["run_id"].tolist()
    _sel_run = st.selectbox(
        "Périmètre", _run_opts, key="stat_run_filter",
        help="Sélectionnez un run pour explorer ses données en détail, "
             "ou gardez « Tous les runs » pour une vue agrégée.",
    )
    _run_id: str | None = None if _sel_run == "Tous les runs" else _sel_run

    tab_overview, tab_pubs, tab_people = st.tabs(
        ["Vue d'ensemble", "Publications", "Auteurs & Unités"]
    )

    with tab_overview:
        _render_overview(db, _run_id)

    with tab_pubs:
        _render_publications(db, _run_id)

    with tab_people:
        _render_people(db, _run_id)


def _render_overview(db: PipelineDB, run_id: str | None) -> None:
    _s1, _s2 = st.columns(2)

    with _s1:
        st.markdown(sh("filter_alt", "Entonnoir par source"), unsafe_allow_html=True)
        _src = db.get_sources_breakdown(run_id=run_id)
        _src = _src[_src["source"] != "__total__"] if not _src.empty else _src
        if not _src.empty:
            _fig = go.Figure()
            for col, clr, lbl in [
                ("harvested", "#b0c4de", "Collectés"),
                ("loaded",    PRIMARY,    "Importés"),
                ("rejected",  C_RED,     "Rejetés"),
            ]:
                _fig.add_trace(go.Bar(name=lbl, x=_src["source"], y=_src[col], marker_color=clr))
            _fig.update_layout(
                barmode="group", height=300,
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée de source disponible.")

    with _s2:
        st.markdown(sh("donut_large", "Distribution des statuts"), unsafe_allow_html=True)
        _st_df = db.get_pubs_by_status(run_id)
        if not _st_df.empty:
            _fig = px.pie(
                _st_df, names="status", values="count", color="status",
                color_discrete_map={
                    "workflow":     PRIMARY,  "workspace":    SECONDARY,
                    "deduplicated": C_BLUE,  "rejected":     C_RED,
                    "error":        C_GRAY_600,
                },
                hole=0.42, height=300,
            )
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            _fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée.")

    _s3, _s4 = st.columns(2)

    with _s3:
        st.markdown(sh("lock_open", "Statut Open Access"), unsafe_allow_html=True)
        _oa_df = db.get_pubs_by_oa_status(run_id)
        if not _oa_df.empty:
            _fig = px.pie(
                _oa_df, names="oa_category", values="count", color="oa_category",
                color_discrete_map={
                    "OA + PDF":     C_GREEN,    "OA sans PDF":  SECONDARY,
                    "OA non-libre": C_YELLOW,   "Non-OA":       C_GRAY_600,
                    "Non défini":   C_GRAY_100,
                },
                hole=0.42, height=300,
            )
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            _fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée.")

    with _s4:
        st.markdown(sh("picture_as_pdf", "Proportion avec PDF récupéré"), unsafe_allow_html=True)
        _pdf = db.get_pdf_stats(run_id)
        if _pdf["total"] > 0:
            _fig = px.pie(
                {"label": ["PDF récupéré", "Sans PDF"],
                 "count": [_pdf["with_pdf"], _pdf["total"] - _pdf["with_pdf"]]},
                names="label", values="count", color="label",
                color_discrete_map={"PDF récupéré": C_GREEN, "Sans PDF": C_GRAY_100},
                hole=0.42, height=300,
            )
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0),
                legend=dict(orientation="h", yanchor="top", y=-0.08),
            )
            _fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(_fig, width="stretch")
            _pct = round(100 * _pdf["with_pdf"] / _pdf["total"])
            st.caption(
                f"{_pdf['with_pdf']} PDF sur {_pdf['total']} publications importées ({_pct} %)"
            )
        else:
            st.caption("Aucune donnée.")

    if run_id:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(sh("table_chart", "Détail par source"), unsafe_allow_html=True)
        _detail = db.get_run_stats(run_id)
        if not _detail.empty:
            st.dataframe(
                _detail.rename(columns={
                    "source": "Source", "harvested": "Collectés",
                    "deduplicated": "Dédoublonnés", "loaded": "Importés",
                    "rejected": "Rejetés",
                }),
                width="stretch", hide_index=True,
            )


def _render_publications(db: PipelineDB, run_id: str | None) -> None:
    _p1, _p2 = st.columns(2)

    with _p1:
        st.markdown(sh("category", "Types de documents importés"), unsafe_allow_html=True)
        _type_stat = db.get_pubs_by_type(run_id)
        if not _type_stat.empty:
            _type_stat = _type_stat[_type_stat["type"].notna()].head(15)
            _fig = px.bar(
                _type_stat, x="count", y="type", orientation="h",
                color_discrete_sequence=[PRIMARY],
                labels={"count": "Publications", "type": "Type"},
                height=max(280, len(_type_stat) * 22),
            )
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée.")

    with _p2:
        st.markdown(sh("calendar_today", "Par année de publication"), unsafe_allow_html=True)
        _year_stat = db.get_pubs_by_year(run_id)
        if not _year_stat.empty:
            _fig = px.bar(
                _year_stat, x="year", y="count",
                color_discrete_sequence=[PRIMARY],
                labels={"year": "Année", "count": "Publications"},
                height=280,
            )
            _fig.update_layout(margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white")
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(sh("newspaper", "Top journaux"), unsafe_allow_html=True)
    _jour = db.get_pubs_by_journal(run_id, limit=20)
    if not _jour.empty:
        _fig = px.bar(
            _jour, x="count", y="journal", orientation="h",
            color_discrete_sequence=[PRIMARY],
            labels={"count": "Publications", "journal": "Journal"},
            height=max(300, len(_jour) * 22),
        )
        _fig.update_layout(
            margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(_fig, width="stretch")
    else:
        st.caption("Aucun journal disponible.")


def _render_people(db: PipelineDB, run_id: str | None) -> None:
    from datetime import date
    _col_auth, _col_units = st.columns([3, 2])

    with _col_auth:
        st.markdown(sh("people", "Top auteurs EPFL"), unsafe_allow_html=True)
        _top_auth = db.get_top_epfl_authors(run_id=run_id, limit=20)
        if not _top_auth.empty:
            _fig = px.bar(
                _top_auth, x="pub_count", y="full_name", orientation="h",
                color_discrete_sequence=[PRIMARY],
                custom_data=["main_unit", "sciper"],
                labels={"pub_count": "Publications", "full_name": "Auteur"},
                height=max(300, len(_top_auth) * 26),
            )
            _fig.update_traces(hovertemplate=(
                "<b>%{y}</b><br>Publications : %{x}<br>"
                "Unité : %{customdata[0]}<br>SCIPER : %{customdata[1]}<extra></extra>"
            ))
            _fig.update_layout(
                margin=dict(l=0, r=0, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed"), showlegend=False,
            )
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucun auteur EPFL réconcilié disponible.")

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(sh("manage_search", "Recherche auteurs"), unsafe_allow_html=True)
        _pa1, _pa2 = st.columns(2)
        with _pa1:
            _author_search = st.text_input("Nom", key="stat_author_search")
        with _pa2:
            _all_units = db.get_distinct_units()
            _filter_unit = st.selectbox("Unité", ["Toutes"] + _all_units, key="stat_author_unit")

        _authors_df = db.get_epfl_authors(
            name_search=_author_search.strip() or None,
            unit=_filter_unit if _filter_unit != "Toutes" else None,
            limit=500,
        )
        st.caption(f"{len(_authors_df)} auteur(s)")
        if not _authors_df.empty:
            st.dataframe(
                _authors_df.rename(columns={
                    "sciper": "SCIPER", "full_name": "Nom",
                    "first_name": "Prénom", "last_name": "Famille",
                    "orcid": "ORCID", "epfl_orcid": "ORCID EPFL",
                    "scopus_id": "Scopus", "wos_id": "WoS",
                    "openalex_id": "OpenAlex", "epfl_status": "Statut",
                    "epfl_position": "Poste", "main_unit": "Unité",
                    "dspace_uuid": "UUID DSpace", "last_seen": "Vu le",
                }),
                width="stretch", hide_index=True, height=380,
            )
            st.download_button(
                "⬇ Auteurs CSV",
                data=_authors_df.to_csv(index=False).encode("utf-8"),
                file_name=f"epfl_authors_{date.today()}.csv",
                mime="text/csv",
            )
        else:
            st.info("Aucun auteur EPFL dans la base.")

    with _col_units:
        st.markdown(sh("account_balance", "Unités EPFL"), unsafe_allow_html=True)
        _unit_chart = db.get_pubs_by_unit(run_id, limit=30)
        if not _unit_chart.empty:
            _fig = px.bar(
                _unit_chart, x="count", y="acronym", orientation="h",
                color_discrete_sequence=[C_BLUE],
                labels={"count": "Publications", "acronym": ""},
                height=max(340, len(_unit_chart) * 22),
            )
            _fig.update_layout(
                margin=dict(l=0, r=8, t=4, b=0), plot_bgcolor="white",
                yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
                xaxis=dict(title_font=dict(size=11)), showlegend=False,
            )
            _fig.update_traces(
                hovertemplate="<b>%{y}</b> — %{x} publications<extra></extra>"
            )
            st.plotly_chart(_fig, width="stretch")
        else:
            st.caption("Aucune donnée d'unité disponible.")

        st.markdown("<br>", unsafe_allow_html=True)
        _units_tbl = db.get_units()
        if not _units_tbl.empty:
            st.caption(f"{len(_units_tbl)} unités au total")
            st.dataframe(
                _units_tbl[["acronym", "name_fr", "unit_type", "author_count", "pub_count"]]
                .rename(columns={
                    "acronym": "Acr.", "name_fr": "Nom", "unit_type": "Type",
                    "author_count": "Auteurs", "pub_count": "Pub.",
                }),
                width="stretch", hide_index=True, height=340,
            )
        else:
            st.info("Aucune unité dans la base.")
