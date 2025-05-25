import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    from datetime import datetime
    import pandas as pd
    import marimo as mo
    import datetime as dt

    sys.path.append(os.path.abspath(".."))

    from data_pipeline.harvester import ScopusHarvester, WosHarvester, CrossrefHarvester, OpenAlexCrossrefHarvester
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from data_pipeline.loader import Loader
    from config import default_queries
    app = mo.App()

    return (
        AuthorProcessor,
        CrossrefHarvester,
        DataFrameProcessor,
        Loader,
        OpenAlexCrossrefHarvester,
        PublicationProcessor,
        ScopusHarvester,
        WosHarvester,
        default_queries,
        mo,
        pd,
    )


@app.cell
def _(mo):
    mo.md(
        """
    # 🔍 Demo: Pipeline For Importing Publications into DSpace

    This notebook walks through a pipeline for harvesting, deduplicating, enriching, and loading scientific publications into a DSpace repository.
    """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Harvesting Data From External Sources""")
    return


@app.cell
def _(default_queries, mo):
    form = mo.md('''
        ### Harvesting Criterias Form

        **Please fill in the following harvesting criteria:**

        {sources}

        **Define date range**

        {date_range}

        **Define query by source**

        {scopus_query}
        {wos_query}
        {crossref_query}
        {openalex_query}
        '''
    ).style(max_height="800px", overflow="auto").batch(
        sources=mo.ui.multiselect(
            label="Select Source:",
            options=["Scopus", "WoS", "Crossref", "OpenAlex"]
        ),
        date_range=mo.ui.date_range(
            label="Date Range:",
            start="2025-01-01",
            stop="2025-12-31",
        ),
        scopus_query=mo.ui.text_area(
            label="Scopus query:",
            value=default_queries["scopus"],
            placeholder="Default Scopus query",
            rows=2,
            full_width=True,
        ),
        wos_query=mo.ui.text_area(
            label="WoS query:",
            value=default_queries["wos"],
            placeholder="Default WoS query",
            rows=2,
            full_width=True,

        ),
        crossref_query=mo.ui.text_area(
            label="Crossref query:",
            value=default_queries["crossref"],
            placeholder="Default Crossref query",
            rows=2,
            full_width=True,

        ),
        openalex_query=mo.ui.text_area(
            label="OpenAlex query:",
            value=default_queries["openalex"],
            placeholder="Default OpenAlex query",
            rows=2,
            full_width=True,

        )

    ).form(show_clear_button=True, bordered=True, submit_button_label="🚀 Launch Harvesting")

    return (form,)


@app.cell
def _(form):
    form
    return


@app.cell
def _(
    CrossrefHarvester,
    OpenAlexCrossrefHarvester,
    ScopusHarvester,
    WosHarvester,
    form,
    mo,
    pd,
):
    mo.stop(not form.value)

    start_date = form.value["date_range"][0].isoformat()
    end_date = form.value["date_range"][1].isoformat()
    sources = form.value["sources"]

    scopus_df = wos_df = crossref_df = openalex_df = pd.DataFrame()

    if "Scopus" in sources:
        scopus_df = pd.DataFrame(
            ScopusHarvester(start_date, end_date, form.value["scopus_query"]).harvest()
        )

    if "WoS" in sources:
        wos_df = pd.DataFrame(
            WosHarvester(start_date, end_date, form.value["wos_query"]).harvest()
        )

    if "Crossref" in sources:
        crossref_df = pd.DataFrame(
            CrossrefHarvester(start_date, end_date, form.value["crossref_query"]).harvest()
        )

    if "OpenAlex" in sources:
        openalex_df = pd.DataFrame(
            OpenAlexCrossrefHarvester(start_date, end_date, form.value["openalex_query"]).harvest()
        )

    return crossref_df, openalex_df, scopus_df, wos_df


@app.cell
def _(crossref_df, mo, openalex_df, scopus_df, wos_df):
    harvested_results = []

    if not scopus_df.empty:
        harvested_results.append(mo.md("#### Scopus Results"))
        harvested_results.append(mo.ui.table(scopus_df))

    if not wos_df.empty:
        harvested_results.append(mo.md("#### WoS Results"))
        harvested_results.append(mo.ui.table(wos_df))

    if not crossref_df.empty:
        harvested_results.append(mo.md("#### Crossref Results"))
        harvested_results.append(mo.ui.table(crossref_df)) 

    if not openalex_df.empty:
        harvested_results.append(mo.md("#### OpenAlex Results"))
        harvested_results.append(mo.ui.table(openalex_df))


    harvested_results 
    return


@app.cell
def _(mo):
    mo.md(r"""## Deduplicate and Merge Collected Resuts""")
    return


@app.cell
def _(mo):
    dedup_btn = mo.ui.run_button(label="🧹 Deduplicate")
    dedup_btn
    return (dedup_btn,)


@app.cell
def _(
    DataFrameProcessor,
    crossref_df,
    dedup_btn,
    mo,
    openalex_df,
    scopus_df,
    wos_df,
):
    mo.stop(not dedup_btn.value)
    dataframes = [df for df in [scopus_df, wos_df, crossref_df, openalex_df] if not df.empty]
    deduplicator = DataFrameProcessor(*dataframes)
    dedup_output = []

    deduplicated_sources_df = deduplicator.deduplicate_dataframes()
    dedup_output.append(mo.md("### ✅ Deduplicated Source Data"))
    dedup_output.append(mo.ui.table(deduplicated_sources_df))

    dedup_output
    return deduplicated_sources_df, deduplicator


@app.cell
def _(mo):
    infoscience_btn = mo.ui.run_button(label="📄 Deduplicate with Infoscience")
    infoscience_btn
    return (infoscience_btn,)


@app.cell
def _(deduplicated_sources_df, deduplicator, infoscience_btn, mo):
    mo.stop(not infoscience_btn.value)

    dedupinfoscience_output = []

    df_final,df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)

    df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)

    dedupinfoscience_output.append(mo.md("### ✅ Candidate Items"))
    dedupinfoscience_output.append(mo.ui.table(df_metadata))
    dedupinfoscience_output.append(mo.md("### 👩 Full List of Authors and Affiliations for Candidate Items"))
    dedupinfoscience_output.append(mo.ui.table(df_authors))
    dedupinfoscience_output.append(mo.md("### ❌ Rejected Items"))
    dedupinfoscience_output.append(mo.ui.table(df_unloaded))

    dedupinfoscience_output
    return df_authors, df_metadata


@app.cell
def _(mo):
    mo.md(r"""## Enrich Metadata""")
    return


@app.cell
def _(mo):
    authors_btn = mo.ui.run_button(label="🙋‍♀️ Authors reconciliation")
    authors_btn
    return (authors_btn,)


@app.cell
def _(AuthorProcessor, authors_btn, df_authors, mo):
    mo.stop(not authors_btn.value)

    author_reconcil_output = []

    author_processor = AuthorProcessor(df_authors)

    df_epfl_authors = (
        author_processor.process()
        .filter_epfl_authors()
        .clean_authors()
        .nameparse_authors()
        .api_epfl_reconciliation()
        .generate_dspace_uuid(return_df=True)
    )

    author_reconcil_output.append(mo.md("### 👩‍🏫 EPFL Authors Reconciled"))
    author_reconcil_output.append(mo.ui.table(df_epfl_authors))
    author_reconcil_output
    return (df_epfl_authors,)


@app.cell
def _():
    return


@app.cell
def _(mo):
    publications_btn = mo.ui.run_button(label="📚 Enrich OA Status and Retrieve Fulltexts")
    publications_btn
    return (publications_btn,)


@app.cell
def _(PublicationProcessor, df_metadata, mo, publications_btn):
    mo.stop(not publications_btn.value)

    oa_metadata_output =[]

    publication_processor = PublicationProcessor(df_metadata)
    df_oa_metadata = publication_processor.process(return_df=True)

    oa_metadata_output.append(mo.md("### 📚 Final Enriched Publications"))
    oa_metadata_output.append(mo.ui.table(df_oa_metadata))
    oa_metadata_output
    return (df_oa_metadata,)


@app.cell
def _(mo):
    mo.md(r"""## Loadin Items in DSpace""")
    return


@app.cell
def _(mo):
    load_btn = mo.ui.run_button(label="🚀 Load to DSpace")
    load_btn
    return (load_btn,)


@app.cell
def _(Loader, df_authors, df_epfl_authors, df_oa_metadata, load_btn, mo):
    mo.stop(not load_btn.value)
    loading_output =[]

    loader_instance = Loader(df_oa_metadata, df_epfl_authors, df_authors)
    imported_items = loader_instance.create_complete_publication()

    loading_output.append(mo.md("### ✅ Imported Items Report"))
    loading_output.append(mo.ui.table(imported_items))
    loading_output
    return


if __name__ == "__main__":
    app.run()
