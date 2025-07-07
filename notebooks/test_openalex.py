import marimo

__generated_with = "0.12.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    import pandas as pd
    import json


    sys.path.append(os.path.abspath(".."))
    return json, os, pd, sys


@app.cell
def _():
    from data_pipeline.harvester import OpenAlexCrossrefHarvester
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from config import default_queries
    from datetime import datetime
    return (
        AuthorProcessor,
        DataFrameProcessor,
        OpenAlexCrossrefHarvester,
        PublicationProcessor,
        datetime,
        default_queries,
    )


@app.cell
def _(datetime, os):
    # Création du dossier avec la date actuelle
    current_datetime = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    folder_path = "harvested-data"
    os.makedirs(folder_path, exist_ok=True)
    path = os.path.join(folder_path, current_datetime)

    if not os.path.exists(path):
        os.mkdir(path)
    return current_datetime, folder_path, path


@app.cell
def _(OpenAlexCrossrefHarvester):
    # Define the date range and generic query
    start_date = "2025-05-03"
    end_date = "2025-05-05"
    generic_query = "authorships.institutions.lineage:i5124864"


    # Instantiate the OpenAlexHarvester with the desired parameters
    harvester = OpenAlexCrossrefHarvester(
        start_date=start_date,
        end_date=end_date,
        query=generic_query,
        format="ifs3",  # Output format as defined in your client (e.g., "ifs3")
    )

    # Harvest publications from Crossref based on the given parameters
    df_publications = harvester.harvest()
    return df_publications, end_date, generic_query, harvester, start_date


@app.cell
def _(df_publications):
    df_publications
    return


@app.cell
def _(df_publications, os, path):
    df_publications.to_csv(
        os.path.join(path, "OpenAlexHarvestedData.csv"), index=False, encoding="utf-8"
    )
    return


@app.cell
def _(DataFrameProcessor, df_publications):
    deduplicator = DataFrameProcessor(df_publications)
    # Deduplicate the publications : first deduplicate operation between the sources
    deduplicated_sources_df = deduplicator.deduplicate_dataframes()
    # and second operation : filter by removing founded duplicates in Infoscience
    df_final,df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)
    return deduplicated_sources_df, deduplicator, df_final, df_unloaded


@app.cell
def _(df_unloaded, os, path):
    df_unloaded.to_csv(
        os.path.join(path, "UnloadedDuplicatedPublications.csv"),
        index=False,
        encoding="utf-8",
    )
    return


@app.cell
def _(deduplicator, df_final):
    df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)
    return df_authors, df_metadata


@app.cell
def _(df_authors):
    df_authors
    return


@app.cell
def _(df_authors, df_metadata, os, path):
    df_metadata.to_csv(
        os.path.join(path, "df_metadata.csv"), index=False, encoding="utf-8"
    )
    df_authors.to_csv(
        os.path.join(path, "df_authors.csv"), index=False, encoding="utf-8"
    )
    return


@app.cell
def _(AuthorProcessor, df_authors):
    author_processor = AuthorProcessor(df_authors)

    df_epfl_authors = (
        author_processor.process()
        .filter_epfl_authors()
        .clean_authors()
        .nameparse_authors()
        .reconcile_authors(return_df=True)
    )
    return author_processor, df_epfl_authors


@app.cell
def _(df_epfl_authors, os, path):
    df_epfl_authors.to_csv(
        os.path.join(path, "EpflAuthors.csv"), index=False, encoding="utf-8"
    )
    return


@app.cell
def _(df_epfl_authors):
    df_epfl_authors
    return


@app.cell
def _(df_unloaded):
    publications_in_infoscience_count = df_unloaded['internal_id'].nunique()
    print(f"Nombre de publications déja sur Infoscience : {publications_in_infoscience_count}")
    return (publications_in_infoscience_count,)


@app.cell
def _(df_metadata):
    publications_not_infoscience_count = df_metadata['internal_id'].nunique()
    print(f"Nombre de publications non dispo sur Infoscience : {publications_not_infoscience_count}")
    return (publications_not_infoscience_count,)


@app.cell
def _(alt, df_metadata):
    _doctypeschart = (
        alt.Chart(df_metadata)
        .mark_bar()
        .encode(
            y=alt.Y("doctype", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _doctypeschart
    return


@app.cell
def _(alt, df_epfl_authors):
    _epflauthorschart = (
        alt.Chart(df_epfl_authors)
        .transform_aggregate(count="count()", groupby=["author"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("author", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 30)
        .mark_bar()
        .encode(
            y=alt.Y("author", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Top 10 author", width="container")
    )
    _epflauthorschart
    return


@app.cell
def _(alt, df_epfl_authors):
    _unitschart = (
        alt.Chart(df_epfl_authors)
        .transform_aggregate(count="count()", groupby=["epfl_api_mainunit_name"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("epfl_api_mainunit_name", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("epfl_api_mainunit_name", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Top 10 epfl_api_mainunit_name", width="container")
    )
    _unitschart
    return


@app.cell
def _(df_epfl_authors):
    valid_author_ids = df_epfl_authors[
                df_epfl_authors["epfl_api_mainunit_name"].notnull()
            ]["row_id"].unique()
    return (valid_author_ids,)


@app.cell
def _(df_metadata, valid_author_ids):
    if len(valid_author_ids) > 0:
        filtered_publications = df_metadata[
            df_metadata["row_id"].isin(valid_author_ids)
        ]
    return (filtered_publications,)


@app.cell
def _(filtered_publications):
    filtered_publications
    return


@app.cell
def _():
    from clients.crossref_client import CrossrefClient
    return (CrossrefClient,)


@app.cell
def _(CrossrefClient, filtered_publications):
    def extract_doi_prefix(doi):
        return doi.split('/')[0] if isinstance(doi, str) and '/' in doi else None

    def get_provider(prefix):
        try:
            return CrossrefClient.fetch_prefix_name(prefix)
        except Exception:
            return None

    filtered_publis = filtered_publications.copy()
    filtered_publis['doi_prefix'] = filtered_publis['doi'].apply(extract_doi_prefix)

    filtered_publis['doi_provider'] = filtered_publis['doi_prefix'].apply(get_provider)
    return extract_doi_prefix, filtered_publis, get_provider


@app.cell
def _(filtered_publis):
    filtered_publis
    return


@app.cell
def _(alt, filtered_publis):
    _chart = (
        alt.Chart(filtered_publis)
        .transform_aggregate(count="count()", groupby=["doi_provider"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("doi_provider", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 100)
        .mark_bar()
        .encode(
            y=alt.Y("doi_provider", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Top 10 doi_provider", width="container")
    )
    _chart
    return


@app.cell
def _(df_epfl_authors):
    epfl_authors_not_reconciliated = df_epfl_authors[df_epfl_authors['sciper_id'].isna() | (df_epfl_authors['sciper_id'] == '')]
    epfl_authors_not_reconciliated
    return (epfl_authors_not_reconciliated,)


@app.cell
def _(epfl_authors_not_reconciliated):
    publications_not_reconciliated_count = epfl_authors_not_reconciliated['row_id'].nunique()
    print(f"Nombre de publications non reconciliées : {publications_not_reconciliated_count}")
    return (publications_not_reconciliated_count,)


if __name__ == "__main__":
    app.run()
