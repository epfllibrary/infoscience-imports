import marimo

__generated_with = "0.14.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    import pandas as pd
    import json


    sys.path.append(os.path.abspath(".."))
    return (os,)


@app.cell
def _(os):
    env = os.environ.get("DS_API_ENDPOINT")
    return (env,)


@app.cell
def _(env):
    env
    return


@app.cell
def _():
    from clients.epo_ops_client import EPOClient

    client = EPOClient()
    print("Client initialisé:", client)
    return (client,)


@app.cell
def _(client):
    rec = client.fetch_record_by_unique_id("WO2025252699", format="ifs3")
    rec
    return


@app.cell
def _(client):
    cql = 'pa all "ECOLE POLYTECH* FED LAUSANNE*" AND pd>20251231'
    ids = client.fetch_ids(cql=cql, per_page=5, max_records=10, range_begin=1)
    ids
    return (ids,)


@app.cell
def _(client):
    client.last_response.text
    return


@app.cell
def _(client, ids):
    recs = client.fetch_records_by_ids(ids, format="ifs3")
    return (recs,)


@app.cell
def _(recs):
    recs
    return


@app.cell
def _():
    from data_pipeline.harvester import EPOHarvester

    return (EPOHarvester,)


@app.cell
def _(datetime, os):
    # Création du dossier avec la date actuelle
    current_datetime = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    folder_path = "harvested-data"
    os.makedirs(folder_path, exist_ok=True)
    path = os.path.join(folder_path, current_datetime)

    if not os.path.exists(path):
        os.mkdir(path)
    return


@app.cell
def _(EPOHarvester):
    # Define the date range and generic query
    start_date = "2026-01-01"
    end_date = "2026-02-10"
    query = 'pa all "ECOLE POLYTECH* FED LAUSANNE*" AND (pn=EP OR pn=WO)'

    # Instantiate the OpenAlexHarvester with the desired parameters
    harvester = EPOHarvester(
        start_date=start_date,
        end_date=end_date,
        query=query,
        group_by_family=True,
        format="ifs3", 
        max_records=3,
    )

    # Harvest publications from Crossref based on the given parameters
    df_publications = harvester.harvest()
    return (df_publications,)


@app.cell
def _(df_publications):
    df_publications
    return


@app.cell
def _():
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from config import default_queries
    from datetime import datetime
    return AuthorProcessor, DataFrameProcessor, PublicationProcessor, datetime


@app.cell
def _(DataFrameProcessor, df_publications):
    deduplicator = DataFrameProcessor(df_publications)
    # Deduplicate the publications : first deduplicate operation between the sources
    deduplicated_sources_df = deduplicator.deduplicate_dataframes()

    return deduplicated_sources_df, deduplicator


@app.cell
def _(deduplicated_sources_df):
    deduplicated_sources_df
    return


@app.cell
def _(deduplicated_sources_df, deduplicator):
    df_final,df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)
    return (df_final,)


@app.cell
def _(df_final):
    df_final
    return


@app.cell
def _(deduplicator, df_final):
    df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)
    return df_authors, df_metadata


@app.cell
def _(df_metadata):
    df_metadata
    return


@app.cell
def _(df_authors):
    df_authors
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
    return (df_epfl_authors,)


@app.cell
def _(df_epfl_authors):
    df_epfl_authors
    return


@app.cell
def _(PublicationProcessor, df_metadata):
    # Generate publications dataframe enriched with OA attributes
    publication_processor = PublicationProcessor(df_metadata)
    df_oa_metadata = publication_processor.process(return_df=True)
    return (df_oa_metadata,)


@app.cell
def _(df_oa_metadata):
    df_oa_metadata
    return


@app.cell
def _():
    from data_pipeline.loader import Loader
    return (Loader,)


@app.cell
def _(Loader, df_authors, df_epfl_authors, df_oa_metadata):
    loading_output =[]

    loader_instance = Loader(df_oa_metadata, df_epfl_authors, df_authors)
    imported_items = loader_instance.create_complete_publication()

    loading_output
    return


if __name__ == "__main__":
    app.run()
