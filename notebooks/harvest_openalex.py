import marimo

__generated_with = "0.14.13"
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
def _():
    from data_pipeline.harvester import OpenAlexHarvester
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from config import default_queries
    from datetime import datetime
    return AuthorProcessor, DataFrameProcessor, OpenAlexHarvester, datetime


@app.cell
def _(datetime, os):
    # Création du dossier avec la date actuelle
    current_datetime = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    folder_path = "harvested-data"
    os.makedirs(folder_path, exist_ok=True)
    path = os.path.join(folder_path, current_datetime)

    if not os.path.exists(path):
        os.mkdir(path)
    return (path,)


@app.cell
def _(OpenAlexHarvester):
    # Define the date range and generic query
    start_date = "2013-01-01"
    end_date = "2025-07-28"
    generic_query = "authorships.institutions.lineage:i5124864,type:types/article|types/book-chapter|types/review"


    # Instantiate the OpenAlexHarvester with the desired parameters
    harvester = OpenAlexHarvester(
        start_date=start_date,
        end_date=end_date,
        query=generic_query,
        format="ifs3",  # Output format as defined in your client (e.g., "ifs3")
    )

    # Harvest publications from Crossref based on the given parameters
    df_publications = harvester.harvest()
    return (df_publications,)


@app.cell
def _(df_publications):
    df_publications
    return


@app.cell
def _(df_publications, os, path):
    df_publications.to_csv(
        os.path.join(path, "./eth_bibliometric/raw_data/2013-2025_infoscience_openalex_reconciled.csv"), index=False, encoding="utf-8"
    )
    return


@app.cell
def _(DataFrameProcessor, df_publications):
    deduplicator = DataFrameProcessor(df_publications)
    # Deduplicate the publications : first deduplicate operation between the sources
    deduplicated_sources_df = deduplicator.deduplicate_dataframes()
    # and second operation : filter by removing founded duplicates in Infoscience
    df_final,df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)
    return deduplicator, df_final, df_unloaded


@app.cell
def _(df_unloaded, os, path):
    df_unloaded.to_csv(
        os.path.join(path, "./eth_bibliometric/processed_data/2013-2025_openalex_not_in_infoscience.csv"),
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
        os.path.join(path, "./eth_bibliometric/processed_data/2013-2025_openalex_in_infoscience.csv"), index=False, encoding="utf-8"
    )
    df_authors.to_csv(
        os.path.join(path, "./eth_bibliometric/processed_data/2013-2025_openalex_with_all_authors.csv"), index=False, encoding="utf-8"
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
    return (df_epfl_authors,)


@app.cell
def _(df_epfl_authors, os, path):
    df_epfl_authors.to_csv(
        os.path.join(path, "./eth_bibliometric/processed_data/2013-2025_openalex_not_in_infoscience_epfl_authors"), index=False, encoding="utf-8"
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
    return


@app.cell
def _(df_metadata):
    publications_not_infoscience_count = df_metadata['internal_id'].nunique()
    print(f"Nombre de publications non dispo sur Infoscience : {publications_not_infoscience_count}")
    return


if __name__ == "__main__":
    app.run()
