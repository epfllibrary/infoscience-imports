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
    from data_pipeline.harvester import CrossrefHarvester
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from config import default_queries
    from datetime import datetime
    return (
        AuthorProcessor,
        CrossrefHarvester,
        DataFrameProcessor,
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
def _(CrossrefHarvester):
    # Define the date range and generic query
    start_date = "2025-05-01"
    end_date = "2025-05-05"
    generic_query = None

    # Define additional targeted field queries for Crossref
    field_queries = {
        "query.affiliation": "EPFL",  
    }

    # Instantiate the CrossrefHarvester with the desired parameters
    harvester = CrossrefHarvester(
        start_date=start_date,
        end_date=end_date,
        query=generic_query,
        format="ifs3",  # Output format as defined in your client (e.g., "ifs3")
        field_queries=field_queries,
    )

    # Harvest publications from Crossref based on the given parameters
    df_publications = harvester.harvest()
    return (
        df_publications,
        end_date,
        field_queries,
        generic_query,
        harvester,
        start_date,
    )


@app.cell
def _(df_publications):
    df_publications
    return


@app.cell
def _(df_publications, os, path):
    df_publications.to_csv(
        os.path.join(path, "CrossrefHarvestedData.csv"), index=False, encoding="utf-8"
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
def _(PublicationProcessor, df_metadata):
    # Generate publications dataframe enriched with OA attributes
    publication_processor = PublicationProcessor(df_metadata)
    df_oa_metadata = publication_processor.process(return_df=True)
    return df_oa_metadata, publication_processor


@app.cell
def _(df_oa_metadata, os, path):
    df_oa_metadata.to_csv(
        os.path.join(path, "ResearchOutputsWithOA.csv"),
        index=False,
        encoding="utf-8",
    )
    return


if __name__ == "__main__":
    app.run()
