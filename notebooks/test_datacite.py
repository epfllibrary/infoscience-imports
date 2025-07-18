import marimo

__generated_with = "0.13.11"
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
    from data_pipeline.harvester import DataCiteHarvester
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from config import default_queries
    from datetime import datetime
    return (
        AuthorProcessor,
        DataCiteHarvester,
        DataFrameProcessor,
        PublicationProcessor,
        datetime,
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
    return (path,)


@app.cell
def _(DataCiteHarvester):
    # Define the date range and generic query
    start_date = "2024"
    end_date = "2025"
    # generic_query = '(prefix:10.5281 OR prefix:10.48550) AND (creators.affiliation.name:*EPFL* OR creators.affiliation.name:"École Polytechnique Fédérale de Lausanne" OR creators.affiliation.name:"Swiss Federal Institute of Technology in Lausanne" OR creators.affiliation.affiliationIdentifier:02s376052) -prefix:10.5075'

    generic_query = '(creators.affiliation.name:*EPFL* OR creators.affiliation.name:"École Polytechnique Fédérale de Lausanne" OR creators.affiliation.name:"Swiss Federal Institute of Technology in Lausanne" OR creators.affiliation.affiliationIdentifier:02s376052) -prefix:10.5075'


    filters = {
        "state": "findable",
        "publisher": "true",
        "affiliation": "true",
        "include": "client",
        "registration-agency": "datacite",
        # "prefix": "10.48550",
    }

    # Instantiate the CrossrefHarvester with the desired parameters
    harvester = DataCiteHarvester(
        start_date=start_date,
        end_date=end_date,
        query=generic_query,
        format="ifs3",  # Output format as defined in your client (e.g., "ifs3")
        filters=filters,
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
        os.path.join(path, "DataCiteHarvestedData.csv"), index=False, encoding="utf-8"
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
def _(deduplicated_sources_df, os, path):
    deduplicated_sources_df.to_csv(
        os.path.join(path, "dedupPublications.csv"),
        index=False,
        encoding="utf-8",
    )
    return


@app.cell
def _(df_final, os, path):
    df_final.to_csv(
        os.path.join(path, "Publications.csv"),
        index=False,
        encoding="utf-8",
    )
    return


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
    return (df_epfl_authors,)


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
    return (df_oa_metadata,)


@app.cell
def _(df_oa_metadata, os, path):
    df_oa_metadata.to_csv(
        os.path.join(path, "ResearchOutputsWithOA.csv"),
        index=False,
        encoding="utf-8",
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    import altair as alt
    return (alt,)


@app.cell
def _(alt, df_publications):
    _chart = (
        alt.Chart(df_publications)
        .mark_bar()
        .encode(
            y=alt.Y("publisher", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


if __name__ == "__main__":
    app.run()
