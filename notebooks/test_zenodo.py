import marimo

__generated_with = "0.14.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    import pandas as pd
    import json
    from pathlib import Path
    import marimo as mo
    from datetime import datetime


    sys.path.append(os.path.abspath(".."))

    return Path, datetime, mo, os


@app.cell
def _(datetime, os):
    from config import default_queries
    from data_pipeline.harvester import ZenodoHarvester
    from data_pipeline.deduplicator import DataFrameProcessor
    from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
    from data_pipeline.loader import Loader
    from data_pipeline.reporting import GenerateReports

    # Création du dossier avec la date actuelle
    current_datetime = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    folder_path = "harvested-data"
    os.makedirs(folder_path, exist_ok=True)
    path = os.path.join(folder_path, current_datetime)

    if not os.path.exists(path):
        os.mkdir(path)
    return (
        AuthorProcessor,
        DataFrameProcessor,
        GenerateReports,
        Loader,
        PublicationProcessor,
        ZenodoHarvester,
        path,
    )


@app.cell
def _(ZenodoHarvester):
    # Define the date range and generic query
    start_date = "2025-05-01"
    end_date = "2025-10-07"
    # generic_query = '(prefix:10.5281 OR prefix:10.48550) AND (creators.affiliation.name:*EPFL* OR creators.affiliation.name:"École Polytechnique Fédérale de Lausanne" OR creators.affiliation.name:"Swiss Federal Institute of Technology in Lausanne" OR creators.affiliation.affiliationIdentifier:02s376052) -prefix:10.5075'

    generic_query = 'parent.communities.entries.id:"3c1383da-d7ab-4167-8f12-4d8aa0cc637f"'
    # generic_query = 'doi:10.5281/zenodo.15024667'

    # Instantiate the CrossrefHarvester with the desired parameters
    harvester = ZenodoHarvester(
        start_date=start_date,
        end_date=end_date,
        query=generic_query,
        format="ifs3",  # Output format as defined in your client (e.g., "ifs3")
    )

    # Harvest publications from Crossref based on the given parameters
    df_publications = harvester.harvest()
    df_publications
    return (df_publications,)


@app.cell
def _(DataFrameProcessor, df_publications):
    deduplicator = DataFrameProcessor(df_publications)
    # Deduplicate the publications : first deduplicate operation between the sources
    deduplicated_sources_df = deduplicator.deduplicate_dataframes()
    # and second operation : filter by removing founded duplicates in Infoscience
    df_final,df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)
    return deduplicated_sources_df, deduplicator, df_final, df_unloaded


@app.cell
def _(deduplicated_sources_df):
    deduplicated_sources_df
    return


@app.cell
def _(df_final):
    df_final
    return


@app.cell
def _(df_unloaded):
    df_unloaded
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
        author_processor.process(author_ids_to_check="")
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
def _(df_oa_metadata, os, path):
    df_oa_metadata.to_csv(
        os.path.join(path, "ResearchOutputsWithOA.csv"),
        index=False,
        encoding="utf-8",
    )
    return


@app.cell
def _(df_oa_metadata):
    df_oa_metadata
    return


@app.cell
def _(Loader, df_authors, df_epfl_authors, df_oa_metadata):
    loading_output =[]

    loader_instance = Loader(df_oa_metadata, df_epfl_authors, df_authors)
    imported_items = loader_instance.create_complete_publication()

    loading_output
    return (imported_items,)


@app.cell
def _(df_oa_metadata, imported_items):
    df_rejected = (
        df_oa_metadata[~df_oa_metadata["row_id"].isin(imported_items["row_id"])]
        if "row_id" in df_oa_metadata.columns and "row_id" in imported_items.columns
        else df_oa_metadata.copy()
    )
    df_rejected
    return


@app.cell
def _(
    GenerateReports,
    Path,
    df_epfl_authors,
    df_oa_metadata,
    df_unloaded,
    imported_items,
    mo,
):
    export_dir = Path("reports")
    export_dir.mkdir(exist_ok=True)

    report_path = None

    if any(not df.empty for df in [df_oa_metadata, df_unloaded, df_epfl_authors, imported_items]):
        if "row_id" in imported_items.columns:
            report_generator = GenerateReports(
                df_oa_metadata, df_unloaded, df_epfl_authors, imported_items
            )
            report_path = Path(report_generator.generate_excel_report(output_dir=export_dir))

    # Afficher un lien de téléchargement ou un message
    if report_path:
        with open(report_path, "rb") as f:
            data = f.read()
        download_ui = mo.download(data=data, filename=report_path.name, label="📄 Download Excel Report")
    else:
        download_ui = mo.md("No Report.")

    download_ui
    return


if __name__ == "__main__":
    app.run()
