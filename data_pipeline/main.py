"""Main script to run the data pipeline."""

import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from config import default_queries
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader
from data_pipeline.reporting import GenerateReports

from data_pipeline.harvester import (
    WosHarvester,
    ScopusHarvester,
    CrossrefHarvester,
    OpenAlexCrossrefHarvester,
)

load_dotenv()

recipient_email = os.getenv("RECIPIENT_EMAIL")
sender_email = os.getenv("SENDER_EMAIL")
smtp_server = os.getenv("SMTP_SERVER")


def save_csv(df, filename, export_dir):
    """Saves a DataFrame to CSV if it's not empty and ensures the directory exists."""
    if not df.empty:
        os.makedirs(export_dir, exist_ok=True)  # ✅ Assure la création du dossier
        filepath = os.path.join(export_dir, filename)
        df.to_csv(filepath, index=False, encoding="utf-8")


def main(
    start_date=None,
    end_date=None,
    queries=None,
    authors_ids=None,
    output_dir=None,
):
    """
    Harvests, processes, deduplicates, enriches, and loads publication data from external sources.

    Args:
        start_date (str): The start date for harvesting publications (format: "YYYY-MM-DD").
        end_date (str): The end date for harvesting publications (format: "YYYY-MM-DD").
        queries (dict, optional): A dictionary of queries to override default queries for each data source.
        authors_ids (list, optional): A list of author IDs to filter and enrich EPFL-related authors.
        output_dir (str, optional): Directory where the report should be saved.


    Returns:
        dict: A dictionary containing the following DataFrames:
            - "df_metadata" (pd.DataFrame): Metadata of publications with Open Access enrichment.
            - "df_authors" (pd.DataFrame): General authors dataset.
            - "df_epfl_authors" (pd.DataFrame): EPFL-author-specific dataset with enriched information.
            - "df_unloaded" (pd.DataFrame): Publications that were not loaded into Infoscience.
            - "df_loaded" (pd.DataFrame): Fully processed and loaded publications.
            - "df_rejected" (pd.DataFrame): Publications that were rejected during the process.
            - "report_path" (str): Path to the generated Excel report.


    Example:
        results = main(start_date="2024-01-01", end_date="2025-01-01")
        df_metadata = results["df_metadata"]
        print(df_metadata.head())
    """
    today = datetime.now().date()
    if start_date is None:
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = today.strftime("%Y-%m-%d")

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    if output_dir is None:
        output_dir = project_root / "data"
    output_dir = Path(output_dir).resolve()

    output_dir.mkdir(parents=True, exist_ok=True)

    execution_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    export_dir = os.path.join(output_dir, execution_timestamp)
    os.makedirs(export_dir, exist_ok=True)

    # Merge provided queries with default queries
    merged_queries = {**default_queries, **(queries or {})}

    # Initialize harvesters dynamically
    harvesters = {
        "wos": WosHarvester(start_date, end_date, merged_queries["wos"]),
        "scopus": ScopusHarvester(start_date, end_date, merged_queries["scopus"]),
        "crossref": CrossrefHarvester(
            start_date,
            end_date,
            query=None,
            field_queries={
                "query.affiliation": merged_queries["crossref"],
            },
        ),
        "openalex": OpenAlexCrossrefHarvester(
            start_date, end_date, merged_queries["openalex"]
        ),  # Uncomment if needed
    }

    # Harvest publications
    publications = {name: harvester.harvest() for name, harvester in harvesters.items()}

    for name, df in publications.items():
        save_csv(df, f"Raw_{name.capitalize()}Publications.csv", export_dir)

    # Deduplicate publications (pass only non-empty datasets)
    deduplicator = (
        DataFrameProcessor(*[df for df in publications.values() if not df.empty])
        if any(not df.empty for df in publications.values())
        else None
    )

    df_deduplicated = (
        deduplicator.deduplicate_dataframes() if deduplicator else pd.DataFrame()
    )
    save_csv(df_deduplicated, "DeduplicatedPublications.csv", export_dir)

    df_final, df_unloaded = (
        deduplicator.deduplicate_infoscience(df_deduplicated)
        if not df_deduplicated.empty
        else (pd.DataFrame(), pd.DataFrame())
    )
    save_csv(df_unloaded, "UnloadedPublications.csv", export_dir)

    # Process metadata & authors
    df_metadata, df_authors = (
        deduplicator.generate_main_dataframes(df_final)
        if not df_final.empty
        else (pd.DataFrame(), pd.DataFrame())
    )
    save_csv(df_metadata, "Publications.csv", export_dir)
    save_csv(df_authors, "AuthorsAndAffiliations.csv", export_dir)

    author_processor = AuthorProcessor(df_authors) if not df_authors.empty else None
    df_epfl_authors = (
        author_processor.process(author_ids_to_check=authors_ids)
        .filter_epfl_authors()
        .clean_authors()
        .nameparse_authors()
        .api_epfl_reconciliation()
        .generate_dspace_uuid(return_df=True)
        if author_processor
        else pd.DataFrame()
    )
    save_csv(df_epfl_authors, "EpflAuthors.csv", export_dir)

    # Enrich publications with OA full text
    df_oa_metadata = (
        PublicationProcessor(df_metadata).process(return_df=True)
        if not df_metadata.empty
        else pd.DataFrame()
    )
    save_csv(df_oa_metadata, "PublicationsWithOAMetadata.csv", export_dir)

    # Load final data
    loader_instance = Loader(df_oa_metadata, df_epfl_authors, df_authors) if not df_oa_metadata.empty else None
    df_loaded = loader_instance.create_complete_publication() if loader_instance else pd.DataFrame()
    save_csv(df_loaded, "ImportedPublications.csv", export_dir)

    # Compute rejected publications
    df_rejected = (
        df_oa_metadata[~df_oa_metadata["row_id"].isin(df_loaded["row_id"])]
        if "row_id" in df_oa_metadata.columns and "row_id" in df_loaded.columns
        else df_oa_metadata.copy()
    )
    save_csv(df_rejected, "RejectedPublications.csv", export_dir)

    report_path = None  # Default to None if no report is generated
    if any(
        not df.empty for df in [df_oa_metadata, df_unloaded, df_epfl_authors, df_loaded]
    ):
        if "row_id" in df_loaded.columns:
            report_generator = GenerateReports(
                df_oa_metadata, df_unloaded, df_epfl_authors, df_loaded
            )
            report_path = report_generator.generate_excel_report(output_dir=export_dir)

            if report_path:
                report_generator.send_report_by_email(
                    recipient_email=recipient_email,
                    sender_email=sender_email,
                    smtp_server=smtp_server,
                    import_start_date=start_date,
                    import_end_date=end_date,
                    file_path=report_path,
                )

    return {
        "df_metadata": df_oa_metadata,
        "df_authors": df_authors,
        "df_epfl_authors": df_epfl_authors,
        "df_unloaded": df_unloaded,
        "df_loaded": df_loaded,
        "df_rejected": df_rejected,
        "report_path": report_path,  # None if no report was generated
    }


if __name__ == "__main__":
    main()
