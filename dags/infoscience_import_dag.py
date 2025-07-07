"""A DAG to orchestrate the infoscience import data pipeline"""

import os
import pickle
import base64
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context


# Import project-specific modules
from config import default_queries
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader
from data_pipeline.reporting import GenerateReports
from data_pipeline.harvester import WosHarvester, ScopusHarvester


load_dotenv()

recipient_email = os.getenv("RECIPIENT_EMAIL")
sender_email = os.getenv("SENDER_EMAIL")
smtp_server = os.getenv("SMTP_SERVER")


def serialize_dataframe(df):
    return base64.b64encode(pickle.dumps(df)).decode() if df is not None else None


def deserialize_dataframe(data):
    return pickle.loads(base64.b64decode(data)) if data else None


# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 15),
    "retries": False,
    "retry_delay": timedelta(minutes=30),
}


def get_date_range():
    """Calculate date range for harvesting"""
    today = datetime.now().date()
    start = (today - timedelta(days=15)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    return start, end

start, end = get_date_range()

def get_execution_path():
    """Generate execution directory based on run timestamp."""
    context = get_current_context()
    execution_date = context["ts_nodash"]  # Include timestamp with hours and minutes
    base_dir = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data"
    execution_dir = base_dir / execution_date
    execution_dir.mkdir(parents=True, exist_ok=True)
    return execution_dir

with DAG(
    "infoscience_import_pipeline",
    default_args=default_args,
    description="A DAG to orchestrate the infoscience import data pipeline",
    schedule_interval="0 7 * * 1-5",
    catchup=False,
) as dag:

    @task()
    def harvest_data(source, start, end, queries):
        """harvest data from external source"""
        harvester_cls = WosHarvester if source == "wos" else ScopusHarvester
        harvester = harvester_cls(start, end, queries[source])
        df = harvester.harvest()
        file_path = get_execution_path() / f"Raw_{source.capitalize()}Publications.csv"
        if not df.empty:
            df.to_csv(file_path, index=False)
        return serialize_dataframe(df)

    @task()
    def deduplicate(wos_data, scopus_data):
        """Deduplicate items between sources"""
        df_wos, df_scopus = deserialize_dataframe(wos_data), deserialize_dataframe(scopus_data)
        df_combined = [df for df in [df_wos, df_scopus] if not df.empty]
        df_dedup = DataFrameProcessor(*df_combined).deduplicate_dataframes() if df_combined else pd.DataFrame()
        file_path = get_execution_path() / "SourcesDedupPublications.csv"
        if not df_dedup.empty:
            df_dedup.to_csv(file_path, index=False)
        return serialize_dataframe(df_dedup)

    @task()
    def process_deduplicated_data(deduplicated_data):
        """Check existing items in the institutional repositoy."""
        df = deserialize_dataframe(deduplicated_data)
        deduplicator = DataFrameProcessor(df)
        df_final, df_unloaded = deduplicator.deduplicate_infoscience(df)

        for name, df_data in {"InfoscienceDedupPublications": df_final, "UnloadedDuplicatesPublications": df_unloaded}.items():
            path = get_execution_path() / f"{name}.csv"
            if not df_data.empty:
                df_data.to_csv(path, index=False)

        return serialize_dataframe({"final": df_final, "unloaded": df_unloaded})

    @task()
    def process_metadata(filtered_data):
        """Split dataframe in two : metadata and authors."""
        result = deserialize_dataframe(filtered_data)
        df_final = result["final"]

        if df_final is None or (isinstance(df_final, pd.DataFrame) and df_final.empty):
            return serialize_dataframe({"metadata": None, "authors": None})

        deduplicator = DataFrameProcessor(df_final)
        df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)

        for name, df_data in {"Publications": df_metadata, "AuthorsAndAffiliations": df_authors}.items():
            path = get_execution_path() / f"{name}.csv"
            df_data.to_csv(path, index=False)

        return serialize_dataframe({"metadata": df_metadata, "authors": df_authors})

    @task()
    def extract_epfl_authors(metadata_authors_data):
        """Process authors and filter EPFL-related authors."""
        result = deserialize_dataframe(metadata_authors_data)
        df_authors = result["authors"]

        if df_authors is None or (isinstance(df_authors, pd.DataFrame) and df_authors.empty):
            return None

        df_epfl_authors = (
            AuthorProcessor(df_authors)
            .process()
            .filter_epfl_authors()
            .clean_authors()
            .nameparse_authors()
            .reconcile_authors(return_df=True)
        )
        epfl_authors_path = get_execution_path() / "EpflAuthors.csv"
        if not df_epfl_authors.empty:
            df_epfl_authors.to_csv(epfl_authors_path, index=False)
        return serialize_dataframe(df_epfl_authors)

    @task()
    def enrich_metadata(metadata_authors_data):
        """Enrich publications with Open Access metadata."""
        result = deserialize_dataframe(metadata_authors_data)
        df_metadata = result["metadata"]

        if df_metadata is None or (isinstance(df_metadata, pd.DataFrame) and df_metadata.empty):
            return None

        df_oa_metadata = PublicationProcessor(df_metadata).process(return_df=True)

        oa_metadata_path = get_execution_path() / "PublicationsWithOAMetadata.csv"

        if not df_oa_metadata.empty:
            df_oa_metadata.to_csv(oa_metadata_path, index=False)
        return serialize_dataframe(df_oa_metadata)

    @task()
    def load_data(oa_metadata_data, epfl_authors_data, metadata_authors_data):
        """Load enriched publication data into the system."""
        df_oa_metadata = deserialize_dataframe(oa_metadata_data)
        df_epfl_authors = deserialize_dataframe(epfl_authors_data)
        df_authors = deserialize_dataframe(metadata_authors_data)["authors"]

        df_loaded = Loader(
            df_oa_metadata, df_epfl_authors, df_authors
        ).create_complete_publication()

        loaded_path = get_execution_path() / "ImportedPublications.csv"
        if not df_loaded.empty:
            df_loaded.to_csv(loaded_path, index=False)
        return serialize_dataframe(df_loaded)

    @task()
    def generate_rejected_publications(oa_metadata_data, loaded_data):
        """Compute rejected publications."""
        df_oa_metadata = deserialize_dataframe(oa_metadata_data)
        df_loaded = deserialize_dataframe(loaded_data)

        df_rejected = (
            df_oa_metadata[~df_oa_metadata["row_id"].isin(df_loaded["row_id"])]
            if df_oa_metadata is not None and df_loaded is not None
            else pd.DataFrame()
        )

        rejected_path = get_execution_path() / "RejectedPublications.csv"

        if not df_rejected.empty:
            df_rejected.to_csv(rejected_path, index=False)

        return serialize_dataframe(df_rejected)

    @task()
    def generate_report(
        oa_metadata_data, filtered_data, epfl_authors_data, loaded_data
    ):
        """Generate and store a report summarizing the pipeline results."""
        df_oa_metadata = deserialize_dataframe(oa_metadata_data)
        result = deserialize_dataframe(filtered_data)
        df_unloaded = result["unloaded"]

        df_epfl_authors = deserialize_dataframe(epfl_authors_data)
        df_loaded = deserialize_dataframe(loaded_data)

        report_generator = GenerateReports(
            df_oa_metadata, df_unloaded, df_epfl_authors, df_loaded
        )
        report_path = report_generator.generate_excel_report(
            output_dir=get_execution_path()
        )

        if report_path:
            report_generator.send_report_by_email(
                recipient_email=recipient_email,
                sender_email=sender_email,
                smtp_server=smtp_server,
                import_start_date=start,
                import_end_date=end,
                file_path=report_path,
            )

        return str(report_path)

    wos_data = harvest_data("wos", start, end, default_queries)
    scopus_data = harvest_data("scopus", start, end, default_queries)
    deduplicated_data = deduplicate(wos_data, scopus_data)
    filtered_data = process_deduplicated_data(deduplicated_data)
    metadata_authors_data = process_metadata(filtered_data)
    epfl_authors_data = extract_epfl_authors(metadata_authors_data)
    oa_metadata_data = enrich_metadata(metadata_authors_data)
    loaded_data = load_data(oa_metadata_data, epfl_authors_data, metadata_authors_data)
    rejected_data = generate_rejected_publications(oa_metadata_data, loaded_data)
    report_file = generate_report(oa_metadata_data, filtered_data, epfl_authors_data, loaded_data)

    # Définition des dépendances
    [wos_data, scopus_data] >> deduplicated_data
    deduplicated_data >> filtered_data
    filtered_data >> metadata_authors_data
    metadata_authors_data >> [oa_metadata_data, epfl_authors_data]
    [oa_metadata_data, epfl_authors_data, metadata_authors_data] >> loaded_data
    loaded_data >> rejected_data
    [
        oa_metadata_data,
        filtered_data,
        epfl_authors_data,
        loaded_data,
    ] >> report_file
