from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
import os
import pandas as pd
from pathlib import Path
import pickle
import base64


# Import project-specific modules
from config import default_queries
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader
from data_pipeline.reporting import GenerateReports
from data_pipeline.harvester import WosHarvester, ScopusHarvester


def serialize_dataframe(df):
    return base64.b64encode(pickle.dumps(df)).decode() if df is not None else None


def deserialize_dataframe(data):
    return pickle.loads(base64.b64decode(data)) if data else None


# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_execution_path():
    """Generate execution directory based on run timestamp."""
    context = get_current_context()
    execution_date = context["ts_nodash"]  # Include timestamp with hours and minutes
    base_dir = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data"
    execution_dir = base_dir / execution_date
    execution_dir.mkdir(parents=True, exist_ok=True)
    return execution_dir


with DAG(
    "publication_data_pipeline",
    default_args=default_args,
    description="A DAG to orchestrate the infoscience import data pipeline",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    @task()
    def harvest_wos(start: str, end: str, queries: dict):
        """Harvest publications from WoS and save as CSV."""
        harvester = WosHarvester(start, end, queries["wos"])
        df_wos = harvester.harvest()
        file_path = get_execution_path() / "Raw_WosPublications.csv"
        if not df_wos.empty:
            df_wos.to_csv(file_path, index=False)
        return serialize_dataframe(df_wos)

    @task()
    def harvest_scopus(start: str, end: str, queries: dict):
        """Harvest publications from Scopus and save as CSV."""
        harvester = ScopusHarvester(start, end, queries["scopus"])
        df_scopus = harvester.harvest()
        file_path = get_execution_path() / "Raw_ScopusPublications.csv"
        if not df_scopus.empty:
            df_scopus.to_csv(file_path, index=False)
        return serialize_dataframe(df_scopus)

    @task()
    def deduplicate(wos_data, scopus_data):
        """Deduplicate publications from harvested data files."""
        df_wos = deserialize_dataframe(wos_data)
        df_scopus = deserialize_dataframe(scopus_data)

        dfs = [df for df in [df_wos, df_scopus] if not df.empty]
        df_deduplicated = (
            DataFrameProcessor(*dfs).deduplicate_dataframes() if dfs else pd.DataFrame()
        )
        file_path = get_execution_path() / "DeduplicatedPublications.csv"
        if not df_deduplicated.empty:
            df_deduplicated.to_csv(file_path, index=False)
        return serialize_dataframe(df_deduplicated)

    @task()
    def deduplicate_infoscience_final(deduplicated_data):
        df_deduplicated = deserialize_dataframe(deduplicated_data)
        deduplicator = DataFrameProcessor(df_deduplicated)
        df_final, df_unloaded = deduplicator.deduplicate_infoscience(df_deduplicated)

        final_path = get_execution_path() / "FinalDeduplicatedPublications.csv"
        if not df_final.empty:
            df_final.to_csv(final_path, index=False)

        unloaded_path = get_execution_path() / "UnloadedPublications.csv"
        if not df_unloaded.empty:
            df_unloaded.to_csv(unloaded_path, index=False)

        return serialize_dataframe({"final": df_final, "unloaded": df_unloaded})

    @task()
    def process_metadata_authors(final_unloaded_data):
        """Generate metadata file from final deduplicated publications and return the DataFrame."""
        result = deserialize_dataframe(final_unloaded_data)
        df_final = result["final"]

        if df_final is None or df_final.empty:
            return serialize_dataframe({"metadata": None, "authors": None})

        deduplicator = DataFrameProcessor(df_final)
        df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)

        # Définir le chemin du fichier de sortie
        metadata_path = get_execution_path() / "Publications.csv"
        df_metadata.to_csv(metadata_path, index=False)

        authors_path = get_execution_path() / "AuthorsAndAffiliations.csv"
        df_authors.to_csv(authors_path, index=False)

        return serialize_dataframe({"metadata": df_metadata, "authors": df_authors})

    @task()
    def extract_epfl_authors(metadata_authors_data):
        """Process authors and filter EPFL-related authors."""
        result = deserialize_dataframe(metadata_authors_data)
        df_authors = result["authors"]
        if df_authors is None or df_authors.empty:
            return None

        df_epfl_authors = (
            AuthorProcessor(df_authors)
            .process()
            .filter_epfl_authors()
            .clean_authors()
            .nameparse_authors()
            .api_epfl_reconciliation()
            .generate_dspace_uuid(return_df=True)
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

        if df_metadata is None or df_metadata.empty:
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
        result = deserialize_dataframe(metadata_authors_data)
        df_authors = result["authors"]

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
        oa_metadata_data, unloaded_data, epfl_authors_data, loaded_data
    ):
        """Generate and store a report summarizing the pipeline results."""
        df_oa_metadata = deserialize_dataframe(oa_metadata_data)
        df_unloaded = deserialize_dataframe(unloaded_data)
        df_epfl_authors = deserialize_dataframe(epfl_authors_data)
        df_loaded = deserialize_dataframe(loaded_data)

        report_path = GenerateReports(
            df_oa_metadata, df_unloaded, df_epfl_authors, df_loaded
        ).generate_excel_report(output_dir=get_execution_path())

        return str(report_path)

    wos_data = harvest_wos("2025-02-10", "2025-02-11", default_queries)
    scopus_data = harvest_scopus("2025-02-10", "2025-02-11", default_queries)

    deduplicated_data = deduplicate(wos_data, scopus_data)
    final_unloaded_data = deduplicate_infoscience_final(deduplicated_data)

    metadata_authors_data = process_metadata_authors(final_unloaded_data)

    epfl_authors_data = extract_epfl_authors(metadata_authors_data)
    oa_metadata_data = enrich_metadata(metadata_authors_data)

    loaded_data = load_data(oa_metadata_data, epfl_authors_data, metadata_authors_data)

    rejected_data = generate_rejected_publications(oa_metadata_data, loaded_data)

    report_file = generate_report(
        oa_metadata_data, final_unloaded_data, epfl_authors_data, loaded_data
    )

    # Définition des dépendances
    [wos_data, scopus_data] >> deduplicated_data
    deduplicated_data >> final_unloaded_data
    final_unloaded_data >> metadata_authors_data
    metadata_authors_data >> [oa_metadata_data, epfl_authors_data]
    [oa_metadata_data, epfl_authors_data, metadata_authors_data] >> loaded_data
    loaded_data >> rejected_data
    [
        oa_metadata_data,
        final_unloaded_data,
        epfl_authors_data,
        loaded_data,
    ] >> report_file
