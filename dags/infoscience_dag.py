from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
import os
import pandas as pd
from pathlib import Path
import json


# Import project-specific modules
from config import default_queries
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader
from data_pipeline.reporting import GenerateReports
from data_pipeline.harvester import WosHarvester, ScopusHarvester

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
        df = harvester.harvest()
        file_path = get_execution_path() / "Raw_WosPublications.csv"
        if not df.empty:
            df.to_csv(file_path, index=False)
        return str(file_path)

    @task()
    def harvest_scopus(start: str, end: str, queries: dict):
        """Harvest publications from Scopus and save as CSV."""
        harvester = ScopusHarvester(start, end, queries["scopus"])
        df = harvester.harvest()
        file_path = get_execution_path() / "Raw_ScopusPublications.csv"
        if not df.empty:
            df.to_csv(file_path, index=False)
        return str(file_path)

    @task()
    def deduplicate(wos_file, scopus_file):
        """Deduplicate publications from harvested data files."""
        dfs = [
            pd.read_csv(file) for file in [wos_file, scopus_file] if Path(file).exists()
        ]
        df_deduplicated = (
            DataFrameProcessor(*dfs).deduplicate_dataframes() if dfs else pd.DataFrame()
        )
        file_path = get_execution_path() / "DeduplicatedPublications.csv"
        if not df_deduplicated.empty:
            df_deduplicated.to_csv(file_path, index=False)
        return str(file_path)

    @task()
    def deduplicate_infoscience_final(deduplicated_file):
        """Generate final deduplicated publications file."""
        if not Path(deduplicated_file).exists():
            return None
        df_deduplicated = pd.read_csv(deduplicated_file)
        deduplicator = DataFrameProcessor(df_deduplicated)
        df_final, _ = deduplicator.deduplicate_infoscience(df_deduplicated)

        final_path = get_execution_path() / "FinalDeduplicatedPublications.csv"
        if not df_final.empty:
            df_final.to_csv(final_path, index=False)

        return df_final

    @task()
    def deduplicate_infoscience_unloaded(deduplicated_file):
        """Generate unloaded publications file."""
        if not Path(deduplicated_file).exists():
            return None
        df_deduplicated = pd.read_csv(deduplicated_file)
        deduplicator = DataFrameProcessor(df_deduplicated)
        _, df_unloaded = deduplicator.deduplicate_infoscience(df_deduplicated)

        unloaded_path = get_execution_path() / "UnloadedPublications.csv"
        if not df_unloaded.empty:
            df_unloaded.to_csv(unloaded_path, index=False)

        return str(unloaded_path)

    @task()
    def process_metadata(df_final):
        """Generate metadata file from final deduplicated publications and return the DataFrame."""
        if df_final is None or df_final.empty:
            return None

        deduplicator = DataFrameProcessor(df_final)
        df_metadata, _ = deduplicator.generate_main_dataframes(df_final)

        if df_metadata.empty:
            return None

        # Définir le chemin du fichier de sortie
        metadata_path = get_execution_path() / "Publications.csv"

        # Sauvegarder en CSV
        df_metadata.to_csv(metadata_path, index=False)

        return df_metadata  # Retourner le DataFrame


    @task()
    def process_authors(df_final):
        """Generate authors file from final deduplicated publications and return the DataFrame."""
        if df_final is None or df_final.empty:
            return None

        deduplicator = DataFrameProcessor(df_final)
        _, df_authors = deduplicator.generate_main_dataframes(df_final)

        if df_authors.empty:
            return None

        # Définir le chemin du fichier de sortie
        authors_path = get_execution_path() / "AuthorsAndAffiliations.csv"

        # Sauvegarder en CSV
        df_authors.to_csv(authors_path, index=False)

        return df_authors  # Retourner le DataFrame


    @task()
    def extract_epfl_authors(authors_file):
        """Process authors and filter EPFL-related authors."""
        if not Path(authors_file).exists():
            return None
        df_authors = pd.read_csv(authors_file)
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
        return str(epfl_authors_path)

    @task()
    def enrich_metadata(metadata_file):
        """Enrich publications with Open Access metadata."""
        if not Path(metadata_file).exists():
            return None
        df_metadata = pd.read_csv(metadata_file)
        df_oa_metadata = PublicationProcessor(df_metadata).process(return_df=True)
        oa_metadata_path = get_execution_path() / "PublicationsWithOAMetadata.csv"
        if not df_oa_metadata.empty:
            df_oa_metadata.to_csv(oa_metadata_path, index=False)
        return str(oa_metadata_path)

    @task()
    def load_data(oa_metadata_file, epfl_authors_file, authors_file):
        """Load enriched publication data into the system."""
        if not Path(oa_metadata_file).exists():
            return None
        df_oa_metadata = pd.read_csv(oa_metadata_file)
        df_epfl_authors = (
            pd.read_csv(epfl_authors_file)
            if Path(epfl_authors_file).exists()
            else pd.DataFrame()
        )
        df_authors = (
            pd.read_csv(authors_file) if Path(authors_file).exists() else pd.DataFrame()
        )
        df_loaded = Loader(
            df_oa_metadata, df_epfl_authors, df_authors
        ).create_complete_publication()
        loaded_path = get_execution_path() / "ImportedPublications.csv"
        if not df_loaded.empty:
            df_loaded.to_csv(loaded_path, index=False)
        return str(loaded_path)

    @task()
    def generate_rejected_publications(oa_metadata_file, loaded_file):
        """Compute rejected publications."""
        if not Path(oa_metadata_file).exists() or not Path(loaded_file).exists():
            return None

        df_oa_metadata = pd.read_csv(oa_metadata_file)
        df_loaded = pd.read_csv(loaded_file)

        df_rejected = df_oa_metadata[~df_oa_metadata["row_id"].isin(df_loaded["row_id"])]
        rejected_path = get_execution_path() / "RejectedPublications.csv"

        if not df_rejected.empty:
            df_rejected.to_csv(rejected_path, index=False)

        return str(rejected_path)

    @task()
    def generate_report(
        oa_metadata_file, unloaded_file, epfl_authors_file, loaded_file
    ):
        """Generate and store a report summarizing the pipeline results."""
        report_path = GenerateReports(
            (
                pd.read_csv(oa_metadata_file)
                if Path(oa_metadata_file).exists()
                else pd.DataFrame()
            ),
            (
                pd.read_csv(unloaded_file)
                if Path(unloaded_file).exists()
                else pd.DataFrame()
            ),
            (
                pd.read_csv(epfl_authors_file)
                if Path(epfl_authors_file).exists()
                else pd.DataFrame()
            ),
            pd.read_csv(loaded_file) if Path(loaded_file).exists() else pd.DataFrame(),
        ).generate_excel_report(output_dir=get_execution_path())
        return str(report_path)

    wos_file = harvest_wos("2025-02-07", "2025-02-09", default_queries)
    scopus_file = harvest_scopus("2025-02-01", "2025-02-09", default_queries)

    deduplicated_file = deduplicate(wos_file, scopus_file)
    final_file = deduplicate_infoscience_final(deduplicated_file)
    unloaded_file = deduplicate_infoscience_unloaded(deduplicated_file)

    metadata_file = process_metadata(final_file)
    authors_file = process_authors(final_file)

    epfl_authors_file = extract_epfl_authors(authors_file)
    oa_metadata_file = enrich_metadata(metadata_file)

    loaded_file = load_data(oa_metadata_file, epfl_authors_file, authors_file)

    rejected_file = generate_rejected_publications(oa_metadata_file, loaded_file)

    report_file = generate_report(
            oa_metadata_file, unloaded_file, epfl_authors_file, loaded_file
        )

    # Define task dependencies

    # Step 1: Harvest raw data
    [wos_file, scopus_file] >> deduplicated_file

    # Step 2: Deduplicate publications
    deduplicated_file >> [final_file, unloaded_file]

    # Step 3: Process metadata and authors
    final_file >> [metadata_file, authors_file]

    # Step 4: Enrich data
    metadata_file >> oa_metadata_file
    authors_file >> epfl_authors_file

    # Step 5: Load final data
    [oa_metadata_file, epfl_authors_file] >> loaded_file

    # Step 6: Compute rejected publications
    loaded_file >> rejected_file

    # Step 7: Generate report
    [oa_metadata_file, unloaded_file, epfl_authors_file, loaded_file] >> report_file
