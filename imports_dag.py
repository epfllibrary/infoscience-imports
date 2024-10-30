from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data_pipeline.harvester import WosHarvester, ScopusHarvester
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader

def harvest_wos(**kwargs):
    wos_harvester = WosHarvester(kwargs['start_date'], kwargs['end_date'], kwargs['queries']['wos'])
    return wos_harvester.harvest()

def harvest_scopus(**kwargs):
    scopus_harvester = ScopusHarvester(kwargs['start_date'], kwargs['end_date'], kwargs['queries']['scopus'])
    return scopus_harvester.harvest()

def deduplicate_dataframes(**kwargs):
    wos_publications = kwargs['ti'].xcom_pull(task_ids='harvest_wos')
    scopus_publications = kwargs['ti'].xcom_pull(task_ids='harvest_scopus')
    deduplicator = DataFrameProcessor(wos_publications, scopus_publications)
    return deduplicator.deduplicate_dataframes()

def deduplicate_infoscience(**kwargs):
    deduplicated_sources_df = kwargs['ti'].xcom_pull(task_ids='deduplicate_dataframes')
    deduplicator = DataFrameProcessor()  # Initialize with necessary parameters
    return deduplicator.deduplicate_infoscience(deduplicated_sources_df)

def generate_main_dataframes(**kwargs):
    df_final, df_unloaded = kwargs['ti'].xcom_pull(task_ids='deduplicate_infoscience')
    deduplicator = DataFrameProcessor()  # Initialize with necessary parameters
    return deduplicator.generate_main_dataframes(df_final)

def process_authors(**kwargs):
    df_authors = kwargs['ti'].xcom_pull(task_ids='generate_main_dataframes')
    author_processor = AuthorProcessor(df_authors)
    return (author_processor.process()
            .filter_epfl_authors()
            .clean_authors()
            .nameparse_authors()
            .api_epfl_reconciliation()
            .generate_dspace_uuid(return_df=True))

def process_publications(**kwargs):
    df_metadata = kwargs['ti'].xcom_pull(task_ids='generate_main_dataframes')
    publication_processor = PublicationProcessor(df_metadata)
    return publication_processor.process(return_df=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email': ['geraldine.geoffroy@epfl.ch'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': "0 0 * * *"  # Corrected to schedule_interval
}

default_queries = {
        "wos": "OG=(Ecole Polytechnique Federale de Lausanne)",
        "scopus": "AF-ID(60028186) OR AF-ID(60210159) OR AF-ID(60070536) OR AF-ID(60204330) OR AF-ID(60070531) OR AF-ID(60070534) OR AF-ID(60070538) OR AF-ID(60014951) OR AF-ID(60070529) OR AF-ID(60070532) OR AF-ID(60070535) OR AF-ID(60122563) OR AF-ID(60210160) OR AF-ID(60204331)",
        "openalex": "OPENALEX_QUERY_HERE",  # Placeholder for OpenAlex query
        "zenodo": "ZENODO_QUERY_HERE"      # Placeholder for Zenodo query
    }

with DAG(dag_id='data_pipeline_dag', default_args=default_args, schedule_interval="0 0 * * *") as dag:
    
    harvest_wos_task = PythonOperator(
        task_id='harvest_wos',
        python_callable=harvest_wos,
        op_kwargs={'start_date': '2024-08-01', 'end_date': '2024-08-02', 'queries': default_queries["wos"]},
    )

    harvest_scopus_task = PythonOperator(
        task_id='harvest_scopus',
        python_callable=harvest_scopus,
        op_kwargs={'start_date': '2024-08-01', 'end_date': '2024-08-02', 'queries': default_queries["scopus"]},
    )

    deduplicate_dataframes_task = PythonOperator(
        task_id='deduplicate_dataframes',
        python_callable=deduplicate_dataframes,
    )

    deduplicate_infoscience_task = PythonOperator(
        task_id='deduplicate_infoscience',
        python_callable=deduplicate_infoscience,
    )

    generate_main_dataframes_task = PythonOperator(
        task_id='generate_main_dataframes',
        python_callable=generate_main_dataframes,
    )

    process_authors_task = PythonOperator(
        task_id='process_authors',
        python_callable=process_authors,
    )

    process_publications_task = PythonOperator(
        task_id='process_publications',
        python_callable=process_publications,
    )

    # Set task dependencies
    harvest_wos_task >> deduplicate_dataframes_task
    harvest_scopus_task >> deduplicate_dataframes_task
    deduplicate_dataframes_task >> deduplicate_infoscience_task
    deduplicate_infoscience_task >> generate_main_dataframes_task
    generate_main_dataframes_task >> process_authors_task
    generate_main_dataframes_task >> process_publications_task