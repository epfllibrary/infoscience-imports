"""DAG to execute the pipeline with dynamic date parameters."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_pipeline.main import main


def execute_pipeline(**kwargs):
    """
    This function retrieves the execution date from Airflow, calculates the start and end dates
    (10 days before execution date until execution date), and runs the main pipeline.
    """
    execution_date = kwargs["ds"]  # Retrieve the DAG execution date
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

    start = (execution_date - timedelta(days=10)).strftime(
        "%Y-%m-%d"
    )  # Calculate start date (10 days before)
    end = execution_date.strftime("%Y-%m-%d")  # Set end date to execution date
    queries = kwargs.get("queries", None)

    return main(
        start_date=start,
        end_date=end,
        queries=queries,
    )

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",  # Owner of the DAG
    "depends_on_past": False,  # DAG runs independently of past runs
    "start_date": datetime(2025, 2, 1),  # DAG start date
    "email_on_failure": True,  # Disable failure email notifications
    "email_on_retry": True,  # Disable retry email notifications
    "retries": 1,  # Number of retries on failure
    "retry_delay": timedelta(minutes=15),  # Delay before retrying a failed task
}

# Initialize the DAG
dag = DAG(
    "infoscience_import_dag",  # DAG name
    default_args=default_args,
    description="DAG to run Main Infoscience Import Pipeline",  # Description of the DAG
    schedule_interval="@daily",  # Run daily
    catchup=False,  # Do not run historical DAG executions
)

# Define the main task to execute the pipeline
task_execute_pipeline = PythonOperator(
    task_id="execute_pipeline",  # Task ID
    python_callable=execute_pipeline,  # Function to call
    provide_context=True,  # Provide Airflow execution context
    dag=dag,  # Assign the DAG
)

# Set task execution order
task_execute_pipeline
