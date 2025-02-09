# Use the official Apache Airflow image
FROM apache/airflow:latest-python3.11

# Set Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root user (needed for installation and file setup)
USER root

# Create required directories
RUN mkdir -p $AIRFLOW_HOME/dags \
    $AIRFLOW_HOME/data_pipeline \
    $AIRFLOW_HOME/clients \
    $AIRFLOW_HOME/dspace \
    $AIRFLOW_HOME/config \
    $AIRFLOW_HOME/logs \
    $AIRFLOW_HOME/data \
    $AIRFLOW_HOME/scripts

# Set correct permissions (use root group instead of airflow group)
RUN chown -R airflow:root $AIRFLOW_HOME

# Copy Airflow files
COPY dags/ $AIRFLOW_HOME/dags/
COPY data_pipeline/ $AIRFLOW_HOME/data_pipeline/
COPY clients/ $AIRFLOW_HOME/clients/
COPY dspace/ $AIRFLOW_HOME/dspace/
COPY config.py $AIRFLOW_HOME/config.py
COPY mappings.py $AIRFLOW_HOME/mappings.py
COPY utils.py $AIRFLOW_HOME/utils.py
COPY .env $AIRFLOW_HOME/.env

# Switch to the existing airflow user before installing dependencies
USER airflow
COPY requirements.txt $AIRFLOW_HOME/requirements.txt
RUN pip install --no-cache-dir -r $AIRFLOW_HOME/requirements.txt
