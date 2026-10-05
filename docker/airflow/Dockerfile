FROM apache/airflow:3.3.2-python3.12

USER airflow

# dbt lives in its own venv so its dependency pins cannot disturb Airflow's constraints.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-postgres==1.11.0

# The pipeline package needs a Postgres driver Airflow does not ship. boto3 and pydantic come
# with the base image. pytest is here so DAG integrity tests run in the same image as the DAGs.
RUN pip install --no-cache-dir "psycopg[binary]>=3.2" "pytest>=8"

ENV PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project
