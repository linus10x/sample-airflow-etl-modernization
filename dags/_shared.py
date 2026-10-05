"""Constants shared by the DAG files. Keep DAG definitions out of here: importing a module that
defines a DAG from another DAG file makes Airflow see the DAG twice."""

from airflow.sdk import Asset

RAW_RECORDS = Asset("postgres://postgres:5432/warehouse/raw/records")
