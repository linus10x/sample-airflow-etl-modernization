# Portfolio card text

Reference copy of the exact card text. It must match the manifest in the hub entry.

Title: Sample: Airflow ETL modernization, vendor JSON on S3 to Postgres + dbt

Role: Architect and lead developer (illustrative sample on synthetic data)

Description: Sample on synthetic retail data. A Python script that turned vendor JSON on S3 into CSVs is rebuilt as an Airflow pipeline into Postgres with dbt models. Each file is checked against a data contract on arrival, so a bad file goes to quarantine with a reason instead of breaking the load. A SKU crosswalk ties three vendors' product codes back to orders. Loads are idempotent and re-runnable by date, a parity test shows the new tables match the old output, and CI runs the whole pipeline in Docker.

Skills: Apache Airflow, Python, PostgreSQL, dbt, ETL Pipeline

Text sha8: 5f050692
