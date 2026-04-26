FROM apache/airflow:2.10.0

# Chuyển sang quyền root để cài đặt thư viện hệ thống
USER root
RUN apt-get update && apt-get install -y git

# Chuyển lại quyền user airflow để cài python packages
USER airflow

# Cài đặt dbt và adapter cho BigQuery
RUN pip install --no-cache-dir dbt-bigquery