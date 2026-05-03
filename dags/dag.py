from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_DIR = "/opt/airflow/project/jobs"
POSTGRES_JAR = "/opt/airflow/jars/postgresql-42.7.2.jar"

default_args = {
    'owner': 'khanhdo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='chotot_used_car_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for Chotot used car data',
    schedule_interval='0 2 * * *', # Auto run at 2 AM every day
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['used_car', 'etl', 'pyspark'],
) as dag:

    # Task 1: Run Python bot to crawl new data
    task_crawl_bronze = BashOperator(
        task_id='crawl_chotot_data',
        bash_command=f'cd {PROJECT_DIR} && python3 ingest_bronze.py',
    )

    # Task 2: Run PySpark to process CDC and push to Delta Lake
    task_clean_silver = BashOperator(
        task_id='clean_data',
        bash_command=f'cd {PROJECT_DIR} && spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.2 clean_data_silver.py',
    )

    # Task 3: Run PySpark to compute Business Logic and overwrite to PostgreSQL
    task_aggregate_gold = BashOperator(
        task_id='aggregate_data',
        bash_command=f'cd {PROJECT_DIR} && spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.2 aggregate_data_gold.py',
    )

    task_crawl_bronze >> task_clean_silver >> task_aggregate_gold