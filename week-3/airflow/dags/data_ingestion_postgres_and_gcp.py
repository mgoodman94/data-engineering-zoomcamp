""" POSTGRES & GCP INGEST DAG """
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract import extract_taxi_data
from scripts.load_to_postgres import ingest_postgres
from scripts.load_to_gcp import ingest_gcp


local_workflow = DAG(
    dag_id="LocalIngestionDAG",
    description="Local Taxi Data Ingestion in Postgres",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 11, 28),
    catchup=False
)

with local_workflow:
    extract = PythonOperator(
        task_id='extract',
        retries=3,
        retry_delay=timedelta(seconds=5),
        python_callable=extract_taxi_data
    )

    load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=ingest_postgres,
        op_kwargs=dict(
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            host=os.getenv('PG_HOST'),
            port=os.getenv('PG_PORT'),
            db=os.getenv('PG_DATABASE'),
            table_name='ny_taxi',
            file=os.getenv('OUTPUT_PATH')
        )
    )

    load_to_gcp = PythonOperator(
        task_id='load_to_gcp',
        python_callable=ingest_gcp,
        op_kwargs=dict(
            file=os.getenv('OUTPUT_PATH'),
            bucket=os.getenv('BUCKET_NAME'),
            dataset_id=os.getenv("DATASET_ID")
        )
    )

    extract >> load_to_postgres >> load_to_gcp
