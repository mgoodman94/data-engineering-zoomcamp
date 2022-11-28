""" LOCAL INGEST DAG """
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract import extract_taxi_data
from scripts.load import main


local_workflow = DAG(
    dag_id="LocalIngestionDAG",
    description="Local Taxi Data Ingestion in Postgres",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 11, 28),
    catchup=False
)

with local_workflow:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_taxi_data
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=main,
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

    extract_task >> load_task
