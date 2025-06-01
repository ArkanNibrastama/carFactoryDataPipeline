from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from load_gsheet_data import gsheet_to_minio

default_args = {
    'owner': 'Muhammad Arkan Nibrastama',
}

with DAG(
    dag_id='gsheet_to_minio',
    default_args=default_args,
    schedule_interval='1 0 * * *',
    start_date = datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    load_data_from_gsheet = PythonOperator(
        task_id='load_data_from_gsheet',
        python_callable=gsheet_to_minio
    )

    load_data_from_gsheet