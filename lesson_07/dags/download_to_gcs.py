import os
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup
import logging

BASE_DIR = Variable.get("base_dir_airflow")
BUCKET_NAME = 'lesson_10_bucket'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['in.kirienko@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='download_files_to_gcs',
    schedule_interval='0 1 * * *',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11),
    default_args=DEFAULT_ARGS,
    catchup=True,
)


def convert_json_to_csv(local_file_path, csv_file_path):
    with open(local_file_path, 'r') as json_file:
        data = json.load(json_file)
    df = pd.DataFrame(data)
    df.to_csv(csv_file_path, index=False)


def process_files(**kwargs):
    logical_date = kwargs['logical_date']
    execution_date_str = logical_date.strftime('%Y-%m-%d')
    local_dir = str(os.path.join(BASE_DIR, 'raw', 'sales', execution_date_str))
    csv_files = []

    for root, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith('.json') and execution_date_str in file:
                local_file_path = os.path.join(root, file)
                csv_file_path = local_file_path.replace('.json', '.csv')
                convert_json_to_csv(local_file_path, csv_file_path)
                csv_files.append(csv_file_path)

    ti = kwargs['ti']
    ti.xcom_push(key='csv_files', value=csv_files)
    logging.info(f"CSV files pushed to XCom: {csv_files}")


def upload_files_to_gcs(**kwargs):
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(task_ids='process_files', key='csv_files')
    logical_date = kwargs['logical_date']

    logging.info(f'CSV Files to upload: {csv_files}')
    logging.info(f'Logical Date: {logical_date}')

    if not csv_files:
        logging.warning('No CSV files found.')
        return

    for csv_file_path in csv_files:
        gcs_object_name = os.path.join(
            f'src1/sales/v1/year={logical_date.year}/month={logical_date.strftime("%m")}/day={logical_date.strftime("%d")}/',
            os.path.basename(csv_file_path)
        )

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{os.path.basename(csv_file_path).replace(".csv", "")}',
            src=csv_file_path,
            dst=gcs_object_name,
            bucket=BUCKET_NAME,
            gcp_conn_id='g_cloud_conn',
            dag=dag
        )
        upload_task.execute(context=kwargs)


start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag,
)

upload_files_task = PythonOperator(
    task_id='upload_files_to_gcs',
    python_callable=upload_files_to_gcs,
    provide_context=True,
    dag=dag,
)

start >> process_files_task >> upload_files_task >> end
