import os
import json
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python_operator import PythonOperator
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
    'retry_delay': 30,
    'start_date': datetime(2022, 8, 9),
    'end_date': datetime(2022, 8, 11),
    'catchup': True,
}

dag = DAG(
    dag_id='download_files_to_gcs',
    schedule_interval='0 1 * * *',
    default_args=DEFAULT_ARGS,
)


start = DummyOperator(task_id="start", dag=dag)


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


process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag,
)


class CustomLocalFilesystemToGCSOperator(LocalFilesystemToGCSOperator):
    template_fields = ('src', 'dst', 'bucket')

    def execute(self, context):
        ti = context['ti']
        csv_files = ti.xcom_pull(task_ids='process_files', key='csv_files')
        self.src = csv_files
        super().execute(context)


upload_files_to_gcs_task = CustomLocalFilesystemToGCSOperator(
    task_id="upload_files_to_gcs",
    src="",
    dst='src1/sales/v1/year={{ execution_date.strftime("%Y") }}/month={{ execution_date.strftime("%m") }}/day={{ execution_date.strftime("%d") }}/',
    bucket=BUCKET_NAME,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> process_files_task >> upload_files_to_gcs_task >> end
