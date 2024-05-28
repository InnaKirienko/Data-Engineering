import os
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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


def process_and_upload_files(**kwargs):
    execution_date = kwargs['execution_date']
    if execution_date is None:
        raise AirflowException("Execution date is not set.")
    execution_date_str = execution_date.strftime("%Y-%m-%d")

    local_dir = str(os.path.join(BASE_DIR, 'raw', 'sales', execution_date_str))
    date = execution_date.date()
    year = date.year
    month = date.month
    day = date.day

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    # Convert JSON files to CSV and upload to GCS
    for root, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith('.json'):
                local_file_path = os.path.join(root, file)
                with open(local_file_path, 'r') as json_file:
                    data = json.load(json_file)
                df = pd.DataFrame(data)

                csv_buffer = BytesIO()
                df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)

                destination_path = f'src1/sales/v1/year={year}/month={month:02d}/day={day:02d}/{file.replace(".json", ".csv")}'
                gcs_hook.upload(
                    bucket_name=BUCKET_NAME,
                    object_name=destination_path,
                    data=csv_buffer.getvalue(),
                    mime_type='text/csv'
                )
                logging.info(f"File {local_file_path} uploaded to {destination_path}")


start = DummyOperator(task_id="start", dag=dag)

process_and_upload_files_task = PythonOperator(
    task_id='process_and_upload_files',
    python_callable=process_and_upload_files,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> process_and_upload_files_task >> end
