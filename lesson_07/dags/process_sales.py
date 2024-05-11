import os
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['in.kirienko@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 30,
    }

dag = DAG(
    dag_id='process_sales',
    description='Process sales data',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval='0 1 * * *',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
)


def set_execution_date(**kwargs):
    execution_date = kwargs['execution_date']
    if execution_date is None:
        raise AirflowException("Execution date is not set.")

    execution_date_str = execution_date.strftime("%Y-%m-%d")
    kwargs['ti'].xcom_push(key='execution_date', value=execution_date_str)
    return execution_date_str


def generate_raw_dir(execution_date, **kwargs):
    base_dir = Variable.get("base_dir")
    execution_date_str = execution_date.strftime("%Y-%m-%d")
    raw_dir = os.path.join(base_dir, "raw", "sales", execution_date_str)
    kwargs['ti'].xcom_push(key='raw_dir', value=raw_dir)
    return raw_dir


def generate_stg_dir(execution_date, **kwargs):
    base_dir = Variable.get("base_dir")
    execution_date_str = execution_date.strftime("%Y-%m-%d")
    stg_dir = os.path.join(base_dir, "stg", "sales", execution_date_str)
    kwargs['ti'].xcom_push(key='stg_dir', value=stg_dir)
    return stg_dir


with (dag):

    task_set_execution_date = PythonOperator(
        task_id='task_set_execution_date',
        python_callable=set_execution_date,
        provide_context=True,
    )

    task_set_raw_dir = PythonOperator(
        task_id='task_set_raw_dir',
        python_callable=generate_raw_dir,
        provide_context=True,
    )

    task_set_stg_dir = PythonOperator(
        task_id='task_set_stg_dir',
        python_callable=generate_stg_dir,
        provide_context=True,
        dag=dag,
    )

    task_extract_data_from_api = HttpOperator(
        task_id='extract_data_from_api',
        method='POST',
        http_conn_id='http_docker_internal_1',
        data=json.dumps({"date": "{{ ti.xcom_pull(task_ids='task_set_execution_date') }}",
                        "raw_dir": "{{ ti.xcom_pull(task_ids='task_set_raw_dir') }}"}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 201,
    )

    task_convert_to_avro = HttpOperator(
        task_id='convert_to_avro',
        method='POST',
        http_conn_id='http_docker_internal_2',
        data=json.dumps({"raw_dir": "{{ ti.xcom_pull(task_ids='task_set_raw_dir') }}",
                        "stg_dir": "{{ ti.xcom_pull(task_ids='task_set_stg_dir') }}"}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 201,
    )

    (task_set_execution_date >>
     task_set_raw_dir >>
     task_set_stg_dir >>
     task_extract_data_from_api >>
     task_convert_to_avro)
