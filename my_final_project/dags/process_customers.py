from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.operators.python import PythonOperator
#from airflow.utils.dates import days_ago
from datetime import datetime

# DAG arguments
PROJECT_ID = 'de-07-inna-kirienko'
LOCATION = 'us'
BUCKET = 'final_project_raw_bucket'
BRONZE_DATASET = 'bronze'
SILVER_DATASET = 'silver'
GCS_PREFIX = 'data/customers/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 9, 1),
    'end_date': datetime(2022, 9, 1),
    'schedule_interval': '0 1 * * *',
    'catchup': True,
}


# Function to create source objects for BigQuery external table
def create_source_objects(task_instance):
    files = task_instance.xcom_pull(task_ids='list_gcs_files')
    source_objects = [f'{file}' for file in files if file.endswith('.csv')]
    return source_objects


# Define the DAG
with DAG(
        'process_customers_dag',
        default_args=default_args,
        description='Pipeline for processing customers data',
        render_template_as_native_obj=True,
) as dag:
    # Task: List all files in GCS
    list_gcs_files = GCSListObjectsOperator(
        task_id='list_gcs_files',
        bucket=BUCKET,
        prefix=GCS_PREFIX
    )

    # Task: Create bronze table with external data
    create_bronze_table = BigQueryCreateExternalTableOperator(
        task_id='create_bronze_table',
        destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.customers',
        bucket=BUCKET,
        source_objects="{{ task_instance.xcom_pull(task_ids='create_source_objects') }}",
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],

        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=','
    )

    # Task: Transform and load data into silver
    transform_and_load_silver = BigQueryExecuteQueryOperator(
        task_id='transform_and_load_silver',
        sql='''
        CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.silver_dataset }}.customers`
        AS
        SELECT DISTINCT
            Id AS client_id,
            FirstName AS first_name,
            LastName AS last_name,
            Email AS email,
            CASE
                WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-\d{2}-\d{2}$') THEN
                    PARSE_DATE('%Y-%m-%d', RegistrationDate)
                WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-\d{2}-\d{1}$') THEN
                    PARSE_DATE('%Y-%m-%e', RegistrationDate)
                ELSE
                    NULL
            END AS registration_date,
            State AS state
        FROM
            `{{ params.project_id }}.{{ params.bronze_dataset }}.customers`;
        ''',
        use_legacy_sql=False,
        params={
            'project_id': PROJECT_ID,
            'bronze_dataset': BRONZE_DATASET,
            'silver_dataset': SILVER_DATASET
        }
    )


    # Task: Create source objects for BigQuery external table
    create_source_objects_task = PythonOperator(
        task_id='create_source_objects',
        python_callable=create_source_objects,
        provide_context=True
    )

    # Set task dependencies
    list_gcs_files >> create_source_objects_task >> create_bronze_table >> transform_and_load_silver

