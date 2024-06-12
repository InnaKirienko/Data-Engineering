from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryExecuteQueryOperator
)
from datetime import datetime

PROJECT_ID = 'de-07-inna-kirienko'
LOCATION = 'us'
BUCKET = 'final_project_raw_bucket'
BRONZE_DATASET = 'bronze'
SILVER_DATASET = 'silver'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 1),
    'catchup': False,
}


with DAG(
        'process_user_profiles_dag',
        default_args=default_args,
        description="Pipeline for processing user's profiles data",
        schedule_interval=None,
        render_template_as_native_obj=True,
) as dag:

    # Task: Create bronze table with external data
    create_bronze_table = BigQueryCreateExternalTableOperator(
        task_id='create_bronze_table',
        destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.user_profiles',
        bucket=BUCKET,
        source_objects=['data/user_profiles/*.json'],
        schema_fields=[
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        source_format='NEWLINE_DELIMITED_JSON',
    )

    # Task: Transform and load data into silver
    transform_and_load_silver = BigQueryExecuteQueryOperator(
        task_id='transform_and_load_silver',
        sql='''
        CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.silver_dataset }}.user_profiles`
        AS
        SELECT DISTINCT
            email,
            SPLIT(full_name, ' ')[SAFE_OFFSET(0)] AS first_name,
            SPLIT(full_name, ' ')[SAFE_OFFSET(1)] AS last_name,
            state,
            PARSE_DATE('%Y-%m-%d', birth_date) AS birth_date,
            phone_number
        FROM
            `{{ params.project_id }}.{{ params.bronze_dataset }}.user_profiles`;
        ''',
        use_legacy_sql=False,
        params={
            'project_id': PROJECT_ID,
            'bronze_dataset': BRONZE_DATASET,
            'silver_dataset': SILVER_DATASET
        }
    )

    create_bronze_table >> transform_and_load_silver
