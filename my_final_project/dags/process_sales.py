from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.operators.python import PythonOperator
from datetime import datetime

PROJECT_ID = 'de-07-inna-kirienko'
LOCATION = 'us'
BUCKET = 'final_project_raw_bucket'
BRONZE_DATASET = 'bronze'
SILVER_DATASET = 'silver'
GCS_PREFIX = 'data/sales/'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 1),
    'schedule_interval': '0 1 * * *',
    'catchup': True,
}


# Function to create source objects for BigQuery external table
def create_source_objects(task_instance):
    files = task_instance.xcom_pull(task_ids='list_gcs_files')
    source_objects = [f'{file}' for file in files if file.endswith('.csv')]
    return source_objects


with DAG(
    'process_sales_dag',
    default_args=default_args,
    description='Pipeline for processing sales data',
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
        destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.sales',
        bucket=BUCKET,
        source_objects="{{ task_instance.xcom_pull(task_ids='create_source_objects') }}",
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=','
    )

    # Task: Transform and load data into silver
    transform_and_load_silver = BigQueryExecuteQueryOperator(
        task_id='transform_and_load_silver',
        sql='''
        CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.silver_dataset }}.sales_partitioned`
        PARTITION BY purchase_date
        AS
        SELECT
            CustomerId AS client_id,
            CASE
                WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\d{2}-\d{2}$') THEN
                    PARSE_DATE('%Y-%m-%d', PurchaseDate)
                WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-\d{2}-\d{1}$') THEN
                    PARSE_DATE('%Y-%m-%e', PurchaseDate)
                WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}/\d{2}/\d{2}$') THEN
                    PARSE_DATE('%Y/%m/%d', PurchaseDate)
                WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-[A-Za-z]{3}-\d{2}$') THEN
                    PARSE_DATE('%Y-%b-%d',REPLACE(PurchaseDate, 'Aug', 'Sep'))
                ELSE
                    NULL
            END AS purchase_date,
            
            CASE
                WHEN Product = 'coffee machine' THEN 
                    'Coffee machine'
                ELSE
                    Product
            END as product_name,
            
            CASE
                WHEN REGEXP_CONTAINS(Price, r'%$') THEN
                    CAST(REGEXP_REPLACE(Price, r'[^0-9.]', '') AS DECIMAL)
                WHEN REGEXP_CONTAINS(Price, r'%USD') THEN
                    CAST(REGEXP_REPLACE(Price, r'[^0-9.]', '') AS DECIMAL)
                ELSE
                    CAST(REGEXP_REPLACE(Price, r'[^0-9.]', '') AS DECIMAL)
            END AS price
        FROM
            `{{ params.project_id }}.{{ params.bronze_dataset }}.sales`;
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

    list_gcs_files >> create_source_objects_task >> create_bronze_table >> transform_and_load_silver
