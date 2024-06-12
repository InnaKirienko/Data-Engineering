from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

PROJECT_ID = 'de-07-inna-kirienko'
SILVER_DATASET = 'silver'
GOLD_DATASET = 'gold'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 1),
    'catchup': False,
}

with DAG(
        'enrich_user_profiles_dag',
        default_args=default_args,
        description="Pipeline for enriching user's profiles data",
        schedule_interval=None,
        render_template_as_native_obj=True,
) as dag:

    load_customers_to_gold = BigQueryExecuteQueryOperator(
        task_id='load_customers_to_gold',
        sql='''
        CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.gold_dataset }}.user_profiles_enriched`
        AS
        SELECT
            c.*,
            CAST(NULL AS DATE) AS birth_date,
            CAST(NULL AS STRING) AS phone_number
        FROM
            `{{ params.project_id }}.{{ params.silver_dataset }}.customers` c
        ''',
        use_legacy_sql=False,
        params={
            'project_id': PROJECT_ID,
            'silver_dataset': SILVER_DATASET,
            'gold_dataset': GOLD_DATASET
        }
    )

    enrich_user_profiles = BigQueryExecuteQueryOperator(
        task_id='enrich_user_profiles',
        sql='''
        UPDATE `{{ params.project_id }}.{{ params.gold_dataset }}.user_profiles_enriched` target
        SET
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.state = source.state,
            target.birth_date = source.birth_date,
            target.phone_number = source.phone_number
        FROM
            `{{ params.project_id }}.{{ params.silver_dataset }}.user_profiles` source
        WHERE
            target.email = source.email
        ''',
        use_legacy_sql=False,
        params={
            'project_id': PROJECT_ID,
            'silver_dataset': SILVER_DATASET,
            'gold_dataset': GOLD_DATASET
        }
    )

    load_customers_to_gold >> enrich_user_profiles
