import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to read SQL files from a directory
def get_sql_files(directory):
    sql_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".sql"):
            with open(os.path.join(directory, filename), 'r') as file:
                sql_files.append((filename, file.read()))
    return sql_files

# Define the DAG
with DAG(
    'bigquery_execute_sql_files',
    default_args=default_args,
    description='A DAG to execute SQL files in BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Create a task for each SQL file
    partition = BigQueryInsertJobOperator(
        task_id="partition",
        configuration={
            "query": {
                "query": "{% include 'sql/1_partition.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )
    merge = BigQueryInsertJobOperator(
        task_id="merge",
        configuration={
            "query": {
                "query": "{% include 'sql/2_merge.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )
    prep = BigQueryInsertJobOperator(
        task_id="prep",
        configuration={
            "query": {
                "query": "{% include 'sql/3_prep.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )
    campaign = BigQueryInsertJobOperator(
        task_id="campaign",
        configuration={
            "query": {
                "query": "{% include 'sql/4_campaign.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )
    sms = BigQueryInsertJobOperator(
        task_id="sms",
        configuration={
            "query": {
                "query": "{% include 'sql/5_esitisms.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )

    notifiche = BigQueryInsertJobOperator(
        task_id="notifiche",
        configuration={
            "query": {
                "query": "{% include 'sql/6_esitinotifiche.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )

    email = BigQueryInsertJobOperator(
        task_id="email",
        configuration={
            "query": {
                "query": "{% include 'sql/7_esitiemail.sql' %}",
                "useLegacySql": False,
            }
        },
        location="EU",
    )
    partition >> merge >> prep >> [campaign, sms, notifiche, email]

