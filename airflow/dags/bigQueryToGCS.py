from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the list of BigQuery tables to export
bq_tables = [
    "campaign",
    "esitiemail",
    "esitinotifiche",
    "esitisms"
]

# Define the DAG
with DAG(
    'bq_to_gcs_export',
    default_args=default_args,
    description='A DAG to export BigQuery tables to Google Cloud Storage',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define GCS bucket and export format
    gcs_bucket = 'etl-tesi-dev-data-output'
    export_format = 'CSV'  # Options are: 'CSV', 'NEWLINE_DELIMITED_JSON', or 'AVRO'

    # Create a task for each table export
    for bq_table in bq_tables:
        gcs_path = f'gs://{gcs_bucket}/{bq_table}'

        export_task = BigQueryToGCSOperator(
            task_id=f'export_{bq_table}',
            source_project_dataset_table=f"etl-tesi-domenico.marketing_final.{bq_table}",
            destination_cloud_storage_uris=[f'{gcs_path}/{bq_table}_*.csv'],
            export_format=export_format,
            print_header=True,
            field_delimiter=','
        )
