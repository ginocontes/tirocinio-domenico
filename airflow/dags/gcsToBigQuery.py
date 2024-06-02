from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/ginocontestabile/dev/tirocinio-domenico/terraform/tf-sa.json"

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to get the first 10 files from a GCS bucket
def list_gcs_files(bucket_name, prefix=''):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs]
    return files


file_to_table_id = {
    "cliente.csv": "cliente",
    "emailclick.csv": "emailclick",
    "emailinvii.csv": "emailinvii",
    "emailunsub.csv": "emailunsub",
    "journey.csv": "journey",
    "journeyActivity.csv": "journeyActivity",
    "notificheclick.csv": "notificheclick",
    "microesiti.csv": "microesiti",
    "notifiche.csv": "notifiche",
    "sms.csv": "sms"
}

# Define the DAG
with DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='A DAG to move the marketing files from GCS to BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define variables
    bucket_name = 'etl-tesi-dev-data-ingest'
    prefix = ''
    dataset_id = 'marketing_raw'
    # table_id = ''

    # Get the list of files to move
    gcs_files = list_gcs_files(bucket_name, prefix)

    # Create a task for each file
    for gcs_file in gcs_files:
        filename = gcs_file.split("/")[-1]
        table_id = file_to_table_id[filename]
        gcs_to_bq_task = GCSToBigQueryOperator(
            task_id=f'gcs_to_bq_{filename}',
            bucket=bucket_name,
            source_objects=[gcs_file],
            destination_project_dataset_table=f'{dataset_id}.{table_id}',
            # schema_fields=[
            #     {'name': 'field1', 'type': 'STRING', 'mode': 'NULLABLE'},
            #     {'name': 'field2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            #     # Add more fields as per your schema
            # ],
            autodetect=True,
            write_disposition='WRITE_TRUNCATE'  # Recreate the table from scratc
        )
