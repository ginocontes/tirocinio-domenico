from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to get the first 10 files from a GCS bucket
def list_gcs_files(bucket_name, prefix='', max_files=10):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs]
    return files[:max_files]

# Define the DAG
with DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='A DAG to move files from GCS to BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define variables
    bucket_name = 'etl-tesi-dev-data-ingest'
    prefix = '/'
    dataset_id = 'marketing-raw'
    # table_id = ''

    # Get the list of files to move
    gcs_files = list_gcs_files(bucket_name, prefix, max_files=10)

    # Create a task for each file
    for gcs_file in gcs_files:
        gcs_to_bq_task = GCSToBigQueryOperator(
            task_id=f'gcs_to_bq_{gcs_file.split("/")[-1]}',
            bucket=bucket_name,
            source_objects=[gcs_file],
            destination_project_dataset_table=f'{dataset_id}.{table_id}',
            schema_fields=[
                {'name': 'field1', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'field2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                # Add more fields as per your schema
            ],
            write_disposition='WRITE_APPEND',  # Change as per your requirement
            google_cloud_storage_conn_id='google_cloud_default',
            bigquery_conn_id='google_cloud_default'
        )
