from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'upload_csv_to_gcs',
    default_args=default_args,
    description='A simple DAG to upload CSV files to Google Cloud Storage',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define the path to your CSV files
    local_path = '/path/to/your/csv/files'
    gcs_path = 'gs://your-gcs-bucket/folder'

    # List all CSV files in the local directory
    csv_files = [f for f in os.listdir(local_path) if f.endswith('.csv')]

    # Create a task for each CSV file
    for csv_file in csv_files[:10]:  # Limiting to 10 files
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{csv_file}',
            src=os.path.join(local_path, csv_file),
            dst=f'{gcs_path}/{csv_file}',
            bucket='your-gcs-bucket',
            google_cloud_storage_conn_id='google_cloud_default'
        )

