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
    'project.dataset.table1',
    'project.dataset.table2',
    'project.dataset.table3',
    'project.dataset.table4',
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
    gcs_bucket = 'your-gcs-bucket'
    export_format = 'CSV'  # Options are: 'CSV', 'NEWLINE_DELIMITED_JSON', or 'AVRO'

    # Create a task for each table export
    for bq_table in bq_tables:
        table_name = bq_table.split('.')[-1]
        gcs_path = f'gs://{gcs_bucket}/{table_name}'

        export_task = BigQueryToGCSOperator(
            task_id=f'export_{table_name}',
            source_project_dataset_table=bq_table,
            destination_cloud_storage_uris=[f'{gcs_path}/*.csv'],
            export_format=export_format,
            print_header=True,
            field_delimiter=',',
            bigquery_conn_id='google_cloud_default',
            google_cloud_storage_conn_id='google_cloud_default'
        )
