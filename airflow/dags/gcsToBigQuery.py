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

table_to_schema= {
    "cliente.csv": [
                {'name': 'cod_istituto', 'type': 'INTEGER', 'mode':  'NULLABLE'},
                {'name': 'COD_NDG_ANAGRAFICA_NSG', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'COD_FISCALE_PARTITA_IVA', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SubscriberKey', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'EMAIL_ADDRESS', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'Numero_Telefono', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],

    "emailclick.csv": [
                {'name': 'sendid', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'EventDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SubscriberKey', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    "emailinvii.csv": [
                {'name': 'journey_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'sendid', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'data_invio', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'EventDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SubscriberKey', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    "emailunsub.csv": [
                {'name': 'sendid', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'EventDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SubscriberKey', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    "journey.csv": [
                {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'JourneyStatus', 'type': 'STRING', 'mode': 'NULLABLE'},

    ],
    "journeyActivity.csv": [
                {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'JourneyID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'Activity_Name', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    "notificheclick.csv": [
                {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'ClickDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'deviceId', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'notificaId', 'type': 'STRING', 'mode': 'NULLABLE'},

    ],
    "microesiti.csv": [
                {'name': 'ESITOMC', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'MICROESITO', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    "notifiche.csv": [
                {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'DateTimeSend', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'deviceId', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'testo', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SubscriberKey', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'journey_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},

    ],
    "sms.csv": [
                {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'logDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'delivered', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'undelivered', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'SubscriberKey', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'journey_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},

    ]
}

# cod_istituto, COD_NDG_ANAGRAFICA_NSG, COD_FISCALE_PARTITA_IVA, SubscriberKey, EMAIL_ADDRESS
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
        if filename not in file_to_table_id.keys():
            continue
        table_id = file_to_table_id[filename]
        gcs_to_bq_task = GCSToBigQueryOperator(
            task_id=f'gcs_to_bq_{filename}',
            bucket=bucket_name,
            source_objects=[gcs_file],
            destination_project_dataset_table=f'{dataset_id}.{table_id}',
            schema_fields=table_to_schema[filename],
            autodetect=False,
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE'  # Recreate the table from scratc
        )
