from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os
import dotenv
import logging
import json
import pandas as pd
logger = logging.getLogger(__name__)
dotenv.load_dotenv()

# DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

SCHEMA_NAME = 'dwh_silver'  # The schema you want to fetch tables from
POSTGRES_CONN_ID = 'postgres_local'
GCP_CONN_ID='google_cloud_dvd_rental'
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_NAME = os.getenv('BQ_DATASET_NAME')

def get_table_names():
    """Fetches all table names from the specified schema in Postgres."""
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{SCHEMA_NAME}' 
        AND table_type = 'BASE TABLE';
    """
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    tables = [row[0] for row in cursor.fetchall()]
    logger.info(f"Tables to process: {tables}")
    cursor.close()
    connection.close()
    return tables

# Define the DAG
with DAG(
    dag_id='dynamic_postgres_to_bigquery',
    default_args=default_args,
    description='Dynamically load data from all tables in a Postgres schema to BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['postgres', 'bigquery', 'dynamic'],
) as dag:

    # Mengambil nama-nama tabel
    get_tables_names_task = PythonOperator(
        task_id='get_tables',
        python_callable=get_table_names
    )

    # Daftar task yang digenerate secara dinamis
    export_and_load_tasks = []

    def generate_table_tasks(**kwargs):
        """Dynamically create tasks for each table in the schema."""
        tables = kwargs['ti'].xcom_pull(task_ids='get_tables')
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        for table in tables:
            logger.info(f"Exporting table {table} from Postgres to GCS")
            export_sql = f"SELECT * FROM {SCHEMA_NAME}.{table}"
            
            connection = postgres_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(export_sql)
            
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            #json_data = [dict(zip(column_names, row)) for row in rows]
            
            gcs_path = f"{SCHEMA_NAME}_{table}.json"
            local_path = f"/tmp/{gcs_path}"
            df = pd.DataFrame(rows, columns=column_names)
            df.to_json(local_path, orient='records', lines=True)
            
            cursor.close()
            connection.close()
            
            # Upload to GCS
            logger.info(f"Uploading {local_path} to GCS bucket {GCS_BUCKET_NAME} as {gcs_path}")
            gcs_hook.upload(
                bucket_name=GCS_BUCKET_NAME,
                object_name=gcs_path,
                filename=local_path
            )
            
            logger.info(f"Loading {gcs_path} from GCS to BigQuery table {BQ_PROJECT_ID}:{BQ_DATASET_NAME}.{table}")
            bq_hook.run_load(
                destination_project_dataset_table=f'{BQ_PROJECT_ID}:{BQ_DATASET_NAME}.{table}',
                source_uris=[f'gs://{GCS_BUCKET_NAME}/{gcs_path}'],
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_TRUNCATE',
                autodetect=True
            )

    generate_tasks = PythonOperator(
        task_id='generate_table_tasks',
        python_callable=generate_table_tasks,
        provide_context=True
    )

    # Set task dependencies
    get_tables_names_task >> generate_tasks
