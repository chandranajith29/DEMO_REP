import yaml
import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
#to get config path
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'table_config.yaml')
CONFIG = load_config(CONFIG_PATH)

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'deep-chimera-459105-q6')

# Load SQL query
sql_file_path = os.path.join(os.path.dirname(__file__), 'sql', 'load_to_main.sql')
with open(sql_file_path, 'r') as file:
    sql_query = file.read()

with DAG(
    dag_id='gcs_to_bigquery_dynamic_dag',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['gcs', 'bigquery', 'dynamic'],
) as dag:

    start = EmptyOperator(task_id='start')

    load_to_staging = GCSToBigQueryOperator(
        task_id='load_csv_to_staging',
        bucket=CONFIG['gcs_bucket'],
        source_objects=[CONFIG['gcs_file_path']],
        destination_project_dataset_table=f"{PROJECT_ID}.{CONFIG['dataset']}.{CONFIG['staging_table']}",
        schema_fields=CONFIG['schema'],
        write_disposition=CONFIG['write_disposition'],
        skip_leading_rows=1,
        field_delimiter=",",
        source_format='CSV',
    )

    load_to_main = BigQueryInsertJobOperator(
        task_id='load_staging_to_main',
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                "params": {
                    "project_id": PROJECT_ID,
                    "dataset": CONFIG['dataset'],
                    "staging_table": CONFIG['staging_table'],
                    "main_table": CONFIG['main_table']
                },
            }
        }
    )

    end = EmptyOperator(task_id='end')

    # Task dependencies
    start >> load_to_staging >> load_to_main >> end
