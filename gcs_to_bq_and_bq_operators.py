# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
# Today = datetime.combine(datetime.today())

# Default arguments
default_args = {
    'start_date':yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='GCS_to_BQ_and_AGG',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

# Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

# GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
                task_id='gcs_to_bq_load',
                bucket='data_eng_demos_0',
                source_objects=['cus.csv'],
                destination_project_dataset_table='deep-chimera-459105-q6.my_dataset.gcs_to_bq_table',
                schema_fields=[
                                {'name': 'ID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                {'name': 'NAME', 'type': 'STRING', 'mode': 'NULLABLE'}
                              
                              ],
                skip_leading_rows=1,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE', 
    dag=dag)


# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
start >> gcs_to_bq_load  >> end
