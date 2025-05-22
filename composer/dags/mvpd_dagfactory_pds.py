from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timezone, timedelta
import pendulum
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import (
    GoogleDriveToGCSOperator,
)
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.cloud.transfers.sheets_to_gcs import (
    GoogleSheetsToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator,
)
from airflow.operators.empty import EmptyOperator
from airflow.configuration import conf
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from airflow.models import Variable, TaskInstance
from airflow.models.param import Param
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.exceptions import AirflowSkipException
from fnmatch import fnmatchcase
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlOperator



from airflow.utils.state import State
import os
import json
import yaml
import re
import string
import logging
from google.cloud import storage
import pandas as pd

from custom_sensors.GCSPatternFileCountSensor import GCSPatternFileCountSensor
from custom_operators.BigQueryDataCheckOperator import BigQueryDataCheckOperator

dags_folder = conf.get("core", "dags_folder")
artifacts_folder = Variable.get("artifacts_path")

dag_context = {}
dag_context["project_id"] = os.environ.get("AIRFLOW_VAR_PROJECT_ID")
dag_context["environment"] = os.environ.get("ENVIRONMENT")
config_path = f"{dags_folder}/mvpd_dagfactory_config.yaml"
sql_path = f"sql_files/pds"
CHECKS_SQL_PATH = "sql_files/data_check"
schema_path = f"{artifacts_folder}/table_schema"

if os.environ.get("ENVIRONMENT") == "dev":
    alert_email_list = ["mohamed.soliman@groupm.com", "oussama.errabia@groupm.com", "shikha.madnani@groupm.com",
                        "evaristo.moreira@groupm.com", "radhika.ayyapusetty@groupm.com", "jayakumar.pp@groupm.com",
                        "kelvin.oyanna@groupm.com",
                        "shivam.soni@groupm.com", "ali.ashfaq@groupm.com"]

elif os.environ.get("ENVIRONMENT") == "prod":
    alert_email_list = ["mohamed.soliman@groupm.com","oussama.errabia@groupm.com","shikha.madnani@groupm.com",
                        "evaristo.moreira@groupm.com", "radhika.ayyapusetty@groupm.com", "jayakumar.pp@groupm.com",
                        "kelvin.oyanna@groupm.com",
                        "shivam.soni@groupm.com", "ali.ashfaq@groupm.com",
                        "Jacinda.Soto@groupm.com", "frances.zhen@groupm.com", "jia.zhou@groupm.com",
                        "emily.lai@groupm.com",
                        "dennis.stein@groupm.com", "colleen.lloyd@groupm.com", "nishant.desai@groupm.com"]
else:
    alert_email_list =["oussama.errabia@groupm.com"]



def parse_config(path, context):
    """
    Load a yaml configuration file and resolve any environment variables
    The environment variables must be in this format to be parsed: ${VAR_NAME}.
    """

    def string_constructor(loader, node):
        t = string.Template(node.value)
        value = t.substitute(context)

        return value

    loader = yaml.SafeLoader
    loader.add_constructor("tag:yaml.org,2002:str", string_constructor)

    # pattern for global vars: look for ${word}
    pattern = re.compile(".*?\${(\w+)}.*?")
    loader.add_implicit_resolver("tag:yaml.org,2002:str", pattern, None)

    with open(path) as conf_data:
        x = yaml.load(conf_data, Loader=loader)

    return x

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def create_dag(**kwargs):
    dag_id = kwargs["dag_id"]
    data_check_flag = kwargs["data_check_flag"]
    data_check_sql_path = kwargs["data_check_sql_path"]
    snowflake_active = kwargs["snowflake_active"]
    input_bucket = kwargs["input_bucket"]
    prefix = kwargs["prefix"]
    source_file_pattern = kwargs["source_file_pattern"]
    source_file_expected_count = kwargs["source_file_expected_count"]
    file_compression = kwargs["file_compression"]
    dttm_pattern = kwargs["dttm_pattern"]
    ignore_date_check = kwargs["ignore_date_check"]
    skip_if_no_files = kwargs["skip_if_no_files"]
    file_timeframe = kwargs["file_timeframe"]
    field_delimiter = kwargs["field_delimiter"]
    quote_character = kwargs["quote_character"]
    skip_leading_rows = kwargs["skip_leading_rows"]
    allow_jagged_rows = kwargs["allow_jagged_rows"]
    gcs_sensor_poke_interval = kwargs["gcs_sensor_poke_interval"]
    gcs_sensor_timeout = kwargs["gcs_sensor_timeout"]
    staging_table = kwargs["staging_table"]
    staging_table_load_method = kwargs["staging_table_load_method"]
    is_excel= kwargs["is_excel"]
    load_dma_agg_table = kwargs["load_dma_agg_table"]
    load_network_agg_table = kwargs["load_network_agg_table"]
    load_lineitem_agg_table = kwargs["load_lineitem_agg_table"]
    staging_table_schema_fields = kwargs["staging_table_schema_fields"]
    pds_load_sql_path = kwargs["pds_load_sql_path"]
    pds_load_dma_agg_sql_path = kwargs["pds_load_dma_agg_sql_path"]
    pds_load_network_agg_sql_path = kwargs["pds_load_network_agg_sql_path"]
    pds_load_lineitem_agg_sql_path = kwargs["pds_load_lineitem_agg_sql_path"]
    skip_source_file_ingestion = kwargs["skip_source_file_ingestion"]
    alert_email = kwargs["alert_email"]
    gcs_connection_id = kwargs["gcs_connection_id"]
    bq_connection_id = kwargs["bq_connection_id"]
    tags = kwargs["tags"]
    fileprocessday = kwargs["fileprocessday"]

    default_args = {
        "owner": "Oussama Errabia",
        "depends_on_past": False,
        "email_on_failure": False,
    }
    dag_param = None
    if not skip_source_file_ingestion:
        dag_param = {
            "source_file_expected_count": Param(
                source_file_expected_count, type="integer"
            ),
            "file_timeframe": Param(file_timeframe, type="integer"),
            "ignore_date_check": Param(ignore_date_check, type="boolean"),
            "skip_if_no_files": Param(skip_if_no_files, type="boolean"),
        }

    @dag(
        dag_id=dag_id,
        start_date=pendulum.datetime(2024, 8, 14, tz="UTC"),
        catchup=False,
        template_searchpath=artifacts_folder,
        schedule_interval="0 */4 * * *",
        default_args=default_args,
        render_template_as_native_obj=True,
        tags=tags,
        params=dag_param,
        max_active_runs=1
    )
    def dagfactory_pds():
        tm = datetime.now(tz=timezone.utc) - timedelta(days=file_timeframe)
        tm_str = tm.strftime(dttm_pattern)

        start = EmptyOperator(task_id="start")

        if fileprocessday:
            process_file = EmptyOperator(task_id="process_file")
            branch_weekday_check = BranchDayOfWeekOperator(
                task_id="branch_weekday_check",
                follow_task_ids_if_true="process_file",
                follow_task_ids_if_false="end",
                week_day=fileprocessday,
                use_task_logical_date=False,
            )

        if not skip_source_file_ingestion:

            def convert_excel_to_csv(**kwargs):
                ti = kwargs['ti']
                # Extract the filename from the conf parameter
                filenames = ti.xcom_pull(task_ids='get_file_name', key='return_value')[0]
                converted_filenames=[]
                for filename in filenames:
                    logging.info(f'FILENAME: {filename}')
                    download_blob(input_bucket, filename, filename.split("/")[-1])
                    data = pd.read_excel(filename.split("/")[-1])
                    data.to_csv(filename.split("/")[-1].replace(".xlsx", ".csv"), index=None)
                    upload_blob(input_bucket, filename.split("/")[-1].replace(".xlsx", ".csv"),
                                "inbound_interm_csv/" + filename.split("/")[-1].replace(".xlsx", ".csv"))

                    converted_filenames.append("inbound_interm_csv/" + filename.split("/")[-1].replace(".xlsx", ".csv"))
                return converted_filenames

            list_gcs_files = GCSPatternFileCountSensor(
                task_id="list_gcs_files",
                bucket=input_bucket,
                prefix=prefix,
                pattern=source_file_pattern,
                expected_file_count= source_file_expected_count,
                days_offset= file_timeframe,
                date_format=dttm_pattern,
                ignore_date_check=ignore_date_check,
                timeout=gcs_sensor_timeout,
                # poke_interval=gcs_sensor_poke_interval,
                gcp_conn_id=gcs_connection_id,
                mode="reschedule",
                soft_fail= skip_if_no_files,
            )
            if is_excel:
                convert_excel_to_csv = PythonOperator(
                    task_id='convert_excel_to_csv',
                    python_callable=convert_excel_to_csv,
                    provide_context=True
                )
            @task(trigger_rule=TriggerRule.ALL_DONE, provide_context=True)
            def check_and_send_email(**kwargs):
                date = kwargs["logical_date"]
                ti = TaskInstance(list_gcs_files, date)

                if fileprocessday:
                    ti_process_file = TaskInstance(process_file, date)

                    if True:
                        # If not fileprocessday, skip this email task
                        raise AirflowSkipException(
                            "Skipping as fileprocessday is not today"
                        )

                if ti.current_state() == "skipped":
                    # If the list_gcs_files task is skipped and files not found
                    print("No files found and previous task skipped")
                    return
                elif ti.current_state() == "failed":
                    # If the list_gcs_files task when no files were found, proceed with sending email
                    print("previous task failed")
                    return
                else:
                    # If files were found and task did not fail or skip, skip this email task
                    raise AirflowSkipException(
                        "Files found or list_gcs_files task succeeded."
                    )

            send_filenotfound_email = EmailOperator(
                task_id="send_filenotfound_email",
                conn_id="sendgrid_default",
                # You can specify more than one recipient with a list.
                to=alert_email,
                subject=f"({dag_context['environment']}) DAG Failure: {dag_id}",
                html_content=f"File(s) not found: {input_bucket}/{prefix} for date: {tm_str}<p>Expected file count: {source_file_expected_count}</p>",
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            move_file_to_preprocess_folder = GCSToGCSOperator(
                task_id="move_file_to_preprocess_folder",
                source_bucket=input_bucket,
                source_objects="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[0] }}",
                destination_bucket=input_bucket,
                destination_object="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[1] }}",
                exact_match=True,
                move_object=True,
                gcp_conn_id=gcs_connection_id,
            )

            if staging_table_load_method == "WRITE_TRUNCATE":
                drop_previous_stage_table = BigQueryDeleteTableOperator(
                    task_id="drop_previous_stage_table",
                    deletion_dataset_table=staging_table,
                    ignore_if_missing=True,
                    gcp_conn_id=bq_connection_id,
                )
            if is_excel:
                load_to_staging = GCSToBigQueryOperator(
                    task_id="load_to_staging",
                    bucket=input_bucket,
                    source_objects="{{ task_instance.xcom_pull(task_ids='convert_excel_to_csv', key='return_value') }}",
                    source_format="csv",
                    compression=file_compression,
                    destination_project_dataset_table=staging_table,
                    write_disposition=staging_table_load_method,
                    external_table=True,
                    autodetect=False if staging_table_schema_fields else True,
                    schema_fields=(
                        None
                        if not staging_table_schema_fields
                        else json.load(
                            open(
                                f"/{schema_path}/{str.split(staging_table, '.')[::-1][0]}.json"
                            )
                        )
                    ),
                    allow_quoted_newlines=True,
                    ignore_unknown_values=False,
                    allow_jagged_rows=allow_jagged_rows,
                    field_delimiter=field_delimiter,
                    quote_character=quote_character,
                    skip_leading_rows=skip_leading_rows,
                    gcp_conn_id=bq_connection_id,
                )
            else:
                load_to_staging = GCSToBigQueryOperator(
                    task_id="load_to_staging",
                    bucket=input_bucket,
                    source_objects="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[4] }}",
                    source_format="csv",
                    compression=file_compression,
                    destination_project_dataset_table=staging_table,
                    write_disposition=staging_table_load_method,
                    external_table=True,
                    autodetect=False if staging_table_schema_fields else True,
                    schema_fields=(
                        None
                        if not staging_table_schema_fields
                        else json.load(
                            open(
                                f"/{schema_path}/{str.split(staging_table, '.')[::-1][0]}.json"
                            )
                        )
                    ),
                    allow_quoted_newlines=True,
                    ignore_unknown_values=False,
                    allow_jagged_rows=allow_jagged_rows,
                    field_delimiter=field_delimiter,
                    quote_character=quote_character,
                    skip_leading_rows=skip_leading_rows,
                    gcp_conn_id=bq_connection_id,
                )
            @task.branch(trigger_rule=TriggerRule.ALL_DONE, provide_context=True)
            def check_and_move_file(monitored_task_ids: list, **kwargs):
                execution_date = kwargs["logical_date"]
                failed_tasks = []
                skipped_tasks = []

                for task_id in monitored_task_ids:
                    ti = TaskInstance(kwargs["dag"].get_task(task_id), execution_date)
                    task_state = ti.current_state()

                    print(f"🔍 Task {task_id} state: {task_state}")
                    if task_state == State.FAILED:
                        failed_tasks.append((task_id, task_state))
                    elif task_state == State.SKIPPED:
                        skipped_tasks.append(task_id)

                if failed_tasks:
                    print(f"❌ These tasks were failed: {failed_tasks}")
                    # Proceed with move to error folder logic
                    return "move_file_to_error_folder"
                elif skipped_tasks:
                    print(f"⚠️ The following tasks were skipped: {skipped_tasks}")
                    raise AirflowSkipException("Tasks were skipped. Not moving to archive or error.")
                else:
                    print("✅ All monitored tasks succeeded. Skipping error move, allowing archive move.")
                    return "move_file_to_archive_folder"

            move_file_to_error_folder = GCSToGCSOperator(
                task_id="move_file_to_error_folder",
                source_bucket=input_bucket,
                source_objects="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[4] }}",
                destination_bucket=input_bucket,
                destination_object="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[3] }}",
                exact_match=True,
                move_object=True,
                gcp_conn_id=gcs_connection_id,
                # trigger_rule=TriggerRule.ONE_FAILED,
            )

            move_file_to_archive_folder = GCSToGCSOperator(
                task_id="move_file_to_archive_folder",
                source_bucket=input_bucket,
                source_objects="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[4] }}",
                destination_bucket=input_bucket,
                destination_object="{{ task_instance.xcom_pull(task_ids='get_file_name', key='return_value')[2] }}",
                exact_match=True,
                move_object=True,
                gcp_conn_id=gcs_connection_id,
            )

        @task
        def get_file_name(filename_list):
            print(f"Looking for file: {filename_list}")

            # Take the first file
            fl_inbnd_1 = filename_list[0]

            # If the first file contains 'date=', process only that file
            if "date=" in fl_inbnd_1:
                fl_inbnd = [fl_inbnd_1]
            else:
                # If no 'date=' in the first file, use all provided files
                fl_inbnd = filename_list

            # Generate the preprocessing list by replacing 'inbound' with 'preprocessing'
            fl_preprocessing = [
                str.replace(fl, "inbound", "preprocessing") for fl in fl_inbnd
            ]

            # Extract the file path from the first file
            file_path = os.path.dirname(fl_inbnd[0])

            # Ensure the path ends with a slash
            if file_path[-1] != "/":
                file_path += "/"

            # Return the paths and processed file list
            if len(fl_inbnd) > 0:
                return (
                    fl_inbnd,  # List of files to process
                    str.replace(file_path, "inbound", "preprocessing"),  # Preprocessing path
                    str.replace(file_path, "inbound", "archived"),  # Archived path
                    str.replace(file_path, "inbound", "error"),  # Error path
                    fl_preprocessing  # List of preprocessing file paths
                )
        load_to_pds_table = BigQueryInsertJobOperator(
            task_id="load_to_pds_table",
            configuration={
                "query": {
                    "query": "{% include '"
                             + f"/{sql_path}/{pds_load_sql_path}"
                             + "' %}",
                    "useLegacySql": False,
                    "priority": "BATCH",
                    "queryParameters": [
                        {
                            "name": "dag_id",
                            "parameterType": {"type": "STRING"},
                            "parameterValue": {"value": dag_id},
                        },
                        {
                            "name": "dag_run_id",
                            "parameterType": {"type": "STRING"},
                            "parameterValue": {"value": "{{ run_id }}"},
                        },
                    ],
                    "parameterMode": "NAMED",
                },
            },
            gcp_conn_id=bq_connection_id,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # -------------------------------------------------------------------------------------------------------------
        # BigQuery aggregation updated

        AGGREGATION_SQLS = dags.get("aggregation_tables", {})

        aggregation_tasks = []
        for agg_name, agg_sql in AGGREGATION_SQLS.items():
            aggregation_tasks.append(BigQueryInsertJobOperator(
                task_id=f"load_to_{agg_name}_agg_table",
                configuration={
                    "query": {
                        "query": "{% include '"
                                 + f"/{sql_path}/{agg_sql}"
                                 + "' %}",
                        "useLegacySql": False,
                        "priority": "BATCH",
                        "queryParameters": [
                            {
                                "name": "dag_id",
                                "parameterType": {"type": "STRING"},
                                "parameterValue": {"value": dag_id},
                            },
                            {
                                "name": "dag_run_id",
                                "parameterType": {"type": "STRING"},
                                "parameterValue": {"value": "{{ run_id }}"},
                            },
                        ],
                        "parameterMode": "NAMED",
                    },
                },
                gcp_conn_id=bq_connection_id,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            ))

        # -------------------------------------------------------------------------------------------------------------
        # snowflake integration start
        if snowflake_active :
            SNOWFLAKE_CONN_ID = "snowflake_default"

            # Path to SQL files
            COPY_INTO_STAGING_SQL_FILE = os.path.join(sql_path, dags.get("snowflake", {}).get("load_staging_table_sql", ""))
            MERGE_INTO_MAIN_SQL_FILE = os.path.join(sql_path, dags.get("snowflake", {}).get("load_main_table_sql", ""))

            # Snowflake table details
            SNOWFLAKE_DATABASE = dags.get("snowflake", {}).get("database", "")
            SNOWFLAKE_SCHEMA = dags.get("snowflake", {}).get("schema", "")
            SNOWFLAKE_M_TABLE = dags.get("snowflake", {}).get("pds_table", "")
            SNOWFLAKE_STAGE_TABLE = dags.get("snowflake", {}).get("stg_table", "")
            SNOWFLAKE_STAGE_NAME = dags.get("snowflake", {}).get("stage_name", "")
            SNOWFLAKE_AGGREGATION_SQLS = dags.get("snowflake", {}).get("snowflake_aggregation_tables", {})
            SNOWFLAKE_FOLDER_PATH = dags.get("snowflake", {}).get("folderpath", "preprocessing")

            # Step 1: Create staging table if not exists
            CREATE_STAGING_TABLE_SQL = f"""
                               CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE_TABLE} AS 
                               SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_M_TABLE} WHERE 1=0;
                               """

            # Step 4: Drop staging table
            DROP_STAGING_SQL = f"""
                               DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE_TABLE};
                               """

            REFRESH_SNOWFLAKE_STAGE = f""" LIST @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE_NAME}/{SNOWFLAKE_FOLDER_PATH}/; """


            snowflake_create_staging_table = SnowflakeOperator(
                task_id="snowflake_create_staging_table",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=CREATE_STAGING_TABLE_SQL,
            )

            snowflake_refresh_stage = SnowflakeOperator(
                task_id="snowflake_refresh_stage",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=REFRESH_SNOWFLAKE_STAGE,
            )

            snowflake_staging = SnowflakeOperator(
                task_id="load_to_snowflake_staging_table",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=COPY_INTO_STAGING_SQL_FILE,
            )

            snowflake_main = SnowflakeOperator(
                task_id="load_to_snowflake_main_table",
                sql=MERGE_INTO_MAIN_SQL_FILE,
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
            )

            snowflake_aggregation_tasks = []
            for agg_name, agg_sql in SNOWFLAKE_AGGREGATION_SQLS.items():
                snowflake_aggregation_tasks.append(SnowflakeOperator(
                    task_id=f"load_to_snowflake_{agg_name}_agg_table",
                    sql=f"{sql_path}/{agg_sql}",
                    snowflake_conn_id=SNOWFLAKE_CONN_ID,
                ))

            snowflake_drop_staging_table = SnowflakeOperator(
                task_id="snowflake_drop_staging_table",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                sql=DROP_STAGING_SQL,
            )

        # snowflake integration end
        # -------------------------------------------------------------------------------------------------------------
        if data_check_flag:
            data_check_email_subject = (
                "Data Validation Failed in DAG"
                if not "Data Validation Failed in DAG"#config.data_check_email_subject
                else "Data Validation Failed in DAG"#config.data_check_email_subject
            )

            data_check = BigQueryDataCheckOperator(
                task_id="data_check",
                sql="{% include '"
                    + f"/{CHECKS_SQL_PATH}/{data_check_sql_path}"
                    + "' %}",
                to_email=(
                    alert_email_list
                ),
                subject=data_check_email_subject,
                header_title=data_check_email_subject,
                gcp_conn_id=bq_connection_id,
                retries=0,
                # contact_emails="oussama.errabia@publicismedia.com",
                # team_name="Tech Team",
            )
        end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)
        monitored_tasks = [load_to_staging, load_to_pds_table]
        # check_status = check_and_move_file()
        if skip_source_file_ingestion:
            start >> load_to_pds_table >> end
        else:
            if fileprocessday:
                start >> branch_weekday_check >> [process_file, end]
                process_file >> list_gcs_files
            else:
                start >> list_gcs_files

            list_gcs_files >> check_and_send_email() >> send_filenotfound_email

            if is_excel:
                (get_file_name(list_gcs_files.output)
                >> convert_excel_to_csv
                >> move_file_to_preprocess_folder
                >> drop_previous_stage_table
                >> load_to_staging
                >> load_to_pds_table)

            else:
                (get_file_name(list_gcs_files.output)
                >> move_file_to_preprocess_folder
                >> drop_previous_stage_table
                >> load_to_staging
                >> load_to_pds_table)

            if data_check_flag:
                load_to_staging >> data_check

            if aggregation_tasks:
                monitored_tasks.extend(aggregation_tasks)
                load_to_pds_table >> [task for task in aggregation_tasks]
                aggregation_complete = [task for task in aggregation_tasks]
            else:
                aggregation_complete = [load_to_pds_table]

            # Snowflake flow if activated
            if snowflake_active:
                aggregation_complete >> snowflake_create_staging_table >> snowflake_refresh_stage >> snowflake_staging >> snowflake_main
                monitored_tasks.extend([
                    snowflake_create_staging_table,
                    snowflake_refresh_stage,
                    snowflake_staging,
                    snowflake_main,
                    snowflake_drop_staging_table
                ])

                if snowflake_aggregation_tasks:
                    snowflake_main >> [task for task in snowflake_aggregation_tasks] >> snowflake_drop_staging_table
                    monitored_tasks.extend(snowflake_aggregation_tasks)
                    snowflake_flow_complete = snowflake_drop_staging_table
                else:
                    snowflake_main >> snowflake_drop_staging_table
                    snowflake_flow_complete = snowflake_drop_staging_table
            else:
                snowflake_flow_complete = aggregation_complete

            check_status = check_and_move_file(monitored_task_ids=[t.task_id for t in monitored_tasks])
            for t in monitored_tasks:
                t >> check_status

            snowflake_flow_complete >> check_status
            check_status >> [move_file_to_error_folder,move_file_to_archive_folder]

            move_file_to_archive_folder >> end
            move_file_to_error_folder >> end
            send_filenotfound_email >> end

    generated_dag = dagfactory_pds()

    return generated_dag


dag_configs = parse_config(config_path, dag_context)
pds_dags = dag_configs.get("configs", {}).get("sub_dags", {}).get("pds")

if pds_dags:
    for dags in dag_configs["configs"]["sub_dags"]["pds"]:
        dag_id = dags["dag_id"]
        snowflake_active = dags.get("snowflake_active", False)
        data_check_flag = dags.get("data_check",{}).get("data_check_flag",None)
        data_check_sql_path = dags.get("data_check",{}).get("data_check_sql_path",None)
        source_file = dags.get("source_file", None)
        skip_source_file_ingestion = dags.get("skip_source_file_ingestion", False)
        input_bucket = None
        storage_type = None
        prefix = ""
        source_file_pattern = None
        source_file_expected_count = None
        file_compression = None
        file_timeframe = 0
        dttm_pattern = "%Y%m%d"
        field_delimiter = ","
        quote_character = None
        skip_leading_rows = 0
        allow_jagged_rows = False
        gcs_sensor_poke_interval = 900
        gcs_sensor_timeout = 7200
        staging_table = None
        staging_table_load_method = "WRITE_TRUNCATE"
        staging_table_schema_fields = None
        alert_email = None
        drive_folder_id = None
        drive_file_name = None
        gsheet_file_id = None
        drive_connection_id = None
        tags = dags.get("tags", None)
        fileprocessday = None
        ignore_date_check = False
        skip_if_no_files = False
        is_excel = False
        load_dma_agg_table = False
        load_network_agg_table = False
        load_lineitem_agg_table = False


        if not skip_source_file_ingestion and source_file:
            input_bucket = dags["input_bucket"]
            storage_type = source_file["storage_type"]
            fileprocessday = source_file.get("fileprocessday", None)
            prefix = source_file["prefix"]
            source_file_pattern = source_file.get("file_pattern", None)
            source_file_expected_count = source_file.get("expected_count", 1)
            file_compression = source_file.get("file_compression", "NONE")
            file_timeframe = source_file.get("file_timeframe", 0)
            dttm_pattern = source_file.get("dttm_pattern", "%Y%m%d")
            ignore_date_check = source_file.get("ignore_date_check", False)
            skip_if_no_files = source_file.get("skip_if_no_files", False)
            field_delimiter = source_file.get("field_delimiter", ",")
            quote_character = source_file.get("quote_character", '"')
            skip_leading_rows = source_file.get("skip_leading_rows", 1)
            gcs_sensor_poke_interval = source_file.get("gcs_sensor_poke_interval", 900)
            gcs_sensor_timeout = source_file.get("gcs_sensor_timeout", 7200)
            is_excel = source_file.get("is_excel",False)
            load_dma_agg_table = source_file.get("load_dma_agg_table", False)
            load_network_agg_table = source_file.get("load_network_agg_table", False)
            load_lineitem_agg_table = source_file.get("load_lineitem_agg_table", False)
            staging_table = dags["staging_table"]["name"]
            staging_table_load_method = dags["staging_table"].get(
                "load_method", "WRITE_TRUNCATE"
            )
            staging_table_schema_fields = dags["staging_table"].get("schema", None)
            allow_jagged_rows = dags["staging_table"].get("allow_jagged_rows", False)
            alert_email = dags.get(
                "alert_email",
                alert_email_list,
            )
            drive_folder = source_file.get("drive_folder", None)
            if drive_folder:
                drive_folder_id = drive_folder["folder_id"]
                drive_file_name = drive_folder["file_name"]
                gsheet_file_id = drive_folder.get("gsheet_file_id", None)
                drive_connection_id = drive_folder.get(
                    "connection_id", "google_cloud_conn_id"
                )

        globals()[dag_id] = create_dag(
            dag_id=dag_id,
            snowflake_active=snowflake_active,
            data_check_flag = data_check_flag,
            data_check_sql_path = data_check_sql_path,
            input_bucket=input_bucket,
            storage_type=storage_type,
            drive_folder_id=drive_folder_id,
            drive_file_name=drive_file_name,
            gsheet_file_id=gsheet_file_id,
            prefix=prefix,
            source_file_pattern=source_file_pattern,
            source_file_expected_count=source_file_expected_count,
            file_compression=file_compression,
            file_timeframe=file_timeframe,
            dttm_pattern=dttm_pattern,
            ignore_date_check=ignore_date_check,
            skip_if_no_files=skip_if_no_files,
            field_delimiter=field_delimiter,
            quote_character=quote_character,
            skip_leading_rows=skip_leading_rows,
            allow_jagged_rows=allow_jagged_rows,
            gcs_sensor_poke_interval=gcs_sensor_poke_interval,
            gcs_sensor_timeout=gcs_sensor_timeout,
            staging_table=staging_table,
            staging_table_load_method=staging_table_load_method,
            is_excel = is_excel,
            load_dma_agg_table= load_dma_agg_table,
            load_network_agg_table=load_network_agg_table,
            load_lineitem_agg_table=load_lineitem_agg_table,
            staging_table_schema_fields=staging_table_schema_fields,
            pds_load_sql_path=dags["pds_load_sql_path"],
            pds_load_dma_agg_sql_path=dags.get("pds_load_dma_agg_sql_path",None),
            pds_load_lineitem_agg_sql_path=dags.get("pds_load_lineitem_agg_sql_path", None),
            pds_load_network_agg_sql_path=dags.get("pds_load_network_agg_sql_path", None),
            skip_source_file_ingestion=skip_source_file_ingestion,
            alert_email=alert_email,
            gcs_connection_id=dags.get("gcs_connection_id", "google_cloud_default"),
            bq_connection_id=dags.get("bq_connection_id", "google_cloud_default"),
            drive_connection_id=drive_connection_id,
            tags=tags,
            fileprocessday=fileprocessday,
        )