from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from time import time
import logging
import pytz

# Constants
gcp_project_id = Variable.get('gcp_project_id')
process_id = '999'
log_table = 'Log'
bq_dataset = 'DL_Managment'
local_timezone = pytz.timezone('Asia/Jerusalem')
local_time = datetime.now(local_timezone).strftime('%Y-%m-%d %H:%M:%S')

# Define the on-failure callback function
def insert_failure_log_into_bq(context):
    """
    Generate a BigQuery insert query for logging failures and execute it.

    This function constructs a BigQuery insert query to log task failure details
    and executes the query to insert the log entry into the BigQuery log table.

    Parameters:
    context (dict): The context dictionary provided by Airflow, which includes the task instance (ti).

    Returns:
    None
    """
    logging.info("Writing error to Log table.")
    logging.error(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")
    ti = context['task_instance']
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    error_message = str(context.get('exception'))
    log_message = (
        f"Task Failure Alert: DAG ID: {dag_id} Task ID: {task_id} "
        f"Execution Date: {execution_date} Current Timestamp: {local_time} "
        f"Error Message: {error_message} Task Log Error: Task {task_id} has failed"
    )

    # Pull the RunID from XCom
    run_id = ti.xcom_pull(task_ids='read_from_vw_get_process_id', key='run_id')
    # Escape single quotes in the log message
    escaped_log_message = log_message.replace("\n", " ").replace("'", "''").replace("''", " ")
    logging.error(f"log message: {escaped_log_message}")
    insert_error_query = f"""
    INSERT INTO `{gcp_project_id}.{bq_dataset}.{log_table}` (ProcessID, RunID, Level, Info, LogTime)
    VALUES ({process_id}, CAST('{run_id}' AS INT64),
    'Error', '{escaped_log_message}', '{local_time}')
    """
    logging.info(f"Insert clause: {insert_error_query}")
    # Execute the query using BigQueryInsertJobOperator
    insert_log_task = BigQueryInsertJobOperator(
        task_id='insert_failure_log_into_bq',
        configuration={
            "query": {
                "query": insert_error_query,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        dag=context['dag']
    )
    insert_log_task.execute(context=context)
    logging.error(f"Error log was inserted to {bq_dataset}.{log_table} table")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': insert_failure_log_into_bq

}

# Define the DAG
with DAG(
    dag_id='dag_for_cicd',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    on_success_callback=None,
    on_failure_callback=insert_failure_log_into_bq,
) as dag:

    def generate_run_id(**context):
        """Generates a unique run ID based on the current process ID and Unix epoch time.

        This function is intended to be used as a Python callable within an Airflow DAG.
        It retrieves the current task instance from the context, generates a unique run ID,
        logs the run ID, and pushes it to XCom for downstream tasks to access.

        Args:
            **context: The Airflow context dictionary containing information about the task instance.

        Returns:
            None

        Raises:
            None
        """
        ti=context['task_instance']
        unix_epoch = int(time())
        run_id = process_id + str(unix_epoch)
        logging.info(f'run id: {run_id}')
        ti.xcom_push(key='run_id', value=run_id)

    def insert_log_into_bq( log_level, log_message,process_id, **context):
        """
        Insert a log entry into the BigQuery log table.

        This function constructs a BigQuery insert query for logging messages with a specified log level
        and executes the query to insert the log entry into the BigQuery log table.

        Parameters:
        log_level (str): The level of the log (e.g., 'Info', 'Error').
        log_message (str): The log message to be inserted.
        **context: The context dictionary provided by Airflow, which includes the task instance (ti).

        Returns:
            None

        Raises:
            None
        """

        logging.info(f"Inserting {log_level} log into BigQuery.")
        ti = context['task_instance']
        run_id = ti.xcom_pull(task_ids='generate_run_id', key='run_id')
        logging.info(f"run ids was pulled from xcom with value of {run_id}")
        insert_log_query = f"""
        INSERT INTO `{gcp_project_id}.{bq_dataset}.{log_table}` (ProcessID, RunID, Level, Info, LogTime)
        VALUES ({process_id}, CAST('{run_id}' AS INT64), '{log_level}', '{log_message}', '{local_time}')
        """
        logging.info(f"Insert clause: {insert_log_query}")
        insert_log_task = BigQueryInsertJobOperator(
            task_id=f'insert_log_into_bq_{run_id}',
            configuration={
                "query": {
                    "query": insert_log_query,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default',
            dag=context['dag']
        )
        insert_log_task.execute(context=context)
        logging.info(f"{log_level} log was inserted to {bq_dataset}.{log_table} table at: {local_time}")

    generate_run_id_task = PythonOperator(
        task_id='generate_run_id',
        python_callable=generate_run_id,
        provide_context=True,
        doc_md="""
        ### Task Documentation
        This task generate run_id based on unix epoch.
        """
    )


    insert_into_log_start_task = PythonOperator(
        task_id='insert_into_log_start',
        python_callable=insert_log_into_bq,
        op_kwargs={
            'log_level': 'Info',
            'log_message': f'Test Dag for CI/CD started at: {local_time}',
            'process_id': process_id
        },
        provide_context=True,
        doc_md="""
        ### Task Documentation
        This task inserts a log entry into the BigQuery log table indicating that the  DAG has started.
        """
    )

    insert_into_log_end_task = PythonOperator(
        task_id='insert_into_log_end',
        python_callable=insert_log_into_bq,
        op_kwargs={
            'log_level': 'Info',
            'log_message': f'Test Dag for CI/CD ended at: {local_time}',
            'process_id': process_id
        },
        provide_context=True,
        doc_md="""
        ### Task Documentation
        This task inserts a log entry into the BigQuery log table indicating that the  DAG has ended.
        """
    )