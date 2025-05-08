from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# Define the DAG
dag = DAG(
    'simple_example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',
)

# Define the function for Task 1
def print_hello_world():
    print("Hello World!")

# Define the function for Task 2
def print_current_timestamp():
    print(f"Current Timestamp: {datetime.now()}")

# Define Task 1
task1 = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag,
)

# Define Task 2
task2 = PythonOperator(
    task_id='print_current_timestamp',
    python_callable=print_current_timestamp,
    dag=dag,
)

# Set the task dependencies
task1 >> task2
