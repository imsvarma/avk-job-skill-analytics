from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---- Define simple task function ----
def say_hello():
    print("👋 Hello Mani! Airflow DAG is running successfully.")

# ---- Default arguments for the DAG ----
default_args = {
    'owner': 'Mani',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ---- Define the DAG ----
with DAG(
    dag_id='sampledag',
    default_args=default_args,
    description='A simple Hello World DAG',
    start_date=datetime(2025, 10, 1),
    schedule_interval='@daily',   # runs once every day
    catchup=False,                # don't run old dates
    tags=['example', 'hello'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

    hello_task
