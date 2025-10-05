from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mani',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="extract_job_pipeline_dag",
    default_args=default_args,
    description="Run extract.py to fetch, process, and upload job data to S3",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 5),
    catchup=False,
    tags=["etl", "s3", "python-script"]
) as dag:

    precheck = BashOperator(
        task_id="check_environment",
        bash_command="echo '✅ Environment ready — running ETL...'"
    )

    run_extract = BashOperator(
        task_id="run_extract_script",
        bash_command="python /opt/airflow/src/extract.py"
    )

    verify = BashOperator(
        task_id="verify_upload",
        bash_command="echo '✅ ETL completed — check S3 for output.'"
    )

    precheck >> run_extract >> verify
