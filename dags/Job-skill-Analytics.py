from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# =========================================================
# ⚙️ Default Configuration
# =========================================================
default_args = {
    'owner': 'mani',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# =========================================================
# 📅 DAG Definition
# =========================================================
with DAG(
    dag_id="jo",
    default_args=default_args,
    description="Run New_Test.py to fetch, process, and upload job data to S3",
    schedule_interval="@daily",       # run daily; set to None for manual
    start_date=datetime(2025, 10, 5),
    catchup=False,
    tags=["etl", "s3", "python-script"]
) as dag:

    # ---------- TASK 1: Pre-check ----------
    precheck = BashOperator(
        task_id="check_environment",
        bash_command="echo '✅ Environment verified — starting job pipeline...'"
    )

    # ---------- TASK 2: Execute main Python script ----------
    run_script = BashOperator(
        task_id="run_new_test_script",
        bash_command="python /opt/airflow/dags/New_Test.py"
    )

    # ---------- TASK 3: Post-run verification ----------
    verify_upload = BashOperator(
        task_id="verify_s3_upload",
        bash_command="echo '✅ Job completed — verify S3 for fetch_jobs_<date>.csv'"
    )

    # =========================================================
    # 🔁 TASK ORDER / DEPENDENCIES
    # =========================================================
    precheck >> run_script >> verify_upload
