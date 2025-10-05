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
    dag_id="extract_job_pipeline_dag",
    default_args=default_args,
    description="ETL job that runs extract.py and validates environment variables",
    schedule_interval="@daily",       # or None for manual
    start_date=datetime(2025, 10, 5),
    catchup=False,
    tags=["etl", "aws", "openai", "env-check"]
) as dag:

    # ---------- ✅ TASK 1: Verify environment variables ----------
    check_env = BashOperator(
        task_id="check_env_vars",
        bash_command="""
        echo "🔍 Checking Environment Variables..."
        echo "OPENAI_API_KEY: ${OPENAI_API_KEY:0:8}********"
        echo "AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:0:4}********"
        echo "AWS_REGION: ${AWS_DEFAULT_REGION}"
        """,
    )

    # ---------- TASK 2: Install dependencies ----------
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install -r /opt/airflow/dags/repo/requirements.txt --quiet"
    )

    # ---------- TASK 3: Run extract.py ----------
    run_extract = BashOperator(
        task_id="run_extract_script",
        bash_command="python /opt/airflow/dags/repo/src/extract.py"
    )

    # ---------- TASK 4: Post verification ----------
    post_check = BashOperator(
        task_id="post_check",
        bash_command="echo '✅ DAG finished successfully — check S3 and logs!'"
    )

    # =========================================================
    # 🔁 TASK ORDER
    # =========================================================
    check_env >> install_deps >> run_extract >> post_check
