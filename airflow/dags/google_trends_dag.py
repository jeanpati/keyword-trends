from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="google_trends_dag",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    description="Process and save Google Trends files as a table",
    tags=["google", "trends", "table", "creation"],
) as dag:

    google_trends_task = BashOperator(
        task_id="google_trends_ingestion",
        bash_command="python /opt/airflow/project_root/google_trends_ingestion.py",
    )
    google_trends_task
