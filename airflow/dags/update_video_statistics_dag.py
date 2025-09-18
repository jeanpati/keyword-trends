from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="update_video_statistics_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    description="Update statistics for stored videos",
    tags=["youtube", "keyword", "statistics", "update", "views"],
) as dag:

    update_video_statistics_task = BashOperator(
        task_id="update_video_statistics",
        bash_command="python /opt/airflow/project_root/update_video_statistics.py",
    )
    update_video_statistics_task
