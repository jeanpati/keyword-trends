from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="youtube_search_dag",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    description="Search Youtube with a keyword and save results",
    tags=["youtube", "keyword", "table", "creation", "search"],
) as dag:

    youtube_search_task = BashOperator(
        task_id="youtube_search_task",
        bash_command="python /opt/airflow/project_root/youtube_search.py",
    )
    youtube_search_task
