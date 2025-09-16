from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="csv_to_parquet_dag",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    description="Converts CSV files in Minio to Parquet format",
    tags=["convert", "csv", "parquet", "minio"],
) as dag:

    csv_to_parquet_task = BashOperator(
        task_id="csv_to_parquet",
        bash_command="python /opt/airflow/project_root/csv_to_parquet.py",
    )
    csv_to_parquet_task
