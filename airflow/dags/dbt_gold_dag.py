from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="dbt_gold_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    description="Run gold models",
    tags=["dbt", "transformation", "gold", "scheduled"],
) as dag:

    DBT_PROJECT_DIR = "/opt/airflow/project_root/dbt/keyword_trends"
    DBT_PROFILES_DIR = "/opt/airflow/project_root/dbt/keyword_trends"

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}",
        dag=dag,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
        dag=dag,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select gold --profiles-dir {DBT_PROFILES_DIR}",
        dag=dag,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select gold --profiles-dir {DBT_PROFILES_DIR}",
        dag=dag,
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
        dag=dag,
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command="python /opt/airflow/project_root/gold_to_postgres.py",
        dag=dag,
    )

    (
        dbt_debug
        >> dbt_deps
        >> dbt_run_gold
        >> dbt_test_staging
        >> dbt_docs_generate
        >> gold_to_postgres
    )
