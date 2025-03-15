from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'ticket_workflow',
    default_args=default_args,
    description='Ingest, transform, and validate ticket data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    ingest_csv = BashOperator(
        task_id='ingest_csv',
        bash_command='python c:/Users/rajat/Psql_Sample/scripts/ingest_script.py'
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='dbt run --profiles-dir c:/Users/rajat/Psql_Sample'
    )

    validate_dbt = PostgresOperator(
        task_id='validate_dbt',
        postgres_conn_id='postgres_default',
        sql='SELECT COUNT(*) FROM monthly_summary;'
    )

    ingest_csv >> run_dbt >> validate_dbt
