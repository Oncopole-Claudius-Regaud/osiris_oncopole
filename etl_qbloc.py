
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.extract_and_load_qbloc import extract_and_load_chirurgie

DAG_ID = "extract_qbloc"

default_args = {
    "owner": "DATAIA",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["oracle", "QBLOC", "osiris",],
) as dag:

    extract_load_task = PythonOperator(
        task_id="extract_and_load_qbloc",
        python_callable=extract_and_load_chirurgie,
        execution_timeout=timedelta(hours=1),
    )

    extract_load_task

