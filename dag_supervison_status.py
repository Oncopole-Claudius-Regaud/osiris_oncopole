
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from osiris_oncopole.utils.generate_status_table import build_dag_status_table

with DAG(
    dag_id="dag_supervision_status",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["monitoring"],
) as dag:

    generate_html = PythonOperator(
        task_id="generate_status_table",
        python_callable=build_dag_status_table,
    )

    send_email = EmailOperator(
        task_id="send_status_email",
        to=["Gutkowski.Florian@iuct-oncopole.fr"],
        subject="Airflow – Statut des derniers DAGs",
        html_content="{{ ti.xcom_pull(task_ids='generate_status_table') }}",
    )

    generate_html >> send_email

