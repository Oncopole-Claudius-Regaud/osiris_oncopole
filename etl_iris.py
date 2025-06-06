from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.db import connect_to_iris
from utils.extract import extract_all_data
from utils.loader_iris import load_to_postgresql
from utils.logger import configure_logger
from airflow.utils.email import send_email


def notify_success(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    subject = f"[SUCCÈS] Tâche {task_id} du DAG {dag_id}"
    body = f"""
     La tâche **{task_id}** du DAG **{dag_id}** s'est terminée avec succès.

     Date d'exécution : {execution_date}
    """
    send_email(to="barry.mamadoudjoulde@iuct-oncopole.fr", subject=subject, html_content=body)


# Configuration du logger au lancement
configure_logger()

# Définition de la fonction d'extraction


def extract_data_from_iris_osiris(**kwargs):
    conn = connect_to_iris()
    cursor = conn.cursor()
    data = extract_all_data(cursor)
    return data


# Définir les arguments de base du DAG
default_args = {
    'owner': 'DATA-IA',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'email': ['barry.mamadoudjoulde@iuct-oncopole.fr'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Création du DAG
dag = DAG(
    'etl_iris_data',
    default_args=default_args,
    description='Extraction de IRIS ODBC et chargement dans PostgreSQL',
    schedule_interval=None,
    catchup=False
)

# Tâche d'extraction
extract_task = PythonOperator(
    task_id='extract_data_from_iris_osiris',
    python_callable=extract_data_from_iris_osiris,
    provide_context=True,
    do_xcom_push=True,
    on_success_callback=notify_success,
    dag=dag
)

# Tâche de chargement
load_task = PythonOperator(
    task_id='load_to_postgresql_osiris',
    python_callable=load_to_postgresql,
    provide_context=True,
    on_success_callback=notify_success,
    dag=dag
)

# Ordre d'exécution
extract_task >> load_task
