from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.db import connect_to_iris
from utils.extract import extract_all_data
from utils.loader_iris import load_to_postgresql
from utils.logger import configure_logger
from utils.email_notifier import notify_failure


# Configuration du logger au lancement
configure_logger()


# Définition de la fonction d'extraction
def extract_data_from_iris_osiris(**kwargs):
    conn = connect_to_iris()
    cursor = conn.cursor()
    patient_data, admission_data, measure_data = extract_all_data(cursor)
    return patient_data, admission_data, measure_data


# Définir les arguments de base du DAG
default_args = {
    'owner': 'DATA-IA',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'email': ['data.alerte@iuct-oncopole.fr'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


# Création du DAG
dag = DAG(
    'etl_iris_data',
    default_args=default_args,
    description='Extraction des données de IRIS et chargement dans PostgreSQL',
    schedule_interval=None,
    catchup=False
)


# Tâche d'extraction
extract_task = PythonOperator(
    task_id='extract_data_from_iris',
    python_callable=extract_data_from_iris_osiris,
    provide_context=True,
    do_xcom_push=True,
    on_failure_callback=notify_failure,
    dag=dag
)


# Tâche de chargement
load_task = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    provide_context=True,
    on_failure_callback=notify_failure,
    dag=dag
)


# Ordre d'exécution
extract_task >> load_task
