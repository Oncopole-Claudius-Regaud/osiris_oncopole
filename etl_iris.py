from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.db import connect_to_iris
from utils.extract import extract_all_data
from utils.loader_iris import load_to_postgresql
from utils.logger import configure_logger

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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Création du DAG
dag = DAG(
    'etl_iris',
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
    dag=dag
)

# Tâche de chargement
load_task = PythonOperator(
    task_id='load_to_postgresql_osiris',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag
)

# Ordre d'exécution
extract_task >> load_task

