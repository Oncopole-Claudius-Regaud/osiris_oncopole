from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

from utils.extract_chimio import extract_chimio_data
from utils.transform_chimio import transform_all
from utils.email_notifier import notify_success, notify_failure

from utils.loader_chimio import (
    load_treatment_lines,
    load_drug_administrations
)

default_args = {
    'owner': 'DATA-IA',
    'start_date': datetime(2024, 1, 1),
    'email': ['data.alerte@iuct-oncopole.fr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    dag_id='etl_chimio_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL Oracle -> PostgreSQL pour le parcours de chimiothérapie',
    doc_md="""
    ## DAG : ETL Parcours Chimio

    Ce DAG effectue les étapes suivantes :

    1. Extraction des données Oracle : lignes de traitement, cycles, médicaments.
    2. Transformation : nettoyage des dates, ajout de hash, jointure avec les patients.
    3. Chargement dans PostgreSQL : uniquement les nouvelles données sont insérées.

    Ce DAG est destiné au chargement du parcours de chimiothérapie dans `un modele d'arch osiris`.
    """
)


def etl_chimio(**kwargs):
    logging.info("[ETL Chimio]  Démarrage du processus complet.")

    # Étape 1 : Extraction
    logging.info("[ETL Chimio] 1- Extraction des données Oracle...")
    treatment_df, drugs_df = extract_chimio_data()

    # Étape 2 : Transformation
    logging.info("[ETL Chimio] 2 - Transformation des données...")
    treatment_clean, drugs_clean = transform_all(
        treatment_df, drugs_df
    )

    # Étape 3 : Chargement
    logging.info("[ETL Chimio] 3- Chargement dans PostgreSQL...")
    load_treatment_lines(treatment_clean)
    load_drug_administrations(drugs_clean)

    logging.info("[ETL Chimio]  ETL terminé avec succès.")


etl_task = PythonOperator(
    task_id='etl_chimio_complete',
    python_callable=etl_chimio,
    provide_context=True,
    on_failure_callback=notify_failure,
    dag=dag
)

