import sys
import os
import gc
sys.path.append(os.path.dirname(__file__))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.stats import Stats

from osiris_oncopole.utils.db import connect_to_iris
from osiris_oncopole.utils.logger import configure_logger
from osiris_oncopole.utils.email_notifier import notify_failure, notify_success
from osiris_oncopole.utils.loader_iris import load_to_postgresql

# Logger global
configure_logger()

# Args par défaut
default_args = {
    'owner': 'DATA-IA',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

TMP_DIR = "/tmp/etl_iris"

def _safe_rm(path: str):
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


def extract_data_from_iris_osiris(**kwargs):
    """
    Extraction en STREAMING :
    - écrit directement patients/admissions/treatments/tumeur/diagnostic en NDJSON (.jsonl)
    - aucune grosse liste retournée
    """
    try:
        # 0) Prépare le dossier de travail (nettoyage des anciens fichiers)
        os.makedirs(TMP_DIR, exist_ok=True)
        for fname in ("patients.jsonl", "admissions.jsonl", "treatments.jsonl",
                      "tumeur.jsonl", "diagnostic.jsonl", "measures.jsonl",
                      "observations.jsonl", "rdv.jsonl", "contact.jsonl"):
            _safe_rm(os.path.join(TMP_DIR, fname))

        # 1) Connexion IRIS
        conn = connect_to_iris()
        cursor = conn.cursor()

        # 2) Extraction en streaming (écrit .jsonl directement)
        from osiris_oncopole.utils.extract import extract_all_data_streaming
        extract_all_data_streaming(cursor)

        # 3) Ferme DB et force un GC
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

        gc.collect()
        return {"output_dir": TMP_DIR}

    except Exception as e:
        Stats.incr("custom.task_failure.extract_data_from_iris")
        raise e


with DAG(
    dag_id='etl_iris_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["lymphome-data", "osiris", "PROD"],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_from_iris',
        python_callable=extract_data_from_iris_osiris,
    )

    load_task = PythonOperator(
        task_id='load_to_postgresql',
        python_callable=load_to_postgresql,
    )

    extract_task >> load_task

