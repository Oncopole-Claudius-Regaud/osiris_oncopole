import sys
import os
import gc
sys.path.append(os.path.dirname(__file__))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.stats import Stats

from utils.db import connect_to_iris
from utils.logger import configure_logger
from utils.email_notifier import notify_failure, notify_success
from utils.loader_iris import load_to_postgresql

# Import dynamique (OK)
import importlib
extract_measure_batch_by_date = importlib.import_module("utils.measure_runner").extract_measure_batch_by_date

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
                      "tumeur.jsonl", "diagnostic.jsonl", "measures.jsonl"):
            _safe_rm(os.path.join(TMP_DIR, fname))

        # 1) Connexion IRIS
        conn = connect_to_iris()
        cursor = conn.cursor()

        # 2) Extraction en streaming (écrit .jsonl directement)
        #    -> nécessite utils.extract.extract_all_data_streaming(...) comme on l’a proposé
        from utils.extract import extract_all_data_streaming
        extract_all_data_streaming(cursor)  # pas de return; écrit en /tmp/etl_iris/*.jsonl

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
        return {"output_dir": TMP_DIR}  # léger (facultatif)

    except Exception as e:
        Stats.incr("custom.task_failure.extract_data_from_iris")
        raise e


@task
def get_periods():
    # Fenêtres mensuelles pour les mesures (évite la volumétrie d’un coup)
    from utils.date_utils import generate_month_periods
    from datetime import date as _date
    min_date = _date(2020, 10, 1)
    today = datetime.today().date()
    periods = generate_month_periods(min_date, today)  # [('YYYY-MM-01','YYYY-MM-31'), ...] 
    if periods:
        print(f"[get_periods] nb={len(periods)} first={periods[0]} last={periods[-1]}")
    else:
        print("[get_periods] nb=0")
    return periods


@task
def extract_measures_for_period(period: tuple):
    """
    Appel par période : append NDJSON sur /tmp/etl_iris/measures.jsonl
    -> la version précédente mergait en RAM le JSON complet puis réécrivait:contentReference[oaicite:3]{index=3}.
    """
    start_date, end_date = period
    extract_measure_batch_by_date(start_date=start_date, end_date=end_date)


with DAG(
    dag_id='etl_iris_data',
    default_args=default_args,
    start_date=datetime(2025, 4, 3),
    catchup=False,
    schedule=None,
    tags=["lymphome-data", "osiris", "PROD"],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_from_iris',
        python_callable=extract_data_from_iris_osiris,
        execution_timeout=timedelta(hours=1),
    )

    periods = get_periods()
    extract_batches = extract_measures_for_period.expand(period=periods)

    load_task = PythonOperator(
        task_id='load_to_postgresql',
        python_callable=load_to_postgresql,  # adapter pour lire .jsonl en flux (ligne par ligne) plutôt que json.load intégral:contentReference[oaicite:5]{index=5}.
        
    )

    extract_task >> periods >> extract_batches >> load_task

