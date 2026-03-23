
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import pandas as pd
import os

# Import des fonctions
from osiris_oncopole.utils.extract_chimio import extract_query_to_jsonl
from osiris_oncopole.utils.transform_chimio import transform_all
from osiris_oncopole.utils.loader_chimio import load_chimio_data

# Configuration de base du DAG
default_args = {
    'owner': 'DATA-IA',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='etl_chimio_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL Oracle -> PostgreSQL pour le parcours de chimiothérapie',
    tags=["lymphome-data", "osiris", "chimio"],
)

# Chemin de base pour les fichiers intermédiaires
BASE_PATH = "/tmp/etl_iris"
CHUNK_SIZE = 20000

# ----------------------------------------------------------------------
# FONCTIONS DES TÂCHES AIRFLOW
# ----------------------------------------------------------------------


def _iter_jsonl_chunks(path: str, chunksize: int):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return iter(())
    try:
        return pd.read_json(path, orient='records', lines=True, chunksize=chunksize)
    except ValueError:
        return iter(())

def extract_and_persist_data(**kwargs):
    """
    Tâche 1/3 : Extrait les données CHIMIO et les persiste sur disque au format JSONL.
    Pousse le chemin du fichier via XCom.
    """
    logging.info("[ETL Chimio] 1 - Démarrage de l'Extraction et de la Persistance...")
    
    # Création du répertoire cible
    os.makedirs(BASE_PATH, exist_ok=True)
    
    # 1. Extraction Oracle -> JSONL (streaming)
    chimio_raw_path = os.path.join(BASE_PATH, 'chimio_raw.jsonl')
    chimio_rows = extract_query_to_jsonl("extract_chimio.sql", chimio_raw_path, chunk_size=CHUNK_SIZE)
    
    # Pousser le chemin vers XCom
    kwargs['ti'].xcom_push(key='chimio_raw_path', value=chimio_raw_path)

    logging.info(
        "[ETL Chimio] 1 - Extraction et persistance terminées. CHIMIO=%s",
        chimio_rows,
    )


def transform_data(**kwargs):
    """
    Tâche 2/3 : Récupère df_chimio_raw, effectue la transformation, et persiste le résultat.
    """
    logging.info("[ETL Chimio] 2 - Démarrage de la Transformation des données...")
    ti = kwargs['ti']
    
    # 1. Récupérer le chemin du fichier CHIMIO RAW
    chimio_raw_path = ti.xcom_pull(task_ids='extract_and_persist', key='chimio_raw_path')
    if not chimio_raw_path:
        raise ValueError("Impossible de récupérer le chemin du fichier CHIMIO brut.")
        
    # 2. Transformation en chunks pour limiter la mémoire
    chimio_clean_path = os.path.join(BASE_PATH, 'chimio_clean.jsonl')
    if os.path.exists(chimio_clean_path):
        os.remove(chimio_clean_path)

    total_in = 0
    total_out = 0
    first_chunk = True

    for treatment_df in _iter_jsonl_chunks(chimio_raw_path, CHUNK_SIZE):
        if treatment_df is None or treatment_df.empty:
            continue
        total_in += len(treatment_df)
        treatment_clean = transform_all(treatment_df)
        total_out += len(treatment_clean)
        treatment_clean.to_json(
            chimio_clean_path,
            orient='records',
            lines=True,
            date_format='iso',
            mode='w' if first_chunk else 'a'
        )
        first_chunk = False

    if first_chunk:
        open(chimio_clean_path, "w", encoding="utf-8").close()
    
    # Pousser le chemin du fichier nettoyé vers XCom
    ti.xcom_push(key='chimio_clean_path', value=chimio_clean_path)
    logging.info(
        "[ETL Chimio] 2 - Transformation terminée et persistée. in=%s out=%s",
        total_in,
        total_out,
    )


def load_data(**kwargs):
    """
    Tâche 3/3 : Récupère le chemin du DataFrame CHIMIO CLEAN et le charge.
    """
    logging.info("[ETL Chimio] 3 - Démarrage du Chargement dans PostgreSQL...")
    ti = kwargs['ti']
    
    # 1. Récupérer le chemin
    chimio_clean_path = ti.xcom_pull(task_ids='transform_data', key='chimio_clean_path')
    
    if not chimio_clean_path:
        raise ValueError("Chemin du fichier CHIMIO de chargement manquant.")

    # 2. Chargement chunké de la table d'administration (osiris.chimiotherapie)
    first_chimio_chunk = True
    total_chimio = 0
    for df_chimio_clean in _iter_jsonl_chunks(chimio_clean_path, CHUNK_SIZE):
        if df_chimio_clean is None:
            continue
        if 'dat_admini' in df_chimio_clean.columns:
            df_chimio_clean['dat_admini'] = pd.to_datetime(df_chimio_clean['dat_admini'], errors='coerce')
        total_chimio += len(df_chimio_clean)
        load_chimio_data(df_chimio_clean, truncate_table=first_chimio_chunk)
        first_chimio_chunk = False

    if first_chimio_chunk:
        load_chimio_data(pd.DataFrame(columns=[
            'num_doss', 'jour', 'dat_admini', 'cod_categ_proto', 'cod_typ_proto',
            'num_pdt', 'nom_pdt', 'cod_voie', 'uf_real', 'lib_uf_real',
            'dose_tot', 'nom_proto', 'nom_moda', 'ce_etat_chimio'
        ]), truncate_table=True)
    
    logging.info(
        "[ETL Chimio] 3 - Chargement terminé avec succès. CHIMIO=%s",
        total_chimio,
    )


# ----------------------------------------------------------------------
# DÉFINITION DES TÂCHES ET ORCHESTRATION
# ----------------------------------------------------------------------

extract_task = PythonOperator(
    task_id='extract_and_persist',
    python_callable=extract_and_persist_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Définition de l'ordre d'exécution
extract_task >> transform_task >> load_task
