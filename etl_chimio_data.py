
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import logging
import pandas as pd
import os
import json # Import nécessaire pour la sérialisation/désérialisation si on passait par XCom

# Import des fonctions (assurez-vous que extract_chimio_data renvoie les deux DF)
from utils.extract_chimio import extract_chimio_data 
from utils.transform_chimio import transform_all
from utils.email_notifier import notify_failure
from utils.loader_chimio import load_chimio_data, load_chimio_plan_data 

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
BASE_PATH = "tmp/etl_iris" 

# ----------------------------------------------------------------------
# FONCTIONS DES TÂCHES AIRFLOW
# ----------------------------------------------------------------------

def extract_and_persist_data(**kwargs):
    """
    Tâche 1/3 : Extrait les données et les persiste sur disque au format JSONL.
    Pousse les chemins des fichiers via XCom.
    """
    logging.info("[ETL Chimio] 1 - Démarrage de l'Extraction et de la Persistance...")
    
    # Création du répertoire cible
    os.makedirs(BASE_PATH, exist_ok=True)
    
    # 1. Extraction (retourne df_chimio_final, df_plan)
    df_chimio, df_plan = extract_chimio_data() 
    
    # 2. Persistance des deux DataFrames (RAW)
    
    # Fichier Chimio (administrations)
    chimio_raw_path = os.path.join(BASE_PATH, 'chimio_raw.jsonl')
    df_chimio.to_json(chimio_raw_path, orient='records', lines=True, date_format='iso')
    
    # Fichier Plan (planification)
    plan_raw_path = os.path.join(BASE_PATH, 'plan_raw.jsonl')
    df_plan.to_json(plan_raw_path, orient='records', lines=True, date_format='iso')
    
    # Pousser les chemins vers XCom
    kwargs['ti'].xcom_push(key='chimio_raw_path', value=chimio_raw_path)
    kwargs['ti'].xcom_push(key='plan_raw_path', value=plan_raw_path)

    logging.info("[ETL Chimio] 1 - Extraction et persistance terminées.")


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
        
    # 2. Charger le DataFrame CHIMIO RAW depuis le disque
    treatment_df = pd.read_json(chimio_raw_path, orient='records', lines=True)
    
    # 3. Application des transformations
    treatment_clean = transform_all(treatment_df)
    
    # 4. Persistance du DataFrame CHIMIO nettoyé
    chimio_clean_path = os.path.join(BASE_PATH, 'chimio_clean.jsonl')
    treatment_clean.to_json(chimio_clean_path, orient='records', lines=True, date_format='iso')
    
    # Pousser le chemin du fichier nettoyé vers XCom
    ti.xcom_push(key='chimio_clean_path', value=chimio_clean_path)
    logging.info("[ETL Chimio] 2 - Transformation terminée et persistée.")


def load_data(**kwargs):
    """
    Tâche 3/3 : Récupère les chemins des DataFrames (Chimio CLEAN et Plan RAW) et les charge.
    """
    logging.info("[ETL Chimio] 3 - Démarrage du Chargement dans PostgreSQL...")
    ti = kwargs['ti']
    
    # 1. Récupérer les chemins
    chimio_clean_path = ti.xcom_pull(task_ids='transform_data', key='chimio_clean_path')
    plan_raw_path = ti.xcom_pull(task_ids='extract_and_persist', key='plan_raw_path')
    
    if not chimio_clean_path or not plan_raw_path:
        raise ValueError("Chemins des fichiers de chargement manquants.")
        
    # 2. Charger les DataFrames depuis le disque
    df_chimio_clean = pd.read_json(chimio_clean_path, orient='records', lines=True)
    df_plan_raw = pd.read_json(plan_raw_path, orient='records', lines=True)

    # Reconvertir les colonnes de date pour le loader (car JSONL les a sérialisées en chaînes)
    # Ceci est crucial car le loader pourrait dépendre du type datetime pour l'insertion
    df_chimio_clean['dat_admini'] = pd.to_datetime(df_chimio_clean['dat_admini'])
    df_plan_raw['dat_ouv'] = pd.to_datetime(df_plan_raw['dat_ouv'])

    # 3. Chargement de la table de planification (osiris.chimio_plan)
    load_chimio_plan_data(df_plan_raw)

    # 4. Chargement de la table d'administration (osiris.chimiotherapie)
    load_chimio_data(df_chimio_clean)
    
    logging.info("[ETL Chimio] 3 - Chargement des deux tables terminé avec succès.")


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
