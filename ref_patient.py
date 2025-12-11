
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from datetime import datetime

# Importez TOUTES les fonctions depuis votre fichier de logique, y compris les nouvelles
from ref_patient_extract import (
    extraire_donnees_patient,
    charger_donnees_patient,
    charger_patient_tracker
)

# Le nom du task_id de l'extraction est crucial pour le XCom pull
TASK_ID_EXTRACTION = 'extraire_donnees_oracle'

with DAG(
    dag_id='etl_patient_ref_to_osiris_oeci_jsonl', # Nom du DAG mis à jour
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'oracle', 'postgres', 'patient', 'multiple_target'],
) as dag:

    # 1. Tâche d'Extraction (produit le chemin du fichier JSONL via XCom)
    task_extraction = PythonOperator(
        task_id=TASK_ID_EXTRACTION, 
        python_callable=extraire_donnees_patient,
        provide_context=True,
    )

    # 2. Tâche de Chargement vers OSIRIS (consomme le chemin XCom)
    task_chargement_osiris = PythonOperator(
        task_id='charger_donnees_osiris',
        python_callable=charger_donnees_patient,
        # La fonction a besoin de 'ti' pour xcom_pull
        provide_context=True, 
    )

    # 3. 🆕 Tâche de Chargement vers OECI (consomme le même chemin XCom)
    task_chargement_oeci = PythonOperator(
        task_id='charger_patient_tracker',
        python_callable=charger_patient_tracker,
        # La fonction a besoin de 'ti' pour xcom_pull
        provide_context=True,
    )
    

    # Définition de la séquence des tâches :
    # L'extraction doit se terminer en premier.
    # Les deux chargements peuvent potentiellement être en parallèle (si les workers le permettent)
    # ou en séquence. Ici, nous les mettons en séquence pour la robustesse.
    # Le nettoyage doit se faire APRÈS les deux chargements.
    
    task_extraction >> task_chargement_osiris >> task_chargement_oeci

    # ALTERNATIVE (Plus rapide si le fichier JSONL est sur un disque partagé ou Workers uniques) :
    # task_extraction >> [task_chargement_osiris, task_chargement_oeci] >> task_nettoyage
    # L'approche séquentielle ci-dessus est généralement plus sûre.
