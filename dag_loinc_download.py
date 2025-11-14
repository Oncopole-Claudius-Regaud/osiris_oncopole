
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import subprocess
import zipfile
import pandas as pd
import psycopg2
import requests


# ===  Paramètres généraux ===
OUTPUT_DIR = "/tmp/smt"
TARGET_TABLE = "ref_source_externe.loinc_extract_fr"


# ===============================
#   UTILITAIRES SMT
# ===============================

def get_latest_version(terminology_id: str, api_key: str) -> str:
    """Récupère la dernière version disponible d’une terminologie SMT"""
    url = f"https://smt.esante.gouv.fr/wp-json/ans/terminologies/versions-details?terminologyId={terminology_id}"
    headers = {"accept": "application/json", "X-API-KEY": api_key}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    if not data:
        raise ValueError(f"Aucune version trouvée pour {terminology_id}")
    latest_version = data[0]["versionInfo"]
    print(f"✅ Dernière version trouvée pour {terminology_id} : {latest_version}")
    return latest_version


def download_loinc_zip():
    """Télécharge directement le ZIP LOINC Biologie (FRA) sans vérifier la version"""
    api_key = Variable.get("smt_api_key", default_var=None)
    if not api_key:
        raise ValueError("Variable Airflow 'smt_api_key' manquante")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    url = (
        "https://smt.esante.gouv.fr/wp-json/ans/terminologies/zip?"
        "terminologyId=terminologie-loinc-biologie-fra"
        "&version=2.23%20(2.79-2.80)"
        "&licenceConsent=true&dataTransferConsent=true&sizeConsent=true"
    )

    output_path = f"{OUTPUT_DIR}/loinc-biologie.zip"
    print(f"⬇️ Téléchargement direct LOINC : {url}")

    cmd = [
        "/usr/bin/curl",
        "-s", "-X", "GET", url,
        "-H", "accept: application/zip",
        "-H", f"X-API-KEY: {api_key}",
        "-o", output_path
    ]

    subprocess.run(cmd, check=True)
    print(f"📦 ZIP LOINC téléchargé : {output_path}")


def download_snomed_zip():
    """Télécharge automatiquement la dernière version de SNOMED CT FR"""
    api_key = Variable.get("smt_api_key", default_var=None)
    if not api_key:
        raise ValueError("Variable Airflow 'smt_api_key' manquante")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Récupération dynamique de la version SNOMED
    version = get_latest_version("terminologie-snomed-ct-fr", api_key)
    safe_version = version.replace(" ", "%20")

    url = (
        f"https://smt.esante.gouv.fr/wp-json/ans/terminologies/zip?"
        f"terminologyId=terminologie-snomed-ct-fr&version={safe_version}"
        f"&licenceConsent=true&dataTransferConsent=true&sizeConsent=true"
    )

    output_path = f"{OUTPUT_DIR}/snomed-ct-fr.zip"
    print(f"⬇️ Téléchargement SNOMED : {url}")

    cmd = [
        "/usr/bin/curl",
        "-s", "-X", "GET", url,
        "-H", "accept: application/zip",
        "-H", f"X-API-KEY: {api_key}",
        "-o", output_path
    ]

    subprocess.run(cmd, check=True)
    print(f"📦 ZIP SNOMED téléchargé : {output_path}")


# ===============================
#   EXTRACTION + LOAD LOINC
# ===============================

def extract_and_load_to_postgres():
    """Unzip, lecture Excel, nettoyage, et chargement PostgreSQL"""
    ZIP_PATH = f"{OUTPUT_DIR}/loinc-biologie.zip"
    EXCEL_NAME = "LOINCFR_JeuDeValeurs.xlsx"

    if not os.path.exists(ZIP_PATH):
        raise FileNotFoundError(f"Fichier ZIP introuvable : {ZIP_PATH}")

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(OUTPUT_DIR)
    print(f"Archive extraite dans {OUTPUT_DIR}")

    extracted_dirs = [
        os.path.join(OUTPUT_DIR, d)
        for d in os.listdir(OUTPUT_DIR)
        if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and "loinc" in d.lower()
    ]
    if not extracted_dirs:
        raise FileNotFoundError("Aucun dossier LOINC trouvé après extraction.")
    base_dir = extracted_dirs[0]
    dat_dir = os.path.join(base_dir, "dat")

    excel_path = os.path.join(dat_dir, EXCEL_NAME)
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Fichier Excel introuvable : {excel_path}")

    print(f"Fichier Excel trouvé : {excel_path}")

    xls = pd.ExcelFile(excel_path)
    sheet_name = [s for s in xls.sheet_names if "Jeu" in s][0]
    df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
    df = df.iloc[1:, :].reset_index(drop=True)
    df = df.fillna("").astype(str)

    expected_columns = [
        "code_loinc",
        "libelle_francais_ref",
        "composant_anglais",
        "composant_francais",
        "synonymes",
        "grandeur",
        "temps",
        "milieu_biologique",
        "echelle",
        "technique",
        "chapitre",
        "date_de_validation",
        "version_vp",
        "statut",
        "version_item_corrige",
    ]

    if len(df.columns) < len(expected_columns):
        raise ValueError(f"Trop peu de colonnes ({len(df.columns)}) dans le fichier Excel.")

    df = df.iloc[:, :len(expected_columns)]
    df.columns = expected_columns

    print(f"Colonnes renommées : {df.columns.tolist()}")
    print(f"DataFrame nettoyé ({len(df)} lignes).")

    conn = BaseHook.get_connection("postgres_test")
    conn_params = {
        "host": conn.host,
        "port": conn.port,
        "dbname": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }
    connection = psycopg2.connect(**conn_params)
    cursor = connection.cursor()

    cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
    print(f"Table {TARGET_TABLE} vidée")

    insert_query = f"""
        INSERT INTO {TARGET_TABLE} (
            code_loinc, libelle_francais_ref, composant_anglais, composant_francais, synonymes,
            grandeur, temps, milieu_biologique, echelle, technique, chapitre,
            date_de_validation, version_vp, statut, version_item_corrige
        ) VALUES ({', '.join(['%s'] * len(expected_columns))})
    """

    data = [tuple(x) for x in df.to_numpy()]
    cursor.executemany(insert_query, data)
    connection.commit()

    cursor.close()
    connection.close()
    print(f"{len(df)} lignes insérées dans {TARGET_TABLE}")


# ===============================
#   DÉFINITION DU DAG
# ===============================

with DAG(
    dag_id="extract_SMT",
    description="Télécharge et charge LOINC, puis télécharge SNOMED depuis SMT",
    start_date=datetime(2025, 11, 14),
    schedule_interval="@monthly",
    catchup=False,
    tags=["smt", "loinc", "snomed", "etl"],
) as dag:

    # Télécharger LOINC (sans version dynamic)
    download_loinc = PythonOperator(
        task_id="download_loinc_zip",
        python_callable=download_loinc_zip,
    )

    # Charger LOINC dans PostgreSQL
    load_loinc = PythonOperator(
        task_id="extract_and_load_to_postgres",
        python_callable=extract_and_load_to_postgres,
    )

    # Télécharger SNOMED (avec version dynamique)
    download_snomed = PythonOperator(
        task_id="download_snomed_zip",
        python_callable=download_snomed_zip,
    )

    
    download_loinc >> load_loinc >> download_snomed

