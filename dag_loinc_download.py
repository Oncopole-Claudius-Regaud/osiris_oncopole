
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
import lxml.etree as ET
import csv

# === Configuration des chemins et tables ===
OUTPUT_DIR = "/tmp/smt"
LOINC_TARGET_TABLE = "ref_source_externe.loinc_extract_fr"

# Tables SNOMED (VARCHAR)
TABLE_CONCEPTS = "ref_source_externe.snomed_concepts"
TABLE_HIERARCHY = "ref_source_externe.snomed_hierarchy"
TABLE_RELATIONS = "ref_source_externe.snomed_relations"

# CSV temporaires pour SNOMED (Bulk Load)
CSV_CONCEPTS = f"{OUTPUT_DIR}/snomed_concepts.csv"
CSV_HIERARCHY = f"{OUTPUT_DIR}/snomed_hierarchy.csv"
CSV_RELATIONS = f"{OUTPUT_DIR}/snomed_relations.csv"

# ===============================
#   UTILITAIRES COMMUNS
# ===============================

def get_db_connection():
    """Connexion via le Hook Airflow postgres_test"""
    conn = BaseHook.get_connection("postgres_test")
    return psycopg2.connect(
        host=conn.host, port=conn.port, dbname=conn.schema,
        user=conn.login, password=conn.password
    )

def get_latest_version(terminology_id, api_key):
    """Récupère dynamiquement la version SMT la plus récente"""
    url = f"https://smt.esante.gouv.fr/wp-json/ans/terminologies/versions-details?terminologyId={terminology_id}"
    headers = {"accept": "application/json", "X-API-KEY": api_key}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()[0]["versionInfo"]

# ===============================
#   PARTIE LOINC
# ===============================

def download_loinc_zip():
    api_key = Variable.get("smt_api_key")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    url = ("https://smt.esante.gouv.fr/wp-json/ans/terminologies/zip?"
           "terminologyId=terminologie-loinc-biologie-fra&version=2.23%20(2.79-2.80)"
           "&licenceConsent=true&dataTransferConsent=true&sizeConsent=true")
    output_path = f"{OUTPUT_DIR}/loinc-biologie.zip"
    subprocess.run(["curl", "-s", "-X", "GET", url, "-H", f"X-API-KEY: {api_key}", "-o", output_path], check=True)

def extract_and_load_loinc():
    ZIP_PATH = f"{OUTPUT_DIR}/loinc-biologie.zip"
    extract_path = f"{OUTPUT_DIR}/loinc_unzipped"
    
    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(extract_path)
    
    # 1. Recherche récursive du fichier Excel
    excel_path = None
    for root, _, files in os.walk(extract_path):
        for f in files:
            if f.endswith(".xlsx") and "LOINC" in f.upper():
                excel_path = os.path.join(root, f)
                break
    
    if not excel_path:
        raise FileNotFoundError("Fichier Excel LOINC introuvable dans l'archive.")

    # 2. Identification dynamique de l'onglet de données (cherche 15 colonnes)
    xl = pd.ExcelFile(excel_path)
    correct_sheet = None
    for sheet in xl.sheet_names:
        temp_df = pd.read_excel(excel_path, sheet_name=sheet, nrows=5, skiprows=1)
        if len(temp_df.columns) >= 15:
            correct_sheet = sheet
            break
    
    if not correct_sheet:
        correct_sheet = xl.sheet_names[1] # Fallback historique

    # 3. Lecture et formatage
    df = pd.read_excel(excel_path, sheet_name=correct_sheet, dtype=str, skiprows=1)
    df = df.fillna("").astype(str)

    expected_columns = [
        "code_loinc", "libelle_francais_ref", "composant_anglais", "composant_francais", 
        "synonymes", "grandeur", "temps", "milieu_biologique", "echelle", "technique", 
        "chapitre", "date_de_validation", "version_vp", "statut", "version_item_corrige"
    ]
    df = df.iloc[:, :15] # On force 15 colonnes
    df.columns = expected_columns

    # 4. Insertion PostgreSQL
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {LOINC_TARGET_TABLE};")
        placeholders = ", ".join(["%s"] * 15)
        columns_str = ", ".join(expected_columns)
        insert_query = f"INSERT INTO {LOINC_TARGET_TABLE} ({columns_str}) VALUES ({placeholders})"
        cursor.executemany(insert_query, [tuple(x) for x in df.to_numpy()])
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ===============================
#   PARTIE SNOMED
# ===============================

def download_snomed_zip():
    api_key = Variable.get("smt_api_key")
    version = get_latest_version("terminologie-snomed-ct-fr", api_key)
    url = (f"https://smt.esante.gouv.fr/wp-json/ans/terminologies/zip?"
           f"terminologyId=terminologie-snomed-ct-fr&version={version.replace(' ', '%20')}"
           f"&licenceConsent=true&dataTransferConsent=true&sizeConsent=true")
    output_path = f"{OUTPUT_DIR}/snomed-ct-fr.zip"
    subprocess.run(["curl", "-s", "-X", "GET", url, "-H", f"X-API-KEY: {api_key}", "-o", output_path], check=True)

def parse_snomed_owl():
    ZIP_PATH = f"{OUTPUT_DIR}/snomed-ct-fr.zip"
    extract_path = f"{OUTPUT_DIR}/snomed_unzipped"
    
    # Extraction (Indispensable pour rafraîchir les fichiers)
    if os.path.exists(extract_path):
        import shutil
        shutil.rmtree(extract_path)
    os.makedirs(extract_path, exist_ok=True)
    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    owl_path = None
    for root, _, files in os.walk(extract_path):
        for f in files:
            if f.endswith(".owl"):
                owl_path = os.path.join(root, f)
                break
    
    # Dictionnaire des Namespaces pour lxml
    ns = {
        'owl': 'http://www.w3.org/2002/07/owl#', 
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#'
    }

    with open(CSV_CONCEPTS, 'w', newline='', encoding='utf-8') as fc, \
         open(CSV_HIERARCHY, 'w', newline='', encoding='utf-8') as fh, \
         open(CSV_RELATIONS, 'w', newline='', encoding='utf-8') as fr:
        
        wc = csv.DictWriter(fc, fieldnames=['sctid', 'label_fr', 'label_en'])
        wh = csv.DictWriter(fh, fieldnames=['child_id', 'parent_id'])
        wr = csv.DictWriter(fr, fieldnames=['source_id', 'type_id', 'destination_id'])
        for w in [wc, wh, wr]: w.writeheader()

        # On utilise l'analyseur lxml plus souple
        tree = ET.iterparse(owl_path, events=('end',), tag='{http://www.w3.org/2002/07/owl#}Class')
        
        for _, elem in tree:
            # Récupération de l'ID : on cherche about OU ID
            about = elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about') or \
                    elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}ID')
            
            if not about:
                continue
                
            sctid = about.split('/')[-1].split('#')[-1] # Gère les deux formats d'URL

            # 1. Labels
            labels = {l.get('{http://www.w3.org/XML/1998/namespace}lang'): l.text for l in elem.findall('rdfs:label', ns)}
            wc.writerow({'sctid': sctid, 'label_fr': labels.get('fr', ''), 'label_en': labels.get('en', '')})

            # 2. Hiérarchie
            for sub in elem.xpath('.//*[local-name()="subClassOf"][@rdf:resource]', namespaces=ns):
                parent_url = sub.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
                wh.writerow({'child_id': sctid, 'parent_id': parent_url.split('/')[-1]})

            # 3. Relations (Version corrigée selon ton test)
            # On cherche toutes les restrictions peu importe le namespace
            for rest in elem.xpath('.//*[local-name()="Restriction"]'):
                # On cherche la propriété et la destination via n'importe quelle balise ayant un rdf:resource
                prop_res = rest.xpath('.//*[local-name()="onProperty"]/@rdf:resource', namespaces=ns)
                dest_res = rest.xpath('.//*[local-name()="someValuesFrom"]//@rdf:resource', namespaces=ns)

                if prop_res and dest_res:
                    type_id = prop_res[0].split('/')[-1]
                    dest_id = dest_res[0].split('/')[-1]
                    
                    if type_id != "609096000" and dest_id != sctid:
                        wr.writerow({
                            'source_id': sctid,
                            'type_id': type_id,
                            'destination_id': dest_id
                        })

            elem.clear()

def load_snomed_to_postgres():
    conn = get_db_connection()
    cursor = conn.cursor()
    mapping = [
        (TABLE_CONCEPTS, CSV_CONCEPTS, "(sctid, label_fr, label_en)"),
        (TABLE_HIERARCHY, CSV_HIERARCHY, "(child_id, parent_id)"),
        (TABLE_RELATIONS, CSV_RELATIONS, "(source_id, type_id, destination_id)")
    ]
    try:
        for table, path, cols in mapping:
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            with open(path, 'r', encoding='utf-8') as f:
                cursor.copy_expert(f"COPY {table} {cols} FROM STDIN WITH CSV HEADER", f)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ===============================
#   DAG DEFINITION
# ===============================

with DAG(
    dag_id="ETL_SMT_LOINC_SNOMED_STABLE",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["smt", "biologie", "snomed"]
) as dag:

    t1 = PythonOperator(task_id="download_loinc", python_callable=download_loinc_zip)
    t2 = PythonOperator(task_id="load_loinc", python_callable=extract_and_load_loinc)
    t3 = PythonOperator(task_id="download_snomed", python_callable=download_snomed_zip)
    t4 = PythonOperator(task_id="parse_snomed_owl", python_callable=parse_snomed_owl)
    t5 = PythonOperator(task_id="load_snomed", python_callable=load_snomed_to_postgres)

    t1 >> t2 >> t3 >> t4 >> t5
