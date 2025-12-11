import logging
import json
import os
from tempfile import gettempdir
import datetime
import cx_Oracle

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable
import psycopg2.extras
import psycopg2.errors

# Assurez-vous que cette fonction connect_to_oracle est disponible
from utils.db import connect_to_oracle

# ----------------------------------------------------
# PARAMÈTRES ET MAPPINGS
# ----------------------------------------------------

# Paramètres généraux
SCHEMA_SOURCE = "EAIDATA"
TABLE_SOURCE = "REF_PATIENT"
CONN_ID_POSTGRES_DEFAULT = "postgres_test"
BATCH_SIZE = 5000 # Taille du bloc pour l'insertion par execute_batch

# MAPPING 1 : Colonnes Oracle (Source) -> PostgreSQL (osiris.patient - Cible)
COLONNES_MAPPING_OSIRIS = {
    'REFP_IPP': 'ipp_ocr',
    'REFP_IPP_CHU': 'ipp_chu',
    'REFP_SEXE': 'gender',
    'REFP_DC_DATE': 'date_of_death',
    'REFP_NOM_UTILISE': 'nom',
    'REFP_PRENOM_UTILISE': 'prenom',
    'REFP_NAISS_DATE': 'date_of_birth',
    'REFP_NAISS_VILLE': 'birth_city',
}
SCHEMA_CIBLE_OSIRIS = "osiris"
TABLE_CIBLE_OSIRIS = "patient"

# MAPPING 2 : JSONL (Source) -> PostgreSQL (oeci.patient_trackare - Cible)
# Les clés sont les colonnes du JSONL (qui proviennent des ALIAS SQL en MAJUSCULES de l'extraction).
# Les valeurs sont les colonnes cibles PostgreSQL (en minuscules).
COLONNES_MAPPING_TRACKARE = {
    'IPP_OCR': 'ipp_ocr',
    'IPP_CHU': 'ipp_chu',
    'NOM': 'nom',
    'PRENOM': 'prenom',
    'DATE_OF_BIRTH': 'date_naissance', 
    'GENDER': 'sexe',                    
    'DATE_OF_DEATH': 'date_dc',           
    'BIRTH_CITY': 'ville_naissance' 
    # 'source_deces' n'est pas mappé ici car il n'est pas dans le JSONL
}
SCHEMA_CIBLE_TRACKARE = "oeci"
TABLE_CIBLE_TRACKARE = "patients_trackcare"


def get_temp_file_path(dag_run_id):
    """Retourne un chemin unique pour le fichier temporaire (JSONL)."""
    # Utilise le run_id pour garantir l'unicité du fichier par exécution du DAG
    return os.path.join(gettempdir(), f"patient_data_{dag_run_id}.jsonl")

# ----------------------------------------------------
# FONCTION 1 : EXTRACTION ORACLE -> JSONL
# ----------------------------------------------------

def extraire_donnees_patient(**context):
    """
    Établit la connexion Oracle, filtre les lignes sans ID, stocke en JSONL.
    """
    logging.info(f"🚀 Début de l'extraction JSONL depuis {SCHEMA_SOURCE}.{TABLE_SOURCE}")

    oracle_conn = None
    oracle_cursor = None

    dag_run_id = context['dag_run'].run_id
    temp_file = get_temp_file_path(dag_run_id)
    logging.info(f"💾 Fichier temporaire cible : {temp_file}")

    # Préparation de la requête SELECT, utilisant le mapping OSIRIS pour les ALIAS SQL
    select_clauses = [f"{oracle_col} AS {alias.upper()}" for oracle_col, alias in COLONNES_MAPPING_OSIRIS.items()]
    select_list = ", ".join(select_clauses)

    sql_query = f"""
    SELECT {select_list}
    FROM {SCHEMA_SOURCE}.{TABLE_SOURCE}
    WHERE REFP_IPP IS NOT NULL
      AND TRIM(REFP_IPP) IS NOT NULL
    """
    logging.info(f"Requête Oracle exécutée : {sql_query.strip()}")

    try:
        oracle_conn = connect_to_oracle()
        oracle_cursor = oracle_conn.cursor()
        logging.info("✅ Connexion Oracle établie.")

        oracle_cursor.execute(sql_query)

        # Les noms de colonnes seront en MAJUSCULES grâce aux ALIAS
        colonnes_cible = [col[0] for col in oracle_cursor.description]
        logging.info(f"Colonnes extraites (Ordre Oracle): {colonnes_cible}")

        row_count = 0
        log_count = 0

        # Écriture des données en JSONL
        with open(temp_file, 'w', encoding='utf-8') as f:
            for row in oracle_cursor:
                record = dict(zip(colonnes_cible, row))

                # --- LOG DE DÉBOGAGE (20 premières lignes) ---
                if log_count < 20:
                    logging.info(f"LOG Ligne {row_count + 1}: {record}")
                    log_count += 1
                # -----------------------

                # Gestion de la sérialisation des objets datetime
                for key, value in record.items():
                    if isinstance(value, (datetime.datetime, datetime.date)):
                        record[key] = value.isoformat()

                f.write(json.dumps(record) + '\n')
                row_count += 1

        logging.info(f"⭐ Extraction terminée. {row_count} lignes écrites en JSONL.")

        return temp_file

    except Exception as e:
        logging.error(f"❌ Erreur lors de l'extraction : {e}")
        raise
    finally:
        if oracle_cursor: oracle_cursor.close()
        if oracle_conn: oracle_conn.close()
        logging.info("🚪 Connexion Oracle fermée.")


# ----------------------------------------------------
# FONCTION 2 : CHARGEMENT JSONL -> osiris.patient
# ----------------------------------------------------

def charger_donnees_patient(ti, **context):
    """
    Récupère le chemin du fichier JSONL et insère les données dans osiris.patient.
    """
    logging.info(f"🚀 Début du chargement vers {SCHEMA_CIBLE_OSIRIS}.{TABLE_CIBLE_OSIRIS}")
    
    extraction_task_id = 'extraire_donnees_oracle'
    temp_file = ti.xcom_pull(task_ids=extraction_task_id, key='return_value')

    if not temp_file or not os.path.exists(temp_file):
        logging.warning("⚠️ Chemin du fichier temporaire non trouvé ou non existant. Chargement osiris ignoré.")
        return 0

    pg_conn = None
    pg_cur = None

    try:
        conn_id = Variable.get("target_pg_conn_id", default_var=CONN_ID_POSTGRES_DEFAULT)
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        pg_conn = pg_hook.get_conn()
        pg_cur = pg_conn.cursor()
        logging.info(f"✅ Connexion PostgreSQL établie à '{conn_id}'.")

        lignes_a_inserer = []
        # Colonnes cibles PostgreSQL (en minuscules)
        colonnes_cible = list(COLONNES_MAPPING_OSIRIS.values())

        with open(temp_file, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                
                # ⬅️ CORRECTION DE CASSE : Utilise la clé du JSONL (MAJUSCULES)
                values = [record.get(col.upper()) for col in colonnes_cible]
                lignes_a_inserer.append(tuple(values))

        if not lignes_a_inserer:
            logging.warning("⚠️ Le fichier JSONL est vide. Aucune insertion dans osiris.patient.")
            return 0

        colonnes_str = ", ".join(colonnes_cible)
        valeurs_placeholders = ", ".join(["%s"] * len(colonnes_cible))
        upsert_clause = f"ON CONFLICT (ipp_ocr) DO NOTHING"

        sql_insert = f"""
        INSERT INTO {SCHEMA_CIBLE_OSIRIS}.{TABLE_CIBLE_OSIRIS} ({colonnes_str})
        VALUES ({valeurs_placeholders})
        {upsert_clause};
        """

        logging.info(f"Requête INSERT utilisée : {sql_insert.strip()}")
        logging.info("LOG Tuples Python prêts à insérer (5 premiers) :")
        for j, tuple_data in enumerate(lignes_a_inserer[:5]):
            logging.info(f"Tuple {j+1}: {tuple_data}")

        logging.info(f"▶️ Début du chargement de {len(lignes_a_inserer)} lignes par blocs de {BATCH_SIZE}...")

        lignes_inserees_total = 0

        # Itération par blocs avec gestion des erreurs
        for i in range(0, len(lignes_a_inserer), BATCH_SIZE):
            batch = lignes_a_inserer[i:i + BATCH_SIZE]
            try:
                psycopg2.extras.execute_batch(pg_cur, sql_insert, batch)
                lignes_inserees_total += pg_cur.rowcount
                pg_conn.commit()
            except psycopg2.errors.NotNullViolation as e:
                pg_conn.rollback()
                logging.warning(f"⚠️ Bloc de lignes {i} à {i+len(batch)} IGNORÉ (Violation NOT NULL) : {e.pgerror.strip()}")
            except Exception as e:
                logging.error(f"❌ Erreur critique lors du chargement d'un bloc : {e}")
                pg_conn.rollback()
                raise

        logging.info(f"⭐ Chargement terminé. {lignes_inserees_total} lignes insérées (uniques).")
        return lignes_inserees_total

    except Exception as e:
        logging.error(f"❌ Erreur générale lors du chargement de osiris.patient : {e}")
        if pg_conn: pg_conn.rollback()
        raise
    finally:
        if pg_cur: pg_cur.close()
        if pg_conn:
            pg_conn.close()
            logging.info("🚪 Connexion PostgreSQL fermée (osiris.patient).")


# ----------------------------------------------------
# FONCTION 3 : CHARGEMENT JSONL -> oeci.patient_trackare (Nouveau)
# ----------------------------------------------------

def charger_patient_tracker(ti, **context):
    """
    Récupère le chemin du fichier JSONL et insère les données dans oeci.patient_trackare.
    """
    logging.info(f"🚀 Début du chargement vers {SCHEMA_CIBLE_TRACKARE}.{TABLE_CIBLE_TRACKARE}")
    
    extraction_task_id = 'extraire_donnees_oracle'
    temp_file = ti.xcom_pull(task_ids=extraction_task_id, key='return_value')

    if not temp_file or not os.path.exists(temp_file):
        logging.warning("⚠️ Chemin du fichier temporaire non trouvé ou non existant. Chargement trackare ignoré.")
        return 0

    pg_conn = None
    pg_cur = None

    try:
        conn_id = Variable.get("target_pg_conn_id", default_var=CONN_ID_POSTGRES_DEFAULT)
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        pg_conn = pg_hook.get_conn()
        pg_cur = pg_conn.cursor()
        logging.info(f"✅ Connexion PostgreSQL établie à '{conn_id}'.")

        lignes_a_inserer = []
        # Colonnes cibles PostgreSQL (en minuscules)
        colonnes_cible = list(COLONNES_MAPPING_TRACKARE.values()) 
        
        # Clés JSONL (en MAJUSCULES)
        json_keys = list(COLONNES_MAPPING_TRACKARE.keys())

        with open(temp_file, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                
                # Mappage : l'ordre des valeurs est déterminé par l'ordre des clés dans COLONNES_MAPPING_TRACKARE
                values = [record.get(key) for key in json_keys]
                
                lignes_a_inserer.append(tuple(values))

        if not lignes_a_inserer:
            logging.warning("⚠️ Le fichier JSONL est vide. Aucune insertion dans oeci.patient_trackare.")
            return 0

        colonnes_str = ", ".join(colonnes_cible)
        valeurs_placeholders = ", ".join(["%s"] * len(colonnes_cible))

        # Gestion des doublons sur la clé primaire (ipp_ocr est présumée)
        upsert_clause = f"ON CONFLICT (ipp_ocr) DO NOTHING"

        sql_insert = f"""
        INSERT INTO {SCHEMA_CIBLE_TRACKARE}.{TABLE_CIBLE_TRACKARE} ({colonnes_str})
        VALUES ({valeurs_placeholders})
        {upsert_clause};
        """

        logging.info(f"Requête INSERT utilisée : {sql_insert.strip()}")
        logging.info(f"Colonnes insérées (Ordre cible) : {colonnes_str}")

        logging.info(f"▶️ Début du chargement de {len(lignes_a_inserer)} lignes par blocs de {BATCH_SIZE}...")

        lignes_inserees_total = 0

        # Itération par blocs avec gestion des erreurs
        for i in range(0, len(lignes_a_inserer), BATCH_SIZE):
            batch = lignes_a_inserer[i:i + BATCH_SIZE]

            try:
                psycopg2.extras.execute_batch(pg_cur, sql_insert, batch)
                lignes_inserees_total += pg_cur.rowcount
                pg_conn.commit()

            except psycopg2.errors.NotNullViolation as e:
                pg_conn.rollback()
                logging.warning(f"⚠️ Bloc de lignes {i} à {i+len(batch)} IGNORÉ (Violation NOT NULL) : {e.pgerror.strip()}")

            except Exception as e:
                logging.error(f"❌ Erreur critique lors du chargement d'un bloc : {e}")
                pg_conn.rollback()
                raise

        logging.info(f"⭐ Chargement terminé. {lignes_inserees_total} lignes insérées (uniques).")
        return lignes_inserees_total

    except Exception as e:
        logging.error(f"❌ Erreur générale lors du chargement de patient_trackare : {e}")
        if pg_conn: pg_conn.rollback()
        raise
    finally:
        if pg_cur: pg_cur.close()
        if pg_conn:
            pg_conn.close()
            logging.info("🚪 Connexion PostgreSQL fermée (patient_trackare).")
