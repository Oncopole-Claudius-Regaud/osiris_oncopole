import pandas as pd
import logging
# Les imports suivants sont supposés exister dans votre environnement
from osiris_oncopole.utils.db import get_postgres_hook, oracle_radio
from osiris_oncopole.utils.sql_loader import load_sql
from osiris_oncopole.utils.transform_chimio import clean_dataframe 

# ----------------------------------------------------------------------
# FONCTIONS D'EXTRACTION ORACLE (mêmes que précédemment)
# ----------------------------------------------------------------------

def extract_data_from_oracle(query_input):
    """
    Exécute une requête SQL sur Oracle et retourne un DataFrame.
    """
    conn = oracle_radio()
    cursor = conn.cursor()

    if query_input.strip().lower().endswith(".sql"):
        sql = load_sql(query_input)
    else:
        sql = query_input

    cursor.execute(sql)
    columns = [col[0].lower() for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    cursor.close()
    conn.close()
    return df


def extract_chimio_plan_data():
    """
    Extrait et nettoie toutes les données de planification (CHIMIO_PLAN) tel quel.
    """
    logging.info("    [1.1] Extraction et nettoyage complètes des données de planification (CHIMIO_PLAN)")
    
    df_plan_raw = extract_data_from_oracle("extract_chimio_plan.sql")
    df_plan_raw.columns = df_plan_raw.columns.map(lambda x: x.lower())

    df_plan = clean_dataframe(df_plan_raw, date_columns=["dat_ouv"])
    
    if 'num_doss' in df_plan.columns:
         df_plan['num_doss'] = df_plan['num_doss'].astype(str)
         
    return df_plan[['num_doss', 'dat_ouv', 'code_loc']]


def extract_chimio_data():
    """
    Étape d'extraction des deux DataFrames : df_chimio et df_plan.
    """
    logging.info("[1] Extraction & préparation des données chimiothérapie (Deux tables séparées)")

    # 1. Extraction des données de planification
    df_plan = extract_chimio_plan_data()
    
    # 2. Extraction des données d'administration
    logging.info("    [1.2] Extraction et nettoyage des données d'administration (CHIMIOTHERAPIE)")
    df_chimio_raw = extract_data_from_oracle("extract_chimio.sql")
    df_chimio_raw.columns = df_chimio_raw.columns.map(lambda x: x.lower())

    df_chimio = clean_dataframe(df_chimio_raw, date_columns=["dat_admini"])
    
    if 'num_doss' in df_chimio.columns:
         df_chimio['num_doss'] = df_chimio['num_doss'].astype(str)
         
    df_chimio_final = df_chimio.copy()
    
    logging.info(
        "[OK] Extraction terminée. CHIMIOTHERAPIE: %d lignes. CHIMIO_PLAN: %d lignes.",
        len(df_chimio_final),
        len(df_plan)
    )

    # Retourne les deux DataFrames
    return df_chimio_final, df_plan
