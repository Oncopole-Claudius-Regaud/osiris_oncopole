import pandas as pd
import logging
from utils.db import get_postgres_hook, connect_to_oracle
from utils.sql_loader import load_sql
from utils.transform_chimio import (
    clean_dataframe,
    remove_duplicates_and_hash,
    filter_existing_records
)


def extract_data_from_oracle(query_input):
    """
    Exécute une requête SQL (fichier ou chaîne) sur Oracle et retourne un DataFrame.
    """
    conn = connect_to_oracle()
    cursor = conn.cursor()

    if query_input.strip().lower().endswith(".sql"):
        sql = load_sql(query_input)
    else:
        sql = query_input

    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1]

    cursor.execute(sql)
    columns = [col[0].lower() for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    cursor.close()
    conn.close()
    return df


def extract_chimio_data():
    """
    Étape d'extraction + nettoyage des données depuis Oracle.
    Renvoie 2 DataFrames : lignes de traitement, médicaments.
    """
    logging.info("[1] Extraction & préparation des données chimiothérapie (NOOBSPAT comme identifiant patient)")

    postgres = get_postgres_hook()

    # --- Lignes de traitement ---
    df_treatment = extract_data_from_oracle("extract_treatment_line.sql")
    df_treatment = clean_dataframe(df_treatment, date_columns=["start_date", "end_date"])

    expected_cols_treatment = [
        "noobspat", "treatment_line_number", "treatment_label",
        "treatment_comment", "protocol_name", "protocol_detail",
        "protocol_category", "protocol_type", "local_code",
        "valid_protocol", "start_date", "end_date",
        "nb_cycles", "radiation"
    ]
    missing_treat = set(expected_cols_treatment) - set(df_treatment.columns)
    if missing_treat:
        logging.warning("Colonnes manquantes dans df_treatment : %s", sorted(missing_treat))

    df_treatment, hash_col_treatment = remove_duplicates_and_hash(
        df_treatment,
        [
            "noobspat", "treatment_line_number", "treatment_label",
            "treatment_comment", "protocol_name", "protocol_detail",
            "protocol_category", "protocol_type", "local_code",
            "valid_protocol", "start_date", "end_date",
            "nb_cycles", "radiation"
        ]
    )
    df_treatment = filter_existing_records(
        df_treatment, postgres, "osiris.treatment_line", hash_col_treatment
    )

    # --- Médicaments ---
    df_drugs = extract_data_from_oracle("extract_drug.sql")
    df_drugs = clean_dataframe(df_drugs, date_columns=["start_date", "end_date"])

    expected_cols_drugs = [
        "noobspat", "cycle_number", "start_date", "end_date",
        "protocol_name", "protocol_type", "codepdt", "drug_name",
        "code_voie", "unite", "dose_adm", "is_real_drug"
    ]
    missing_drug = set(expected_cols_drugs) - set(df_drugs.columns)
    if missing_drug:
        logging.warning("Colonnes manquantes dans df_drugs : %s", sorted(missing_drug))

    df_drugs, hash_col_drug = remove_duplicates_and_hash(
        df_drugs,
        [
            "noobspat", "cycle_number", "start_date", "end_date",
            "protocol_name", "protocol_type", "codepdt", "drug_name",
            "code_voie", "unite", "dose_adm", "is_real_drug"
        ]
    )
    df_drugs = filter_existing_records(
        df_drugs, postgres, "osiris.drug_administration", hash_col_drug
    )

    # --- Log final ---
    logging.info(
        "[OK] Extraction terminée : %d lignes (treatment_line), %d lignes (drug_administration)",
        len(df_treatment), len(df_drugs)
    )

    return df_treatment, df_drugs

