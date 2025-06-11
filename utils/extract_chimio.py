import pandas as pd

import logging
from utils.db import get_postgres_hook, connect_to_oracle
from utils.sql_loader import load_sql
from utils.patients import get_patient_ids
from utils.transform_chimio import (
    clean_dataframe,
    enrich_with_patient_id,
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

    cursor.execute(sql)
    columns = [col[0].lower() for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    cursor.close()
    conn.close()
    return df


def extract_chimio_data():
    """
    Étape d'extraction + nettoyage + enrichissement des données depuis Oracle.
    Renvoie 2 DataFrames : lignes de traitement, médicaments.
    """
    logging.info("[1] Extraction & préparation des données chimiothérapie")

    postgres = get_postgres_hook()
    patients_df = postgres.get_pandas_df(
        "SELECT patient_id, ipp_ocr FROM osiris.patient")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{p}'" for p in patient_ids)

    # Lignes de traitement
    sql_template = load_sql("extract_treatment_line.sql")
    sql = sql_template.format(patient_list=patient_list_sql)
    df_treatment = extract_data_from_oracle(sql)
    df_treatment = clean_dataframe(
        df_treatment, date_columns=[
            "start_date", "end_date"])
    df_treatment = enrich_with_patient_id(
        df_treatment, patients_df, join_key='noobspat')

    expected_cols = [
        "patient_id", "noobspat", "treatment_line_number", "treatment_label",
        "treatment_comment", "protocol_name", "protocol_detail", "protocol_category",
        "protocol_type", "local_code", "valid_protocol", "start_date", "end_date",
        "nb_cycles", "radiation"
    ]
    actual_cols = df_treatment.columns.tolist()
    missing = set(expected_cols) - set(actual_cols)
    print(" Colonnes manquantes dans df_treatment :", missing)

    df_treatment, hash_col_treatment = remove_duplicates_and_hash(df_treatment, [
        "patient_id", "noobspat", "treatment_line_number", "treatment_label",
        "treatment_comment", "protocol_name", "protocol_detail", "protocol_category",
        "protocol_type", "local_code", "valid_protocol", "start_date", "end_date",
        "nb_cycles", "radiation"
    ])
    df_treatment = filter_existing_records(
        df_treatment,
        postgres,
        "osiris.treatment_line",
        hash_col_treatment)

    # Médicaments
    df_drugs = extract_data_from_oracle("extract_drug.sql")
    df_drugs = clean_dataframe(
        df_drugs, date_columns=[
            "start_date", "end_date"])
    df_drugs = enrich_with_patient_id(
        df_drugs, patients_df, join_key='noobspat')
    df_drugs, hash_col_drug = remove_duplicates_and_hash(df_drugs, [
        "patient_id", "noobspat", "cycle_number", "start_date", "end_date",
        "protocol_name", "protocol_type", "codepdt", "drug_name", "code_voie",
        "unite", "dose_adm", "is_real_drug"
    ])
    df_drugs = filter_existing_records(
        df_drugs,
        postgres,
        "osiris.drug_administration",
        hash_col_drug)

    return df_treatment, df_drugs
