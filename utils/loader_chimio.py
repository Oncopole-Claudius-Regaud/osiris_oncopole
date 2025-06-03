from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from utils.transform import clean_int
import pandas as pd

def load_treatment_lines(df):
    postgres = PostgresHook(postgres_conn_id="postgres_chimio")

    for _, row in df.iterrows():

        start_date = row['start_date']
        end_date = row['end_date']

        # Convertir NaT en None
        if pd.isna(start_date):
            start_date = None
        if pd.isna(end_date):
            end_date = None

        postgres.run(
            """
            INSERT INTO osiris.treatment_line (
                patient_id, noobspat, treatment_line_number, treatment_label,
                treatment_comment, protocol_name, protocol_detail,
                protocol_category, protocol_type, local_code,
                valid_protocol, start_date, end_date, nb_cycles,
                radiation, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash) DO NOTHING
            """,
            parameters=(
                row["patient_id"],
                row["noobspat"],
                row["treatment_line_number"],
                row["treatment_label"],
                row["treatment_comment"],
                row.get("protocol_name"),
                row.get("protocol_detail"),
                row.get("protocol_category"),
                row.get("protocol_type"),
                clean_int(row.get("local_code")),
                clean_int(row.get("valid_protocol")),
                start_date,
                end_date,
                row["nb_cycles"],
                row["radiation"],
                row["record_hash"]
            )
        )
    logging.info(f"{len(df)} lignes de traitement insérées.")


def load_treatment_cycles(df):
    postgres = PostgresHook(postgres_conn_id="postgres_chimio")
    df = df.where(pd.notnull(df), None)

    for _, row in df.iterrows():

        start_date = row['start_date']
        end_date = row['end_date']

        # Convertir NaT en None
        if pd.isna(start_date):
            start_date = None
        if pd.isna(end_date):
            end_date = None

        postgres.run(
            """
            INSERT INTO osiris.treatment_cycle (
                patient_id, noobspat, cycle_number, start_date, end_date, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash) DO NOTHING
            """,
            parameters=(
                row["patient_id"],
                row["noobspat"],
                row["cycle_number"],
                start_date,
                end_date,
                row["record_hash"]
            )
        )
    logging.info(f"{len(df)} cycles insérés.")


def load_drug_administrations(df):
    postgres = PostgresHook(postgres_conn_id="postgres_chimio")
    df = df.where(pd.notnull(df), None)

    for _, row in df.iterrows():

        start_date = row['start_date']
        end_date = row['end_date']

        # Convertir NaT en None
        if pd.isna(start_date):
            start_date = None
        if pd.isna(end_date):
            end_date = None

        postgres.run(
            """
            INSERT INTO osiris.drug_administration (
                patient_id, noobspat, cycle_number, start_date, end_date,
                protocol_name, protocol_type, codepdt, drug_name, code_voie,
                unite, dose_adm, is_real_drug, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash) DO NOTHING
            """,
            parameters=(
                row["patient_id"],
                row["noobspat"],
                row["cycle_number"],
                start_date,
                end_date,
                row.get("protocol_name"),
                row.get("protocol_type"),
                clean_int(row.get("codepdt")),
                row.get("drug_name"),
                row.get("code_voie"),
                clean_int(row.get("unite")),
                clean_int(row.get("dose_adm")),
                bool(row["is_real_drug"]),
                row["record_hash"]
            )
        )
    logging.info(f"{len(df)} médicaments insérés.")

