from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from utils.transform_chimio import clean_int
import pandas as pd
from airflow.models import Variable


def _is_valid_ipp(val):
    """
    Vérifie si un ipp_ocr est valide :
    - non nul
    - non vide
    - différent de -1
    """
    if val is None:
        return False
    s = str(val).strip()
    if s == "" or s == "-1" or s.lower() in ("nan", "none", "null"):
        return False
    return True


def _to_none_if_nat(value):
    """Convertit NaT/NaN en None pour insertion SQL."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def load_treatment_lines(df):
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    postgres = PostgresHook(postgres_conn_id=conn_id)

    inserted = 0
    for _, row in df.iterrows():
        ipp_value = row.get("noobspat") or row.get("ipp_ocr")

        # skip si l'IPP est invalide
        if not _is_valid_ipp(ipp_value):
            logging.warning("Ligne ignorée (IPP invalide) : %s", ipp_value)
            continue

        start_date = _to_none_if_nat(row.get("start_date"))
        end_date = _to_none_if_nat(row.get("end_date"))

        postgres.run(
            """
            INSERT INTO osiris.treatment_line (
                ipp_ocr, treatment_line_number, treatment_label,
                treatment_comment, protocol_name, protocol_detail,
                protocol_category, protocol_type, local_code,
                valid_protocol, start_date, end_date, nb_cycles,
                radiation, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash) DO NOTHING
            """,
            parameters=(
                ipp_value,
                row.get("treatment_line_number"),
                row.get("treatment_label"),
                row.get("treatment_comment"),
                row.get("protocol_name"),
                row.get("protocol_detail"),
                row.get("protocol_category"),
                row.get("protocol_type"),
                clean_int(row.get("local_code")),
                clean_int(row.get("valid_protocol")),
                start_date,
                end_date,
                row.get("nb_cycles"),
                row.get("radiation"),
                row.get("record_hash"),
            ),
        )
        inserted += 1

    logging.info(f"{inserted} lignes de traitement insérées.")


def load_drug_administrations(df):
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    postgres = PostgresHook(postgres_conn_id=conn_id)
    df = df.where(pd.notnull(df), None)

    inserted = 0
    for _, row in df.iterrows():
        ipp_value = row.get("noobspat") or row.get("ipp_ocr")

        if not _is_valid_ipp(ipp_value):
            logging.warning("Ligne ignorée (IPP invalide) : %s", ipp_value)
            continue

        start_date = _to_none_if_nat(row.get("start_date"))
        end_date = _to_none_if_nat(row.get("end_date"))

        postgres.run(
            """
            INSERT INTO osiris.drug_administration (
                ipp_ocr, cycle_number, start_date, end_date,
                protocol_name, protocol_type, codepdt, drug_name, code_voie,
                unite, dose_adm, is_real_drug, record_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash) DO NOTHING
            """,
            parameters=(
                ipp_value,
                row.get("cycle_number"),
                start_date,
                end_date,
                row.get("protocol_name"),
                row.get("protocol_type"),
                clean_int(row.get("codepdt")),
                row.get("drug_name"),
                row.get("code_voie"),
                clean_int(row.get("unite")),
                clean_int(row.get("dose_adm")),
                bool(row.get("is_real_drug")),
                row.get("record_hash"),
            ),
        )
        inserted += 1

    logging.info(f"{inserted} médicaments insérés.")

