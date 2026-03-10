import logging
import os

import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from osiris_oncopole.utils.db import connect_to_qprod
from osiris_oncopole.utils.sql_loader import load_sql


BATCH_SIZE = 5000
OUTDIR = "/tmp/etl_iris"
OUTFILE = os.path.join(OUTDIR, "ordonnance_sortie.jsonl")
TARGET_TABLE = "osiris.ordonnance_sortie"
EXPECTED_COLUMNS = [
    "ipp_ocr",
    "date_ordonnance",
    "type_produit",
    "libelle_produit",
    "posologie",
    "vmp",
    "duree",
    "fac_code",
]


def _clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]

    missing_columns = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_columns:
        raise KeyError(f"Colonnes manquantes depuis Oracle: {missing_columns}")

    df = df[EXPECTED_COLUMNS].copy()
    df["date_ordonnance"] = pd.to_datetime(df["date_ordonnance"], errors="coerce").dt.date
    return df


def _flush_buffer(pg_cur, buffer):
    if not buffer:
        return 0
    execute_values(
        pg_cur,
        f"""
        INSERT INTO {TARGET_TABLE} (
            ipp_ocr,
            date_ordonnance,
            type_produit,
            libelle_produit,
            posologie,
            vmp,
            duree,
            fac_code
        ) VALUES %s
        """,
        buffer,
    )
    written = len(buffer)
    buffer.clear()
    return written


def extract_and_load_ordonnance_sortie():
    logging.info("Start extraction ordonnance_sortie from QPROD")

    ora_conn = connect_to_qprod("QPROD")
    try:
        sql_query = load_sql("extract_ordonnance_sortie.sql")
        df = pd.read_sql(sql_query, ora_conn)
    finally:
        ora_conn.close()

    logging.info("Oracle rows extracted: %d", len(df))
    df = _prepare_dataframe(df)

    os.makedirs(OUTDIR, exist_ok=True)
    df.to_json(OUTFILE, orient="records", lines=True, force_ascii=False, date_format="iso")
    logging.info("JSONL written: %s", OUTFILE)

    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    inserted_rows = 0
    try:
        logging.info("Truncate target table: %s", TARGET_TABLE)
        pg_cur.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
        pg_conn.commit()

        buffer = []
        for row in df.itertuples(index=False):
            buffer.append(
                (
                    _clean_text(row.ipp_ocr),
                    row.date_ordonnance,
                    _clean_text(row.type_produit),
                    _clean_text(row.libelle_produit),
                    _clean_text(row.posologie),
                    _clean_text(row.vmp),
                    _clean_text(row.duree),
                    _clean_text(row.fac_code),
                )
            )
            if len(buffer) >= BATCH_SIZE:
                inserted_rows += _flush_buffer(pg_cur, buffer)
                pg_conn.commit()

        inserted_rows += _flush_buffer(pg_cur, buffer)
        pg_conn.commit()
    finally:
        pg_cur.close()
        pg_conn.close()

    logging.info("Load completed in %s with %d rows", TARGET_TABLE, inserted_rows)
