from datetime import date, datetime, time
import json
import logging
import os
import socket

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

from osiris_oncopole.utils.db import connect_to_chimio
from osiris_oncopole.utils.loader_collecteur_acte_icr import load_collecteur_acte_icr
from osiris_oncopole.utils.sql_loader import load_sql


default_args = {
    "owner": "DATA-IA",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    dag_id="etl_collecteur_acte_icr",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL Oracle DMI_ICR.COLLECTEUR_ACTE_ICR -> PostgreSQL",
    tags=["osiris", "chimio", "collecteur_acte_icr"],
)

BASE_PATH = "/tmp/etl_iris"
CHUNK_SIZE = 20000


def _get_worker_host() -> str:
    return socket.gethostname()


def _assert_local_artifact_visibility(producer_host: str, artifact_path: str, producer_task: str):
    current_host = _get_worker_host()
    is_tmp_artifact = os.path.abspath(artifact_path).startswith(os.path.abspath(BASE_PATH))

    if producer_host and is_tmp_artifact and producer_host != current_host:
        raise ValueError(
            "Artefact intermediaire local non partage entre taches Airflow: "
            f"{producer_task} a ecrit {artifact_path!r} sur host={producer_host}, "
            f"mais la tache courante tourne sur host={current_host}. "
            "Utiliser un stockage partage ou une execution sur le meme worker."
        )


def _json_default(value):
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value


def _iter_jsonl_chunks(path: str, chunksize: int):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return iter(())
    try:
        return pd.read_json(path, orient="records", lines=True, chunksize=chunksize)
    except ValueError:
        return iter(())


def extract_and_persist_data(**kwargs):
    logging.info("[ETL Collecteur Acte ICR] 1 - Demarrage extraction Oracle...")

    os.makedirs(BASE_PATH, exist_ok=True)

    output_path = os.path.join(BASE_PATH, "collecteur_acte_icr.jsonl")
    worker_host = _get_worker_host()
    sql = load_sql("extract_collecteur_acte_icr.sql")
    rows_written = 0

    conn = connect_to_chimio()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [column[0].lower() for column in cursor.description]
        with open(output_path, "w", encoding="utf-8") as output_file:
            while True:
                rows = cursor.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                for row in rows:
                    obj = {columns[index]: row[index] for index in range(len(columns))}
                    output_file.write(json.dumps(obj, ensure_ascii=False, default=_json_default) + "\n")
                    rows_written += 1
    finally:
        cursor.close()
        conn.close()

    kwargs["ti"].xcom_push(key="raw_path", value=output_path)
    kwargs["ti"].xcom_push(key="extract_worker_host", value=worker_host)
    kwargs["ti"].xcom_push(key="rows_extracted", value=rows_written)

    logging.info(
        "[ETL Collecteur Acte ICR] 1 - Extraction terminee: %s lignes, path=%s",
        rows_written,
        output_path,
    )


def load_data(**kwargs):
    logging.info("[ETL Collecteur Acte ICR] 2 - Demarrage chargement PostgreSQL...")
    ti = kwargs["ti"]

    raw_path = ti.xcom_pull(task_ids="extract_and_persist", key="raw_path")
    extract_worker_host = ti.xcom_pull(task_ids="extract_and_persist", key="extract_worker_host")
    rows_extracted = ti.xcom_pull(task_ids="extract_and_persist", key="rows_extracted")

    if not raw_path:
        raise ValueError("Chemin du fichier COLLECTEUR_ACTE_ICR manquant.")
    _assert_local_artifact_visibility(extract_worker_host, raw_path, "extract_and_persist")

    first_chunk = True
    total_loaded = 0
    chunk_index = 0
    for df_chunk in _iter_jsonl_chunks(raw_path, CHUNK_SIZE):
        chunk_index += 1
        if df_chunk is None:
            continue
        logging.info(
            "[ETL Collecteur Acte ICR] 2 - Chunk %s lu depuis %s: %s lignes",
            chunk_index,
            raw_path,
            len(df_chunk),
        )
        if "cai_date_real" in df_chunk.columns:
            df_chunk["cai_date_real"] = pd.to_datetime(df_chunk["cai_date_real"], errors="coerce")
        if "cai_date_suppression" in df_chunk.columns:
            df_chunk["cai_date_suppression"] = pd.to_datetime(df_chunk["cai_date_suppression"], errors="coerce")

        total_loaded += len(df_chunk)
        load_collecteur_acte_icr(df_chunk, truncate_table=first_chunk)
        logging.info(
            "[ETL Collecteur Acte ICR] 2 - Chunk %s charge: total=%s lignes",
            chunk_index,
            total_loaded,
        )
        first_chunk = False

    if first_chunk:
        load_collecteur_acte_icr(
            pd.DataFrame(columns=[
                "cai_numdoss",
                "cai_date_real",
                "cai_code_ccam",
                "cai_theme",
                "cai_code_ccam_fact",
                "cai_date_suppression",
            ]),
            truncate_table=True,
        )

    logging.info(
        "[ETL Collecteur Acte ICR] 2 - Chargement termine: loaded=%s extracted=%s",
        total_loaded,
        rows_extracted,
    )


extract_task = PythonOperator(
    task_id="extract_and_persist",
    python_callable=extract_and_persist_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

extract_task >> load_task
