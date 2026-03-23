import json
import logging
import os
from datetime import date, datetime, time

import pandas as pd

from osiris_oncopole.utils.db import connect_to_chimio
from osiris_oncopole.utils.sql_loader import load_sql


DEBUG_LOG_SAMPLE_LIMIT = 12


def _normalize_num_doss(value):
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized.lower() in ("", "none", "null", "nan"):
        return None
    return normalized


def _log_debug_num_doss_rows(stage: str, debug_num_doss: str | None, rows: list[dict]):
    if not debug_num_doss:
        return

    if not rows:
        logging.info("[Chimio][Debug %s] num_doss=%s -> 0 ligne", stage, debug_num_doss)
        return

    debug_df = pd.DataFrame(rows)
    if "dat_admini" in debug_df.columns:
        debug_df["dat_admini"] = pd.to_datetime(debug_df["dat_admini"], errors="coerce")
        min_date = debug_df["dat_admini"].min()
        max_date = debug_df["dat_admini"].max()
    else:
        min_date = None
        max_date = None

    logging.info(
        "[Chimio][Debug %s] num_doss=%s -> count=%s min_dat_admini=%s max_dat_admini=%s",
        stage,
        debug_num_doss,
        len(debug_df),
        min_date,
        max_date,
    )

    sort_columns = [col for col in ["dat_admini", "jour", "nom_proto", "num_pdt"] if col in debug_df.columns]
    if sort_columns:
        debug_df = debug_df.sort_values(by=sort_columns, na_position="first")

    display_columns = [col for col in ["num_doss", "jour", "dat_admini", "nom_proto", "num_pdt", "ce_etat_chimio"] if col in debug_df.columns]
    if not display_columns:
        display_columns = list(debug_df.columns)

    for row in debug_df[display_columns].head(DEBUG_LOG_SAMPLE_LIMIT).to_dict("records"):
        logging.info("[Chimio][Debug %s] row=%s", stage, row)


def extract_data_from_oracle(query_input):
    """
    Execute une requete SQL sur Oracle et retourne un DataFrame.
    """
    conn = connect_to_chimio()
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


def _json_default(value):
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value


def extract_query_to_jsonl(
    query_input: str,
    output_path: str,
    chunk_size: int = 20000,
    debug_num_doss: str | None = None,
    debug_stage: str | None = None,
) -> int:
    """
    Execute une requete Oracle en streaming et ecrit les resultats en JSONL.
    Retourne le nombre total de lignes ecrites.
    """
    conn = connect_to_chimio()
    cursor = conn.cursor()
    normalized_debug_num_doss = _normalize_num_doss(debug_num_doss)

    if query_input.strip().lower().endswith(".sql"):
        sql = load_sql(query_input)
    else:
        sql = query_input

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    wrote = 0
    debug_rows = []

    try:
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]

        with open(output_path, "w", encoding="utf-8") as output_file:
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    obj = {columns[i]: row[i] for i in range(len(columns))}
                    output_file.write(json.dumps(obj, ensure_ascii=False, default=_json_default) + "\n")
                    wrote += 1
                    if _normalize_num_doss(obj.get("num_doss")) == normalized_debug_num_doss:
                        debug_rows.append(obj)
    finally:
        cursor.close()
        conn.close()

    logging.info("[Chimio] %s -> %s (%s lignes)", query_input, output_path, wrote)
    _log_debug_num_doss_rows(debug_stage or query_input, normalized_debug_num_doss, debug_rows)
    return wrote
