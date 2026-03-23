import pandas as pd
import logging
import json
import os
from datetime import date, datetime, time
from osiris_oncopole.utils.db import connect_to_chimio
from osiris_oncopole.utils.sql_loader import load_sql

# ----------------------------------------------------------------------
# FONCTIONS D'EXTRACTION ORACLE (mêmes que précédemment)
# ----------------------------------------------------------------------

def extract_data_from_oracle(query_input):
    """
    Exécute une requête SQL sur Oracle et retourne un DataFrame.
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


def extract_query_to_jsonl(query_input: str, output_path: str, chunk_size: int = 20000) -> int:
    """
    Exécute une requête Oracle en streaming et écrit les résultats en JSONL.
    Retourne le nombre total de lignes écrites.
    """
    conn = connect_to_chimio()
    cursor = conn.cursor()

    if query_input.strip().lower().endswith(".sql"):
        sql = load_sql(query_input)
    else:
        sql = query_input

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    wrote = 0

    try:
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]

        with open(output_path, "w", encoding="utf-8") as f:
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    obj = {columns[i]: row[i] for i in range(len(columns))}
                    f.write(json.dumps(obj, ensure_ascii=False, default=_json_default) + "\n")
                    wrote += 1
    finally:
        cursor.close()
        conn.close()

    logging.info("[Chimio] %s -> %s (%s lignes)", query_input, output_path, wrote)
    return wrote
