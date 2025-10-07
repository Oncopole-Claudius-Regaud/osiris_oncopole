import os
import logging
from utils.db import connect_to_iris
from utils.extract import extract_measure_data_to_file  # ✅ on réutilise la fonction streaming

TMP_DIR = "/tmp/etl_iris"

def extract_measure_batch_by_date(start_date, end_date, **kwargs):
    """
    Pour chaque période, append les mesures en NDJSON dans /tmp/etl_iris/measures.jsonl
    (plus de merge JSON en mémoire).
    """
    os.makedirs(TMP_DIR, exist_ok=True)
    path = os.path.join(TMP_DIR, "measures.jsonl")

    conn = connect_to_iris()
    cursor = conn.cursor()

    try:
        res = extract_measure_data_to_file(cursor, start_date=start_date, end_date=end_date, path=path, append=True)
        logging.info(f"[MEASURE] Période {start_date}→{end_date}: {res['written']} lignes appendées dans {path}")
        return res
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

