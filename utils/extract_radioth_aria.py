import os
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from osiris_oncopole.utils.db import oracle_radio
from psycopg2.extras import execute_values

BATCH_SIZE = 5000

def extract_and_load_radioth_aria():
    logging.info("🚀 Début extraction DPIP (RADIOTH_ARIA_NSCHEDU_ACTIV) ...")

    # --- Connexion Oracle DPIP
    ora_conn = oracle_radio("dpip")
    base_dir = os.path.join(os.path.dirname(__file__), "../sql")

    # --- lecture de la requête d’extraction
    sql_path = os.path.join(base_dir, "extract_radioth.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_query = f.read()

    logging.info(f"▶️ Lecture requête Oracle : {sql_path}")
    df = pd.read_sql(sql_query, ora_conn)
    logging.info(f"✅ {len(df)} lignes extraites avant nettoyage.")

    # --- Nettoyage et normalisation
    df = df.drop_duplicates(subset=["ipp_ocr", "rana_activitycode", "rana_duedate"])
    logging.info(f"🧹 {len(df)} lignes après suppression des doublons.")

    colonnes_finales = ["ipp_ocr", "rana_duedate", "rana_activitycode", "rana_lookupvalue"]
    colonnes_absentes = [c for c in colonnes_finales if c not in df.columns]
    if colonnes_absentes:
        raise KeyError(f"Colonnes manquantes dans DataFrame : {colonnes_absentes}")

    df = df[colonnes_finales].fillna("")
    if "rana_duedate" in df.columns:
        df["rana_duedate"] = df["rana_duedate"].astype(str)

    # --- chargement dans PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_test")
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()
    
    logging.info(f"Vidage de la table cible : osiris.radioth")
    pg_cur.execute(f"TRUNCATE TABLE osiris.radioth;")
    pg_conn.commit()

    records = df.to_records(index=False)
    buffer, count_total = [], 0

    # --- Insertion par lots
    for row in records:
        buffer.append(tuple(row))
        if len(buffer) >= BATCH_SIZE:
            try:
                execute_values(pg_cur, f"""
                    INSERT INTO osiris.radioth (ipp_ocr, rana_duedate, rana_activitycode, rana_lookupvalue)
                    VALUES %s
                """, buffer)
                pg_conn.commit()
                count_total += len(buffer)
                logging.info(f"💾 {count_total} lignes insérées ...")
            except Exception as e:
                logging.warning(f"⚠️ Erreur sur lot ignorée : {e}")
                pg_conn.rollback()
            buffer.clear()

    # --- Dernier lot
    if buffer:
        try:
            execute_values(pg_cur, f"""
                INSERT INTO osiris.radioth (ipp_ocr, rana_duedate, rana_activitycode, rana_lookupvalue)
                VALUES %s
            """, buffer)
            pg_conn.commit()
            count_total += len(buffer)
        except Exception as e:
            logging.warning(f"⚠️ Erreur sur dernier lot ignorée : {e}")
            pg_conn.rollback()

    logging.info(f"✅ Chargement finalisé : {count_total} lignes insérées dans osiris.radioth")

    # --- Fermetures
    pg_cur.close()
    pg_conn.close()
    ora_conn.close()
    logging.info("🏁 Processus terminé avec succès (RADIOTH_ARIA_NSCHEDU_ACTIV).")

