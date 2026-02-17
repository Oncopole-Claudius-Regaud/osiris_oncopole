import os
import logging
import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from osiris_oncopole.utils.db import connect_to_oracle_ref
from psycopg2.extras import execute_values

BATCH_SIZE = 5000
PATIENTS_PATH = "/tmp/etl_iris/patients.jsonl"

def extract_and_load_chirurgie():
    logging.info("Début extraction OR_INTERVENTIONS (chirurgie - QBloc) ...")

    # --- Connexion Oracle QBloc
    ora_conn = connect_to_oracle_ref("qblocp")
    base_dir = os.path.join(os.path.dirname(__file__), "../sql")

    # --- Lecture du SQL mis à jour (avec IN_CODE)
    chirurgie_sql_path = os.path.join(base_dir, "extract_qbloc.sql")
    with open(chirurgie_sql_path, "r", encoding="utf-8") as f:
        sql_chirurgie = f.read()

    logging.info(f"▶️ Lecture requête Chirurgie : {chirurgie_sql_path}")

    # --- Extraction Oracle → DataFrame
    df_chirurgie = pd.read_sql(sql_chirurgie, ora_conn)
    df_chirurgie.to_json("/tmp/etl_iris/chirurgie.jsonl", orient="records", lines=True, force_ascii=False)
    logging.info(f"✅ {len(df_chirurgie)} lignes extraites avant nettoyage.")
    logging.info(f"📊 Colonnes Oracle détectées : {list(df_chirurgie.columns)}")

    # --- Normaliser les noms de colonnes
    df_chirurgie.columns = [c.strip().lower() for c in df_chirurgie.columns]

    # --- Vérification de la colonne in_code
    if "in_code" not in df_chirurgie.columns:
        logging.warning("⚠️ Colonne 'IN_CODE' absente de la requête Oracle.")
    else:
        logging.info("🩺 Colonne 'IN_CODE' détectée (code CCAM).")

    # --- Suppression des doublons
    df_chirurgie = df_chirurgie.drop_duplicates(subset=["p_code", "i_label", "i_planned_start", "in_code"])
    logging.info(f"🧹 {len(df_chirurgie)} lignes après suppression des doublons.")

    # --- Conversion des types
    df_chirurgie["p_code"] = df_chirurgie["p_code"].astype(str).fillna("")
    df_chirurgie["i_label"] = df_chirurgie["i_label"].astype(str).fillna("")
    df_chirurgie["i_patient_key"] = df_chirurgie["i_patient_key"].astype(str).fillna("")
    df_chirurgie["in_code"] = df_chirurgie.get("in_code", "").astype(str).fillna("")

    for col in ["i_planned_start", "i_planned_end"]:
        df_chirurgie[col] = pd.to_datetime(df_chirurgie[col], errors="coerce")
        df_chirurgie[col] = df_chirurgie[col].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
        )

    logging.info(f"🕒 Exemple de dates converties : {df_chirurgie[['i_planned_start', 'i_planned_end']].head(2).to_dict('records')}")

    # --- 1️⃣ Lecture du fichier patients
    if not os.path.exists(PATIENTS_PATH):
        raise FileNotFoundError(f"❌ Fichier patients introuvable : {PATIENTS_PATH}")

    logging.info(f"📥 Lecture du fichier patients : {PATIENTS_PATH}")
    patients_df = pd.read_json(PATIENTS_PATH, lines=True)
    patients_df = patients_df.dropna(subset=["ipp_ocr"])
    patient_list = set(patients_df["ipp_ocr"].astype(str).unique())

    logging.info(f"✅ {len(patients_df)} patients chargés depuis le fichier JSONL.")

    # --- 2️⃣ Filtrage : garder uniquement les chirurgies avec un ipp_ocr connu
    before_filter = len(df_chirurgie)
    df_chirurgie = df_chirurgie[df_chirurgie["p_code"].astype(str).isin(patient_list)]
    after_filter = len(df_chirurgie)
    removed = before_filter - after_filter

    logging.info(f"🧩 {removed} lignes supprimées car ipp_ocr absent dans patients.jsonl.")
    logging.info(f"📊 {after_filter} lignes restantes à charger.")

    # --- 3️⃣ Connexion PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_test")
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    pg_table = "osiris.chirurgie"

    # --- Vérifie la présence de la colonne code_ccam
    logging.info(f"📋 Vérification structure table {pg_table}")
    pg_cur.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{pg_table.split('.')[-1]}';
    """)
    columns_pg = [r[0] for r in pg_cur.fetchall()]
    if "code_ccam" not in columns_pg:
        logging.warning("⚠️ La colonne 'code_ccam' n'existe pas dans PostgreSQL — à créer manuellement.")

    # --- Vidage table
    logging.info(f"🧹 Vidage de la table cible : {pg_table}")
    pg_cur.execute(f"TRUNCATE TABLE {pg_table} RESTART IDENTITY CASCADE;")
    pg_conn.commit()

    # --- 4️⃣ Insertion par lots
    records = df_chirurgie.to_records(index=False)
    buffer = []
    count_total = 0

    for row in records:
        ipp_ocr = str(row.p_code) if pd.notnull(row.p_code) else None
        nom_interv = str(row.i_label) if pd.notnull(row.i_label) else None
        dat_deb_reel = row.i_planned_start if pd.notnull(row.i_planned_start) else None
        dat_fin_reel = row.i_planned_end if pd.notnull(row.i_planned_end) else None
        patient_key = str(row.i_patient_key) if pd.notnull(row.i_patient_key) else None
        code_ccam = str(row.in_code) if pd.notnull(row.in_code) else None
        i_state = str(row.i_state) if  pd.notnull(row.i_state) else None

        buffer.append((ipp_ocr, nom_interv, dat_deb_reel, dat_fin_reel, patient_key, code_ccam,i_state))

        if len(buffer) >= BATCH_SIZE:
            try:
                execute_values(pg_cur, f"""
                    INSERT INTO {pg_table} (
                        ipp_ocr, nom_interv, dat_deb_reel, dat_fin_reel, patient_key, code_ccam, i_state
                    ) VALUES %s
                """, buffer)
                pg_conn.commit()
                count_total += len(buffer)
                logging.info(f"💾 {count_total} lignes insérées ...")
            except Exception as e:
                logging.warning(f"⚠️ Erreur sur lot ignorée : {e}")
                pg_conn.rollback()
            buffer.clear()

    # --- Dernier flush
    if buffer:
        try:
            execute_values(pg_cur, f"""
                INSERT INTO {pg_table} (
                    ipp_ocr, nom_interv, dat_deb_reel, dat_fin_reel, patient_key, code_ccam, i_state
                ) VALUES %s
            """, buffer)
            pg_conn.commit()
            count_total += len(buffer)
        except Exception as e:
            logging.warning(f"⚠️ Erreur sur dernier lot ignorée : {e}")
            pg_conn.rollback()

    logging.info(f"✅ Chargement finalisé : {count_total} lignes insérées dans {pg_table}")

    # --- Fermetures
    pg_cur.close()
    pg_conn.close()
    ora_conn.close()
    logging.info("🏁 Processus terminé avec succès (QBloc chirurgie).")

