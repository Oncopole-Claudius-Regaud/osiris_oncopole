
from psycopg2.extras import execute_values
import logging
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

BATCH_SIZE = 5000

def _is_valid_num_doss(val):
    """
    Vérifie si le NUM_DOSS est valide : non nul, non vide, et différent de -1.
    """
    if val is None or pd.isna(val):
        return False

    s = str(val).strip()

    if s == "" or s == "-1" or s.lower() in ("nan", "none", "null"):
        return False

    return True


def load_chimio_plan_data(df_plan, truncate_table: bool = True):
    """
    Charge les données de planification dans la table cible osiris.chimio_plan.
    """
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    if truncate_table:
        logging.info("Vidage de la table osiris.chimio_plan")
        pg_cur.execute("TRUNCATE TABLE osiris.chimio_plan;")
        pg_conn.commit()

    df = df_plan
    df.columns = [c.upper() for c in df.columns]
    df = df.where(pd.notnull(df), None)

    df = df[df["NUM_DOSS"].apply(_is_valid_num_doss)]
    if df.empty:
        logging.info("Aucune donnée valide pour osiris.chimio_plan.")
        pg_cur.close()
        pg_conn.close()
        return

    target_cols = ["NUM_DOSS", "DAT_OUV", "CODE_LOC"]
    df_insert = df[target_cols] 

    buffer = []

    def flush():
        if not buffer: return
        execute_values(
            pg_cur,
            f"""INSERT INTO osiris.chimio_plan ({", ".join(target_cols)}) VALUES %s""",
            buffer,
            page_size=1000
        )
        pg_conn.commit()
        buffer.clear()

    inserted_count = 0
    for row in df_insert.itertuples(index=False, name=None):
        buffer.append(row)
        inserted_count += 1
        if len(buffer) >= BATCH_SIZE:
            flush()

    flush()
    pg_cur.close()
    pg_conn.close()
    logging.info("Chargement chimio_plan terminé ✔️ (%s lignes)", inserted_count)


def load_chimio_data(df_chimio, truncate_table: bool = True):
    """
    Charge les données de chimiothérapie dans la table cible osiris.chimiotherapie.
    """
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    if truncate_table:
        logging.info("Vidage de la table osiris.chimiotherapie")
        pg_cur.execute("TRUNCATE TABLE osiris.chimiotherapie;")
        pg_conn.commit()

    df = df_chimio
    df.columns = [c.upper() for c in df.columns]
    df = df.where(pd.notnull(df), None)

    df = df[df["NUM_DOSS"].apply(_is_valid_num_doss)]
    if df.empty:
        logging.info("Aucune donnée valide pour chimiothérapie.")
        pg_cur.close()
        pg_conn.close()
        return

    target_cols = [
        "NUM_DOSS", "JOUR", "DAT_ADMINI", "COD_CATEG_PROTO", "COD_TYP_PROTO",
        "NUM_PDT", "NOM_PDT", "COD_VOIE", "UF_REAL", "LIB_UF_REAL",
        "DOSE_TOT", "NOM_PROTO", "NOM_MODA", "CE_ETAT_CHIMIO"
    ]

    df_insert = df[target_cols] 

    buffer = []

    def flush():
        if not buffer: return
        execute_values(
            pg_cur,
            f"""INSERT INTO osiris.chimiotherapie ({", ".join(target_cols)}) VALUES %s""",
            buffer,
            page_size=1000
        )
        pg_conn.commit()
        buffer.clear()

    inserted_count = 0
    for row in df_insert.itertuples(index=False, name=None):
        buffer.append(row)
        inserted_count += 1
        if len(buffer) >= BATCH_SIZE:
            flush()

    flush()
    pg_cur.close()
    pg_conn.close()
    logging.info("Chargement chimiothérapie terminé ✔️ (%s lignes)", inserted_count)
