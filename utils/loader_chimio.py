
from psycopg2.extras import execute_values
import logging
import math
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

BATCH_SIZE = 5000


def _to_pg_value(value):
    """Normalise une valeur pandas vers un type insérable en PostgreSQL."""
    if value is None:
        return None
    # Capture NaN/NaT pandas et numpy
    if pd.isna(value):
        return None
    # Cas rare où NaT a déjà été converti en chaîne
    if isinstance(value, str) and value.strip().lower() == "nat":
        return None
    # Timestamp pandas -> datetime Python natif
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _normalize_row_for_pg(row_tuple):
    return tuple(_to_pg_value(v) for v in row_tuple)


def _normalize_num_doss(val):
    if val is None or pd.isna(val):
        return None

    if isinstance(val, float):
        if math.isnan(val):
            return None
        if val.is_integer():
            return str(int(val))

    s = str(val).strip()
    if s.lower() in ("", "nan", "none", "null"):
        return None

    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]

    return s


def _is_valid_num_doss(val):
    """
    Vérifie si le NUM_DOSS est valide : non nul, non vide, et différent de -1.
    """
    s = _normalize_num_doss(val)
    if s is None:
        return False

    if s == "" or s == "-1" or s.lower() in ("nan", "none", "null"):
        return False

    return True

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
    if "NUM_DOSS" in df.columns:
        df["NUM_DOSS"] = df["NUM_DOSS"].apply(_normalize_num_doss)

    df = df[df["NUM_DOSS"].apply(_is_valid_num_doss)]
    if df.empty:
        logging.info("Aucune donnée valide pour chimiothérapie.")
        pg_cur.close()
        pg_conn.close()
        return

    target_cols = [
        "NUM_DOSS", "JOUR", "DAT_ADMINI", "COD_TYP_PROTO",
        "NUM_PDT", "NOM_PDT", "COD_VOIE", "UF_REAL", "LIB_UF_REAL",
        "DOSE_TOT", "NOM_MODA", "CE_ETAT_CHIMIO",
        "CODE_UCD", "CODE_DCI", "LIB_DCI", "LIB_UCD", "CP_CODE_VOIE_ADM",
        "CP_LIB_MED_PRESC", "CP_CODE_DCI", "NUM_CYCLE"
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
        buffer.append(_normalize_row_for_pg(row))
        inserted_count += 1
        if len(buffer) >= BATCH_SIZE:
            flush()

    flush()
    pg_cur.close()
    pg_conn.close()
    logging.info("Chargement chimiothérapie terminé ✔️ (%s lignes)", inserted_count)
