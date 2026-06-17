import logging

import pandas as pd
from psycopg2.extras import execute_values

from osiris_oncopole.utils.db import get_postgres_hook


BATCH_SIZE = 5000
TARGET_TABLE = "osiris.collecteur_acte_icr"
TARGET_COLS = [
    "CAI_NUMDOSS",
    "CAI_DATE_REAL",
    "CAI_CODE_CCAM",
    "CAI_THEME",
    "CAI_CODE_CCAM_FACT",
    "CAI_DATE_SUPPRESSION",
]


def _to_pg_value(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _normalize_row_for_pg(row_tuple):
    return tuple(_to_pg_value(value) for value in row_tuple)


def load_collecteur_acte_icr(df_collecteur_acte_icr: pd.DataFrame, truncate_table: bool = True):
    """
    Charge les donnees COLLECTEUR_ACTE_ICR dans osiris.collecteur_acte_icr.
    """
    pg_hook = get_postgres_hook()
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    try:
        if truncate_table:
            logging.info("Vidage de la table %s", TARGET_TABLE)
            pg_cur.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
            pg_conn.commit()

        df = df_collecteur_acte_icr.copy()
        df.columns = [str(column).upper() for column in df.columns]
        df = df.where(pd.notnull(df), None)

        pg_cur.execute(
            """
            SELECT UPPER(column_name)
            FROM information_schema.columns
            WHERE table_schema = 'osiris'
              AND table_name = 'collecteur_acte_icr'
            """
        )
        existing_target_cols = {row[0] for row in pg_cur.fetchall()}
        missing_target_cols = [col for col in TARGET_COLS if col not in existing_target_cols]
        if missing_target_cols:
            raise ValueError(
                "Colonnes absentes de osiris.collecteur_acte_icr: "
                f"{missing_target_cols}"
            )

        missing_df_cols = [col for col in TARGET_COLS if col not in df.columns]
        if missing_df_cols:
            raise ValueError(
                "Colonnes absentes du DataFrame collecteur_acte_icr: "
                f"{missing_df_cols}"
            )

        if df.empty:
            logging.info("Aucune donnee a charger dans %s.", TARGET_TABLE)
            return

        df_insert = df[TARGET_COLS]
        buffer = []
        inserted_count = 0

        def flush():
            if not buffer:
                return
            execute_values(
                pg_cur,
                f"""INSERT INTO {TARGET_TABLE} ({", ".join(TARGET_COLS)}) VALUES %s""",
                buffer,
                page_size=1000,
            )
            pg_conn.commit()
            buffer.clear()

        for row in df_insert.itertuples(index=False, name=None):
            buffer.append(_normalize_row_for_pg(row))
            inserted_count += 1
            if len(buffer) >= BATCH_SIZE:
                flush()

        flush()
        logging.info("Chargement %s termine (%s lignes)", TARGET_TABLE, inserted_count)
    finally:
        pg_cur.close()
        pg_conn.close()
