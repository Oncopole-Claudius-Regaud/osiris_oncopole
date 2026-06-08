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

    sort_columns = [col for col in ["dat_admini", "jour", "num_cycle", "num_pdt"] if col in debug_df.columns]
    if sort_columns:
        debug_df = debug_df.sort_values(by=sort_columns, na_position="first")

    display_columns = [col for col in ["num_doss", "jour", "dat_admini", "num_cycle", "code_ucd", "code_dci", "lib_dci", "lib_ucd", "cp_code_voie_adm", "cp_lib_med_presc", "cp_code_dci", "num_pdt", "ce_etat_chimio"] if col in debug_df.columns]
    if not display_columns:
        display_columns = list(debug_df.columns)

    for row in debug_df[display_columns].head(DEBUG_LOG_SAMPLE_LIMIT).to_dict("records"):
        logging.info("[Chimio][Debug %s] row=%s", stage, row)


def _fetch_count(cursor, query: str, parameters: dict | None = None):
    cursor.execute(query, parameters or {})
    row = cursor.fetchone()
    return row[0] if row else None


def _log_zero_row_diagnostics(cursor, debug_num_doss: str | None):
    logging.warning("[Chimio][Diag] Extraction vide: lancement des diagnostics Oracle")

    diagnostics = [
        (
            "prescription_total",
            "SELECT COUNT(*) FROM DMI_ICR.CHIMIO_PRESCRIPTION",
            None,
        ),
        (
            "prescription_administre_exact",
            """
            SELECT COUNT(*)
            FROM DMI_ICR.CHIMIO_PRESCRIPTION
            WHERE CP_LIB_ETAPE_PRESC = 'ADMINISTRE'
            """,
            None,
        ),
        (
            "prescription_administre_normalise",
            """
            SELECT COUNT(*)
            FROM DMI_ICR.CHIMIO_PRESCRIPTION
            WHERE UPPER(TRIM(CP_LIB_ETAPE_PRESC)) = 'ADMINISTRE'
            """,
            None,
        ),
    ]

    if debug_num_doss:
        diagnostics.extend([
            (
                "debug_num_doss_total",
                """
                SELECT COUNT(*)
                FROM DMI_ICR.CHIMIO_PRESCRIPTION
                WHERE TRIM(CP_NUMDOSS) = :num_doss
                """,
                {"num_doss": debug_num_doss},
            ),
            (
                "debug_num_doss_administre",
                """
                SELECT COUNT(*)
                FROM DMI_ICR.CHIMIO_PRESCRIPTION
                WHERE TRIM(CP_NUMDOSS) = :num_doss
                  AND CP_LIB_ETAPE_PRESC = 'ADMINISTRE'
                """,
                {"num_doss": debug_num_doss},
            ),
            (
                "debug_num_doss_administre_normalise",
                """
                SELECT COUNT(*)
                FROM DMI_ICR.CHIMIO_PRESCRIPTION C
                WHERE TRIM(C.CP_NUMDOSS) = :num_doss
                  AND UPPER(TRIM(C.CP_LIB_ETAPE_PRESC)) = 'ADMINISTRE'
                """,
                {"num_doss": debug_num_doss},
            ),
        ])

    for label, query, parameters in diagnostics:
        try:
            logging.warning("[Chimio][Diag] %s=%s", label, _fetch_count(cursor, query, parameters))
        except Exception as exc:
            logging.warning("[Chimio][Diag] %s impossible: %s", label, exc)

    try:
        cursor.execute("""
            SELECT CP_LIB_ETAPE_PRESC, COUNT(*) AS row_count
            FROM DMI_ICR.CHIMIO_PRESCRIPTION
            GROUP BY CP_LIB_ETAPE_PRESC
            ORDER BY row_count DESC
            FETCH FIRST 10 ROWS ONLY
        """)
        for row in cursor.fetchall():
            logging.warning("[Chimio][Diag] etape=%r count=%s", row[0], row[1])
    except Exception as exc:
        logging.warning("[Chimio][Diag] distribution etapes impossible: %s", exc)

    if not debug_num_doss:
        return

    try:
        cursor.execute(
            """
            SELECT
                C.CP_NUMDOSS,
                C.CP_LIB_ETAPE_PRESC,
                C.CP_DATE_ADM,
                C.CP_NUM_J,
                C.CP_NUM_CURE,
                C.CP_CODE_PDT,
                C.CP_LIB_UCD
            FROM DMI_ICR.CHIMIO_PRESCRIPTION C
            WHERE TRIM(C.CP_NUMDOSS) = :num_doss
            FETCH FIRST 12 ROWS ONLY
            """,
            {"num_doss": debug_num_doss},
        )
        for row in cursor.fetchall():
            logging.warning("[Chimio][Diag] sample_debug_row=%s", row)
    except Exception as exc:
        logging.warning("[Chimio][Diag] sample num_doss impossible: %s", exc)


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

        if wrote == 0:
            _log_zero_row_diagnostics(cursor, normalized_debug_num_doss)
    finally:
        cursor.close()
        conn.close()

    logging.info("[Chimio] %s -> %s (%s lignes)", query_input, output_path, wrote)
    _log_debug_num_doss_rows(debug_stage or query_input, normalized_debug_num_doss, debug_rows)
    return wrote
