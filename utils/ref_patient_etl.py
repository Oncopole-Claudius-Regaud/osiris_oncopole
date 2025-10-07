import os
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import pandas as pd
from airflow.models import Variable
from utils.db import connect_to_oracle_ref

# --------- Résolution de chemin absolu vers /dags/sql ----------
# __file__ = /home/administrateur/airflow/dags/utils/ref_patient_etl.py
BASE_DIR = Path(__file__).resolve().parents[1]          # -> /home/administrateur/airflow/dags
DEFAULT_SQL_DIR = BASE_DIR / "sql"
DEFAULT_SQL_PATH = DEFAULT_SQL_DIR / "extract_ref_patient.sql"

def _norm_gender(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in {"m", "masculin", "male", "m."}:
        return "M"
    if s in {"f", "féminin", "feminin", "female", "f."}:
        return "F"
    return "U"

def _resolve_sql_path(sql_filename: str = "extract_ref_patient.sql") -> Path:
    """
    Priorités:
      1) Variable Airflow 'sql_dir' si définie, ex: /home/administrateur/airflow/dags/sql
      2) Dossier par défaut /dags/sql (calculé via __file__)
    """
    sql_dir_env = Variable.get("sql_dir", default_var=str(DEFAULT_SQL_DIR))
    sql_path = Path(sql_dir_env) / sql_filename
    return sql_path

def extract_ref_patient_df(
    sql_filename: str = "extract_ref_patient.sql",
    oracle_conn_id: str = "oracle_conn_ref"
) -> pd.DataFrame:
    """
    Exécute la requête Oracle présente dans sql/{sql_filename} et retourne un DataFrame
    avec les colonnes alignées sur osiris.patient:
    [ipp_ocr, ipp_chu, gender, date_of_death, nom, prenom, date_of_birth, birth_city, date_record_create]
    """
    sql_path = _resolve_sql_path(sql_filename)

    if not sql_path.exists():
        # fallback supplémentaire si quelqu’un déplace le fichier
        if DEFAULT_SQL_PATH.exists() and sql_filename == "extract_ref_patient.sql":
            sql_path = DEFAULT_SQL_PATH
        else:
            raise FileNotFoundError(f"SQL introuvable: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with connect_to_oracle_ref(oracle_conn_id) as con:
        df = pd.read_sql(sql, con)

    cols = [
        "ipp_ocr", "ipp_chu", "gender", "date_of_death",
        "nom", "prenom", "date_of_birth", "birth_city", "date_record_create"
    ]
    # Garantir l'ordre et l’existence des colonnes
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans l'extraction Oracle: {missing}")

    df = df[cols]

    # normalisation simple
    df["gender"] = df["gender"].apply(_norm_gender)

    # cast dates
    for c in ["date_of_death", "date_of_birth", "date_record_create"]:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # trim strings
    for c in ["ipp_ocr", "ipp_chu", "nom", "prenom", "birth_city"]:
        df[c] = df[c].astype("string").str.strip()

    return df

def df_to_records(df: pd.DataFrame) -> Tuple[List[str], List[Tuple]]:
    """Convertit le DataFrame en (colonnes, liste de tuples) propre pour psycopg2."""
    cols = [
        "ipp_ocr", "ipp_chu", "gender", "date_of_death",
        "nom", "prenom", "date_of_birth", "birth_city", "date_record_create"
    ]
    records: List[Tuple] = [
        tuple(None if (pd.isna(v) or v == "NaT") else v for v in row)
        for row in df[cols].itertuples(index=False, name=None)
    ]
    return cols, records

