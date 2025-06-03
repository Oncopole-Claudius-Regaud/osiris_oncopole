import pandas as pd
import hashlib
import logging

def clean_int(val):
    """
    Tente de convertir une valeur en entier, retourne None si impossible.
    """
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def clean_dataframe(df, date_columns=None):
    """
    Nettoie un DataFrame : convertit les dates, remplace les NaN par None.
    """
    if date_columns:
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df.where(pd.notnull(df), None)


def generate_hash(row, columns):
    """
    Génère un hash SHA-256 à partir de la concaténation des colonnes spécifiées.
    """
    raw_string = "|".join(str(row[col]) if row[col] is not None else "" for col in columns)
    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()


def enrich_with_patient_id(df, patients_df, join_key='noobspat'):
    """
    Ajoute la colonne 'patient_id' à un DataFrame Oracle en faisant la jointure avec osiris.patient.
    """
    merged = df.merge(
        patients_df[['patient_id', 'ipp_ocr']],
        how='left',
        left_on=join_key,
        right_on='ipp_ocr'
    )

    merged.drop(columns=['ipp_ocr'], inplace=True)

    missing = merged[merged["patient_id"].isnull()]
    if not missing.empty:
        logging.warning(f"{len(missing)} patients Oracle non trouvés dans osiris.patient.")
        missing.to_csv("/mnt/data/patients_non_trouves.csv", index=False)

    # On garde uniquement les patients avec une correspondance
    return merged[merged["patient_id"].notnull()]


def remove_duplicates_and_hash(df, cols_to_hash):
    """
    Supprime les doublons selon un sous-ensemble de colonnes,
    et ajoute une colonne de hash unique (SHA256).
    Retourne le DataFrame nettoyé + le nom de la colonne de hash.
    """
    hash_col = "record_hash"

    # Remplace les NaN par une chaîne vide pour concaténation sûre
    df[hash_col] = df[cols_to_hash].fillna("").astype(str).agg("|".join, axis=1)

    # Applique le hash
    df[hash_col] = df[hash_col].apply(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())

    # Supprime les doublons selon le hash
    df = df.drop_duplicates(subset=[hash_col])

    return df, hash_col


def filter_existing_records(df: pd.DataFrame, pg_hook, table_name: str, hash_column: str) -> pd.DataFrame:
    """
    Supprime les enregistrements déjà présents dans la table PostgreSQL (basé sur la colonne de hash).
    """
    query = f"SELECT {hash_column} FROM {table_name}"
    existing_hashes = pg_hook.get_pandas_df(query)[hash_column].tolist()
    return df[~df[hash_column].isin(existing_hashes)]


def transform_all(treatment_df, cycles_df, drugs_df):
    """
    Applique les transformations de base : nettoyage (dates, NaN) sur les 3 DataFrames.
    """
    treatment_clean = clean_dataframe(treatment_df, date_columns=["start_date", "end_date"])
    cycles_clean = clean_dataframe(cycles_df, date_columns=["start_date", "end_date"])
    drugs_clean = clean_dataframe(drugs_df, date_columns=["start_date", "end_date"])
    return treatment_clean, cycles_clean, drugs_clean
