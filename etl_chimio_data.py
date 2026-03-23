
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import pandas as pd
import os
import re
import shutil
import socket
import math

# Import des fonctions
from osiris_oncopole.utils.extract_chimio import extract_query_to_jsonl
from osiris_oncopole.utils.transform_chimio import transform_all
from osiris_oncopole.utils.db import get_postgres_hook
from osiris_oncopole.utils.loader_chimio import load_chimio_data

# Configuration de base du DAG
default_args = {
    'owner': 'DATA-IA',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='etl_chimio_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL Oracle -> PostgreSQL pour le parcours de chimiothérapie',
    tags=["lymphome-data", "osiris", "chimio"],
)

# Chemin de base pour les fichiers intermédiaires
BASE_PATH = "/tmp/etl_iris"
CHUNK_SIZE = 20000
DEBUG_NUM_DOSS_FALLBACK = "202403653"
DEBUG_LOG_SAMPLE_LIMIT = 12

# ----------------------------------------------------------------------
# FONCTIONS DES TÂCHES AIRFLOW
# ----------------------------------------------------------------------


def _iter_jsonl_chunks(path: str, chunksize: int):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return iter(())
    try:
        return pd.read_json(path, orient='records', lines=True, chunksize=chunksize)
    except ValueError:
        return iter(())


def _get_debug_num_doss() -> str | None:
    debug_num_doss = str(Variable.get("chimio_debug_num_doss", default_var=DEBUG_NUM_DOSS_FALLBACK)).strip()
    return debug_num_doss or None


def _normalize_num_doss_value(value):
    if value is None or pd.isna(value):
        return None

    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))

    text = str(value).strip()
    if text.lower() in ("", "none", "null", "nan"):
        return None

    if re.fullmatch(r"-?\d+\.0+", text):
        return text.split(".", 1)[0]

    return text


def _normalize_num_doss_column(df: pd.DataFrame):
    if df is not None and "num_doss" in df.columns:
        df["num_doss"] = df["num_doss"].apply(_normalize_num_doss_value)


def _filter_debug_df(df: pd.DataFrame | None, debug_num_doss: str | None) -> pd.DataFrame | None:
    if df is None or df.empty or not debug_num_doss:
        return None
    if "num_doss" not in df.columns:
        return None

    normalized_debug_num_doss = _normalize_num_doss_value(debug_num_doss)
    normalized_num_doss = df["num_doss"].apply(_normalize_num_doss_value)
    filtered = df[normalized_num_doss == normalized_debug_num_doss].copy()
    if filtered.empty:
        return None
    return filtered


def _log_debug_dataframe(stage: str, debug_num_doss: str | None, frames: list[pd.DataFrame]):
    if not debug_num_doss:
        return

    if not frames:
        logging.info("[ETL Chimio][Debug %s] num_doss=%s -> 0 ligne", stage, debug_num_doss)
        return

    debug_df = pd.concat(frames, ignore_index=True)
    debug_df.columns = [str(col).lower() for col in debug_df.columns]

    if "dat_admini" in debug_df.columns:
        debug_df["dat_admini"] = pd.to_datetime(debug_df["dat_admini"], errors="coerce")
        min_date = debug_df["dat_admini"].min()
        max_date = debug_df["dat_admini"].max()
    else:
        min_date = None
        max_date = None

    logging.info(
        "[ETL Chimio][Debug %s] num_doss=%s -> count=%s min_dat_admini=%s max_dat_admini=%s",
        stage,
        debug_num_doss,
        len(debug_df),
        min_date,
        max_date,
    )

    sort_columns = [col for col in ["dat_admini", "jour", "nom_proto", "num_pdt"] if col in debug_df.columns]
    if sort_columns:
        debug_df = debug_df.sort_values(by=sort_columns, na_position="first")

    display_columns = [col for col in ["num_doss", "jour", "dat_admini", "nom_proto", "num_pdt", "ce_etat_chimio"] if col in debug_df.columns]
    if not display_columns:
        display_columns = list(debug_df.columns)

    for row in debug_df[display_columns].head(DEBUG_LOG_SAMPLE_LIMIT).to_dict("records"):
        logging.info("[ETL Chimio][Debug %s] row=%s", stage, row)


def _log_debug_target_in_postgres(debug_num_doss: str | None):
    if not debug_num_doss:
        return

    pg_hook = get_postgres_hook()
    stats = pg_hook.get_first(
        """
        SELECT COUNT(*) AS row_count, MIN(dat_admini) AS min_dat_admini, MAX(dat_admini) AS max_dat_admini
        FROM osiris.chimiotherapie
        WHERE CAST(num_doss AS TEXT) = %s
        """,
        parameters=(debug_num_doss,),
    )

    logging.info(
        "[ETL Chimio][Debug postgres] num_doss=%s -> count=%s min_dat_admini=%s max_dat_admini=%s",
        debug_num_doss,
        stats[0] if stats else None,
        stats[1] if stats else None,
        stats[2] if stats else None,
    )

    rows = pg_hook.get_records(
        """
        SELECT num_doss, jour, dat_admini, nom_proto, num_pdt, ce_etat_chimio
        FROM osiris.chimiotherapie
        WHERE CAST(num_doss AS TEXT) = %s
        ORDER BY dat_admini NULLS FIRST, jour NULLS FIRST, nom_proto NULLS FIRST, num_pdt NULLS FIRST
        LIMIT %s
        """,
        parameters=(debug_num_doss, DEBUG_LOG_SAMPLE_LIMIT),
    )
    for row in rows:
        logging.info("[ETL Chimio][Debug postgres] row=%s", row)


def _safe_run_token(**kwargs) -> str:
    dag_run = kwargs.get("dag_run")
    raw_token = None

    if dag_run is not None and getattr(dag_run, "run_id", None):
        raw_token = dag_run.run_id
    elif kwargs.get("run_id"):
        raw_token = kwargs["run_id"]
    elif kwargs.get("ts_nodash"):
        raw_token = kwargs["ts_nodash"]
    else:
        raw_token = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw_token)


def _get_run_base_path(**kwargs) -> str:
    return os.path.join(BASE_PATH, _safe_run_token(**kwargs))


def _get_worker_host() -> str:
    return socket.gethostname()


def _assert_local_artifact_visibility(producer_host: str, artifact_path: str, producer_task: str):
    current_host = _get_worker_host()
    is_tmp_artifact = os.path.abspath(artifact_path).startswith(os.path.abspath(BASE_PATH))

    if producer_host and is_tmp_artifact and producer_host != current_host:
        raise ValueError(
            "Artefact intermediaire local non partage entre taches Airflow: "
            f"{producer_task} a ecrit {artifact_path!r} sur host={producer_host}, "
            f"mais la tache courante tourne sur host={current_host}. "
            "Utiliser un stockage partage ou une execution sur le meme worker."
        )

def extract_and_persist_data(**kwargs):
    """
    Tâche 1/3 : Extrait les données CHIMIO et les persiste sur disque au format JSONL.
    Pousse le chemin du fichier via XCom.
    """
    logging.info("[ETL Chimio] 1 - Démarrage de l'Extraction et de la Persistance...")
    
    # Création d'un répertoire dédié au run pour éviter toute réutilisation de fichiers obsolètes.
    run_base_path = _get_run_base_path(**kwargs)
    shutil.rmtree(run_base_path, ignore_errors=True)
    os.makedirs(run_base_path, exist_ok=True)
    worker_host = _get_worker_host()
    debug_num_doss = _get_debug_num_doss()
    logging.info("[ETL Chimio] 1 - Répertoire d'exécution: %s (host=%s)", run_base_path, worker_host)
    logging.info("[ETL Chimio][Debug] num_doss tracé=%s", debug_num_doss)
    
    # 1. Extraction Oracle -> JSONL (streaming)
    chimio_raw_path = os.path.join(run_base_path, 'chimio_raw.jsonl')
    chimio_rows = extract_query_to_jsonl(
        "extract_chimio.sql",
        chimio_raw_path,
        chunk_size=CHUNK_SIZE,
        debug_num_doss=debug_num_doss,
        debug_stage="extract_raw",
    )
    
    # Pousser le chemin vers XCom
    kwargs['ti'].xcom_push(key='chimio_raw_path', value=chimio_raw_path)
    kwargs['ti'].xcom_push(key='run_base_path', value=run_base_path)
    kwargs['ti'].xcom_push(key='extract_worker_host', value=worker_host)
    kwargs['ti'].xcom_push(key='chimio_rows_extracted', value=chimio_rows)
    kwargs['ti'].xcom_push(key='chimio_debug_num_doss', value=debug_num_doss)

    logging.info(
        "[ETL Chimio] 1 - Extraction et persistance terminées. CHIMIO=%s path=%s",
        chimio_rows,
        chimio_raw_path,
    )


def transform_data(**kwargs):
    """
    Tâche 2/3 : Récupère df_chimio_raw, effectue la transformation, et persiste le résultat.
    """
    logging.info("[ETL Chimio] 2 - Démarrage de la Transformation des données...")
    ti = kwargs['ti']
    
    # 1. Récupérer le chemin du fichier CHIMIO RAW
    chimio_raw_path = ti.xcom_pull(task_ids='extract_and_persist', key='chimio_raw_path')
    extract_worker_host = ti.xcom_pull(task_ids='extract_and_persist', key='extract_worker_host')
    debug_num_doss = ti.xcom_pull(task_ids='extract_and_persist', key='chimio_debug_num_doss') or _get_debug_num_doss()
    if not chimio_raw_path:
        raise ValueError("Impossible de récupérer le chemin du fichier CHIMIO brut.")
    _assert_local_artifact_visibility(extract_worker_host, chimio_raw_path, "extract_and_persist")
         
    # 2. Transformation en chunks pour limiter la mémoire
    run_base_path = ti.xcom_pull(task_ids='extract_and_persist', key='run_base_path') or os.path.dirname(chimio_raw_path)
    chimio_clean_path = os.path.join(run_base_path, 'chimio_clean.jsonl')
    if os.path.exists(chimio_clean_path):
        os.remove(chimio_clean_path)
    worker_host = _get_worker_host()
    logging.info("[ETL Chimio] 2 - Lecture=%s Ecriture=%s (host=%s)", chimio_raw_path, chimio_clean_path, worker_host)

    total_in = 0
    total_out = 0
    first_chunk = True
    debug_input_frames = []
    debug_output_frames = []

    for treatment_df in _iter_jsonl_chunks(chimio_raw_path, CHUNK_SIZE):
        if treatment_df is None or treatment_df.empty:
            continue
        _normalize_num_doss_column(treatment_df)
        total_in += len(treatment_df)
        debug_input_frame = _filter_debug_df(treatment_df, debug_num_doss)
        if debug_input_frame is not None:
            debug_input_frames.append(debug_input_frame)
        treatment_clean = transform_all(treatment_df)
        _normalize_num_doss_column(treatment_clean)
        total_out += len(treatment_clean)
        debug_output_frame = _filter_debug_df(treatment_clean, debug_num_doss)
        if debug_output_frame is not None:
            debug_output_frames.append(debug_output_frame)
        treatment_clean.to_json(
            chimio_clean_path,
            orient='records',
            lines=True,
            date_format='iso',
            mode='w' if first_chunk else 'a'
        )
        first_chunk = False

    if first_chunk:
        open(chimio_clean_path, "w", encoding="utf-8").close()
    
    # Pousser le chemin du fichier nettoyé vers XCom
    ti.xcom_push(key='chimio_clean_path', value=chimio_clean_path)
    ti.xcom_push(key='transform_worker_host', value=worker_host)
    ti.xcom_push(key='chimio_rows_transformed_in', value=total_in)
    ti.xcom_push(key='chimio_rows_transformed_out', value=total_out)
    _log_debug_dataframe("transform_input", debug_num_doss, debug_input_frames)
    _log_debug_dataframe("transform_output", debug_num_doss, debug_output_frames)
    logging.info(
        "[ETL Chimio] 2 - Transformation terminée et persistée. in=%s out=%s path=%s",
        total_in,
        total_out,
        chimio_clean_path,
    )


def load_data(**kwargs):
    """
    Tâche 3/3 : Récupère le chemin du DataFrame CHIMIO CLEAN et le charge.
    """
    logging.info("[ETL Chimio] 3 - Démarrage du Chargement dans PostgreSQL...")
    ti = kwargs['ti']
    
    # 1. Récupérer le chemin
    chimio_clean_path = ti.xcom_pull(task_ids='transform_data', key='chimio_clean_path')
    transform_worker_host = ti.xcom_pull(task_ids='transform_data', key='transform_worker_host')
    transformed_rows = ti.xcom_pull(task_ids='transform_data', key='chimio_rows_transformed_out')
    debug_num_doss = ti.xcom_pull(task_ids='extract_and_persist', key='chimio_debug_num_doss') or _get_debug_num_doss()
    
    if not chimio_clean_path:
        raise ValueError("Chemin du fichier CHIMIO de chargement manquant.")
    _assert_local_artifact_visibility(transform_worker_host, chimio_clean_path, "transform_data")
    worker_host = _get_worker_host()
    logging.info("[ETL Chimio] 3 - Chargement depuis %s (host=%s)", chimio_clean_path, worker_host)

    # 2. Chargement chunké de la table d'administration (osiris.chimiotherapie)
    first_chimio_chunk = True
    total_chimio = 0
    debug_load_frames = []
    for df_chimio_clean in _iter_jsonl_chunks(chimio_clean_path, CHUNK_SIZE):
        if df_chimio_clean is None:
            continue
        _normalize_num_doss_column(df_chimio_clean)
        if 'dat_admini' in df_chimio_clean.columns:
            df_chimio_clean['dat_admini'] = pd.to_datetime(df_chimio_clean['dat_admini'], errors='coerce')
        total_chimio += len(df_chimio_clean)
        debug_load_frame = _filter_debug_df(df_chimio_clean, debug_num_doss)
        if debug_load_frame is not None:
            debug_load_frames.append(debug_load_frame)
        load_chimio_data(df_chimio_clean, truncate_table=first_chimio_chunk)
        first_chimio_chunk = False

    if first_chimio_chunk:
        load_chimio_data(pd.DataFrame(columns=[
            'num_doss', 'jour', 'dat_admini', 'cod_categ_proto', 'cod_typ_proto',
            'num_pdt', 'nom_pdt', 'cod_voie', 'uf_real', 'lib_uf_real',
            'dose_tot', 'nom_proto', 'nom_moda', 'ce_etat_chimio'
        ]), truncate_table=True)
    
    _log_debug_dataframe("load_input", debug_num_doss, debug_load_frames)
    _log_debug_target_in_postgres(debug_num_doss)
    logging.info(
        "[ETL Chimio] 3 - Chargement terminé avec succès. CHIMIO=%s transformed_out=%s",
        total_chimio,
        transformed_rows,
    )


# ----------------------------------------------------------------------
# DÉFINITION DES TÂCHES ET ORCHESTRATION
# ----------------------------------------------------------------------

extract_task = PythonOperator(
    task_id='extract_and_persist',
    python_callable=extract_and_persist_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Définition de l'ordre d'exécution
extract_task >> transform_task >> load_task
