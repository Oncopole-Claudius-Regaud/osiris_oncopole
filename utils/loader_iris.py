import logging
import json
import os
import re
from datetime import datetime, date
from typing import Iterable, Dict, Any, List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from psycopg2.extras import execute_values

# utilise TA fonction existante
from utils.helpers import compute_diagnostic_hash

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
OUTPUT_DIR = "/tmp/etl_iris"
BATCH_SIZE = 5_000
VALIDATE_BUFFERS = True  # Active des contrôles simples avant INSERT

# --------------------------------------------------------------------
# Helpers génériques
# --------------------------------------------------------------------
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def none_if_empty(v):
    """Transforme '' en None, trim sur str, laisse le reste intact."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" else s
    return v


def to_bool_or_none(v):
    """Convertit différentes représentations en bool ou None."""
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "t", "1", "y", "yes", "oui"}:
        return True
    if s in {"false", "f", "0", "n", "no", "non"}:
        return False
    return None


# ---------------- Dates: coercition "soft" -> None si invalide ----------------

def is_iso_date_str(x: Any) -> bool:
    if not isinstance(x, str):
        return False
    if not ISO_DATE_RE.match(x):
        return False
    try:
        datetime.strptime(x, "%Y-%m-%d")
        return True
    except Exception:
        return False


def coerce_date_or_none(*candidates: Any):
    """Retourne une date (YYYY-MM-DD ou objet date) si valide, sinon None.
    - Accepte str ISO "YYYY-MM-DD", date, datetime
    - Rejette tout le reste (ex: entiers "237341488")
    """
    for v in candidates:
        v = none_if_empty(v)
        if v is None:
            continue
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        if is_iso_date_str(v):
            return v  # on garde la chaîne ISO (Postgres accepte)
    return None


def looks_like_date(x: Any) -> bool:
    return (
        isinstance(x, (date, datetime)) or (isinstance(x, str) and is_iso_date_str(x))
    )


# ---------------- Codes CIM: éviter d'avaler des dates ----------------

def looks_like_cim(x: Any) -> bool:
    """Heuristique simple pour CIM-10: 'C50.9', 'Z51.1', etc."""
    if not isinstance(x, str):
        return False
    s = x.strip()
    if is_iso_date_str(s):
        return False
    # Lettre (sauf U en France), 2 chiffres, option point + décimales
    return bool(re.match(r"^[A-TV-Z]\d{2}(?:\.\w+)?$", s)) or bool(
        re.match(r"^[A-TV-Z]\d{2}[A-Z0-9]?$", s)
    )


def pick_code_cim(*candidates: Any):
    for v in candidates:
        v = none_if_empty(v)
        if v and looks_like_cim(v):
            return v
    # fallback prudente: garder une valeur non vide qui n'est pas une date
    for v in candidates:
        v = none_if_empty(v)
        if v and not looks_like_date(v):
            return v
    return None


# --------------------------------------------------------------------
# IO helpers
# --------------------------------------------------------------------

def _rows_from_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    """Lit un fichier NDJSON (.jsonl) ligne par ligne."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _rows_from_json_array(path: str) -> Iterable[Dict[str, Any]]:
    """Fallback anciens .json (tableau complet) – itère sans tout garder en RAM."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        for obj in data:
            yield obj


def _stream_rows(basename: str) -> Iterable[Dict[str, Any]]:
    """
    Renvoie un itérateur sur /tmp/etl_iris/<basename>.jsonl (prioritaire),
    sinon essaie /tmp/etl_iris/<basename>.json, sinon itérateur vide.
    """
    ndjson_path = os.path.join(OUTPUT_DIR, f"{basename}.jsonl")
    json_path = os.path.join(OUTPUT_DIR, f"{basename}.json")

    if os.path.exists(ndjson_path):
        return _rows_from_ndjson(ndjson_path)
    elif os.path.exists(json_path):
        return _rows_from_json_array(json_path)
    else:
        logging.warning(f"[ETL] Fichier introuvable pour {basename} (ni .jsonl ni .json).")
        return iter(())


def _flush_values(cur, sql_stmt: str, buffer: List[Tuple], label: str = "", commit_conn=None):
    """Flush un buffer de tuples via execute_values, commit éventuel, clear, log.
    - Pour diagnostics: si une valeur de date n'est pas valide -> NULL (sans swap)
      et si code_cim ressemble à une date -> NULL.
    """
    if not buffer:
        return

    safe_buffer = buffer
    coerced = 0

    # Sanitize à l'ultime moment: on ne swap PAS, on met juste NULL si invalide
    if label.startswith("diagnostic") or label.startswith("diagnostics"):
        safe_buffer = []
        for tup in buffer:
            # ordre attendu pour diagnostics
            # 0: ipp_ocr (text)
            # 1: date_diagnostic (date)
            # 2: code_cim (text)
            # 3: libelle_cim (text)
            # 4: diagnostic_status (text)
            # 5: diagnostic_deleted_flag (bool)
            # 6: date_diagnostic_created_at (date)
            # 7: date_diagnostic_updated_at (date)
            # 8: date_diagnostic_end (date)
            # 12..: autres champs text
            lst = list(tup)

            # date_diagnostic
            if lst[1] is not None and not looks_like_date(lst[1]):
                lst[1] = None; coerced += 1
            # code_cim ne doit pas être une date
            if isinstance(lst[2], str) and is_iso_date_str(lst[2]):
                lst[2] = None; coerced += 1
            # dates supplémentaires
            for idx in (6, 7, 8):
                if lst[idx] is not None and not looks_like_date(lst[idx]):
                    lst[idx] = None; coerced += 1
            safe_buffer.append(tuple(lst))

    if VALIDATE_BUFFERS and (label.startswith("diagnostic") or label.startswith("diagnostics")):
        for i, tup in enumerate(safe_buffer, 1):
            try:
                _, date_diag, code_cim, *_ = tup
                if date_diag is not None and not looks_like_date(date_diag):
                    raise ValueError(
                        f"[buffer check] date_diagnostic invalide (#{i}): {date_diag!r}"
                    )
                if isinstance(code_cim, str) and is_iso_date_str(code_cim):
                    raise ValueError(
                        f"[buffer check] code_cim ressemble à une date (#{i}): {code_cim!r}"
                    )
            except Exception as e:
                logging.exception(e)
                raise

    execute_values(cur, sql_stmt, safe_buffer)
    if commit_conn:
        commit_conn.commit()
    if label:
        if coerced:
            logging.info(f"[ETL] {label}: {len(safe_buffer)} lignes insérées, {coerced} champs date/code_cim forcés à NULL")
        else:
            logging.info(f"[ETL] {label}: {len(safe_buffer)} lignes insérées")
    buffer.clear()


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def load_to_postgresql(**kwargs):
    logging.info("Début du chargement OECI dans PostgreSQL (rewrite avec coercition dates)")

    # Connexion Postgres via Airflow
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    logging.info(f"[ETL] Utilisation de la connexion PostgreSQL : {conn_id}")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    # ---------------- PATIENTS (stream + batch) ----------------
    patient_buffer: List[Tuple] = []
    seen_ipp = set()

    # même ordre que la table cible :
    # (ipp_ocr, ipp_chu, gender, date_of_death, nom, prenom, date_of_birth, birth_city)
    for p in _stream_rows("patients"):
        ipp = none_if_empty(p.get("ipp_ocr"))
        if not ipp or ipp in seen_ipp:
            continue
        seen_ipp.add(ipp)

        patient_buffer.append((
            ipp,                                    # ipp_ocr
            none_if_empty(p.get("ipp_chu")),        # ipp_chu
            none_if_empty(p.get("gender")),         # gender
            coerce_date_or_none(p.get("date_of_death")),  # date_of_death
            none_if_empty(p.get("nom")),            # nom
            none_if_empty(p.get("prenom")),         # prenom
            coerce_date_or_none(p.get("date_of_birth")),  # date_of_birth
            none_if_empty(p.get("birth_city")),       # birth_city
        ))

        if len(patient_buffer) >= BATCH_SIZE:
            _flush_values(
                pg_cur,
                """
                INSERT INTO osiris.patient (
                    ipp_ocr, ipp_chu, gender, date_of_death, nom, prenom, date_of_birth, birth_city
                ) VALUES %s
                ON CONFLICT (ipp_ocr) DO UPDATE
                SET
                  ipp_ocr      = EXCLUDED.ipp_ocr,
                  ipp_chu      = COALESCE(EXCLUDED.ipp_chu,      osiris.patient.ipp_chu),
                  gender       = COALESCE(EXCLUDED.gender,       osiris.patient.gender),
                  date_of_death= COALESCE(EXCLUDED.date_of_death,osiris.patient.date_of_death),
                  nom          = COALESCE(EXCLUDED.nom,          osiris.patient.nom),
                  prenom       = COALESCE(EXCLUDED.prenom,       osiris.patient.prenom),
                  date_of_birth= COALESCE(EXCLUDED.date_of_birth,osiris.patient.date_of_birth),
                  birth_city   = COALESCE(NULLIF(EXCLUDED.birth_city, ''), osiris.patient.birth_city)
                """,
                patient_buffer,
                label="patients (batch)",
                commit_conn=pg_conn,
            )

    # flush final
    _flush_values(
        pg_cur,
        """
        INSERT INTO osiris.patient (
            ipp_ocr, ipp_chu, gender, date_of_death, nom, prenom, date_of_birth, birth_city
        ) VALUES %s
        ON CONFLICT (ipp_ocr) DO UPDATE
        SET
          ipp_ocr      = EXCLUDED.ipp_ocr,
          ipp_chu      = COALESCE(EXCLUDED.ipp_chu,      osiris.patient.ipp_chu),
          gender       = COALESCE(EXCLUDED.gender,       osiris.patient.gender),
          date_of_death= COALESCE(EXCLUDED.date_of_death,osiris.patient.date_of_death),
          nom          = COALESCE(EXCLUDED.nom,          osiris.patient.nom),
          prenom       = COALESCE(EXCLUDED.prenom,       osiris.patient.prenom),
          date_of_birth= COALESCE(EXCLUDED.date_of_birth,osiris.patient.date_of_birth),
          birth_city   = COALESCE(NULLIF(EXCLUDED.birth_city, ''), osiris.patient.birth_city)
        """,
        patient_buffer,
        label="patients (final)",
        commit_conn=pg_conn,
    )
    
    # ---------------- VISITS (stream + batch, upsert) ----------------
    visit_buffer: List[Tuple] = []
    seen_visit_keys = set()

    def _to_str_date(x):
        if x is None: return None
        if isinstance(x, (datetime, date)): return x.isoformat()
        s = str(x).strip()
        return s if s else None

    def _to_str_time(x):
        if x is None: return None
        if hasattr(x, "strftime"):  # datetime.time
            try:
                return x.strftime("%H:%M:%S")
            except Exception:
                pass
        s = str(x).strip()
        return s if s else None

    # priorité au nouveau fichier; fallback sur l'ancien si besoin
    visits_iter = _stream_rows("visits")

    for v in visits_iter:
        ipp = none_if_empty(v.get("ipp_ocr"))
        ep  = none_if_empty(v.get("visit_episode_id"))
        if not ipp or not ep:
            continue  # clés minimales
        key = (ipp, ep)
        if key in seen_visit_keys:
            continue
        seen_visit_keys.add(key)

        is_pre = v.get("is_preadmission")
        # normaliser en 'Y'/'N' si booléen ; sinon garder la chaîne trim
        if isinstance(is_pre, bool):
            is_pre_str = "Y" if is_pre else "N"
        else:
            s = (str(is_pre).strip() if is_pre is not None else "")
            is_pre_str = s if s else None

        visit_buffer.append((
            ipp,
            _to_str_date(v.get("visit_start_date")),
            _to_str_time(v.get("visit_start_time")),
            _to_str_date(v.get("visit_end_date")),
            _to_str_time(v.get("visit_end_time")),
            _to_str_date(v.get("visit_estimated_end_date")),
            _to_str_time(v.get("visit_estimated_end_time")),
            none_if_empty(v.get("visit_functional_unit")),
            none_if_empty(v.get("visit_code_unit")),
            none_if_empty(v.get("visit_responsible_unit_desc")),
            none_if_empty(v.get("visit_type")),
            none_if_empty(v.get("visit_status")),
            none_if_empty(v.get("visit_reason")),
            _to_str_date(v.get("visit_reason_create_date")),
            none_if_empty(v.get("visit_reason_deleted_flag")),
            is_pre_str,
            ep,  # visit_episode_id
        ))

        if len(visit_buffer) >= BATCH_SIZE:
            _flush_values(
                pg_cur,
                """
                INSERT INTO osiris.visit (
                    ipp_ocr,
                    visit_start_date, visit_start_time,
                    visit_end_date, visit_end_time,
                    visit_estimated_end_date, visit_estimated_end_time,
                    visit_functional_unit, visit_type, visit_status,
                    visit_reason, visit_reason_create_date, visit_reason_deleted_flag,
                    is_preadmission,
                    visit_episode_id, visit_code_unit, visit_responsible_unit_desc
                ) VALUES %s
                ON CONFLICT (ipp_ocr, visit_episode_id) DO UPDATE SET
                    visit_start_date          = COALESCE(EXCLUDED.visit_start_date,          osiris.visit.visit_start_date),
                    visit_start_time          = COALESCE(EXCLUDED.visit_start_time,          osiris.visit.visit_start_time),
                    visit_end_date            = COALESCE(EXCLUDED.visit_end_date,            osiris.visit.visit_end_date),
                    visit_end_time            = COALESCE(EXCLUDED.visit_end_time,            osiris.visit.visit_end_time),
                    visit_estimated_end_date  = COALESCE(EXCLUDED.visit_estimated_end_date,  osiris.visit.visit_estimated_end_date),
                    visit_estimated_end_time  = COALESCE(EXCLUDED.visit_estimated_end_time,  osiris.visit.visit_estimated_end_time),
                    visit_functional_unit     = COALESCE(NULLIF(EXCLUDED.visit_functional_unit,''), osiris.visit.visit_functional_unit),
                    visit_code_unit    = COALESCE(NULLIF(EXCLUDED.visit_code_unit,''), osiris.visit.visit_code_unit),
                    visit_responsible_unit_desc    = COALESCE(NULLIF(EXCLUDED.visit_responsible_unit_desc,''), osiris.visit.visit_responsible_unit_desc),
                    visit_type                = COALESCE(NULLIF(EXCLUDED.visit_type,''),     osiris.visit.visit_type),
                    visit_status              = COALESCE(NULLIF(EXCLUDED.visit_status,''),   osiris.visit.visit_status),
                    visit_reason              = COALESCE(NULLIF(EXCLUDED.visit_reason,''),   osiris.visit.visit_reason),
                    visit_reason_create_date  = COALESCE(EXCLUDED.visit_reason_create_date,  osiris.visit.visit_reason_create_date),
                    visit_reason_deleted_flag = COALESCE(NULLIF(EXCLUDED.visit_reason_deleted_flag,''), osiris.visit.visit_reason_deleted_flag),
                    is_preadmission           = COALESCE(NULLIF(EXCLUDED.is_preadmission,''), osiris.visit.is_preadmission)
                """,
                visit_buffer,
                label="visits (batch)",
                commit_conn=pg_conn,
            )

    # flush final
    _flush_values(
        pg_cur,
        """
        INSERT INTO osiris.visit (
            ipp_ocr,
            visit_start_date, visit_start_time,
            visit_end_date, visit_end_time,
            visit_estimated_end_date, visit_estimated_end_time,
            visit_functional_unit, visit_type, visit_status,
            visit_reason, visit_reason_create_date, visit_reason_deleted_flag,
            is_preadmission,
            visit_episode_id, visit_code_unit, visit_responsible_unit_desc
        ) VALUES %s
        ON CONFLICT (ipp_ocr, visit_episode_id) DO UPDATE SET
            visit_start_date          = COALESCE(EXCLUDED.visit_start_date,          osiris.visit.visit_start_date),
            visit_start_time          = COALESCE(EXCLUDED.visit_start_time,          osiris.visit.visit_start_time),
            visit_end_date            = COALESCE(EXCLUDED.visit_end_date,            osiris.visit.visit_end_date),
            visit_end_time            = COALESCE(EXCLUDED.visit_end_time,            osiris.visit.visit_end_time),
            visit_estimated_end_date  = COALESCE(EXCLUDED.visit_estimated_end_date,  osiris.visit.visit_estimated_end_date),
            visit_estimated_end_time  = COALESCE(EXCLUDED.visit_estimated_end_time,  osiris.visit.visit_estimated_end_time),
            visit_functional_unit     = COALESCE(NULLIF(EXCLUDED.visit_functional_unit,''), osiris.visit.visit_functional_unit),
            visit_code_unit    = COALESCE(NULLIF(EXCLUDED.visit_code_unit,''), osiris.visit.visit_code_unit),
            visit_responsible_unit_desc    = COALESCE(NULLIF(EXCLUDED.visit_responsible_unit_desc,''), osiris.visit.visit_responsible_unit_desc),
            visit_type                = COALESCE(NULLIF(EXCLUDED.visit_type,''),     osiris.visit.visit_type),
            visit_status              = COALESCE(NULLIF(EXCLUDED.visit_status,''),   osiris.visit.visit_status),
            visit_reason              = COALESCE(NULLIF(EXCLUDED.visit_reason,''),   osiris.visit.visit_reason),
            visit_reason_create_date  = COALESCE(EXCLUDED.visit_reason_create_date,  osiris.visit.visit_reason_create_date),
            visit_reason_deleted_flag = COALESCE(NULLIF(EXCLUDED.visit_reason_deleted_flag,''), osiris.visit.visit_reason_deleted_flag),
            is_preadmission           = COALESCE(NULLIF(EXCLUDED.is_preadmission,''), osiris.visit.is_preadmission)
        """,
        visit_buffer,
        label="visits (final)",
        commit_conn=pg_conn,
    )
    # ---------------- DIAGNOSTICS (stream + batch + hash + UPSERT) ----------------
    #  plus de TRUNCATE ici
    diag_buffer: List[Tuple] = []
    seen_diag_hash_batch = set()  # suivi des hash dans le batch courant (skip des doublons)

    for d in _stream_rows("diagnostic"):
        # Dictionnaire pour le CALCUL DU HASH (on garde ta logique existante)
        row_dict = {
            "ipp_ocr": none_if_empty(d.get("ipp_ocr")),
            "concept_id": none_if_empty(d.get("concept_id")),
            "diagnostic_source_value": pick_code_cim(
                d.get("diagnostic_source_value"), d.get("condition_source_value")
            ),
            "diagnostic_concept_label": none_if_empty(
                d.get("diagnostic_concept_label") or d.get("condition_concept_label")
            ),
            "libelle_cim_reference": none_if_empty(
                d.get("libelle_cim") or d.get("libelle_cim_reference")
            ),
            "code_morphologique": none_if_empty(d.get("code_morphologique")),
            "date_prelevement": coerce_date_or_none(
                d.get("diagnostic_start_date"),
                d.get("condition_start_date"),
                d.get("date_prelevement"),
                d.get("date_diagnostic"),
            ),
            "diagnostic_end_date": coerce_date_or_none(
                d.get("diagnostic_end_date"), d.get("condition_end_date"), d.get("date_diagnostic_end")
            ),
            "diagnostic_status": none_if_empty(
                d.get("diagnostic_status") or d.get("condition_status")
            ),
            "diagnostic_deleted_flag": none_if_empty(
                d.get("diagnostic_deleted_flag") or d.get("condition_deleted_flag")
            ),
            "diagnostic_create_date": coerce_date_or_none(
                d.get("diagnostic_create_date") or d.get("condition_create_date") or d.get("date_diagnostic_created_at")
            ),
            "cim_created_at": coerce_date_or_none(d.get("cim_created_at")),
            "cim_updated_at": coerce_date_or_none(
                d.get("cim_updated_at"), d.get("diagnostic_update_date"), d.get("condition_update_date"), d.get("date_diagnostic_updated_at")
            ),
        }
        if not row_dict["ipp_ocr"]:
            continue

        # HASH via utilitaire (on NE CHANGE PAS la fonction existante)
        diag_hash = compute_diagnostic_hash(row_dict)
        if not diag_hash:
            continue

        # 🔸 SKIP simple : on ne pousse pas 2 fois le même hash dans un même INSERT ... VALUES
        if diag_hash in seen_diag_hash_batch:
            continue
        seen_diag_hash_batch.add(diag_hash)

        # --- Nouvelles colonnes "Stage Cancer" provenant des alias SQL ---
        tnm_code              = none_if_empty(d.get("tnm_code"))
        cancer_type           = none_if_empty(d.get("cancer_type"))
        cancer_site           = none_if_empty(d.get("cancer_site"))

        t_stage_code          = none_if_empty(d.get("t_stage_code"))
        t_stage_desc          = none_if_empty(d.get("t_stage_desc"))
        stage_t_after_path    = none_if_empty(d.get("stage_t_after_path"))
        stage_t_after_adjuv   = none_if_empty(d.get("stage_t_after_adjuv"))
        stage_t_recurrent     = none_if_empty(d.get("stage_t_recurrent"))

        n_stage_code          = none_if_empty(d.get("n_stage_code"))
        n_stage_desc          = none_if_empty(d.get("n_stage_desc"))
        stage_n_after_path    = none_if_empty(d.get("stage_n_after_path"))
        stage_n_after_adjuv   = none_if_empty(d.get("stage_n_after_adjuv"))
        stage_n_recurrent     = none_if_empty(d.get("stage_n_recurrent"))

        m_stage_code          = none_if_empty(d.get("m_stage_code"))
        m_stage_desc          = none_if_empty(d.get("m_stage_desc"))
        stage_m_after_path    = none_if_empty(d.get("stage_m_after_path"))
        stage_m_after_adjuv   = none_if_empty(d.get("stage_m_after_adjuv"))
        stage_m_recurrent     = none_if_empty(d.get("stage_m_recurrent"))

        stage_date            = coerce_date_or_none(d.get("stage_date"))

        # Respecter l'ordre des colonnes → on GARDE le même ordre de tête
        # pour ne pas casser la sanitation dans _flush_values (indices 1/2/6/7/8)
        diag_buffer.append(
            (
                row_dict["ipp_ocr"],                 # 0
                row_dict["date_prelevement"],   # 1
                row_dict["diagnostic_source_value"], # 2
                row_dict["diagnostic_concept_label"],# 3
                row_dict["diagnostic_status"],       # 4
                to_bool_or_none(row_dict["diagnostic_deleted_flag"]),  # 5
                row_dict["diagnostic_create_date"],  # 6
                row_dict["cim_updated_at"],          # 7  (on conserve ta logique existante)
                row_dict["diagnostic_end_date"],     # 8
                diag_hash,                            # 9  <-- clé de conflit
                row_dict["code_morphologique"],      # 10

                # ---- Nouvelles colonnes (à partir de l'index 11) ----
                tnm_code,                             # 11
                cancer_type,                          # 12
                cancer_site,                          # 13
                t_stage_code,                         # 14
                t_stage_desc,                         # 15
                stage_t_after_path,                   # 16
                stage_t_after_adjuv,                  # 17
                stage_t_recurrent,                    # 18
                n_stage_code,                         # 19
                n_stage_desc,                         # 20
                stage_n_after_path,                   # 21
                stage_n_after_adjuv,                  # 22
                stage_n_recurrent,                    # 23
                m_stage_code,                         # 24
                m_stage_desc,                         # 25
                stage_m_after_path,                   # 26
                stage_m_after_adjuv,                  # 27
                stage_m_recurrent,                    # 28
                stage_date,                           # 29 (date)
            )
        )

        if len(diag_buffer) >= BATCH_SIZE:
            _flush_values(
                pg_cur,
                """
                INSERT INTO osiris.diagnostic (
                    ipp_ocr, date_prelevement, code_cim, libelle_cim,
                    diagnostic_status, diagnostic_deleted_flag,
                    date_diagnostic_created_at, date_diagnostic_updated_at, date_diagnostic_end,
                    diagnostic_hash, code_morphologique,
                    tnm_code, cancer_type, cancer_site,
                    t_stage_code, t_stage_desc, stage_t_after_path, stage_t_after_adjuv, stage_t_recurrent,
                    n_stage_code, n_stage_desc, stage_n_after_path, stage_n_after_adjuv, stage_n_recurrent,
                    m_stage_code, m_stage_desc, stage_m_after_path, stage_m_after_adjuv, stage_m_recurrent,
                    stage_date
                ) VALUES %s
                ON CONFLICT (diagnostic_hash) DO UPDATE SET
                    ipp_ocr                     = EXCLUDED.ipp_ocr,
                    date_prelevement       = COALESCE(EXCLUDED.date_prelevement, osiris.diagnostic.date_prelevement),
                    code_cim                    = COALESCE(NULLIF(EXCLUDED.code_cim,''), osiris.diagnostic.code_cim),
                    libelle_cim                 = COALESCE(NULLIF(EXCLUDED.libelle_cim,''), osiris.diagnostic.libelle_cim),
                    diagnostic_status           = COALESCE(NULLIF(EXCLUDED.diagnostic_status,''), osiris.diagnostic.diagnostic_status),
                    diagnostic_deleted_flag     = COALESCE(EXCLUDED.diagnostic_deleted_flag, osiris.diagnostic.diagnostic_deleted_flag),
                    date_diagnostic_created_at  = COALESCE(EXCLUDED.date_diagnostic_created_at, osiris.diagnostic.date_diagnostic_created_at),
                    date_diagnostic_updated_at  = COALESCE(EXCLUDED.date_diagnostic_updated_at, osiris.diagnostic.date_diagnostic_updated_at),
                    date_diagnostic_end         = COALESCE(EXCLUDED.date_diagnostic_end, osiris.diagnostic.date_diagnostic_end),
                    code_morphologique          = COALESCE(NULLIF(EXCLUDED.code_morphologique,''), osiris.diagnostic.code_morphologique),
                    tnm_code                    = COALESCE(NULLIF(EXCLUDED.tnm_code,''), osiris.diagnostic.tnm_code),
                    cancer_type                 = COALESCE(NULLIF(EXCLUDED.cancer_type,''), osiris.diagnostic.cancer_type),
                    cancer_site                 = COALESCE(NULLIF(EXCLUDED.cancer_site,''), osiris.diagnostic.cancer_site),
                    t_stage_code                = COALESCE(NULLIF(EXCLUDED.t_stage_code,''), osiris.diagnostic.t_stage_code),
                    t_stage_desc                = COALESCE(NULLIF(EXCLUDED.t_stage_desc,''), osiris.diagnostic.t_stage_desc),
                    stage_t_after_path          = COALESCE(NULLIF(EXCLUDED.stage_t_after_path,''), osiris.diagnostic.stage_t_after_path),
                    stage_t_after_adjuv         = COALESCE(NULLIF(EXCLUDED.stage_t_after_adjuv,''), osiris.diagnostic.stage_t_after_adjuv),
                    stage_t_recurrent           = COALESCE(NULLIF(EXCLUDED.stage_t_recurrent,''), osiris.diagnostic.stage_t_recurrent),
                    n_stage_code                = COALESCE(NULLIF(EXCLUDED.n_stage_code,''), osiris.diagnostic.n_stage_code),
                    n_stage_desc                = COALESCE(NULLIF(EXCLUDED.n_stage_desc,''), osiris.diagnostic.n_stage_desc),
                    stage_n_after_path          = COALESCE(NULLIF(EXCLUDED.stage_n_after_path,''), osiris.diagnostic.stage_n_after_path),
                    stage_n_after_adjuv         = COALESCE(NULLIF(EXCLUDED.stage_n_after_adjuv,''), osiris.diagnostic.stage_n_after_adjuv),
                    stage_n_recurrent           = COALESCE(NULLIF(EXCLUDED.stage_n_recurrent,''), osiris.diagnostic.stage_n_recurrent),
                    m_stage_code                = COALESCE(NULLIF(EXCLUDED.m_stage_code,''), osiris.diagnostic.m_stage_code),
                    m_stage_desc                = COALESCE(NULLIF(EXCLUDED.m_stage_desc,''), osiris.diagnostic.m_stage_desc),
                    stage_m_after_path          = COALESCE(NULLIF(EXCLUDED.stage_m_after_path,''), osiris.diagnostic.stage_m_after_path),
                    stage_m_after_adjuv         = COALESCE(NULLIF(EXCLUDED.stage_m_after_adjuv,''), osiris.diagnostic.stage_m_after_adjuv),
                    stage_m_recurrent           = COALESCE(NULLIF(EXCLUDED.stage_m_recurrent,''), osiris.diagnostic.stage_m_recurrent),
                    stage_date                  = COALESCE(EXCLUDED.stage_date, osiris.diagnostic.stage_date)
                """,
                diag_buffer,
                label="diagnostics (batch)",
                commit_conn=pg_conn,
            )
            # 🔸 On réinitialise le set pour le prochain batch
            seen_diag_hash_batch.clear()

    _flush_values(
        pg_cur,
        """
        INSERT INTO osiris.diagnostic (
            ipp_ocr, date_prelevement, code_cim, libelle_cim,
            diagnostic_status, diagnostic_deleted_flag,
            date_diagnostic_created_at, date_diagnostic_updated_at, date_diagnostic_end,
            diagnostic_hash, code_morphologique,
            tnm_code, cancer_type, cancer_site,
            t_stage_code, t_stage_desc, stage_t_after_path, stage_t_after_adjuv, stage_t_recurrent,
            n_stage_code, n_stage_desc, stage_n_after_path, stage_n_after_adjuv, stage_n_recurrent,
            m_stage_code, m_stage_desc, stage_m_after_path, stage_m_after_adjuv, stage_m_recurrent,
            stage_date
        ) VALUES %s
        ON CONFLICT (diagnostic_hash) DO UPDATE SET
            ipp_ocr                     = EXCLUDED.ipp_ocr,
            date_prelevement       = COALESCE(EXCLUDED.date_prelevement, osiris.diagnostic.date_prelevement),
            code_cim                    = COALESCE(NULLIF(EXCLUDED.code_cim,''), osiris.diagnostic.code_cim),
            libelle_cim                 = COALESCE(NULLIF(EXCLUDED.libelle_cim,''), osiris.diagnostic.libelle_cim),
            diagnostic_status           = COALESCE(NULLIF(EXCLUDED.diagnostic_status,''), osiris.diagnostic.diagnostic_status),
            diagnostic_deleted_flag     = COALESCE(EXCLUDED.diagnostic_deleted_flag, osiris.diagnostic.diagnostic_deleted_flag),
            date_diagnostic_created_at  = COALESCE(EXCLUDED.date_diagnostic_created_at, osiris.diagnostic.date_diagnostic_created_at),
            date_diagnostic_updated_at  = COALESCE(EXCLUDED.date_diagnostic_updated_at, osiris.diagnostic.date_diagnostic_updated_at),
            date_diagnostic_end         = COALESCE(EXCLUDED.date_diagnostic_end, osiris.diagnostic.date_diagnostic_end),
            code_morphologique          = COALESCE(NULLIF(EXCLUDED.code_morphologique,''), osiris.diagnostic.code_morphologique),
            tnm_code                    = COALESCE(NULLIF(EXCLUDED.tnm_code,''), osiris.diagnostic.tnm_code),
            cancer_type                 = COALESCE(NULLIF(EXCLUDED.cancer_type,''), osiris.diagnostic.cancer_type),
            cancer_site                 = COALESCE(NULLIF(EXCLUDED.cancer_site,''), osiris.diagnostic.cancer_site),
            t_stage_code                = COALESCE(NULLIF(EXCLUDED.t_stage_code,''), osiris.diagnostic.t_stage_code),
            t_stage_desc                = COALESCE(NULLIF(EXCLUDED.t_stage_desc,''), osiris.diagnostic.t_stage_desc),
            stage_t_after_path          = COALESCE(NULLIF(EXCLUDED.stage_t_after_path,''), osiris.diagnostic.stage_t_after_path),
            stage_t_after_adjuv         = COALESCE(NULLIF(EXCLUDED.stage_t_after_adjuv,''), osiris.diagnostic.stage_t_after_adjuv),
            stage_t_recurrent           = COALESCE(NULLIF(EXCLUDED.stage_t_recurrent,''), osiris.diagnostic.stage_t_recurrent),
            n_stage_code                = COALESCE(NULLIF(EXCLUDED.n_stage_code,''), osiris.diagnostic.n_stage_code),
            n_stage_desc                = COALESCE(NULLIF(EXCLUDED.n_stage_desc,''), osiris.diagnostic.n_stage_desc),
            stage_n_after_path          = COALESCE(NULLIF(EXCLUDED.stage_n_after_path,''), osiris.diagnostic.stage_n_after_path),
            stage_n_after_adjuv         = COALESCE(NULLIF(EXCLUDED.stage_n_after_adjuv,''), osiris.diagnostic.stage_n_after_adjuv),
            stage_n_recurrent           = COALESCE(NULLIF(EXCLUDED.stage_n_recurrent,''), osiris.diagnostic.stage_n_recurrent),
            m_stage_code                = COALESCE(NULLIF(EXCLUDED.m_stage_code,''), osiris.diagnostic.m_stage_code),
            m_stage_desc                = COALESCE(NULLIF(EXCLUDED.m_stage_desc,''), osiris.diagnostic.m_stage_desc),
            stage_m_after_path          = COALESCE(NULLIF(EXCLUDED.stage_m_after_path,''), osiris.diagnostic.stage_m_after_path),
            stage_m_after_adjuv         = COALESCE(NULLIF(EXCLUDED.stage_m_after_adjuv,''), osiris.diagnostic.stage_m_after_adjuv),
            stage_m_recurrent           = COALESCE(NULLIF(EXCLUDED.stage_m_recurrent,''), osiris.diagnostic.stage_m_recurrent),
            stage_date                  = COALESCE(EXCLUDED.stage_date, osiris.diagnostic.stage_date)
        """,
        diag_buffer,
        label="diagnostic (final)",
        commit_conn=pg_conn,
    )
    # 🔸 clear par cohérence (pas indispensable ici)
    seen_diag_hash_batch.clear()


    # ---------------- TRAITEMENTS (stream + batch, upsert + treatment_hash) ----------------
    trt_buffer: List[Tuple] = []
    seen_trt_hashes_batch = set()  # dédoublonnage dans le batch courant uniquement

    def _to_iso_str(x):
        if x is None:
            return ""
        if isinstance(x, (datetime, date)):
            return x.strftime("%Y-%m-%d")
        s = str(x).strip()
        return s

    def _build_treatment_hash(ipp, dci_code, date_debut, date_fin, visit_iep, forme_libelle):
        # Normalisation simple → robuste aux None/espaces/casse
        import hashlib
        parts = [
            (ipp or "").strip().upper(),
            (dci_code or "").strip().upper(),
            _to_iso_str(date_debut),
            _to_iso_str(date_fin),
            (visit_iep or "").strip().upper(),
            (forme_libelle or "").strip().upper(),
        ]
        payload = "|".join(parts)
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    for t in _stream_rows("treatments"):
        ipp        = none_if_empty(t.get("ipp_ocr"))
        dci_code   = none_if_empty(t.get("dci_code"))
        date_debut = coerce_date_or_none(t.get("date_debut_traitement"))
        date_fin   = coerce_date_or_none(t.get("date_fin_traitement"))
        visit_iep  = none_if_empty(t.get("visit_iep"))
        dci_lbl    = none_if_empty(t.get("dci_libelle"))
        forme_lbl  = none_if_empty(t.get("forme_libelle"))
        source     = none_if_empty(t.get("source")) or "TKC"

        if not ipp:
            continue

        trt_hash = _build_treatment_hash(ipp, dci_code, date_debut, date_fin, visit_iep, forme_lbl)
        # skip doublons DANS LE MÊME BATCH pour éviter CardinalityViolation
        if trt_hash in seen_trt_hashes_batch:
            continue
        seen_trt_hashes_batch.add(trt_hash)

        # ⚠️ ORDRE STRICT conforme: ... , source, treatment_hash, visit_iep
        trt_buffer.append((
            ipp,            # ipp_ocr
            date_debut,     # date_debut_traitement
            date_fin,       # date_fin_traitement
            dci_code,       # dci_code
            dci_lbl,        # dci_libelle
            forme_lbl,      # forme_libelle
            source,         # source
            trt_hash,       # treatment_hash   <-- ajouté ici
            visit_iep,      # visit_iep        <-- après treatment_hash
        ))

        if len(trt_buffer) >= BATCH_SIZE:
            _flush_values(
                pg_cur,
                """
                INSERT INTO osiris.treatments_tracker (
                    ipp_ocr, date_debut_traitement, date_fin_traitement,
                    dci_code, dci_libelle, forme_libelle, source,
                    treatment_hash, visit_iep
                ) VALUES %s
                ON CONFLICT (treatment_hash) DO UPDATE SET
                    date_debut_traitement = COALESCE(EXCLUDED.date_debut_traitement, osiris.treatments_tracker.date_debut_traitement),
                    date_fin_traitement   = COALESCE(EXCLUDED.date_fin_traitement,   osiris.treatments_tracker.date_fin_traitement),
                    dci_code              = COALESCE(NULLIF(EXCLUDED.dci_code,''),   osiris.treatments_tracker.dci_code),
                    dci_libelle           = COALESCE(NULLIF(EXCLUDED.dci_libelle,''),osiris.treatments_tracker.dci_libelle),
                    forme_libelle         = COALESCE(NULLIF(EXCLUDED.forme_libelle,''), osiris.treatments_tracker.forme_libelle),
                    source                = COALESCE(NULLIF(EXCLUDED.source,''),     osiris.treatments_tracker.source),
                    visit_iep             = COALESCE(NULLIF(EXCLUDED.visit_iep,''),  osiris.treatments_tracker.visit_iep)
                """,
                trt_buffer,
                label="traitements (batch)",
                commit_conn=pg_conn,
            )
            seen_trt_hashes_batch.clear()  # reset pour le batch suivant

    # Flush final
    _flush_values(
        pg_cur,
        """
        INSERT INTO osiris.treatments_tracker (
            ipp_ocr, date_debut_traitement, date_fin_traitement,
            dci_code, dci_libelle, forme_libelle, source,
            treatment_hash, visit_iep
        ) VALUES %s
        ON CONFLICT (treatment_hash) DO UPDATE SET
            date_debut_traitement = COALESCE(EXCLUDED.date_debut_traitement, osiris.treatments_tracker.date_debut_traitement),
            date_fin_traitement   = COALESCE(EXCLUDED.date_fin_traitement,   osiris.treatments_tracker.date_fin_traitement),
            dci_code              = COALESCE(NULLIF(EXCLUDED.dci_code,''),   osiris.treatments_tracker.dci_code),
            dci_libelle           = COALESCE(NULLIF(EXCLUDED.dci_libelle,''),osiris.treatments_tracker.dci_libelle),
            forme_libelle         = COALESCE(NULLIF(EXCLUDED.forme_libelle,''), osiris.treatments_tracker.forme_libelle),
            source                = COALESCE(NULLIF(EXCLUDED.source,''),     osiris.treatments_tracker.source),
            visit_iep             = COALESCE(NULLIF(EXCLUDED.visit_iep,''),  osiris.treatments_tracker.visit_iep)
        """,
        trt_buffer,
        label="traitements (final)",
        commit_conn=pg_conn,
    )
    seen_trt_hashes_batch.clear()

    
     # ---------------- RDV (stream + batch, upsert) ----------------
    rdv_buffer: List[Tuple] = []
    seen_rdv = set()

    for t in _stream_rows("rdv"):
        ipp      = none_if_empty(t.get("ipp_ocr"))
        date_rdv = coerce_date_or_none(t.get("date_rdv"))
        libelle  = none_if_empty(t.get("libelle_examen"))

        key = (ipp, date_rdv, libelle)

        # Filtre: ignorer lignes incomplètes ou doublons d'entrée
        if not ipp or not date_rdv or not libelle or key in seen_rdv:
            continue

        seen_rdv.add(key)
        rdv_buffer.append((ipp, date_rdv, libelle))

        if len(rdv_buffer) >= BATCH_SIZE:
            _flush_values(
                pg_cur,
                """
                INSERT INTO osiris.rdv (
                    ipp_ocr, date_rdv, libelle_examen
                ) VALUES %s
                ON CONFLICT (ipp_ocr, date_rdv, libelle_examen) DO UPDATE SET
                    libelle_examen = COALESCE(NULLIF(EXCLUDED.libelle_examen,''), osiris.rdv.libelle_examen)
                """,
                rdv_buffer,
                label="rdv (batch)",
                commit_conn=pg_conn,
            )

    # Flush final
    _flush_values(
        pg_cur,
        """
        INSERT INTO osiris.rdv (
            ipp_ocr, date_rdv, libelle_examen
        ) VALUES %s
        ON CONFLICT (ipp_ocr, date_rdv, libelle_examen) DO UPDATE SET
            libelle_examen = COALESCE(NULLIF(EXCLUDED.libelle_examen,''), osiris.rdv.libelle_examen)
        """,
        rdv_buffer,
        label="rdv (final)",
        commit_conn=pg_conn,
    )
        
    
    
    

    # ---------------- CLEANUP ----------------
    pg_cur.close()
    pg_conn.close()
    logging.info("Chargement terminé avec succès")






