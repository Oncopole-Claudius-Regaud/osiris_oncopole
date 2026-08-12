import logging, os, json, gc
from datetime import datetime, date, time
from osiris_oncopole.utils.sql_loader import load_sql

CHUNK = 10_000
OUTDIR = "/tmp/etl_iris"

def _is_date_like(x):
    return isinstance(x, (datetime, date))

def _dump(obj, f):
    # dates -> ISO
    def _ser(x):
        if isinstance(x, (datetime, date, time)): return x.isoformat()
        return x
    f.write(json.dumps({k: _ser(v) for k, v in obj.items()}, ensure_ascii=False) + "\n")


def extract_patient_data_to_file(cursor):
    """
    Écrit patients en NDJSON, en dédupliquant les IPP avec zéros à gauche
    à identité égale (name1, name2, dob, gender, ipp_chu).
    On ne conserve que l’IPP “canonique” (sans padding) s’il existe.
    Mémoire O(taille du groupe courant) si l’ORDER BY regroupe les identités.
    """
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/patients.jsonl"
    logging.info("Début extraction patient_data (streaming + dédup 0-gauche) → %s", path)

    # IMPORTANT : regrouper par identité côté SQL (ORDER BY) pour limiter la RAM
    sql = load_sql("extract_patients.sql")
    cursor.execute(sql)

    wrote, dropped = 0, 0

    def _norm_str(x):
        return (x or "").strip().upper()

    def _ident_key(p):
        gender_key = _norm_str(p.get("gender")) if isinstance(p.get("gender"), str) else p.get("gender")
        return (
            _norm_str(p.get("name1")),
            _norm_str(p.get("name2")),
            p.get("date_of_birth"),
            gender_key,
            (p.get("ipp_chu") or "").strip()
        )

    def _flush_group(bucket, out_f, ident_key):
        """Applique la dédup 0-gauche dans un groupe et écrit les survivants."""
        nonlocal dropped, wrote
        if not bucket:
            return

        ipps = [(p, (p.get("ipp_ocr") or "").strip()) for p in bucket]
        ipp_values = set(ipp for _, ipp in ipps if ipp != "")

        to_drop = set()
        for _, ipp in ipps:
            if not ipp or ipp in to_drop:
                continue
            tmp = ipp
            # supprime si c'est un padding d'un autre IPP présent dans le groupe
            while tmp.startswith("0"):
                tmp = tmp[1:]
                if tmp in ipp_values:
                    to_drop.add(ipp)
                    break

        for p, ipp in ipps:
            if ipp in to_drop:
                logging.warning(f"[DEDUP] Suppression IPP_OCR avec 0 en trop: {ipp} | ident={ident_key}")
                dropped += 1
            else:
                _dump(p, out_f)
                wrote += 1

    current_key = None
    bucket = []

    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            for row in rows:
                ipp_val = (row.ipp_ocr or "").strip()

                # ⛔️ Exclure cet IPP spécifique
                if ipp_val == "0200500024":
                    logging.warning(f"[SKIP] Ligne ignorée car IPP interdit : {ipp_val}")
                    continue

                rec = {
                    "ipp_ocr": ipp_val,
                    "ipp_chu": row.ipp_chu or "",
                    # Compat "nom/prenom" vs "name1/name2" selon les alias SQL
                    "nom": getattr(row, "nom", None) or getattr(row, "nom", "") or "",
                    "prenom": getattr(row, "prenom", None) or getattr(row, "prenom", "") or "",
                    "date_of_birth": row.date_of_birth if _is_date_like(row.date_of_birth) else None,
                    "gender": row.gender,
                    "date_of_death": row.date_of_death if _is_date_like(row.date_of_death) else None,
                    "birth_city": row.birth_city or "",
                    "patientupdate": row.patientupdate if _is_date_like(row.patientupdate) else None,
                }

                k = _ident_key(rec)

                if current_key is None:
                    current_key = k
                if k != current_key:
                    _flush_group(bucket, f, current_key)
                    bucket.clear()
                    current_key = k

                bucket.append(rec)

            gc.collect()

        if current_key is not None and bucket:
            _flush_group(bucket, f, current_key)
            bucket.clear()

    logging.info(f"Fin extraction patient_data – {wrote} lignes écrites ; {dropped} IPP padding supprimés")

def extract_admission_data_to_file(cursor):
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/visits.jsonl"
    logging.info("Début extraction admissions (streaming) → %s", path)

    sql = load_sql("extract_visits.sql")
    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows: break
            for row in rows:
                rec = {
                    "ipp_ocr": row.ipp_ocr,
                    "visit_episode_id": row.visit_episode_id or "",
                    "visit_start_date": row.visit_start_date if _is_date_like(row.visit_start_date) else None,
                    "visit_start_time": row.visit_start_time if isinstance(row.visit_start_time, time) else None,
                    "visit_end_date": row.visit_end_date if _is_date_like(row.visit_end_date) else None,
                    "visit_end_time": row.visit_end_time if isinstance(row.visit_end_time, time) else None,
                    "visit_estimated_end_date": row.visit_estimated_end_date if _is_date_like(row.visit_estimated_end_date) else None,
                    "visit_estimated_end_time": row.visit_estimated_end_time if isinstance(row.visit_estimated_end_time, time) else None,
                    "visit_functional_unit": row.visit_functional_unit or "",
                    "visit_code_unit": row.code_unit or "",
                    "visit_responsible_unit_desc": row.visit_responsible_unit_desc or "",
                    "visit_type": row.visit_type or "",
                    "visit_status": row.visit_status or "",
                    "visit_reason": row.visit_reason or "",
                    "visit_reason_create_date": row.visit_reason_create_date if _is_date_like(row.visit_reason_create_date) else None,
                    "is_preadmission": ((row.visit_status or "").strip().upper() == "P"),
                }
                _dump(rec, f); wrote += 1
            gc.collect()
    logging.info("Fin extraction admissions – %d lignes écrites", wrote)

def extract_treatment_data_to_file(cursor):
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/treatments.jsonl"
    logging.info("Début extraction treatments (streaming) → %s", path)

    sql = load_sql("extract_treatment.sql")
    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows: break
            for row in rows:
                rec = {
                    "ipp_ocr": row.ipp_ocr,
                    "visit_iep": row.iep,
                    "date_debut_traitement": row.date_debut_traitement if _is_date_like(row.date_debut_traitement) else None,
                    "date_fin_traitement": row.date_fin_traitement if _is_date_like(row.date_fin_traitement) else None,
                    "dci_code": row.dci_code,
                    "dci_libelle": row.dci_libelle,
                    "forme_libelle": row.forme_libelle,
                    "source": row.source or "TKC",
                }
                _dump(rec, f); wrote += 1
            gc.collect()
    logging.info("Fin extraction treatments – %d lignes écrites", wrote)

def extract_tumeur_data_to_file(cursor):
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/tumeur.jsonl"
    logging.info("Début extraction tumeur (streaming) → %s", path)

    sql = load_sql("extract_tumeur.sql")
    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows: break
            for row in rows:
                rec = {
                    "ipp_ocr": (row.ipp_ocr or "").strip(),
                    "ipp_chu": (row.ipp_chu or "").strip(),
                    "date_diagnostic": row.date_diagnostic if _is_date_like(row.date_diagnostic) else None,
                    "description_tnm": (row.description_tnm or "").strip(),
                    "cancer_type_desc": (row.cancer_type_desc or "").strip(),
                    "cancer_site_desc": (row.cancer_site_desc or "").strip(),
                    "tumour_size": (row.tumour_size or "").strip(),
                    "tumour_size_desc": (row.tumour_size_desc or "").strip(),
                    "t_after_path": (row.t_after_path or "").strip(),
                    "t_after_adjuv": (row.t_after_adjuv or "").strip(),
                }
                _dump(rec, f); wrote += 1
            gc.collect()
    logging.info("Fin extraction tumeur – %d lignes écrites", wrote)

def extract_diagnostic_data_to_file(cursor):
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/diagnostic.jsonl"
    logging.info("Début extraction diagnostic (streaming) → %s", path)

    sql = load_sql("extract_diagnostic.sql")
    cursor.execute(sql)

    wrote = 0

    # petit utilitaire pour accéder en sécurité aux attributs éventuellement absents
    def _get(row, attr, default=None):
        return getattr(row, attr, default)

    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break

            for row in rows:
                # 🔹 Filtre spécifique : on ignore cet ipp_ocr
                if str(row.ipp_ocr) == "0200500024":
                    logging.warning("[ETL] Diagnostic ignoré pour ipp_ocr=0200500024")
                    continue

                raw_code_cim = row.code_cim or ""
                normalized = raw_code_cim
                if raw_code_cim and len(raw_code_cim) == 4 and '.' not in raw_code_cim:
                    normalized = f"{raw_code_cim[:3]}.{raw_code_cim[3:]}"
                    logging.warning(f"[NORMALISATION] Code CIM brut → {raw_code_cim} devient {normalized}")

                rec = {
                    # === Identifiants / dates / statuts
                    "ipp_ocr": row.ipp_ocr,
                    "diagnostic_start_date": row.date_diagnostic if _is_date_like(row.date_diagnostic) else None,
                    "diagnostic_create_date": row.date_diagnostic_created_at if _is_date_like(row.date_diagnostic_created_at) else None,
                    "diagnostic_update_date": row.date_diagnostic_updated_at if _is_date_like(row.date_diagnostic_updated_at) else None,
                    "diagnostic_status": row.diagnostic_status or "",
                    "diagnostic_deleted_flag": row.diagnostic_deleted_flag or "",

                    # === CIM10
                    "code_cim": normalized,
                    "diagnostic_source_value": raw_code_cim,
                    "diagnostic_concept_label": row.libelle_cim or "",
                    "cim_created_at": row.cim_created_at if _is_date_like(row.cim_created_at) else None,
                    "cim_updated_at": row.cim_updated_at if _is_date_like(row.cim_updated_at) else None,
                    "cim_active_from": row.cim_active_from if _is_date_like(row.cim_active_from) else None,
                    "cim_active_to": row.cim_active_to if _is_date_like(row.cim_active_to) else None,

                    # === Morphologie
                    "code_morphologique": _get(row, "code_morphologique", "") or "",

                    # === Stage Cancer (NEW)
                    "tnm_code": _get(row, "tnm_code", "") or "",                       # NEW
                    "cancer_type": _get(row, "cancer_type", "") or "",                 # NEW
                    "cancer_site": _get(row, "cancer_site", "") or "",                 # NEW

                    "t_stage_code": _get(row, "t_stage_code", "") or "",               # NEW
                    "t_stage_desc": _get(row, "t_stage_desc", "") or "",               # NEW
                    "stage_t_after_path": _get(row, "stage_t_after_path", "") or "",   # NEW
                    "stage_t_after_adjuv": _get(row, "stage_t_after_adjuv", "") or "", # NEW
                    "stage_t_recurrent": _get(row, "stage_t_recurrent", "") or "",     # NEW

                    "n_stage_code": _get(row, "n_stage_code", "") or "",               # NEW
                    "n_stage_desc": _get(row, "n_stage_desc", "") or "",               # NEW
                    "stage_n_after_path": _get(row, "stage_n_after_path", "") or "",   # NEW
                    "stage_n_after_adjuv": _get(row, "stage_n_after_adjuv", "") or "", # NEW
                    "stage_n_recurrent": _get(row, "stage_n_recurrent", "") or "",     # NEW

                    "m_stage_code": _get(row, "m_stage_code", "") or "",               # NEW
                    "m_stage_desc": _get(row, "m_stage_desc", "") or "",               # NEW
                    "stage_m_after_path": _get(row, "stage_m_after_path", "") or "",   # NEW
                    "stage_m_after_adjuv": _get(row, "stage_m_after_adjuv", "") or "", # NEW
                    "stage_m_recurrent": _get(row, "stage_m_recurrent", "") or "",     # NEW

                    "stage_date": _get(row, "stage_date", None)
                        if _is_date_like(_get(row, "stage_date", None)) else None      # NEW (date)
                }

                _dump(rec, f)
                wrote += 1

            gc.collect()

    logging.info("Fin extraction diagnostic – %d lignes écrites", wrote)

    
def extract_measure_data_to_file(cursor, start_date, end_date, path=f"{OUTDIR}/measures.jsonl", append=True):
    """
    Extraction des mesures en STREAMING vers NDJSON.
    Écrit 1 ligne JSON par enregistrement dans `path` (append par défaut).
    Retourne un dict léger avec le nombre de lignes écrites.
    """
    os.makedirs(OUTDIR, exist_ok=True)

    mode = "a" if append else "w"
    sql_template = load_sql("extract_measurements.sql")
    sql = sql_template.format(start_date=start_date, end_date=end_date)

    logging.info(f"[MEASURE] Extraction streaming du {start_date} au {end_date} → {path} (append={append})")
    cursor.execute(sql)

    wrote = 0
    with open(path, mode, encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            for row in rows:
                try:
                    # measure_time : accepter str "HH:MM:SS" ou time
                    mt = None
                    if isinstance(row.measure_time, time):
                        mt = row.measure_time
                    elif isinstance(row.measure_time, str) and row.measure_time:
                        mt = row.measure_time  # _dump ne convertit pas les str

                    rec = {
                        "ipp_ocr": row.ipp_ocr,
                        "measure_date": row.measure_date if _is_date_like(row.measure_date) else None,
                        "measure_time": mt,
                        "obs_update_at": row.obs_updated_at if _is_date_like(row.obs_updated_at) else None,
                        "code_cim": getattr(row, "code_cim", None),
                        "measure_type": row.measure_type,
                        "measure_value": "" if row.measure_value is None else str(row.measure_value),
                    }
                    _dump(rec, f)
                    wrote += 1
                except Exception as e:
                    logging.error(f"[MEASURE] Erreur ligne {row}: {e}")
            gc.collect()

    logging.info(f"[MEASURE] {wrote} mesures écrites dans {path}")
    return {"written": wrote, "path": path, "start_date": str(start_date), "end_date": str(end_date)}
    
    
def extract_observation_data_to_file(cursor):
    """
    Extraction des observations cliniques IRIS en STREAMING vers NDJSON.
    Une ligne = une observation patient/date/heure/item.
    """
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/observations.jsonl"
    logging.info("Debut extraction observations cliniques (streaming) -> %s", path)

    sql = load_sql("extract_observations.sql")
    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            for row in rows:
                rec = {
                    "ipp": (row.ipp or "").strip(),
                    "date_observation": row.date_observation if _is_date_like(row.date_observation) else None,
                    "heure_observation": row.heure_observation,
                    "source_admission_id": "" if row.source_admission_id is None else str(row.source_admission_id),
                    "date_admission": row.date_admission if _is_date_like(row.date_admission) else None,
                    "item_id": "" if row.item_id is None else str(row.item_id),
                    "item_code": row.item_code or "",
                    "item_libelle": row.item_libelle or "",
                    "valeur_brute": "" if row.valeur_brute is None else str(row.valeur_brute),
                    "valeur_libelle": row.valeur_libelle or "",
                    "type_observation": row.type_observation or "",
                    "source_system": "IRIS",
                }
                _dump(rec, f)
                wrote += 1
            gc.collect()

    logging.info("Fin extraction observations cliniques - %d lignes ecrites", wrote)
    return {"written": wrote, "path": path}


def extract_rdv_data_to_file(cursor):
    """
    Extraction des RDV en STREAMING vers NDJSON.
    Colonnes : ipp_ocr, date_rdv, libelle_examen
    """
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/rdv.jsonl"
    logging.info("Début extraction RDV (streaming) → %s", path)

    sql = load_sql("extract_rdv.sql")
    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            for row in rows:
                rec = {
                    "ipp_ocr": (row.ipp_ocr or "").strip(),
                    "date_rdv": row.date_rdv if _is_date_like(row.date_rdv) else None,
                    "libelle_examen": (row.libelle_examen or "").strip(),
                    "date_booked":row.date_booked if _is_date_like(row.date_rdv) else None,
                    "rdv_status": row.rdv_status or "",
                }
                _dump(rec, f)
                wrote += 1
            gc.collect()

    logging.info("Fin extraction RDV – %d lignes écrites", wrote)
    return {"written": wrote, "path": path}

def extract_contact_data_to_file(cursor):
    """
    Extraction des dates de contact telephone en STREAMING vers NDJSON.
    Colonnes : ipp_ocr, contact_date
    """
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/contact.jsonl"
    logging.info("Debut extraction contacts telephone (streaming) -> %s", path)

    try:
        sql = load_sql("contact.sql")
    except FileNotFoundError:
        logging.warning("[ETL] Fichier sql/contact.sql absent: extraction contact ignoree.")
        return {"written": 0, "path": path}

    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            for row in rows:
                rec = {
                    "ipp_ocr": (row.ipp_ocr or "").strip(),
                    "contact_date": row.contact_date if _is_date_like(row.contact_date) else None,
                }
                _dump(rec, f)
                wrote += 1
            gc.collect()

    logging.info("Fin extraction contacts telephone - %d lignes ecrites", wrote)
    return {"written": wrote, "path": path}

def extract_consentement_data_to_file(cursor):
    """
    Extraction du dernier consentement questionnaire Q04=4 par patient.
    Colonnes : ipp_ocr, consentement, date_consentement
    """
    os.makedirs(OUTDIR, exist_ok=True)
    path = f"{OUTDIR}/consentement.jsonl"
    logging.info("Debut extraction consentements (streaming) -> %s", path)

    sql = load_sql("extract_consentement.sql")
    cursor.execute(sql)

    wrote = 0
    with open(path, "w", encoding="utf-8") as f:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            for row in rows:
                rec = {
                    "ipp_ocr": (row.ipp_ocr or "").strip(),
                    "consentement": "" if row.consentement is None else str(row.consentement).strip(),
                    "date_consentement": row.date_consentement if _is_date_like(row.date_consentement) else None,
                }
                _dump(rec, f)
                wrote += 1
            gc.collect()

    logging.info("Fin extraction consentements - %d lignes ecrites", wrote)
    return {"written": wrote, "path": path}

def extract_all_data_streaming(cursor):
    # Orchestration uniquement (pas de listes retournées)
    extract_patient_data_to_file(cursor)
    extract_admission_data_to_file(cursor)
    extract_treatment_data_to_file(cursor)
    extract_tumeur_data_to_file(cursor)
    extract_diagnostic_data_to_file(cursor)
    extract_observation_data_to_file(cursor)
    extract_rdv_data_to_file(cursor)
    extract_contact_data_to_file(cursor)
    extract_consentement_data_to_file(cursor)






