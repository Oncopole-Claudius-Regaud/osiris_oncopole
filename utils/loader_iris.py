import logging
import hashlib
from datetime import datetime, date, time
from utils.db import load_credentials
from utils.helpers import compute_condition_hash, compute_visit_hash
from psycopg2.extras import execute_values
from utils.enrich_measurements import enrich_measure_data
import psycopg2

def load_to_postgresql(**kwargs):
    logging.info("Début du chargement dans PostgreSQL")

    credentials = load_credentials("credentials.yml")
    pg_config = credentials['database']['postgresql']

    conn = psycopg2.connect(
        host=pg_config['host'],
        port=pg_config['port'],
        user=pg_config['user'],
        password=pg_config['password'],
        database=pg_config['database']
    )
    cur = conn.cursor()

    # Récupération du référentiel CIM10
    cur.execute("SELECT code, description FROM osiris.cim10")
    cim_reference_map = {row[0]: row[1] for row in cur.fetchall()}
    logging.info("Référentiel CIM10 chargé")

    ti = kwargs['ti']
    patient_data, admission_data, measure_data = ti.xcom_pull(task_ids='extract_data_from_iris_osiris')

    # --- PATIENT ---
    unique_patients = {
        d["ipp_ocr"]: (
            d["ipp_ocr"], d["ipp_chu"], d["annee_naissance"], d["sexe"], d["death_of_death"]
        ) for d in patient_data if d.get("ipp_ocr")
    }

    if unique_patients:
        execute_values(cur, """
            INSERT INTO osiris.patient (ipp_ocr, ipp_chu, year_of_birth, gender, date_of_death)
            VALUES %s
            ON CONFLICT ON CONSTRAINT patient_ipp_ocr_unique DO NOTHING
        """, list(unique_patients.values()))
        conn.commit()
        logging.info(f"{len(unique_patients)} patients insérés")

    cur.execute("SELECT patient_id, ipp_ocr FROM osiris.patient")
    ipp_to_id = {row[1]: row[0] for row in cur.fetchall()}

    # --- CONDITION ---
    condition_rows = []
    for d in patient_data:
        pid = ipp_to_id.get(d["ipp_ocr"])
        if not pid: continue
        row_dict = {
            **d,
            "patient_id": pid,
            "libelle_cim_reference": cim_reference_map.get(d.get("condition_source_value"))
        }
        row_hash = compute_condition_hash(row_dict)
        condition_rows.append(tuple([
            pid,
            d.get("concept_id"),
            d.get("condition_source_value"),
            d.get("condition_concept_label"),
            row_dict["libelle_cim_reference"],
            d.get("condition_start_date"),
            d.get("condition_end_date"),
            d.get("condition_status"),
            d.get("condition_deleted_flag"),
            d.get("condition_create_date"),
            d.get("condition_update_date"),
            d.get("cim_created_at"),
            d.get("cim_updated_at"),
            d.get("cim_active_from"),
            d.get("cim_active_to"),
            row_hash
        ]))

    if condition_rows:
        execute_values(cur, """
            INSERT INTO osiris.condition (
                patient_id, concept_id, condition_source_value, condition_concept_label,
                libelle_cim_reference, condition_start_date, condition_end_date,
                condition_status, condition_deleted_flag, condition_create_date,
                condition_update_date, cim_created_at, cim_updated_at, cim_active_from,
                cim_active_to, condition_hash
            ) VALUES %s
            ON CONFLICT (condition_hash) DO NOTHING
        """, condition_rows)
        conn.commit()
        logging.info(f"{len(condition_rows)} conditions insérées")

    # --- VISIT ---
    visit_rows = []
    for v in admission_data:
        pid = ipp_to_id.get(v["ipp_ocr"])
        if not pid: continue
        row = {
            "patient_id": pid,
            **{k: v.get(k) for k in [
                "visit_episode_id", "visit_start_date", "visit_start_time", "visit_end_date", "visit_end_time",
                "visit_estimated_end_date", "visit_estimated_end_time", "visit_functional_unit", "visit_type",
                "visit_status", "visit_reason", "visit_reason_create_date", "visit_reason_deleted_flag", "is_preadmission"
            ]}
        }
        row_hash = compute_visit_hash(row)
        visit_rows.append(tuple(row.values()) + (row_hash,))

    if visit_rows:
        execute_values(cur, """
            INSERT INTO osiris.visit_occurrence (
                patient_id, visit_episode_id, visit_start_date, visit_start_time,
                visit_end_date, visit_end_time, visit_estimated_end_date, visit_estimated_end_time,
                visit_functional_unit, visit_type, visit_status, visit_reason,
                visit_reason_create_date, visit_reason_deleted_flag, is_preadmission, visit_hash
            ) VALUES %s
            ON CONFLICT (visit_hash) DO NOTHING
        """, visit_rows)
        conn.commit()
        logging.info(f"{len(visit_rows)} visites insérées")

    # --- RAW MEASURE ---
    measure_rows = []
    seen_hashes = set()

    for m in measure_data:
        pid = ipp_to_id.get(m["ipp_ocr"])
        if not pid: continue
        row = {
            "patient_id": pid,
            "measure_date": m.get("measure_date"),
            "measure_time": m.get("measure_time"),
            "obs_update_at": m.get("obs_update_at"),
            "code_cim": m.get("code_cim"),
            "measure_type": m.get("measure_type"),
            "measure_value": m.get("measure_value"),
        }
        row_hash = hashlib.sha256("|".join([str(v or "") for v in row.values()]).encode("utf-8")).hexdigest()
        row["measure_hash"] = row_hash

        if row_hash not in seen_hashes:
            seen_hashes.add(row_hash)
            measure_rows.append(tuple(row.values()))

    if measure_rows:
        execute_values(cur, """
            INSERT INTO osiris.measure (
                patient_id, measure_date, measure_time, obs_update_at,
                code_cim, measure_type, measure_value, measure_hash
            ) VALUES %s
            ON CONFLICT (measure_hash) DO NOTHING
        """, measure_rows)
        conn.commit()
        logging.info(f"{len(measure_rows)} mesures brutes insérées")

    # --- ENRICHED MEASURE ---
    for m in measure_data:
        m["patient_id"] = ipp_to_id.get(m["ipp_ocr"])

    enriched_rows = enrich_measure_data(measure_data)

    if enriched_rows:
        execute_values(cur, """
            INSERT INTO osiris.measure_enriched (
                patient_id, measure_date, measure_time, obs_update_at, code_cim,
                poids, taille, imc, imc_calcule,
                score_oms, score_karnofsky, first_line_oms_score, enrished_hash
            ) VALUES %s
            ON CONFLICT (enrished_hash) DO NOTHING
        """, enriched_rows)
        conn.commit()
        logging.info(f"{len(enriched_rows)} mesures enrichies insérées")

    logging.info("Chargement terminé")
    cur.close()
    conn.close()

