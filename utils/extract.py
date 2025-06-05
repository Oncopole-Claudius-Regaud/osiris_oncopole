import logging
from datetime import datetime, date, time
from utils.sql_loader import load_sql
from utils.patients import get_patient_ids
from utils.helpers import make_serializable


def extract_patient_data(cursor):
    logging.info("Début de l'extraction patient_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_patients.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    patient_data = []

    for row in rows:
        raw_code_cim = row.code_cim or ""
        normalized_code_cim = raw_code_cim
        if raw_code_cim and len(raw_code_cim) == 4 and '.' not in raw_code_cim:
            normalized_code_cim = f"{raw_code_cim[:3]}.{raw_code_cim[3:]}"
            logging.warning(f"[NORMALISATION] Code CIM brut transformé → {raw_code_cim} devient {normalized_code_cim}")  # noqa: E501

        patient_data.append({
            "ipp_ocr": row.ipp_ocr,
            "ipp_chu": row.ipp_chu or "",
            "annee_naissance": int(row.annee_naissance) if row.annee_naissance else None,
            "sexe": row.sexe.encode('utf-8').decode('utf-8') if isinstance(row.sexe, str) else row.sexe,
            "death_of_death": row.death_of_death if isinstance(row.death_of_death, (datetime, date)) else None,

            "condition_start_date": row.date_diagnostic if isinstance(row.date_diagnostic, (datetime, date)) else None,
            "condition_end_date": row.date_diagnostic_end if isinstance(row.date_diagnostic_end, (datetime, date)) else None,  # noqa: E501
            "condition_create_date": row.date_diagnostic_created_at if isinstance(row.date_diagnostic_created_at, (datetime, date)) else None,  # noqa: E501
            "condition_update_date": row.date_diagnostic_updated_at if isinstance(row.date_diagnostic_updated_at, (datetime, date)) else None,  # noqa: E501

            "condition_status": row.diagnostic_status or "",
            "condition_deleted_flag": row.diagnostic_deleted_flag or "",

            "concept_id": normalized_code_cim,
            "condition_source_value": raw_code_cim,
            "condition_concept_label": row.libelle_cim or "",

            "cim_created_at": row.cim_created_at if isinstance(row.cim_created_at, (datetime, date)) else None,
            "cim_updated_at": row.cim_updated_at if isinstance(row.cim_updated_at, (datetime, date)) else None,
            "cim_active_from": row.cim_active_from if isinstance(row.cim_active_from, (datetime, date)) else None,
            "cim_active_to": row.cim_active_to if isinstance(row.cim_active_to, (datetime, date)) else None
        })

    return patient_data


def extract_admission_data(cursor):
    logging.info("Début de l'extraction admission_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_visits.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    admission_data = []
    for row in rows:
        preadmission_bool = (row.visit_status or "").strip().upper() == "P"

        admission_data.append({
            "ipp_ocr": row.ipp_ocr,
            "visit_episode_id": row.visit_episode_id or "",
            "visit_start_date": row.visit_start_date if isinstance(row.visit_start_date, (datetime, date)) else None,   # noqa: E501
            "visit_start_time": row.visit_start_time if isinstance(row.visit_start_time, time) else None,
            "visit_end_date": row.visit_end_date if isinstance(row.visit_end_date, (datetime, date)) else None,
            "visit_end_time": row.visit_end_time if isinstance(row.visit_end_time, time) else None,
            "visit_estimated_end_date": row.visit_estimated_end_date if isinstance(row.visit_estimated_end_date, (datetime, date)) else None,   # noqa: E501
            "visit_estimated_end_time": row.visit_estimated_end_time if isinstance(row.visit_estimated_end_time, time) else None,               # noqa: E501
            "visit_functional_unit": row.visit_functional_unit or "",
            "visit_type": row.visit_type or "",
            "visit_status": row.visit_status or "",
            "visit_reason": row.visit_reason or "",
            "visit_reason_create_date": row.visit_reason_create_date if isinstance(row.visit_reason_create_date, (datetime, date)) else None,  # noqa: E501
            "visit_reason_deleted_flag": row.visit_reason_deleted_flag or "",
            "is_preadmission": preadmission_bool
        })

    return admission_data


def extract_measure_data(cursor):
    logging.info("Début de l'extraction measure_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_measurements.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    measure_data = []
    for row in rows:
        try:
            raw = {
                "ipp_ocr": row.ipp_ocr,
                "measure_date": row.measure_date if isinstance(row.measure_date, (datetime, date)) else None,
                "measure_time": datetime.strptime(row.measure_time, "%H:%M:%S").time() if row.measure_time else None,  # noqa: E501
                "obs_update_at": row.obs_updated_at if isinstance(row.obs_updated_at, (datetime, date)) else None,     # noqa: E501
                "code_cim": row.code_cim,
                "measure_type": row.measure_type,
                "measure_value": str(row.measure_value) if row.measure_value is not None else "",
            }
            measure_data.append(raw)

        except Exception as row_error:
            logging.error(f"Erreur ligne measure : {row} → {row_error}")
            continue

    return measure_data


def extract_all_data(cursor):
    """Orchestre les 3 extractions et retourne des listes prêtes à sérialiser"""
    patient_data = extract_patient_data(cursor)
    admission_data = extract_admission_data(cursor)
    measure_data = extract_measure_data(cursor)

    return (
        make_serializable(patient_data),
        make_serializable(admission_data),
        make_serializable(measure_data)
    )
