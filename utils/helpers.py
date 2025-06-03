import hashlib
from datetime import datetime, date, time

# Convertir les objets datetime/date en texte pour qu’ils soient exploitables plus tard
def serialize(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    return obj

# Applique la fonction serialize sur toute une liste de dictionnaires
def make_serializable(data_list):
    return [
        {k: serialize(v) for k, v in item.items()}
        for item in data_list
    ]

# Génère un hash unique pour chaque visite, basé sur ses champs clés
def compute_visit_hash(row):
    key_parts = [
        str(row.get("patient_id") or ""),
        str(row.get("visit_episode_id") or ""),
        str(row.get("visit_start_date") or ""),
        str(row.get("visit_start_time") or ""),
        str(row.get("visit_end_date") or ""),
        str(row.get("visit_end_time") or ""),
        str(row.get("visit_estimated_end_date") or ""),
        str(row.get("visit_estimated_end_time") or ""),
        str(row.get("visit_functional_unit") or ""),
        str(row.get("visit_type") or ""),
        str(row.get("visit_status") or ""),
        str(row.get("visit_reason") or ""),
        str(row.get("visit_reason_create_date") or ""),
        str(row.get("visit_reason_deleted_flag") or ""),
        str(row.get("is_preadmission") or ""),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


# Pareillement pour les diagnostics/conditions médicales
def compute_condition_hash(row):
    key_parts = [
        str(row.get("patient_id") or ""),
        str(row.get("concept_id") or ""),
        str(row.get("condition_source_value") or ""),
        str(row.get("condition_concept_label") or ""),
        str(row.get("libelle_cim_reference") or ""),
        str(row.get("condition_start_date") or ""),
        str(row.get("condition_end_date") or ""),
        str(row.get("condition_status") or ""),
        str(row.get("condition_deleted_flag") or ""),
        str(row.get("condition_create_date") or ""),
        str(row.get("condition_update_date") or ""),
        str(row.get("cim_created_at") or ""),
        str(row.get("cim_updated_at") or ""),
        str(row.get("cim_active_from") or ""),
        str(row.get("cim_active_to") or ""),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()

