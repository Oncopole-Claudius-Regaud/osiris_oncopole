import hashlib
from datetime import datetime, date, time

# Convertir les objets datetime/date en texte pour qu’ils soient
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
def compute_diagnostic_hash(row_dict: dict) -> str:
    """Construit un hash stable pour un diagnostic basé sur ses champs clés"""
    key_fields = [
        row_dict.get("ipp_ocr"),
        row_dict.get("concept_id"),
        row_dict.get("diagnostic_source_value"),
        row_dict.get("diagnostic_concept_label"),
        row_dict.get("libelle_cim_reference"),
        row_dict.get("diagnostic_start_date"),
        row_dict.get("diagnostic_end_date"),
        row_dict.get("diagnostic_status"),
        row_dict.get("diagnostic_deleted_flag"),
        row_dict.get("diagnostic_create_date"),
        row_dict.get("cim_created_at"),
        row_dict.get("cim_updated_at"),
        row_dict.get("cim_active_from"),
        row_dict.get("cim_active_to"),
        row_dict.get("premiere_consultation_flag"),
        row_dict.get("date_consultation"),
        row_dict.get("date_consultation_created"),
    ]
    return hashlib.sha256("|".join(str(v or "") for v in key_fields).encode("utf-8")).hexdigest()


#Treatments
def compute_treatment_hash(data: dict) -> str:
    fields = [
        str(data.get("ipp_ocr") or ""),
        str(data.get("date_debut_traitement") or ""),
        str(data.get("date_fin_traitement") or ""),
        str(data.get("dci_code") or ""),
        str(data.get("dci_libelle") or ""),
        str(data.get("forme_libelle") or ""),
        str(data.get("source") or "TKC")
    ]
    concat = "|".join(fields)
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()

