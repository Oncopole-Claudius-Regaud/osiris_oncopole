from collections import defaultdict
from datetime import datetime
import hashlib


def float_or_none(value):
    try:
        return float(value)
    except BaseException:
        return None


def enrich_measure_data(measure_data):
    """
    Transforme les données brutes en données enrichies à partir de raw_measurement_data.
    Retourne une liste de tuples prêts à être insérés dans osiris.measurement_enriched.
    """

    # 1. Récupérer les premiers scores OMS par patient
    oms_by_patient = defaultdict(list)
    for row in measure_data:
        item = (row.get("measure_type") or "").strip().upper()
        if item in ("SCORE OMS / KARNOFSKY", "OMS", "KARNOFSKY"):
            try:
                if "_" not in (row.get("measure_value") or ""):
                    continue
                score_oms_part = row["measure_value"].split("_")[0]
                oms_score = int(score_oms_part)
                obs_date = row["measure_date"]
                if isinstance(obs_date, str):
                    obs_date = datetime.strptime(obs_date, "%Y-%m-%d").date()
                oms_by_patient[row["patient_id"]].append((obs_date, oms_score))
            except Exception:
                continue

    # Mapper le 1er score OMS par personne
    first_oms_map = {
        pid: sorted(values, key=lambda x: x[0])[0][1]
        for pid, values in oms_by_patient.items()
    }

    # 2. Regrouper les mesures par patient/date/heure
    grouped = defaultdict(dict)

    for row in measure_data:
        key = (
            row["patient_id"],
            row["measure_date"],
            row["measure_time"],
            row.get("code_cim")
        )
        item = (row.get("measure_type") or "").strip().lower().replace(" ", "")
        raw_val = row.get("measure_value")

        # Ajout des valeurs uniquement si valides
        if item == "poids" and raw_val not in (None, ""):
            try:
                grouped[key]["poids"] = float(raw_val)
            except ValueError:
                pass

        elif item == "taille" and raw_val not in (None, ""):
            try:
                grouped[key]["taille"] = float(raw_val)
            except ValueError:
                pass

        elif item in ("indicedemassecorporelle", "imc") and raw_val not in (None, ""):
            try:
                grouped[key]["imc"] = float(raw_val)
            except ValueError:
                pass

        elif item in ("scoreoms/karnofsky", "oms", "karnofsky") and "_" in (raw_val or ""):
            try:
                oms, karno = raw_val.split("_")
                grouped[key]["score_oms"] = int(oms)
                grouped[key]["score_karnofsky"] = int(karno)
            except Exception:
                pass

        # Champs communs
        grouped[key]["patient_id"] = row["patient_id"]
        grouped[key]["measure_date"] = row["measure_date"]
        grouped[key]["measure_time"] = row["measure_time"]
        grouped[key]["obs_update_at"] = row["obs_update_at"]
        grouped[key]["code_cim"] = row.get("code_cim")

    # 3. Construire les lignes enrichies
    enriched_rows = []
    seen_hashes = set()

    for key, values in grouped.items():
        poids = values.get("poids")
        taille = values.get("taille")
        imc_calcule = round(poids / ((taille / 100) ** 2),
                            3) if poids and taille else None

        line = {
            "patient_id": values["patient_id"],
            "measure_date": values["measure_date"],
            "measure_time": values["measure_time"],
            "obs_update_at": values["obs_update_at"],
            "code_cim": values.get("code_cim"),
            "poids": poids,
            "taille": taille,
            "imc": values.get("imc"),
            "imc_calcule": imc_calcule,
            "score_oms": values.get("score_oms"),
            "score_karnofsky": values.get("score_karnofsky"),
            "first_line_oms_score": (
                first_oms_map.get(
                    values["patient_id"]) if values.get("score_oms") is not None else None)}

        # hash unique
        hash_str = hashlib.sha256(
            "|".join([str(line[k] or "") for k in line]).encode("utf-8")).hexdigest()
        if hash_str not in seen_hashes:
            seen_hashes.add(hash_str)
            enriched_rows.append(tuple(line.values()) + (hash_str,))

    return enriched_rows
