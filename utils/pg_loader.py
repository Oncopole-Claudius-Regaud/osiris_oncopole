from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from pathlib import Path
import csv
import io

def get_pg_hook() -> PostgresHook:
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)


def truncate_tables():
    hook = get_pg_hook()
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            TRUNCATE osiris.mesh_tree_numbers,
                     osiris.mesh_pharma_actions,
                     osiris.mesh_qualifiers,
                     osiris.mesh_concepts,
                     osiris.mesh_descriptors;
        """)


def _transform_array_fields(csv_path: Path, array_columns: list[str]) -> io.StringIO:
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            for col in array_columns:
                if col in row and row[col]:
                    row[col] = "{" + row[col].strip("[]").replace(';', ',').replace('"', '').replace("'", "") + "}"
            writer.writerow(row)

        output.seek(0)
        return output



def copy_csv(table: str, csv_path: Path):
    hook = get_pg_hook()

    columns_map = {
        "osiris.mesh_qualifiers": "(mesh_id, qualifier_ui, label_fr, label_en, abbr)",
        "osiris.mesh_concepts": "(mesh_id, concept_ui, preferred_yn, label, registry_number, cas_name, scope_note, related_registry_numbers)",
        "osiris.mesh_descriptors": "(mesh_id, label_fr, label_en, date_created, date_revised, date_established, history_note, public_note, scope_note, online_note, previous_indexing)",
        "osiris.mesh_tree_numbers": "(mesh_id, tree_number)",
        "osiris.mesh_pharma_actions": "(mesh_id, action_ui, action_name)",
    }

    array_map = {
        "osiris.mesh_concepts": ["related_registry_numbers"],
        "osiris.mesh_descriptors": ["previous_indexing"],
    }

    if table not in columns_map:
        raise ValueError(f"Table inconnue ou non supportée : {table}")

    sql = f"COPY {table} {columns_map[table]} FROM STDIN WITH CSV HEADER"

    # Lire pour compter les lignes
    with csv_path.open("r", encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1

    # Re-transformer les champs array si besoin
    array_cols = array_map.get(table, [])
    if array_cols:
        csv_buffer = _transform_array_fields(csv_path, array_cols)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.copy_expert(sql, csv_buffer)
        conn.commit()
    else:
        with hook.get_conn() as conn, conn.cursor() as cur, csv_path.open("r", encoding="utf-8") as f:
            cur.copy_expert(sql, f)
        conn.commit()

    return f"{table} → {csv_path.name} chargé avec succès ({row_count} lignes)"
