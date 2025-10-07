from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from datetime import datetime
from utils.mesh_parser import parse_mesh
from utils.pg_loader import truncate_tables, copy_csv


def load_all_csvs():
    return [
        copy_csv('osiris.mesh_descriptors', CSV_DIR / "descriptors.csv"),
        copy_csv('osiris.mesh_qualifiers',  CSV_DIR / "qualifiers.csv"),
        copy_csv('osiris.mesh_concepts',    CSV_DIR / "concepts.csv"),
        copy_csv('osiris.mesh_tree_numbers',    CSV_DIR / "tree_numbers.csv"),
        copy_csv('osiris.mesh_pharma_actions',  CSV_DIR / "pharma_actions.csv"),
    ]


XML_PATH = Path("/home/administrateur/airflow/dags/docs/mesh_fre_20250101.xml")
CSV_DIR  = Path("/tmp/mesh")

default_args = {"owner": "DATA-IA", "retries": 1, "start_date": datetime(2024, 1, 1)}

with DAG(
    dag_id="load_mesh_from_xml",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["mesh"]
) as dag:

    parse = PythonOperator(
        task_id="parse_xml_to_csv",
        python_callable=parse_mesh,
        op_kwargs={"xml_path": XML_PATH, "out_dir": CSV_DIR},
    )

    truncate = PythonOperator(
        task_id="truncate_tables",
        python_callable=truncate_tables
    )

    load = PythonOperator(
        task_id="copy_csv_to_pg",
        python_callable=load_all_csvs
    )

    parse >> truncate >> load
