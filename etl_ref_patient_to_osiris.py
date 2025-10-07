from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.ref_patient_etl import extract_ref_patient_df, df_to_records
from utils.db import get_postgres_hook

logger = LoggingMixin().log
DAG_ID = "ref_patient_to_osiris"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,        # déclenchement manuel (à adapter si besoin)
    catchup=False,
    default_args=default_args,
    tags=["osiris", "patient", "oracle", "postgres"],
) as dag:

    @task(multiple_outputs=True)
    def extract_df() -> Dict[str, Any]:
        # Lit sql/extract_ref_patient.sql via la résolution absolue dans utils
        df = extract_ref_patient_df(sql_filename="extract_ref_patient.sql",
                                    oracle_conn_id="oracle_conn_ref")
        cols, records = df_to_records(df)
        logger.info("Extraction Oracle OK — %s lignes.", len(records))
        return {"cols": cols, "records": records, "rowcount": len(records)}

    @task()
    def load_to_pg(payload: Dict[str, Any]) -> Dict[str, int]:
        cols: List[str] = payload["cols"]
        records: List[Tuple] = payload["records"]

        if not records:
            logger.info("Aucune donnée à charger.")
            return {"inserted_rows": 0, "updated_rows": 0}

        hook = get_postgres_hook()  # Variable target_pg_conn_id sinon "postgres_test"
        conn = hook.get_conn()
        conn.autocommit = False

        with conn.cursor() as cur:
            # 1) staging temporaire
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS stage_ref_patient (
                    ipp_ocr TEXT PRIMARY KEY,
                    ipp_chu TEXT,
                    gender TEXT,
                    date_of_death DATE,
                    nom TEXT,
                    prenom TEXT,
                    date_of_birth DATE,
                    birth_city TEXT,
                    date_record_create DATE
                ) ON COMMIT DROP;
            """)
            cur.execute("TRUNCATE stage_ref_patient;")

            # 2) bulk insert staging
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                f"INSERT INTO stage_ref_patient ({', '.join(cols)}) VALUES %s",
                records,
                page_size=1000
            )

            # 3) compter les nouvelles clés avant insert
            cur.execute("""
                WITH new_keys AS (
                    SELECT s.ipp_ocr
                    FROM stage_ref_patient s
                    LEFT JOIN osiris.patient p USING (ipp_ocr)
                    WHERE p.ipp_ocr IS NULL
                )
                SELECT COUNT(*) FROM new_keys;
            """)
            inserted_rows_pre = cur.fetchone()[0]

            # 4) INSERT des nouveaux ipp_ocr
            cur.execute(f"""
                INSERT INTO osiris.patient ({', '.join(cols)})
                SELECT {', '.join('s.' + c for c in cols)}
                FROM stage_ref_patient s
                LEFT JOIN osiris.patient p USING (ipp_ocr)
                WHERE p.ipp_ocr IS NULL;
            """)
            inserted_rows = cur.rowcount  # garde-fou (concurrence)
            logger.info("INSERT nouveaux: attendu=%s, effectués=%s",
                        inserted_rows_pre, inserted_rows)

            # 5) UPDATE des existants
            cur.execute("""
                UPDATE osiris.patient p
                   SET ipp_chu = s.ipp_chu,
                       gender = s.gender,
                       date_of_death = s.date_of_death,
                       nom = s.nom,
                       prenom = s.prenom,
                       date_of_birth = s.date_of_birth,
                       birth_city = s.birth_city,
                       date_record_create = s.date_record_create
                  FROM stage_ref_patient s
                 WHERE p.ipp_ocr = s.ipp_ocr;
            """)
            updated_rows = cur.rowcount
            logger.info("UPDATE existants: %s", updated_rows)

            conn.commit()

        # on retourne le nombre *pré-insert* (vraies nouvelles clés), et les updates
        return {"inserted_rows": inserted_rows_pre, "updated_rows": updated_rows}

    result = load_to_pg(extract_df())

