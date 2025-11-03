import pyodbc
import cx_Oracle
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import logging


def connect_to_iris():
    """Connexion à IRIS via ODBC (Airflow connection : iris_odbc)."""
    conn = BaseHook.get_connection("iris_odbc")
    dsn = conn.host

    connection = pyodbc.connect(
        f"DSN={dsn};UID={conn.login};PWD={conn.password}"
    )
    connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    connection.setencoding(encoding='utf-8')

    return connection


def connect_to_oracle():
    """Connexion Oracle standard (Airflow connection : oracle_conn)."""
    conn = BaseHook.get_connection("oracle_conn")
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        # Le client Oracle peut déjà être initialisé — ce n’est pas bloquant
        pass

    return cx_Oracle.connect(
        conn.login,
        conn.password,
        conn.host,
        encoding="UTF-8"
    )


def connect_to_oracle_ref(conn_id: str = "qblocp"):
    """
    Connexion Oracle (Airflow connection : qblocp par défaut).
    Fonctionne de la même manière que connect_to_oracle().
    """
    conn = BaseHook.get_connection(conn_id)
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        # Si déjà initialisé
        pass

    connection = cx_Oracle.connect(
        conn.login,
        conn.password,
        conn.host,
        encoding="UTF-8"
    )

    return connection


def get_postgres_hook(conn_id=None):
    """Récupère un hook PostgreSQL via Airflow Variable (ou fallback postgres_test)."""
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)







def oracle_radio(conn_id: str = "dpip"):
    """
    Connexion Oracle (multi-host, failover, service_name via extra).
    """
    conn = BaseHook.get_connection(conn_id)
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        pass

    hosts = [h.strip() for h in conn.host.split(",") if h.strip()]
    port = conn.port or 1521
    service_name = "DPIP.icr.local"

    if not hosts or not service_name:
        raise ValueError(f"⚠️ Connexion Oracle invalide : host={hosts}, service_name={service_name}")

    # DSN Oracle (compatible RAC)
    if len(hosts) > 1:
        address_list = "".join([f"(ADDRESS=(PROTOCOL=TCP)(HOST={h})(PORT={port}))" for h in hosts])
        dsn = f"(DESCRIPTION=(LOAD_BALANCE=YES)(FAILOVER=YES){address_list}(CONNECT_DATA=(SERVICE_NAME={service_name})))"
    else:
        dsn = f"//{hosts[0]}:{port}/{service_name}"

    logging.info(f"🛰️ Tentative de connexion Oracle via DSN : {dsn}")

    connection = cx_Oracle.connect(
        user=conn.login,
        password=conn.password,
        dsn=dsn,
        encoding="UTF-8"
    )

    logging.info("✅ Connexion Oracle établie avec succès.")
    return connection


