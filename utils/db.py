import pyodbc
import cx_Oracle
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable


def connect_to_iris():
    """
    Connexion à IRIS via ODBC en utilisant la connexion Airflow.
    """
    conn = BaseHook.get_connection("iris_odbc")  
    dsn = conn.host

    connection = pyodbc.connect(
        f"DSN={dsn};UID={conn.login};PWD={conn.password}"
    )
    connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    connection.setencoding(encoding='utf-8')

    return connection


# Connexion Oracle via cx_Oracle (connexion existante conservée)
def connect_to_oracle():
    conn = BaseHook.get_connection("oracle_conn")
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        pass 

    return cx_Oracle.connect(
        conn.login,
        conn.password,
        conn.host,    
        encoding='UTF-8'
    )


# Nouvelle connexion Oracle (oracle_conn_ref)
def connect_to_oracle_ref(conn_id: str = "oracle_conn_ref"):
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    lib_dir = extra.get("lib_dir")
    if lib_dir:
        try:
            cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        except cx_Oracle.ProgrammingError:
            pass  # déjà initialisé

    encoding = extra.get("encoding", "UTF-8")

    host_field = (conn.host or "").strip()
    if ("/" in host_field) or (":" in host_field):
        dsn = host_field
    else:
        port = int(conn.port) if conn.port else 1521
        service = (conn.schema or "").strip() or None
        dsn = cx_Oracle.makedsn(host_field, port, service_name=service)

    return cx_Oracle.connect(
        user=conn.login,
        password=conn.password,
        dsn=dsn,
        encoding=encoding
    )


def get_postgres_hook(conn_id=None):
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)
