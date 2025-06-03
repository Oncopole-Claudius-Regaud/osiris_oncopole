import os
import yaml
import pyodbc
import cx_Oracle
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Chargement des credentials depuis un fichier YAML
def load_credentials(filename="credentials.yml"):
    """
    Charge les identifiants de connexion depuis le fichier de configuration YAML.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config", filename)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# Connexion IRIS via ODBC
def connect_to_iris():
    """
    Établit une connexion à la base IRIS via ODBC (pyodbc).
    """
    credentials = load_credentials()
    db_info = credentials['database']

    connection = pyodbc.connect(
        f"DSN={db_info['dsn']};UID={db_info['username']};PWD={db_info['password']}"
    )
    connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    connection.setencoding(encoding='utf-8')

    return connection


# Connexion Oracle via cx_Oracle
def connect_to_oracle():
    """
    Initialise Oracle Client et établit une connexion Oracle (cx_Oracle).
    """
    credentials = load_credentials()
    ora_conf = credentials['database']['oracle']

    # Initialisation du client Oracle
    try:
        cx_Oracle.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_7")
    except cx_Oracle.ProgrammingError:
        # Client déjà initialisé -> ignorer l'erreur
        pass

    conn = cx_Oracle.connect(
        ora_conf['user'],
        ora_conf['password'],
        ora_conf['dsn'],
        encoding=ora_conf.get('encoding', 'UTF-8')
    )
    return conn


# Hook PostgreSQL Airflow
def get_postgres_hook(conn_id='postgres_chimio'):
    """
    Retourne un PostgresHook Airflow pour exécuter des requêtes PostgreSQL.
    """
    return PostgresHook(postgres_conn_id=conn_id)
