import pyodbc
import cx_Oracle
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import logging


DEFAULT_ORACLE_SERVICE_NAMES = {
    "dpip": "DPIP.icr.local",
}


def _append_unique(values, value):
    if value and value not in values:
        values.append(value)


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


def connect_to_chimio(conn_id: str = "dpip"):
    """
    Connexion Oracle pour l'extraction chimiothérapie
    (Airflow connection : dpip chimio_prescription).
    """
    conn = BaseHook.get_connection(conn_id)
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")
    extra = conn.extra_dejson or {}
    default_service_name = DEFAULT_ORACLE_SERVICE_NAMES.get((conn_id or "").lower())
    service_name = extra.get("service_name") or extra.get("service")
    sid = extra.get("sid")
    dsn_extra = extra.get("dsn")
    hosts = [h.strip() for h in (conn.host or "").split(",") if h.strip()]
    port = conn.port or 1521

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        # Si déjà initialisé
        pass

    # Avec une connexion Airflow "Generic", le champ schema est souvent utilisé.
    if not service_name and conn.schema:
        service_name = conn.schema
    if not sid and conn.schema:
        sid = conn.schema
    if not service_name and default_service_name:
        service_name = default_service_name
        logging.info(
            "Connexion CHIMIO_DATA: service_name absent, fallback vers %s pour conn_id=%s",
            service_name,
            conn_id,
        )

    dsn_candidates = []
    _append_unique(dsn_candidates, dsn_extra)
    if conn.host and ("/" in conn.host or "=" in conn.host):
        _append_unique(dsn_candidates, conn.host)
    if hosts and service_name:
        if len(hosts) > 1:
            address_list = "".join(
                [f"(ADDRESS=(PROTOCOL=TCP)(HOST={h})(PORT={port}))" for h in hosts]
            )
            _append_unique(
                dsn_candidates,
                f"(DESCRIPTION=(LOAD_BALANCE=YES)(FAILOVER=YES){address_list}"
                f"(CONNECT_DATA=(SERVICE_NAME={service_name})))"
            )
        else:
            _append_unique(dsn_candidates, f"//{hosts[0]}:{port}/{service_name}")
    if hosts and sid and len(hosts) == 1:
        _append_unique(dsn_candidates, cx_Oracle.makedsn(hosts[0], port, sid=sid))

    if not dsn_candidates:
        raise ValueError(
            "Connexion CHIMIO_DATA invalide: renseigner extra.service_name "
            "(ou extra.sid / extra.dsn). "
            f"host={conn.host!r}, schema={conn.schema!r}, extra_keys={sorted(extra.keys())}"
        )

    last_error = None
    for dsn in dsn_candidates:
        try:
            logging.info("Tentative de connexion Oracle CHIMIO_DATA via DSN : %s", dsn)
            return cx_Oracle.connect(
                user=conn.login,
                password=conn.password,
                dsn=dsn,
                encoding="UTF-8",
            )
        except cx_Oracle.DatabaseError as e:
            last_error = e
            logging.warning("Echec connexion CHIMIO_DATA via DSN %s: %s", dsn, e)

    raise last_error


def connect_to_qprod(conn_id: str = "QPROD"):
    """
    Connexion Oracle pour QPROD (Airflow connection : QPROD).
    """
    conn = BaseHook.get_connection(conn_id)
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")
    service_name = conn.extra_dejson.get("service_name", "QPROD.icr.local")
    hosts = [h.strip() for h in (conn.host or "").split(",") if h.strip()]
    port = conn.port or 1521

    if not hosts:
        raise ValueError(f"Connexion Oracle QPROD invalide: host={conn.host!r}")

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        # Si déjà initialisé
        pass

    if len(hosts) > 1:
        address_list = "".join(
            [f"(ADDRESS=(PROTOCOL=TCP)(HOST={h})(PORT={port}))" for h in hosts]
        )
        dsn = (
            f"(DESCRIPTION=(LOAD_BALANCE=YES)(FAILOVER=YES){address_list}"
            f"(CONNECT_DATA=(SERVICE_NAME={service_name})))"
        )
    else:
        dsn = f"//{hosts[0]}:{port}/{service_name}"

    logging.info("Tentative de connexion Oracle QPROD via DSN : %s", dsn)

    connection = cx_Oracle.connect(
        user=conn.login,
        password=conn.password,
        dsn=dsn,
        encoding="UTF-8",
    )

    logging.info("Connexion Oracle QPROD établie avec succès.")
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
    service_name = DEFAULT_ORACLE_SERVICE_NAMES.get((conn_id or "").lower(), "DPIP.icr.local")

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

