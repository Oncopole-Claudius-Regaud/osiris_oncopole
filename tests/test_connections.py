import os
from types import SimpleNamespace

import pytest

from osiris_oncopole.utils.db import (
    connect_to_chimio,
    connect_to_iris,
    connect_to_oracle,
    get_postgres_hook,
)


CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "../config/credentials.yml")
SKIP_DB_TESTS = not os.path.exists(CREDENTIALS_PATH)


@pytest.mark.skipif(SKIP_DB_TESTS, reason="credentials.yml manquant, test ignore")
def test_iris_connection():
    try:
        conn = connect_to_iris()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
    except Exception as exc:
        pytest.fail(f"Echec de la connexion IRIS: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.mark.skipif(SKIP_DB_TESTS, reason="credentials.yml manquant, test ignore")
def test_oracle_connection():
    try:
        conn = connect_to_oracle()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        assert result[0] == 1
    except Exception as exc:
        pytest.fail(f"Echec de la connexion Oracle: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.mark.skipif(SKIP_DB_TESTS, reason="credentials.yml manquant, test ignore")
def test_postgres_connection():
    try:
        hook = get_postgres_hook()
        result = hook.get_first("SELECT 1")
        assert result[0] == 1
    except Exception as exc:
        pytest.fail(f"Echec de la connexion PostgreSQL: {exc}")


def test_connect_to_chimio_falls_back_to_dpip_service_name(monkeypatch):
    fake_airflow_conn = SimpleNamespace(
        host="dpip-scan.icr.local",
        port=1521,
        login="user",
        password="secret",
        schema=None,
        extra_dejson={},
    )
    sentinel_connection = object()
    captured = {}

    monkeypatch.setattr(
        "osiris_oncopole.utils.db.BaseHook.get_connection",
        lambda conn_id: fake_airflow_conn,
    )
    monkeypatch.setattr(
        "osiris_oncopole.utils.db.cx_Oracle.init_oracle_client",
        lambda lib_dir=None: None,
    )

    def fake_connect(*, user, password, dsn, encoding):
        captured["user"] = user
        captured["password"] = password
        captured["dsn"] = dsn
        captured["encoding"] = encoding
        return sentinel_connection

    monkeypatch.setattr("osiris_oncopole.utils.db.cx_Oracle.connect", fake_connect)

    connection = connect_to_chimio("dpip")

    assert connection is sentinel_connection
    assert captured == {
        "user": "user",
        "password": "secret",
        "dsn": "//dpip-scan.icr.local:1521/DPIP.icr.local",
        "encoding": "UTF-8",
    }
