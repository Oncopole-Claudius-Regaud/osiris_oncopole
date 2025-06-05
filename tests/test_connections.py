import pytest
from utils.db import connect_to_iris, connect_to_oracle, get_postgres_hook


def test_iris_connection():
    try:
        conn = connect_to_iris()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
    except Exception as e:
        pytest.fail(f"Échec de la connexion IRIS: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def test_oracle_connection():
    try:
        conn = connect_to_oracle()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        assert result[0] == 1
    except Exception as e:
        pytest.fail(f"Échec de la connexion Oracle: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def test_postgres_connection():
    try:
        hook = get_postgres_hook()
        result = hook.get_first("SELECT 1")
        assert result[0] == 1
    except Exception as e:
        pytest.fail(f"Échec de la connexion PostgreSQL: {e}")
