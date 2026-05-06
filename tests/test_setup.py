# tests/test_setup.py
import pytest
from unittest.mock import MagicMock, patch, call
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_create_db_if_not_exists():
    with patch("psycopg2.connect") as mock_connect:
        admin_conn = MagicMock()
        admin_conn.autocommit = False
        mock_connect.return_value = admin_conn
        admin_cursor = MagicMock()
        admin_conn.cursor.return_value.__enter__ = lambda s: admin_cursor
        admin_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        admin_cursor.fetchone.return_value = None  # DB doesn't exist yet

        from setup_cii import create_db_if_not_exists
        create_db_if_not_exists(host="localhost", port=5433, user="u", password="p")

        calls = [str(c) for c in admin_cursor.execute.call_args_list]
        assert any("CREATE DATABASE" in c and "cii" in c for c in calls)


def test_skip_create_if_db_exists():
    with patch("psycopg2.connect") as mock_connect:
        admin_conn = MagicMock()
        mock_connect.return_value = admin_conn
        admin_cursor = MagicMock()
        admin_conn.cursor.return_value.__enter__ = lambda s: admin_cursor
        admin_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        admin_cursor.fetchone.return_value = ("cii",)  # DB already exists

        from setup_cii import create_db_if_not_exists
        create_db_if_not_exists(host="localhost", port=5433, user="u", password="p")

        calls = [str(c) for c in admin_cursor.execute.call_args_list]
        assert not any("CREATE DATABASE" in c for c in calls)
