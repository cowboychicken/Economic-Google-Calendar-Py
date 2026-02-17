"""Unit tests for DatabaseService (mock DatabaseConnection)."""

from unittest.mock import patch, MagicMock, PropertyMock
import pandas as pd
import numpy as np
import pytest

from services.database_service import DatabaseService


@pytest.fixture
def db_service():
    return DatabaseService(db_config={"database": "test"})


@pytest.fixture
def mock_conn():
    """Provide a mock connection and cursor via DatabaseConnection context manager."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _patch_db_conn(mock_conn):
    """Return a patch that makes DatabaseConnection yield mock_conn."""
    conn, _ = mock_conn
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return patch("services.database_service.DatabaseConnection", return_value=ctx)


# --- ensure_events_table_exists ---

class TestEnsureEventsTableExists:
    def test_success(self, db_service, mock_conn):
        with _patch_db_conn(mock_conn):
            assert db_service.ensure_events_table_exists() is True

    def test_exception(self, db_service):
        with patch("services.database_service.DatabaseConnection", side_effect=Exception("fail")):
            assert db_service.ensure_events_table_exists() is False


# --- insert_events_from_dataframe ---

class TestInsertEventsFromDataframe:
    def test_empty_df(self, db_service):
        assert db_service.insert_events_from_dataframe(pd.DataFrame()) is True

    @patch("services.database_service.execute_values")
    def test_valid_df(self, mock_exec_values, db_service, mock_conn):
        df = pd.DataFrame({
            "event_datetime": pd.to_datetime(["2026-02-16"]),
            "time": ["8:30 AM"],
            "country": ["US"],
            "level": [3],
            "summary": ["GDP"],
        })
        conn, cursor = mock_conn
        cursor.rowcount = 1
        with _patch_db_conn(mock_conn):
            assert db_service.insert_events_from_dataframe(df) is True
        mock_exec_values.assert_called_once()

    @patch("services.database_service.execute_values")
    def test_nan_time_fill(self, mock_exec_values, db_service, mock_conn):
        df = pd.DataFrame({
            "event_datetime": pd.to_datetime(["2026-02-16"]),
            "time": [np.nan],
            "country": ["US"],
            "level": [3],
            "summary": ["GDP"],
        })
        conn, cursor = mock_conn
        cursor.rowcount = 1
        with _patch_db_conn(mock_conn):
            assert db_service.insert_events_from_dataframe(df) is True

    def test_exception(self, db_service):
        df = pd.DataFrame({
            "event_datetime": pd.to_datetime(["2026-02-16"]),
            "time": ["8:30 AM"],
            "country": ["US"],
            "level": [3],
            "summary": ["GDP"],
        })
        with patch("services.database_service.DatabaseConnection", side_effect=Exception("fail")):
            assert db_service.insert_events_from_dataframe(df) is False


# --- get_unsynced_events ---

class TestGetUnsyncedEvents:
    def test_returns_df(self, db_service, mock_conn):
        expected = pd.DataFrame({"id": [1], "summary": ["GDP"]})
        with _patch_db_conn(mock_conn), \
             patch("services.database_service.pd.read_sql_query", return_value=expected):
            result = db_service.get_unsynced_events()
            assert len(result) == 1

    def test_exception(self, db_service):
        with patch("services.database_service.DatabaseConnection", side_effect=Exception("fail")):
            result = db_service.get_unsynced_events()
            assert result.empty


# --- mark_event_as_synced ---

class TestMarkEventAsSynced:
    def test_found(self, db_service, mock_conn):
        _, cursor = mock_conn
        cursor.rowcount = 1
        with _patch_db_conn(mock_conn):
            assert db_service.mark_event_as_synced("2026-02-16", "GDP", "gcal123") is True

    def test_not_found(self, db_service, mock_conn):
        _, cursor = mock_conn
        cursor.rowcount = 0
        with _patch_db_conn(mock_conn):
            assert db_service.mark_event_as_synced("2026-02-16", "GDP", "gcal123") is False

    def test_exception(self, db_service):
        with patch("services.database_service.DatabaseConnection", side_effect=Exception("fail")):
            assert db_service.mark_event_as_synced("2026-02-16", "GDP", "gcal123") is False


# --- get_event_statistics ---

class TestGetEventStatistics:
    def test_returns_dict(self, db_service, mock_conn):
        _, cursor = mock_conn
        cursor.fetchone.return_value = (100, 50, 50, 30, "2026-01-01", "2026-02-16")
        with _patch_db_conn(mock_conn):
            stats = db_service.get_event_statistics()
            assert stats["total_events"] == 100
            assert stats["synced_events"] == 50

    def test_exception(self, db_service):
        with patch("services.database_service.DatabaseConnection", side_effect=Exception("fail")):
            assert db_service.get_event_statistics() == {}


# --- get_events ---

class TestGetEvents:
    def test_returns_df(self, db_service, mock_conn):
        expected = pd.DataFrame({"id": [1], "summary": ["GDP"]})
        with _patch_db_conn(mock_conn), \
             patch("services.database_service.pd.read_sql_query", return_value=expected):
            result = db_service.get_events(days=7, min_level=2)
            assert len(result) == 1

    def test_exception(self, db_service):
        with patch("services.database_service.DatabaseConnection", side_effect=Exception("fail")):
            result = db_service.get_events()
            assert result.empty
