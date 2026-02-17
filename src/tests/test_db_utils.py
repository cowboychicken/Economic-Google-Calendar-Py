# test_db.py
from unittest.mock import patch, MagicMock, call
import psycopg2
import pytest

from utils.db_utils import DatabaseConnection


# --- Unit tests (mocked, no live DB needed) ---

class TestRetryLogic:
    @patch("utils.db_utils.time.sleep")
    @patch("utils.db_utils.psycopg2.connect")
    def test_retry_on_operational_error(self, mock_connect, mock_sleep):
        """Should retry on OperationalError then succeed."""
        mock_connect.side_effect = [
            psycopg2.OperationalError("connection refused"),
            MagicMock(),  # success on 2nd attempt
        ]
        with DatabaseConnection(max_retries=3, wait_seconds=1) as conn:
            assert conn is not None
        mock_sleep.assert_called_once_with(1)

    @patch("utils.db_utils.time.sleep")
    @patch("utils.db_utils.psycopg2.connect")
    def test_exhausted_retries_raises(self, mock_connect, mock_sleep):
        """Should raise after max_retries exhausted."""
        mock_connect.side_effect = psycopg2.OperationalError("always fail")
        with pytest.raises(psycopg2.OperationalError):
            with DatabaseConnection(max_retries=2, wait_seconds=0) as conn:
                pass
        assert mock_connect.call_count == 2


class TestExitBehavior:
    @patch("utils.db_utils.psycopg2.connect")
    def test_exit_commits_on_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with DatabaseConnection(max_retries=1) as conn:
            pass  # no exception

        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("utils.db_utils.psycopg2.connect")
    def test_exit_rollbacks_on_exception(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with pytest.raises(ValueError):
            with DatabaseConnection(max_retries=1) as conn:
                raise ValueError("boom")

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()


# --- Integration test (requires a running PostgreSQL) ---

def test_database_conection_success():

    try:
        with DatabaseConnection() as conn:
            assert conn is not None

            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                assert result==(1,)
    except Exception as e:
        assert False, f"Database connection failed: {e}"
