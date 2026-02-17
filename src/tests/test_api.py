"""Unit tests for FastAPI API endpoints."""

from unittest.mock import patch, MagicMock
import pandas as pd
import pytest


# We need to patch the db instance created at module level in api.py
# and the DatabaseConnection used in health_check
@pytest.fixture
def client():
    with patch("api.DatabaseConnection"), \
         patch("api.db") as mock_db:
        from api import app
        from fastapi.testclient import TestClient
        c = TestClient(app)
        yield c, mock_db


# --- /health ---

class TestHealthEndpoint:
    def test_healthy(self):
        with patch("api.DatabaseConnection") as MockConn:
            mock_conn = MagicMock()
            MockConn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            MockConn.return_value.__exit__ = MagicMock(return_value=False)
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            mock_cur.fetchone.return_value = (1,)

            from api import app
            from fastapi.testclient import TestClient
            resp = TestClient(app).get("/health")
            assert resp.status_code == 200
            assert resp.json()["status"] == "healthy"

    def test_unhealthy(self):
        with patch("api.DatabaseConnection", side_effect=Exception("no db")):
            from api import app
            from fastapi.testclient import TestClient
            resp = TestClient(app).get("/health")
            assert resp.status_code == 200
            assert resp.json()["status"] == "unhealthy"


# --- /events ---

class TestEventsEndpoint:
    def test_get_events(self, client):
        c, mock_db = client
        mock_db.get_events.return_value = pd.DataFrame({
            "id": [1], "summary": ["GDP"], "level": [3],
        })
        resp = c.get("/events?days=7&level=2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1


# --- /events/stats ---

class TestStatsEndpoint:
    def test_get_stats(self, client):
        c, mock_db = client
        mock_db.get_event_statistics.return_value = {
            "total_events": 100,
            "synced_events": 50,
            "unsynced_events": 50,
            "high_importance_events": 30,
            "earliest_event": "2026-01-01",
            "latest_event": "2026-02-16",
        }
        resp = c.get("/events/stats")
        assert resp.status_code == 200
        assert resp.json()["total_events"] == 100


# --- /events/unsynced ---

class TestUnsyncedEndpoint:
    def test_get_unsynced(self, client):
        c, mock_db = client
        mock_db.get_unsynced_events.return_value = pd.DataFrame({
            "id": [1], "summary": ["GDP"],
        })
        resp = c.get("/events/unsynced")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# --- /scrape ---

class TestScrapeEndpoint:
    @patch("processors.event_processor.EventProcessor")
    @patch("scrapers.trading_economics.TradingEconomicsScraper")
    def test_trigger_scrape(self, MockScraper, MockProcessor, client):
        c, mock_db = client

        mock_scraper = MockScraper.return_value
        mock_scraper.scrape_events.return_value = [["d", "t", "c", "1", "GDP"]]

        mock_proc = MockProcessor.return_value
        raw_df = pd.DataFrame({"summary": ["GDP"]})
        mock_proc.raw_events_to_dataframe.return_value = raw_df
        mock_proc.clean_and_transform.return_value = raw_df

        mock_db.ensure_events_table_exists.return_value = True
        mock_db.insert_events_from_dataframe.return_value = True

        resp = c.post("/scrape")
        assert resp.status_code == 200
        assert resp.json()["success"] is True


# --- /sync ---

class TestSyncEndpoint:
    @patch("services.calendar_service.CalendarService")
    def test_trigger_sync(self, MockCal, client):
        c, mock_db = client

        mock_db.get_unsynced_events.return_value = pd.DataFrame({
            "event_datetime": ["2026-02-16"],
            "summary": ["GDP"],
        })
        mock_cal = MockCal.return_value
        mock_cal.create_event.return_value = "gcal123"
        mock_db.mark_event_as_synced.return_value = True

        resp = c.post("/sync")
        assert resp.status_code == 200
        data = resp.json()
        assert data["synced"] == 1
