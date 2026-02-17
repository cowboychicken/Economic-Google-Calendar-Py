"""Unit tests for CalendarService (mock Google API)."""

from unittest.mock import patch, MagicMock
from datetime import datetime
import pytest

from googleapiclient.errors import HttpError


# Patch _authenticate before CalendarService.__init__ runs
@pytest.fixture
def calendar_service():
    with patch("services.calendar_service.CalendarService._authenticate"):
        from services.calendar_service import CalendarService
        svc = CalendarService(calendar_id="test-cal", credentials_path="/fake", token_path="/fake")
    return svc


@pytest.fixture
def calendar_service_with_mock(calendar_service):
    """CalendarService with a mock Google API service object."""
    mock_service = MagicMock()
    calendar_service.service = mock_service
    return calendar_service, mock_service


# --- create_event ---

class TestCreateEvent:
    def test_returns_event_id(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        mock_api.events().insert().execute.return_value = {"id": "evt123", "htmlLink": "http://link"}

        result = svc.create_event({
            "summary": "GDP",
            "event_datetime": datetime(2026, 2, 16, 8, 30),
            "level": 3,
        })
        assert result == "evt123"

    def test_service_none_returns_none(self, calendar_service):
        calendar_service.service = None
        result = calendar_service.create_event({"summary": "x", "event_datetime": datetime.now()})
        assert result is None

    def test_http_error(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        resp = MagicMock(status=403, reason="Forbidden")
        mock_api.events().insert().execute.side_effect = HttpError(resp, b"error")

        result = svc.create_event({
            "summary": "GDP",
            "event_datetime": datetime(2026, 2, 16, 8, 30),
        })
        assert result is None


# --- get_existing_events ---

class TestGetExistingEvents:
    def test_success(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        mock_api.events().list().execute.return_value = {"items": [{"id": "1"}, {"id": "2"}]}

        events = svc.get_existing_events()
        assert len(events) == 2

    def test_empty(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        mock_api.events().list().execute.return_value = {"items": []}

        assert svc.get_existing_events() == []

    def test_service_none(self, calendar_service):
        calendar_service.service = None
        assert calendar_service.get_existing_events() == []


# --- delete_event ---

class TestDeleteEvent:
    def test_success(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        assert svc.delete_event("evt123") is True

    def test_http_error(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        resp = MagicMock(status=404, reason="Not Found")
        mock_api.events().delete().execute.side_effect = HttpError(resp, b"error")

        assert svc.delete_event("evt123") is False


# --- test_connection ---

class TestTestConnection:
    def test_delegates_to_get_existing_events(self, calendar_service_with_mock):
        svc, mock_api = calendar_service_with_mock
        mock_api.events().list().execute.return_value = {"items": []}

        assert svc.test_connection() is True
