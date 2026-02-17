"""Unit tests for TradingEconomicsScraper (mocked HTTP + BS4)."""

from unittest.mock import patch, MagicMock
import pytest
import requests
from bs4 import BeautifulSoup

from scrapers.trading_economics import TradingEconomicsScraper


SAMPLE_HTML = """
<html><body>
<table id="calendar">
  <tr class="table-header"><th>Monday February 16 2026</th></tr>
  <tr>
    <td>8:30 AM<span class="calendar-date-3"></span></td>
    <td>US</td>
    <td>Initial Jobless Claims</td>
  </tr>
  <tr>
    <td>10:00 AM<span class="calendar-date-2"></span></td>
    <td>US</td>
    <td>Consumer Confidence</td>
  </tr>
</table>
</body></html>
"""


@pytest.fixture
def scraper():
    return TradingEconomicsScraper(url="http://fake", headers={"User-Agent": "test"})


# --- scrape_events_table ---

class TestScrapeEventsTable:
    @patch("scrapers.trading_economics.requests.get")
    def test_success(self, mock_get, scraper):
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        table = scraper.scrape_events_table()
        assert table is not None
        assert table.name == "table"

    @patch("scrapers.trading_economics.requests.get")
    def test_missing_table(self, mock_get, scraper):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body><p>No table</p></body></html>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        assert scraper.scrape_events_table() is None

    @patch("scrapers.trading_economics.requests.get", side_effect=requests.RequestException("timeout"))
    def test_request_exception(self, mock_get, scraper):
        assert scraper.scrape_events_table() is None


# --- parse_event_row ---

class TestParseEventRow:
    def test_valid_row(self, scraper):
        html = '<tr><td>8:30 AM<span class="calendar-date-3"></span></td><td>US</td><td>GDP</td></tr>'
        row = BeautifulSoup(html, "html.parser").find("tr")
        result = scraper.parse_event_row(row, "Monday February 16 2026")
        assert result == ["Monday February 16 2026", "8:30 AM", "US", "3", "GDP"]

    def test_missing_cells(self, scraper):
        html = "<tr></tr>"
        row = BeautifulSoup(html, "html.parser").find("tr")
        result = scraper.parse_event_row(row, "Monday February 16 2026")
        assert result == []

    def test_css_class_level_extraction(self, scraper):
        html = '<tr><td>9:00 AM<span class="calendar-date-1"></span></td><td>US</td><td>PMI</td></tr>'
        row = BeautifulSoup(html, "html.parser").find("tr")
        result = scraper.parse_event_row(row, "date")
        assert result[3] == "1"


# --- parse_all_events ---

class TestParseAllEvents:
    def test_date_header_tracking(self, scraper):
        table = BeautifulSoup(SAMPLE_HTML, "html.parser").find("table")
        events = scraper.parse_all_events(table)
        assert len(events) == 2
        assert events[0][0] == "Monday February 16 2026"
        assert events[1][0] == "Monday February 16 2026"

    def test_skipping_empty_rows(self, scraper):
        html = '<table id="calendar"><tr class="table-header"><th>Date</th></tr><tr></tr></table>'
        table = BeautifulSoup(html, "html.parser").find("table")
        events = scraper.parse_all_events(table)
        assert events == []


# --- scrape_events ---

class TestScrapeEvents:
    @patch("scrapers.trading_economics.requests.get")
    def test_table_not_found_returns_empty(self, mock_get, scraper):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body></body></html>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        assert scraper.scrape_events() == []
