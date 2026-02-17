"""Unit tests for scraper_handler Lambda handler."""

from unittest.mock import patch, MagicMock
import pandas as pd
import pytest


@pytest.fixture
def mock_services():
    with patch("scraper_handler.TradingEconomicsScraper") as MockScraper, \
         patch("scraper_handler.EventProcessor") as MockProcessor, \
         patch("scraper_handler.DatabaseService") as MockDB:
        yield MockScraper.return_value, MockProcessor.return_value, MockDB.return_value


class TestHandler:
    def test_scrape_fails(self, mock_services):
        scraper, processor, db = mock_services
        scraper.scrape_events.return_value = []

        from scraper_handler import handler
        result = handler({}, None)
        assert result["success"] is False

    def test_empty_after_processing(self, mock_services):
        scraper, processor, db = mock_services
        scraper.scrape_events.return_value = [["d", "t", "c", "1", "GDP"]]
        processor.raw_events_to_dataframe.return_value = pd.DataFrame({"summary": ["GDP"]})
        processor.clean_and_transform.return_value = pd.DataFrame()

        from scraper_handler import handler
        result = handler({}, None)
        assert result["success"] is True
        assert result["scraped"] == 0

    def test_happy_path(self, mock_services):
        scraper, processor, db = mock_services
        raw = [["d", "t", "c", "1", "GDP"], ["d", "t", "c", "2", "CPI"]]
        scraper.scrape_events.return_value = raw

        raw_df = pd.DataFrame({"summary": ["GDP", "CPI"]})
        processed_df = pd.DataFrame({"summary": ["GDP", "CPI"]})
        processor.raw_events_to_dataframe.return_value = raw_df
        processor.clean_and_transform.return_value = processed_df

        db.ensure_events_table_exists.return_value = True
        db.insert_events_from_dataframe.return_value = True

        from scraper_handler import handler
        result = handler({}, None)
        assert result["success"] is True
        assert result["scraped"] == 2
        assert result["processed"] == 2
        db.insert_events_from_dataframe.assert_called_once()
