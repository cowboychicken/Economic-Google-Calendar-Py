"""Unit tests for EventProcessor."""

import pandas as pd
import numpy as np
import pytest

from processors.event_processor import EventProcessor


@pytest.fixture
def processor():
    return EventProcessor(timezone="UTC")


# --- raw_events_to_dataframe ---

class TestRawEventsToDataframe:
    def test_valid_input(self, processor, sample_raw_events):
        df = processor.raw_events_to_dataframe(sample_raw_events)
        assert len(df) == 3
        assert list(df.columns) == ["date", "time", "country", "level", "summary"]

    def test_empty_input(self, processor):
        df = processor.raw_events_to_dataframe([])
        assert df.empty


# --- _clean_importance_level ---

class TestCleanImportanceLevel:
    def test_digit_strings(self, processor, sample_events_df):
        result = processor._clean_importance_level(sample_events_df)
        assert list(result["level"]) == [3, 2, 1]

    def test_invalid_level_falls_back_to_zero(self, processor):
        df = pd.DataFrame({"level": ["no-number-here"]})
        result = processor._clean_importance_level(df)
        assert result["level"].iloc[0] == 0


# --- _parse_datetime_columns ---

class TestParseDatetimeColumns:
    def test_valid_date_and_time(self, processor):
        df = pd.DataFrame({
            "date": ["Monday February 16 2026"],
            "time": ["8:30 AM"],
        })
        result = processor._parse_datetime_columns(df)
        assert "event_datetime" in result.columns
        dt = result["event_datetime"].iloc[0]
        assert dt.hour == 8
        assert dt.minute == 30

    def test_missing_time_fills_midnight(self, processor):
        df = pd.DataFrame({
            "date": ["Monday February 16 2026"],
            "time": [""],
        })
        result = processor._parse_datetime_columns(df)
        dt = result["event_datetime"].iloc[0]
        assert dt.hour == 0
        assert dt.minute == 0

    def test_utc_localization(self, processor):
        df = pd.DataFrame({
            "date": ["Monday February 16 2026"],
            "time": ["8:30 AM"],
        })
        result = processor._parse_datetime_columns(df)
        assert str(result["event_datetime"].dt.tz) == "UTC"


# --- _remove_invalid_rows ---

class TestRemoveInvalidRows:
    def test_drops_nan_summary(self, processor):
        df = pd.DataFrame({
            "summary": [np.nan, "GDP"],
            "event_datetime": pd.to_datetime(["2026-02-16", "2026-02-17"]).tz_localize("UTC"),
        })
        result = processor._remove_invalid_rows(df)
        assert len(result) == 1
        assert result["summary"].iloc[0] == "GDP"

    def test_drops_empty_summary(self, processor):
        df = pd.DataFrame({
            "summary": ["  ", "GDP"],
            "event_datetime": pd.to_datetime(["2026-02-16", "2026-02-17"]).tz_localize("UTC"),
        })
        result = processor._remove_invalid_rows(df)
        assert len(result) == 1

    def test_drops_nan_datetime(self, processor):
        df = pd.DataFrame({
            "summary": ["GDP", "CPI"],
            "event_datetime": [pd.NaT, pd.Timestamp("2026-02-16", tz="UTC")],
        })
        result = processor._remove_invalid_rows(df)
        assert len(result) == 1


# --- clean_and_transform (end-to-end) ---

class TestCleanAndTransform:
    def test_end_to_end(self, processor, sample_events_df):
        result = processor.clean_and_transform(sample_events_df)
        assert "event_datetime" in result.columns
        assert result["level"].dtype in [int, np.int64]
        assert len(result) == 3

    def test_empty_df(self, processor):
        result = processor.clean_and_transform(pd.DataFrame())
        assert result.empty


# --- filter_high_importance_events ---

class TestFilterHighImportanceEvents:
    def test_filters_by_level(self, processor):
        df = pd.DataFrame({"level": [1, 2, 3, 3], "summary": ["a", "b", "c", "d"]})
        result = processor.filter_high_importance_events(df, min_level=3)
        assert len(result) == 2

    def test_empty_df(self, processor):
        result = processor.filter_high_importance_events(pd.DataFrame())
        assert result.empty
