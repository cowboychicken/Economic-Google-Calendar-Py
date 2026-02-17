import pytest
from dotenv import load_dotenv
from unittest.mock import MagicMock
import os

import pandas as pd
import numpy as np


@pytest.fixture(scope="session", autouse=True)
def load_env():
    """
    A pytest fixture that automatically loads the .env file
    before any tests run.
    """
    # Find the .env file at project root (two levels up from src/tests/)
    env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')

    if os.path.exists(env_path):
        print(f"\n--- Loading environment from {env_path} ---")
        load_dotenv(dotenv_path=env_path)
    else:
        print(f"\n--- .env file not found at {env_path}, skipping load ---")


@pytest.fixture
def sample_raw_events():
    """List of raw event records as returned by the scraper."""
    return [
        ["Monday February 16 2026", "8:30 AM", "US", "calendar-date-3", "Initial Jobless Claims"],
        ["Monday February 16 2026", "10:00 AM", "US", "calendar-date-2", "Consumer Confidence"],
        ["Monday February 16 2026", "", "US", "calendar-date-1", "API Crude Oil Stock Change"],
    ]


@pytest.fixture
def sample_events_df(sample_raw_events):
    """Pre-built clean DataFrame matching sample_raw_events."""
    return pd.DataFrame(
        sample_raw_events,
        columns=["date", "time", "country", "level", "summary"],
    )


@pytest.fixture
def mock_db_connection():
    """Mock psycopg2 connection and cursor."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor
