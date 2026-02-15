"""Configuration settings for Economic Calendar application."""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
RESOURCES_DIR = BASE_DIR / "resources"
CSV_FILE = RESOURCES_DIR / "economic-calendar-events.csv"

# Trading Economics scraping configuration
TRADING_ECONOMICS_URL = "https://tradingeconomics.com/united-states/calendar"
SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}

# Google Calendar configuration  
GOOGLE_CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar.events"]
GOOGLE_CALENDAR_ID = "a2f405442fb6c4687738183931cbe0fa188d41fd0e60d0c021f544f51b639dc9@group.calendar.google.com"
OAUTH_TOKEN_PATH = RESOURCES_DIR / "oauth-token.json"
CREDENTIALS_PATH = RESOURCES_DIR / "credentials.json"

# Database configuration (from environment)
DATABASE_CONFIG = {
    "database": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"), 
    "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    "port": os.environ.get("POSTGRES_PORT", "5432"),
}

# Event filtering configuration
HIGH_IMPORTANCE_LEVEL = 3
DEFAULT_TIMEZONE = "UTC"

# Database connection settings
DB_MAX_RETRIES = 5
DB_WAIT_SECONDS = 2