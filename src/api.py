"""FastAPI REST API for the Economic Calendar application."""

import sys
from pathlib import Path

# Add src directory to path for imports
src_dir = Path(__file__).parent
sys.path.insert(0, str(src_dir))

from dotenv import load_dotenv
load_dotenv()

import logging
import pandas as pd
from fastapi import FastAPI, Query

from services.database_service import DatabaseService
from utils.db_utils import DatabaseConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

app = FastAPI(title="Economic Calendar API")
db = DatabaseService()


def _serialize_df(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-safe list of dicts."""
    events = df.to_dict("records")
    for event in events:
        for key, val in event.items():
            if hasattr(val, "isoformat"):
                event[key] = val.isoformat()
    return events


@app.get("/health")
def health_check():
    """Health check with DB connectivity status."""
    try:
        with DatabaseConnection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}


@app.get("/events")
def get_events(
    days: int = Query(30, description="Lookback period in days"),
    level: int = Query(0, description="Minimum importance level (0-3)"),
):
    """List events from the database."""
    try:
        df = db.get_events(days=days, min_level=level)
        return {"count": len(df), "events": _serialize_df(df)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/events/stats")
def get_event_stats():
    """Get event statistics from the database."""
    try:
        stats = db.get_event_statistics()
        for key in ('earliest_event', 'latest_event'):
            if stats.get(key) and hasattr(stats[key], 'isoformat'):
                stats[key] = stats[key].isoformat()
        return stats
    except Exception as e:
        return {"error": str(e)}


@app.get("/events/unsynced")
def get_unsynced():
    """Get events that haven't been synced to Google Calendar."""
    try:
        df = db.get_unsynced_events()
        return {"count": len(df), "events": _serialize_df(df)}
    except Exception as e:
        return {"error": str(e)}


@app.post("/scrape")
def trigger_scrape():
    """Trigger a scrape from Trading Economics and store in DB."""
    try:
        from scrapers.trading_economics import TradingEconomicsScraper
        from processors.event_processor import EventProcessor

        scraper = TradingEconomicsScraper()
        processor = EventProcessor()

        raw_events = scraper.scrape_events()
        if not raw_events:
            return {"success": False, "error": "Failed to scrape events"}

        raw_df = processor.raw_events_to_dataframe(raw_events)
        processed_df = processor.clean_and_transform(raw_df)

        if processed_df.empty:
            return {"success": True, "message": "No valid events after processing", "scraped": 0}

        db.ensure_events_table_exists()
        db.insert_events_from_dataframe(processed_df)

        return {
            "success": True,
            "scraped": len(raw_events),
            "processed": len(processed_df),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/sync")
def trigger_sync():
    """Trigger Google Calendar sync for unsynced events."""
    try:
        from services.calendar_service import CalendarService

        calendar_service = CalendarService()

        unsynced = db.get_unsynced_events()
        if unsynced.empty:
            return {"success": True, "message": "No unsynced events", "synced": 0}

        synced_count = 0
        errors = []

        for _, event in unsynced.iterrows():
            try:
                gcal_id = calendar_service.create_event(event.to_dict())
                if gcal_id:
                    db.mark_event_as_synced(event["event_datetime"], event["summary"], gcal_id)
                    synced_count += 1
            except Exception as e:
                errors.append(f"{event['summary']}: {e}")

        return {
            "success": len(errors) == 0,
            "synced": synced_count,
            "total": len(unsynced),
            "errors": errors,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
