"""Lambda handler for scheduled economic calendar scraping."""

import sys
from pathlib import Path

# Add src directory to path for imports
src_dir = Path(__file__).parent
sys.path.insert(0, str(src_dir))

from dotenv import load_dotenv
load_dotenv()

import logging

from scrapers.trading_economics import TradingEconomicsScraper
from processors.event_processor import EventProcessor
from services.database_service import DatabaseService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def handler(event, context):
    """AWS Lambda handler for scheduled scraping.

    Args:
        event: Lambda event (unused for scheduled triggers)
        context: Lambda context

    Returns:
        dict with scrape results
    """
    logger.info("Starting scheduled scrape")

    scraper = TradingEconomicsScraper()
    processor = EventProcessor()
    db = DatabaseService()

    # Ensure table exists
    db.ensure_events_table_exists()

    # Scrape
    raw_events = scraper.scrape_events()
    if not raw_events:
        logger.error("Failed to scrape events")
        return {"success": False, "error": "Failed to scrape events"}

    # Process
    raw_df = processor.raw_events_to_dataframe(raw_events)
    processed_df = processor.clean_and_transform(raw_df)

    if processed_df.empty:
        logger.info("No valid events after processing")
        return {"success": True, "scraped": 0, "inserted": 0}

    # Store
    db.insert_events_from_dataframe(processed_df)

    result = {
        "success": True,
        "scraped": len(raw_events),
        "processed": len(processed_df),
    }
    logger.info(f"Scrape complete: {result}")
    return result


if __name__ == "__main__":
    print(handler({}, None))
