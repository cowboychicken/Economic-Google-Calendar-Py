#!/usr/bin/env python3
"""
Economic Calendar Application - Main Entry Point

This application scrapes economic calendar events from Trading Economics,
stores them in a PostgreSQL database, and syncs high-importance events
to a Google Calendar.
"""

import logging
import sys
from pathlib import Path

# Add src directory to path for imports
src_dir = Path(__file__).parent
sys.path.insert(0, str(src_dir))

from dotenv import load_dotenv
from scrapers.trading_economics import TradingEconomicsScraper  
from processors.event_processor import EventProcessor
from services.database_service import DatabaseService
from services.calendar_service import CalendarService
from config.settings import HIGH_IMPORTANCE_LEVEL

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('economic_calendar.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class EconomicCalendarApp:
    """Main application class for the Economic Calendar system."""
    
    def __init__(self):
        """Initialize application components."""
        self.scraper = TradingEconomicsScraper()
        self.processor = EventProcessor()
        self.db_service = DatabaseService()
        self.calendar_service = CalendarService()
    
    def run_full_sync(self) -> bool:
        """Run the complete economic calendar sync process.
        
        Returns:
            True if sync completed successfully, False otherwise
        """
        logger.info("Starting full economic calendar sync")
        
        # Step 1: Ensure database is set up
        if not self.db_service.ensure_events_table_exists():
            logger.error("Failed to ensure database table exists")
            return False
        
        # Step 2: Scrape latest events from Trading Economics
        logger.info("Scraping events from Trading Economics...")
        raw_events = self.scraper.scrape_events()
        
        if not raw_events:
            logger.error("Failed to scrape any events")
            return False
        
        # Step 3: Process and clean the scraped data
        logger.info("Processing scraped event data...")
        raw_events_df = self.processor.raw_events_to_dataframe(raw_events)
        processed_events_df = self.processor.clean_and_transform(raw_events_df)
        
        if processed_events_df.empty:
            logger.warning("No valid events after processing")
            return True  # Not an error, just no new data
        
        # Step 4: Store events in database
        logger.info("Storing events in database...")
        if not self.db_service.insert_events_from_dataframe(processed_events_df):
            logger.error("Failed to store events in database")
            return False
        
        # Step 5: Sync high-importance events to Google Calendar
        logger.info("Syncing high-importance events to Google Calendar...")
        if not self._sync_to_calendar():
            logger.warning("Calendar sync had issues, but database sync completed")
        
        # Step 6: Show summary statistics
        self._print_summary()
        
        logger.info("Full economic calendar sync completed successfully")
        return True
    
    def _sync_to_calendar(self) -> bool:
        """Sync unsynced high-importance events to Google Calendar.
        
        Returns:
            True if all syncs successful, False if any failed
        """
        # Get unsynced high-importance events
        unsynced_events = self.db_service.get_unsynced_events(HIGH_IMPORTANCE_LEVEL)
        
        if unsynced_events.empty:
            logger.info("No unsynced high-importance events found")
            return True
        
        logger.info(f"Found {len(unsynced_events)} events to sync to calendar")
        
        # Test calendar connection first
        if not self.calendar_service.test_connection():
            logger.error("Calendar connection test failed")
            return False
        
        sync_success = True
        synced_count = 0
        
        # Sync each event
        for _, event in unsynced_events.iterrows():
            try:
                # Create event in Google Calendar
                gcal_event_id = self.calendar_service.create_event(event.to_dict())
                
                if gcal_event_id:
                    # Mark as synced in database
                    success = self.db_service.mark_event_as_synced(
                        event['event_datetime'],
                        event['summary'],
                        gcal_event_id
                    )
                    
                    if success:
                        synced_count += 1
                        logger.info(f"Synced: {event['summary'][:50]}...")
                    else:
                        logger.error(f"Failed to mark as synced: {event['summary'][:50]}...")
                        sync_success = False
                else:
                    logger.error(f"Failed to create calendar event: {event['summary'][:50]}...")
                    sync_success = False
                    
            except Exception as e:
                logger.error(f"Error syncing event '{event['summary'][:50]}...': {e}")
                sync_success = False
        
        logger.info(f"Successfully synced {synced_count} of {len(unsynced_events)} events")
        return sync_success
    
    def _print_summary(self):
        """Print summary statistics about the database."""
        stats = self.db_service.get_event_statistics()
        
        if stats:
            logger.info("=== DATABASE SUMMARY ===")
            logger.info(f"Total events: {stats.get('total_events', 0)}")
            logger.info(f"Synced events: {stats.get('synced_events', 0)}")
            logger.info(f"Unsynced events: {stats.get('unsynced_events', 0)}")
            logger.info(f"High importance events: {stats.get('high_importance_events', 0)}")
            
            if stats.get('earliest_event'):
                logger.info(f"Date range: {stats['earliest_event']} to {stats['latest_event']}")
            
            logger.info("========================")
    
    def run_database_only(self) -> bool:
        """Run scraping and database storage only (no calendar sync).
        
        Returns:
            True if completed successfully, False otherwise
        """
        logger.info("Running database-only sync (no calendar)")
        
        # Steps 1-4 from full sync
        if not self.db_service.ensure_events_table_exists():
            return False
        
        raw_events = self.scraper.scrape_events()
        if not raw_events:
            return False
        
        raw_events_df = self.processor.raw_events_to_dataframe(raw_events)
        processed_events_df = self.processor.clean_and_transform(raw_events_df)
        
        if processed_events_df.empty:
            logger.info("No new events to store")
            return True
        
        success = self.db_service.insert_events_from_dataframe(processed_events_df)
        if success:
            self._print_summary()
        
        return success
    
    def run_calendar_only(self) -> bool:
        """Run calendar sync only (no scraping).
        
        Returns:
            True if sync successful, False otherwise  
        """
        logger.info("Running calendar-only sync (no scraping)")
        return self._sync_to_calendar()


def main():
    """Main entry point."""
    try:
        app = EconomicCalendarApp()
        
        # You can modify this to run different modes:
        # app.run_full_sync()        # Complete sync (default)
        # app.run_database_only()    # Just scraping + database
        # app.run_calendar_only()    # Just calendar sync
        
        success = app.run_full_sync()
        
        if success:
            logger.info("Application completed successfully")
            sys.exit(0)
        else:
            logger.error("Application completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()