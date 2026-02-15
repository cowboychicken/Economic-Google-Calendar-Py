"""Database service for managing economic events storage."""

import logging
from typing import Optional

import pandas as pd

from config.settings import DATABASE_CONFIG, DB_MAX_RETRIES, DB_WAIT_SECONDS
from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for managing economic events in PostgreSQL database."""
    
    def __init__(self, db_config: dict = None):
        """Initialize database service.
        
        Args:
            db_config: Database configuration dict, uses default if None
        """
        self.db_config = db_config or DATABASE_CONFIG
        self.max_retries = DB_MAX_RETRIES
        self.wait_seconds = DB_WAIT_SECONDS
    
    def ensure_events_table_exists(self) -> bool:
        """Ensure the events table exists in the database.
        
        Returns:
            True if table exists/was created successfully, False otherwise
        """
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                
                create_table_sql = '''
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    event_datetime TIMESTAMPTZ NOT NULL,
                    summary TEXT NOT NULL,
                    country VARCHAR(100),
                    level INTEGER DEFAULT 0,
                    gcal_event_id VARCHAR(255),
                    date_added TIMESTAMPTZ DEFAULT NOW(),
                    is_synced BOOLEAN DEFAULT FALSE,
                    UNIQUE(event_datetime, summary)
                );
                
                CREATE INDEX IF NOT EXISTS idx_events_datetime ON events(event_datetime);
                CREATE INDEX IF NOT EXISTS idx_events_synced ON events(is_synced);
                CREATE INDEX IF NOT EXISTS idx_events_level ON events(level);
                '''
                
                cursor.execute(create_table_sql)
                conn.commit()
                
                logger.info("Events table ensured to exist")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create events table: {e}")
            return False
    
    def insert_events_from_dataframe(self, events_df: pd.DataFrame) -> bool:
        """Insert events from a DataFrame into the database.
        
        Args:
            events_df: DataFrame containing event data
            
        Returns:
            True if insertion successful, False otherwise
        """
        if events_df.empty:
            logger.info("No events to insert")
            return True
        
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                
                # Prepare insert statement with ON CONFLICT handling
                insert_sql = '''
                INSERT INTO events (event_datetime, summary, country, level)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_datetime, summary) DO NOTHING
                '''
                
                # Convert DataFrame to list of tuples for batch insert
                event_records = [
                    (
                        row['event_datetime'],
                        row['summary'],
                        row['country'],
                        row['level']
                    )
                    for _, row in events_df.iterrows()
                ]
                
                # Execute batch insert
                cursor.executemany(insert_sql, event_records)
                rows_inserted = cursor.rowcount
                conn.commit()
                
                logger.info(f"Successfully inserted {rows_inserted} new events")
                return True
                
        except Exception as e:
            logger.error(f"Failed to insert events: {e}")
            return False
    
    def get_unsynced_events(self, min_importance_level: int = 3) -> pd.DataFrame:
        """Get events that haven't been synced to Google Calendar yet.
        
        Args:
            min_importance_level: Minimum importance level to include
            
        Returns:
            DataFrame of unsynced events
        """
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                query_sql = '''
                SELECT id, event_datetime, summary, country, level
                FROM events 
                WHERE gcal_event_id IS NULL 
                  AND is_synced = FALSE
                  AND level >= %s
                ORDER BY event_datetime ASC
                '''
                
                unsynced_df = pd.read_sql_query(
                    query_sql, 
                    conn, 
                    params=[min_importance_level]
                )
                
                logger.info(f"Found {len(unsynced_df)} unsynced events")
                return unsynced_df
                
        except Exception as e:
            logger.error(f"Failed to get unsynced events: {e}")
            return pd.DataFrame()
    
    def mark_event_as_synced(self, event_datetime, summary: str, gcal_event_id: str) -> bool:
        """Mark an event as synced with Google Calendar.
        
        Args:
            event_datetime: Event datetime
            summary: Event summary/description
            gcal_event_id: Google Calendar event ID
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                
                update_sql = '''
                UPDATE events 
                SET gcal_event_id = %s, is_synced = TRUE
                WHERE event_datetime = %s AND summary = %s
                '''
                
                cursor.execute(update_sql, (gcal_event_id, event_datetime, summary))
                rows_updated = cursor.rowcount
                conn.commit()
                
                if rows_updated > 0:
                    logger.info(f"Marked event as synced: {summary}")
                    return True
                else:
                    logger.warning(f"No event found to mark as synced: {summary}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to mark event as synced: {e}")
            return False
    
    def get_event_statistics(self) -> dict:
        """Get basic statistics about events in the database.
        
        Returns:
            Dictionary with event statistics
        """
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                
                stats_sql = '''
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN is_synced = TRUE THEN 1 END) as synced_events,
                    COUNT(CASE WHEN level >= 3 THEN 1 END) as high_importance_events,
                    MIN(event_datetime) as earliest_event,
                    MAX(event_datetime) as latest_event
                FROM events
                '''
                
                cursor.execute(stats_sql)
                result = cursor.fetchone()
                
                if result:
                    stats = {
                        'total_events': result[0],
                        'synced_events': result[1], 
                        'unsynced_events': result[0] - result[1],
                        'high_importance_events': result[2],
                        'earliest_event': result[3],
                        'latest_event': result[4]
                    }
                    
                    logger.info(f"Database statistics: {stats}")
                    return stats
                
                return {}
                
        except Exception as e:
            logger.error(f"Failed to get database statistics: {e}")
            return {}