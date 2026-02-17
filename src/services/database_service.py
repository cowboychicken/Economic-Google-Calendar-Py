"""Database service for managing economic events storage."""

import logging
from typing import Optional

import pandas as pd
from psycopg2.extras import execute_values

from config.settings import DATABASE_CONFIG, DB_MAX_RETRIES, DB_WAIT_SECONDS
from utils.db_utils import DatabaseConnection

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for managing economic events in PostgreSQL database."""

    def __init__(self, db_config: dict = None):
        self.db_config = db_config or DATABASE_CONFIG
        self.max_retries = DB_MAX_RETRIES
        self.wait_seconds = DB_WAIT_SECONDS

    def ensure_events_table_exists(self) -> bool:
        """Ensure the economic_events table exists in the database."""
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS economic_events (
                        id SERIAL PRIMARY KEY,
                        event_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
                        time TIME NOT NULL,
                        country VARCHAR(50) NOT NULL,
                        level VARCHAR(50),
                        summary TEXT,
                        dateadded TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        gcal_event_id TEXT,
                        UNIQUE(event_datetime, summary)
                    );
                ''')
                conn.commit()
                logger.info("economic_events table ensured to exist")
                return True
        except Exception as e:
            logger.error(f"Failed to create economic_events table: {e}")
            return False

    def insert_events_from_dataframe(self, events_df: pd.DataFrame) -> bool:
        """Insert events from a DataFrame into the database.

        Args:
            events_df: DataFrame with columns: event_datetime, time, country, level, summary
        """
        if events_df.empty:
            logger.info("No events to insert")
            return True

        try:
            df_filtered = events_df[['event_datetime', 'time', 'country', 'level', 'summary']].copy()
            # Fill missing time values with midnight
            df_filtered['time'] = df_filtered['time'].fillna('12:00 AM')
            data_tuples = list(df_filtered.itertuples(index=False, name=None))

            insert_sql = '''
                INSERT INTO economic_events (event_datetime, time, country, level, summary)
                VALUES %s
                ON CONFLICT (event_datetime, summary) DO NOTHING;
            '''

            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                execute_values(cursor, insert_sql, data_tuples)
                rows_inserted = cursor.rowcount
                conn.commit()
                logger.info(f"Inserted {rows_inserted} new events (of {len(data_tuples)} total)")
                return True
        except Exception as e:
            logger.error(f"Failed to insert events: {e}")
            return False

    def get_unsynced_events(self, min_importance_level: int = 3) -> pd.DataFrame:
        """Get events that haven't been synced to Google Calendar yet."""
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                query_sql = '''
                    SELECT id, event_datetime, summary, country, level
                    FROM economic_events
                    WHERE gcal_event_id IS NULL
                      AND (
                          level = %s
                          OR LOWER(summary) LIKE '%%initial jobless claims%%'
                          OR LOWER(summary) LIKE '%%gdp growth rate%%'
                          OR LOWER(summary) LIKE '%%core pce price index mom%%'
                      )
                    ORDER BY event_datetime ASC
                '''
                unsynced_df = pd.read_sql_query(query_sql, conn, params=[str(min_importance_level)])
                logger.info(f"Found {len(unsynced_df)} unsynced events")
                return unsynced_df
        except Exception as e:
            logger.error(f"Failed to get unsynced events: {e}")
            return pd.DataFrame()

    def mark_event_as_synced(self, event_datetime, summary: str, gcal_event_id: str) -> bool:
        """Mark an event as synced with Google Calendar."""
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    UPDATE economic_events
                    SET gcal_event_id = %s
                    WHERE event_datetime = %s AND summary = %s
                    ''',
                    (gcal_event_id, event_datetime, summary),
                )
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
        """Get basic statistics about events in the database."""
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT
                        COUNT(*) as total_events,
                        COUNT(gcal_event_id) as synced_events,
                        COUNT(*) - COUNT(gcal_event_id) as unsynced_events,
                        COUNT(CASE WHEN level::int >= 3 THEN 1 END) as high_importance_events,
                        MIN(event_datetime) as earliest_event,
                        MAX(event_datetime) as latest_event
                    FROM economic_events
                ''')
                result = cursor.fetchone()
                if result:
                    return {
                        'total_events': result[0],
                        'synced_events': result[1],
                        'unsynced_events': result[2],
                        'high_importance_events': result[3],
                        'earliest_event': result[4],
                        'latest_event': result[5],
                    }
                return {}
        except Exception as e:
            logger.error(f"Failed to get database statistics: {e}")
            return {}

    def get_events(self, days: int = 30, min_level: int = 0) -> pd.DataFrame:
        """Get events from the database with optional filters."""
        try:
            with DatabaseConnection(self.max_retries, self.wait_seconds) as conn:
                query = """
                    SELECT id, event_datetime, summary, country, level, gcal_event_id
                    FROM economic_events
                    WHERE event_datetime >= NOW() - INTERVAL '%s days'
                    AND level::int >= %s
                    ORDER BY event_datetime DESC
                """
                return pd.read_sql_query(query, conn, params=[days, min_level])
        except Exception as e:
            logger.error(f"Failed to get events: {e}")
            return pd.DataFrame()
