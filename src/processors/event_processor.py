"""Event data processing and transformation logic."""

import datetime
import logging
from typing import List

import numpy as np
import pandas as pd
import pytz

from config.settings import DEFAULT_TIMEZONE

logger = logging.getLogger(__name__)


class EventProcessor:
    """Processes and transforms raw economic event data."""
    
    def __init__(self, timezone: str = DEFAULT_TIMEZONE):
        self.timezone = pytz.timezone(timezone) if timezone != "UTC" else pytz.UTC
    
    def raw_events_to_dataframe(self, raw_events: List[List[str]]) -> pd.DataFrame:
        """Convert raw scraped events to a pandas DataFrame.
        
        Args:
            raw_events: List of event records from scraper
            
        Returns:
            DataFrame with standardized column names
        """
        if not raw_events:
            logger.warning("No raw events provided")
            return pd.DataFrame()
        
        df = pd.DataFrame(
            raw_events, 
            columns=["date", "time", "country", "level", "summary"]
        )
        
        logger.info(f"Created DataFrame with {len(df)} raw events")
        return df
    
    def clean_and_transform(self, raw_events_df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform the raw events DataFrame.
        
        Args:
            raw_events_df: Raw events DataFrame from scraper
            
        Returns:
            Cleaned and transformed DataFrame ready for database
        """
        if raw_events_df.empty:
            logger.warning("Empty DataFrame provided for processing")
            return pd.DataFrame()
        
        processed_df = raw_events_df.copy()
        
        # Clean and transform importance level
        processed_df = self._clean_importance_level(processed_df)
        
        # Clean and transform datetime
        processed_df = self._parse_datetime_columns(processed_df)
        
        # Replace empty strings with NaN for cleaner data
        processed_df = processed_df.replace("", np.nan)
        
        # Remove any rows with critical missing data
        processed_df = self._remove_invalid_rows(processed_df)
        
        logger.info(f"Processed {len(processed_df)} valid events")
        return processed_df
    
    def _clean_importance_level(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract numeric importance level from CSS class strings.
        
        Args:
            df: DataFrame with 'level' column containing CSS classes
            
        Returns:
            DataFrame with numeric 'level' column
        """
        try:
            # Convert 'calendar-date-1' -> '1', etc.
            df["level"] = (
                df["level"]
                .astype(str)
                .str.split("calendar-date-")
                .str[-1]
                .astype(int)
            )
            logger.debug("Successfully cleaned importance levels")
        except Exception as e:
            logger.warning(f"Failed to clean importance levels: {e}")
            # Fallback: set all levels to 0
            df["level"] = 0
        
        return df
    
    def _parse_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and combine date and time columns into proper datetime.
        
        Args:
            df: DataFrame with 'date' and 'time' string columns
            
        Returns:
            DataFrame with additional 'event_datetime' column
        """
        try:
            # Parse date strings like "Friday February 16 2026" 
            df['parsed_date'] = pd.to_datetime(df['date'], format='%A %B %d %Y')
            
            # Parse time strings like "9:00 AM" - handle missing times
            df['parsed_time'] = pd.to_datetime(
                df['time'], 
                format='%I:%M %p', 
                errors='coerce'  # NaT for invalid times
            ).dt.time
            
            # Fill missing times with midnight
            df['parsed_time'] = df['parsed_time'].fillna(datetime.time(0, 0))
            
            # Combine date and time into single datetime column
            df['event_datetime'] = pd.to_datetime(
                df['parsed_date'].astype(str) + ' ' + df['parsed_time'].astype(str)
            )
            
            # Trading Economics times are already in UTC
            df['event_datetime'] = df['event_datetime'].dt.tz_localize('UTC')
            
            # Clean up intermediate columns
            df = df.drop(['parsed_date', 'parsed_time'], axis=1)
            
            logger.debug("Successfully parsed datetime columns")
            
        except Exception as e:
            logger.error(f"Failed to parse datetime columns: {e}")
            # Create a fallback datetime column
            df['event_datetime'] = pd.Timestamp.now(tz=pytz.UTC)
        
        return df
    
    def _remove_invalid_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows with invalid or missing critical data.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame with invalid rows removed
        """
        initial_count = len(df)
        
        # Remove rows missing summary (description)
        df = df.dropna(subset=['summary'])
        
        # Remove rows with invalid datetime
        df = df.dropna(subset=['event_datetime'])
        
        # Remove empty summaries
        df = df[df['summary'].str.strip() != '']
        
        removed_count = initial_count - len(df)
        if removed_count > 0:
            logger.info(f"Removed {removed_count} invalid rows")
        
        return df
    
    def filter_high_importance_events(self, df: pd.DataFrame, min_level: int = 3) -> pd.DataFrame:
        """Filter events to only high importance ones.
        
        Args:
            df: Events DataFrame
            min_level: Minimum importance level to include
            
        Returns:
            Filtered DataFrame
        """
        if df.empty:
            return df
            
        high_importance_df = df[df['level'] >= min_level].copy()
        
        logger.info(
            f"Filtered to {len(high_importance_df)} high importance events "
            f"(level >= {min_level}) from {len(df)} total events"
        )
        
        return high_importance_df