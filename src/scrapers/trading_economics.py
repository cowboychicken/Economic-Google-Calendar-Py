"""Trading Economics website scraper for economic calendar events."""

import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag

from config.settings import TRADING_ECONOMICS_URL, SCRAPER_HEADERS

logger = logging.getLogger(__name__)


class TradingEconomicsScraper:
    """Scraper for Trading Economics economic calendar."""
    
    def __init__(self, url: str = TRADING_ECONOMICS_URL, headers: dict = None):
        self.url = url
        self.headers = headers or SCRAPER_HEADERS.copy()
    
    def scrape_events_table(self) -> Optional[Tag]:
        """Scrape the main events table from Trading Economics.
        
        Returns:
            BeautifulSoup Tag containing the calendar table, or None if failed
        """
        try:
            logger.info(f"Scraping economic calendar from {self.url}")
            response = requests.get(self.url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            calendar_table = soup.find("table", id="calendar")
            
            if not calendar_table:
                logger.error("Could not find calendar table on page")
                return None
                
            return calendar_table
            
        except requests.RequestException as e:
            logger.error(f"Failed to scrape website: {e}")
            return None
    
    def parse_event_row(self, row: Tag, current_date: str) -> list[str]:
        """Parse a single event row from the calendar table.
        
        Args:
            row: BeautifulSoup Tag representing a table row
            current_date: Current date header for this section
            
        Returns:
            List of parsed event data: [date, time, country, level, description]
        """
        try:
            # Extract time
            time_cell = row.find("td")
            if not time_cell:
                return []
                
            time = time_cell.text.strip()
            
            # Extract importance level from CSS class
            level = "0"  # default
            try:
                if time_cell.span and time_cell.span.get("class"):
                    # Extract level from CSS class like "calendar-date-1"
                    css_classes = time_cell.span["class"]
                    for css_class in css_classes:
                        if css_class.startswith("calendar-date-"):
                            level = css_class.split("calendar-date-")[-1]
                            break
            except (AttributeError, IndexError):
                level = "0"
            
            # Extract country
            country_cell = time_cell.find_next_sibling()
            country = country_cell.text.strip() if country_cell else ""
            
            # Extract description
            desc_cell = country_cell.find_next_sibling() if country_cell else None
            description = desc_cell.text.strip() if desc_cell else ""
            
            return [current_date, time, country, level, description]
            
        except Exception as e:
            logger.warning(f"Failed to parse event row: {e}")
            return []
    
    def parse_all_events(self, table: Tag) -> list[list[str]]:
        """Parse all events from the calendar table.
        
        Args:
            table: BeautifulSoup Tag containing the calendar table
            
        Returns:
            List of event records, each as [date, time, country, level, description]
        """
        events_data = []
        current_date = ""
        
        for row in table.find_all(["thead", "tr"], recursive=False):
            try:
                # Check if this is a date header row
                if row.get("class") == ["table-header"]:
                    date_header = row.find("th")
                    if date_header:
                        current_date = date_header.text.strip()
                        logger.debug(f"Processing events for date: {current_date}")
                else:
                    # This is an event row
                    event_data = self.parse_event_row(row, current_date)
                    if event_data:  # Only add if parsing succeeded
                        events_data.append(event_data)
                        
            except Exception as e:
                logger.warning(f"Failed to process table row: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(events_data)} events")
        return events_data
    
    def scrape_events(self) -> list[list[str]]:
        """Main method to scrape and parse all events.
        
        Returns:
            List of event records, each as [date, time, country, level, description]
        """
        table = self.scrape_events_table()
        if not table:
            logger.error("Failed to scrape events table")
            return []
            
        return self.parse_all_events(table)