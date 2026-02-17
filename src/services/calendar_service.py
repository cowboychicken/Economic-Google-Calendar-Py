"""Google Calendar service for managing economic event synchronization."""

import logging
from datetime import datetime, timedelta
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.settings import (
    GOOGLE_CALENDAR_SCOPES,
    GOOGLE_CALENDAR_ID,
    OAUTH_TOKEN_PATH,
    CREDENTIALS_PATH
)

logger = logging.getLogger(__name__)


class CalendarService:
    """Service for managing Google Calendar integration."""
    
    def __init__(
        self, 
        calendar_id: str = GOOGLE_CALENDAR_ID,
        credentials_path: str = str(CREDENTIALS_PATH),
        token_path: str = str(OAUTH_TOKEN_PATH)
    ):
        """Initialize calendar service.
        
        Args:
            calendar_id: Google Calendar ID to use
            credentials_path: Path to credentials.json file
            token_path: Path to oauth-token.json file
        """
        self.calendar_id = calendar_id
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None
        self._authenticate()
    
    def _authenticate(self) -> bool:
        """Authenticate with Google Calendar API.
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            creds = None
            
            # Load existing token if available
            if OAUTH_TOKEN_PATH.exists():
                creds = Credentials.from_authorized_user_file(
                    str(self.token_path), 
                    GOOGLE_CALENDAR_SCOPES
                )
            
            # If there are no valid credentials, get new ones
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    logger.info("Refreshing expired credentials")
                    creds.refresh(Request())
                else:
                    logger.info("Starting OAuth flow for new credentials")
                    if not CREDENTIALS_PATH.exists():
                        raise FileNotFoundError(f"Credentials file not found: {self.credentials_path}")
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, 
                        GOOGLE_CALENDAR_SCOPES
                    )
                    creds = flow.run_local_server(port=0)
                
                # Save credentials for next run
                with open(self.token_path, 'w') as token_file:
                    token_file.write(creds.to_json())
                logger.info("Saved new credentials to token file")
            
            # Build the service
            self.service = build("calendar", "v3", credentials=creds)
            logger.info("Successfully authenticated with Google Calendar")
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Calendar: {e}")
            return False
    
    def create_event(self, event_data: dict) -> Optional[str]:
        """Create a new event in Google Calendar.
        
        Args:
            event_data: Dictionary containing event information with keys:
                - summary: Event title
                - event_datetime: datetime object for the event
                
        Returns:
            Google Calendar event ID if successful, None otherwise
        """
        if not self.service:
            logger.error("Calendar service not authenticated")
            return None
        
        try:
            event_datetime = event_data["event_datetime"]
            summary = event_data["summary"]
            
            # Create event template
            event_template = {
                "summary": summary,
                "start": {
                    "dateTime": event_datetime.strftime('%Y-%m-%dT%H:%M:%S'),
                    "timeZone": "America/Chicago",
                },
                "end": {
                    # 5-minute events by default
                    "dateTime": (event_datetime + timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S'),
                    "timeZone": "America/Chicago",
                },
                "description": f"Economic event from Trading Economics\\nImportance: Level {event_data.get('level', 'N/A')}"
            }
            
            # Insert event
            created_event = (
                self.service.events()
                .insert(calendarId=self.calendar_id, body=event_template)
                .execute()
            )
            
            event_id = created_event.get('id')
            event_link = created_event.get('htmlLink')
            
            logger.info(f"Created calendar event: {summary}")
            logger.debug(f"Event link: {event_link}")
            
            return event_id
            
        except HttpError as e:
            logger.error(f"Google Calendar API error creating event: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating event: {e}")
            return None
    
    def get_existing_events(self, max_results: int = 1000) -> list:
        """Get existing events from the calendar.
        
        Args:
            max_results: Maximum number of events to retrieve
            
        Returns:
            List of calendar events
        """
        if not self.service:
            logger.error("Calendar service not authenticated")
            return []
        
        try:
            events_result = (
                self.service.events()
                .list(
                    calendarId=self.calendar_id,
                    maxResults=max_results,
                    singleEvents=True,
                    orderBy="startTime",
                )
                .execute()
            )
            
            events = events_result.get("items", [])
            logger.info(f"Retrieved {len(events)} existing calendar events")
            
            return events
            
        except HttpError as e:
            logger.error(f"Google Calendar API error retrieving events: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error retrieving events: {e}")
            return []
    
    def delete_event(self, event_id: str) -> bool:
        """Delete an event from the calendar.
        
        Args:
            event_id: Google Calendar event ID
            
        Returns:
            True if deletion successful, False otherwise
        """
        if not self.service:
            logger.error("Calendar service not authenticated")
            return False
        
        try:
            self.service.events().delete(
                calendarId=self.calendar_id,
                eventId=event_id
            ).execute()
            
            logger.info(f"Deleted calendar event: {event_id}")
            return True
            
        except HttpError as e:
            logger.error(f"Google Calendar API error deleting event: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting event: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test the calendar connection.
        
        Returns:
            True if connection is working, False otherwise
        """
        try:
            # Try to get a small number of events as a connection test
            self.get_existing_events(max_results=1)
            logger.info("Calendar connection test successful")
            return True
        except Exception as e:
            logger.error(f"Calendar connection test failed: {e}")
            return False