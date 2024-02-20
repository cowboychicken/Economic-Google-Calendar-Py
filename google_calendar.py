import datetime
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/calendar.events"]
CALENDAR_ID = "a2f405442fb6c4687738183931cbe0fa188d41fd0e60d0c021f544f51b639dc9@group.calendar.google.com"

def main():
  global creds
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())
  try:
    global service
    service = build("calendar", "v3", credentials=creds)
  except HttpError as error:
    print(f"An error occurred: {error}")

def getEventList():
    try:
        service = build("calendar", "v3", credentials=creds)
        events_result = (
            service.events()
            .list(
                calendarId=CALENDAR_ID,
                maxResults=99999,
                singleEvents=True,
                orderBy="startTime"
            )
            .execute()
        )
        events = events_result.get("items", [])

        if not events:
            print("No upcoming events found.")
            return

        # Prints the start and name of the next 10 events
        for event in events:
            start = event["start"].get("dateTime", event["start"].get("date"))

        return events
    except HttpError as error:
        print(f"An error occurred: {error}")

def insertEventFromDict(events):
  
  for event in events:
    summary = event['summary']
    dateYear = int(event['dateYear'])
    dateMonth = int(event['dateMonth'])
    dateDay = int(event['dateDay'])
    miltime = event['miltime']
    dateTimeString = str(dateYear) + '-' + str(dateMonth) + '-' + str(dateDay) + 'T' + miltime
    print(summary,dateTimeString,dateYear,dateMonth,dateDay,miltime,'\n')
    event_template = {
          'summary': summary,
          'start': {
              #'dateTime': '2023-10-19T16:00:00',
              'dateTime': dateTimeString + ':00' ,
              'timeZone': 'Etc/UTC',
          },
          'end': {
              #'dateTime': '2023-10-19T17:00:00',
              'dateTime': dateTimeString + ':05' ,
              'timeZone': 'Etc/UTC',
          }
    }
    event = service.events().insert(calendarId=CALENDAR_ID, body=event_template).execute()
    print ('Event created: %s' % (event.get('htmlLink')))

if __name__ == "__main__":
  main()