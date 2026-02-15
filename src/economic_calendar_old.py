import bs4
import iso8601
import numpy as np
import pandas as pd
import pytz
import requests
import google_calendar as  gcal
import os

from google.cloud import firestore
import datetime

from utils.db_utils import insert_events_from_df, create_events_table, get_unsynced_events, mark_event_as_synced



def send_mock_data_to_fire_db():
    event = {
        "title": "US GDP Report",
        "date": datetime.datetime(2025, 8, 23, 8, 30),  # YYYY, MM, DD, HH, MM
        "importance": "High",
        "source": "Bureau of Economic Analysis",
        "added_on": datetime.datetime.utcnow()
    }
    try:
        db = firestore.Client.from_service_account_json("serviceAccount.json") 
        db.collection("events").add(event)
        print("✅ Event saved to Firestore!")
    except Exception as e:
        print(e)

# Constants
URL = "https://tradingeconomics.com/united-states/calendar"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}
CSV_FILE = "./resources/economic-calendar-events.csv"


# Functions
def scrape_website(url, headers):
    response = requests.get(url, headers=headers)
    soup = bs4.BeautifulSoup(response.text, features="html.parser")
    return soup.find("table", id="calendar")


def parse_row(row, date):
    time = row.find("td").text.strip()
    # level = row.find('td').span['class'][0] if row.find('td').span else ""
    level = '0'
    try:
        if row.find("td").span["class"]:
            # Changed 10/23/24
            # index changed from 0 -> 1
            level = row.find("td").span["class"][1]
    except IndexError:
        level = '0'
    country = row.find("td").find_next_sibling().text.strip()
    description = (
        row.find("td").find_next_sibling().find_next_sibling().text.strip()
    )
    return [date, time, country, level, description]


def convert_to_utc(date_str):
    return iso8601.parse_date(date_str).astimezone(pytz.utc)



def parse_events(table):
    data = []
    date_header=""
    for row in table.find_all(["thead", "tr"], recursive=False):
        try:
            if row["class"] == ["table-header"]:
                date_header = row.th.text.strip()
        except KeyError:
            data.append(parse_row(row, date_header))
    return data

#send_mock_data_to_fire_db()




# Scraping and Parsing
events_table = scrape_website(URL, HEADERS)
events_data = parse_events(events_table)
df = pd.DataFrame(
    events_data, columns=["date", "time", "country", "level", "summary"]
)


'''
# Transformations
# for use in google calendar, should replace with using proper format at time of insert
df["dateYear"] = pd.to_datetime(df["date"]).dt.strftime("%Y").astype(int)
df["dateMonth"] = pd.to_datetime(df["date"]).dt.strftime("%m").astype(int)
df["dateDay"] = pd.to_datetime(df["date"]).dt.strftime("%d").astype(int)

# input is in miltime, should be replaced with proper formatting 
df["miltime"] = df.time
maskAMPM = df["miltime"].str.contains("AM|PM")
df.loc[maskAMPM, "miltime"] = df.miltime.str.replace(" ", ":00 ")
df.loc[maskAMPM, "miltime"] = pd.to_datetime(df["miltime"], format="mixed").dt.strftime("%H:%M")

df["dateAdded"] = pd.Timestamp("now")
'''

# input is 'calendar-date-1' --> level '1'
df["level"] = df.level.str.split("calendar-date-").str[-1].astype(int)
df.replace("", np.nan, inplace=True)

'''
# CSV Operations - Update CSV 
# Read all CSV rows
df_csv = pd.read_csv(CSV_FILE).drop_duplicates()
# left anti join for differences
df_merge = df.merge(df_csv, on=["date", "time", "summary"], how="left")
df_merge = df.merge(
    df_merge.query("dateAdded_y.isnull()"),
    on=["date", "time", "summary"],
    how="inner",
)
df_merge = df_merge[df.columns]
# df-merge = events not in csv
'''





#   PSQL branch......
# copy scraped events to df2
df2=df

#   transform
# convert the 'date' column to a datetime object for DB
df2['date'] = pd.to_datetime(df2['date'], format='%A %B %d %Y')
# convert the 'time' column to a time object
df2['time'] = pd.to_datetime(df2['time'], format='%I:%M %p').dt.time

# some older records in csv will break this... fill with 0
df2["time"] = df2["time"].fillna(datetime.time(0, 0))

# combine date and time into a single column
df2['event_datetime'] = pd.to_datetime(
    df2['date'].astype(str) + ' ' + df2['time'].astype(str)
)
df2["event_datetime"] = pd.to_datetime(df2["event_datetime"], utc=True)

#df2["comp_key"] = df2["event_datetime"].astype(str) + '_' + df2["summary"]


#   New plan is to create comp_key using date_time_description
#   then just add with ON CONFLICT resolution 


create_events_table()

insert_events_from_df(df2)

#   Update Gcal Logic
#   query db for missing gcalid events 
#       level 3 + fiters
events_to_create = get_unsynced_events()
print(events_to_create)
print(events_to_create.info)
# may manually update current db records with 0 for gcal id to prevent updating calendar. 
#   Old Filter:     query_string = 'level==3 OR 
#                   summary.str.contains("Initial jobless claims") or 
#                   summary.str.contains("GDP Growth Rate") or summary.str.contains("CPI") or 
#                   summary.str.contains("Core PCE Price Index MoM") or summary.str.contains("New Home Sales MoM")'
           


#   Loop through events
#       insert into gcal
#       update db with gcal id

if events_to_create.empty: 
    print("No unsynced events. Calendar is up to date.")
else:
    print(f"Found {len(events_to_create)} new events to sync...")
gcal.main()
for event in events_to_create.to_dict('records'):
    try:
        print(f"\n\nTrying to create gcal events...")
        #   Create in Gcal
        new_gcal_id = gcal.create_gcal_event(event)
        print(f"New gcal event id: {new_gcal_id}")
        #   Mark in DB
        mark_event_as_synced(event['event_datetime'], event['summary'], new_gcal_id)
        print(f"Successfully synced event: {event['summary']} level: {event['level']} date: {event['event_datetime']}")

    except Exception as e:
        print(f"Failed to sync event {event['summary']}: {e}")


'''

if df_merge.empty: print("No new events. CSV file is up to date.")
else:
    # write to csv file
    df_csv_new = pd.concat([df_csv, df_merge], ignore_index=True)
    df_csv_new.to_csv(CSV_FILE, index=False)

    # Query Google Calendar
    retries = 0
    max_retries = 1
    success = False
    newlist = []
    while retries <= max_retries and not success:
        try:
            gcal.main()
            eventlist = gcal.getEventList()

            for event in eventlist:
                summary = event["summary"]
                date = event["start"]["dateTime"]
                print(date +" " + summary)
                newlist.append([date, summary])
            
            # Transform Calendar events to merge with CSV contents
            df_calendar = pd.DataFrame(
                newlist, columns=["date", "summary"]
            ).drop_duplicates()
            df_calendar["newdate"] = df_calendar["date"].apply(convert_to_utc)
            df_calendar[["dateYear", "dateMonth", "dateDay"]] = (
                df_calendar["newdate"].dt.strftime("%Y-%m-%d").str.split("-", expand=True)
            )
            df_calendar[["dateYear", "dateMonth", "dateDay"]] = df_calendar[
                ["dateYear", "dateMonth", "dateDay"]
            ].astype(int)
            df_calendar["incal"] = "yes"

            # Filter specific events
            query_string = 'level==3 or summary.str.contains("Initial jobless claims") or summary.str.contains("GDP Growth Rate") or summary.str.contains("CPI") or summary.str.contains("Core PCE Price Index MoM") or summary.str.contains("New Home Sales MoM")'
            df_merge_3 = df_csv_new.query(query_string, engine="python")
            merge4 = df_merge_3.merge(
                df_calendar, on=["dateYear", "dateMonth", "dateDay", "summary"], how="left"
            )
            events_not_in_calendar = merge4.query("incal.isnull()")[
                [
                    "date_x",
                    "level",
                    "summary",
                    "dateYear",
                    "dateMonth",
                    "dateDay",
                    "miltime",
                ]
            ]

            # Insert Events to Calendar
            gcal.insertEventFromDict(events_not_in_calendar.to_dict("records"))

            print("\nEvents Scraped:\t", df["date"].count())
            print("Events in CSV (before):\t", df_csv["date"].count())
            print("Events in CSV (after):\t", df_csv_new["date"].count())
            print("Events in CSV (diff):\t", df_merge["date"].count())
            print("Events in calendar:\t", df_calendar["date"].count())
            print("Events in CSV (after)(filtered):\t", df_merge_3["date"].count())
            print(
                "Events in CSV (after)(filtered) added to calendar:\t",
                events_not_in_calendar["dateDay"].count(),
            )
            
            success = True

        except Exception as e:
            print("Error occurred with Google Calendar:", e)

            # delete current oauth file and retry
            try: 
                os.remove("./resources/oauth-token.json")
                print("old oauth file has been deleted.")
            except FileNotFoundError:
                print("old oauth file was not found.")
            except Exception as delete_error:
                print(f"Error occured while trying to delete old oauth file: {delete_error}")
            
            retries += 1
            if retries > max_retries:
                print(f"Exceeded maximum retry limit of {max_retries}. Aborting.")

'''