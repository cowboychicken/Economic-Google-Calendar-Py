import bs4
import iso8601
import numpy as np
import pandas as pd
import pytz
import requests
import google_calendar as gcal
import os

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


# Scraping and Parsing
soup = scrape_website(URL, HEADERS)
data = []
date = ""

for row in soup.find_all(["thead", "tr"], recursive=False):
    try:
        if row["class"] == ["table-header"]:
            date = row.th.text.strip()
    except KeyError:
        data.append(parse_row(row, date))

# Data Transformation
df = pd.DataFrame(
    data, columns=["date", "time", "country", "level", "summary"]
)

df["level"] = df.level.str.split("calendar-date-").str[-1].astype(int)
df["dateYear"] = pd.to_datetime(df["date"]).dt.strftime("%Y").astype(int)
df["dateMonth"] = pd.to_datetime(df["date"]).dt.strftime("%m").astype(int)
df["dateDay"] = pd.to_datetime(df["date"]).dt.strftime("%d").astype(int)

df["miltime"] = df.time
maskAMPM = df["miltime"].str.contains("AM|PM")
df.loc[maskAMPM, "miltime"] = df.miltime.str.replace(" ", ":00 ")
df.loc[maskAMPM, "miltime"] = pd.to_datetime(df["miltime"], format="mixed").dt.strftime("%H:%M")

df["dateAdded"] = pd.Timestamp("now")
df.replace("", np.nan, inplace=True)

# CSV Operations
df_csv = pd.read_csv(CSV_FILE).drop_duplicates()

# isolate scraped records, not currently in csv
df_merge = df.merge(df_csv, on=["date", "time", "summary"], how="left")
df_merge = df.merge(
    df_merge.query("dateAdded_y.isnull()"),
    on=["date", "time", "summary"],
    how="inner",
)
df_merge = df_merge[df.columns]
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
                # print(date +" " + summary)
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

