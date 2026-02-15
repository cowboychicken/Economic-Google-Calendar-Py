import psycopg2
import os
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import time
import numpy as np
import datetime

load_dotenv()


class DatabaseConnection:
    """Context manager for PostgreSQL connections with retries."""

    def __init__(self, max_retries=5, wait_seconds=2):
        self.db_name = os.environ.get("POSTGRES_DB")
        self.user = os.environ.get("POSTGRES_USER")
        self.password = os.environ.get("POSTGRES_PASSWORD")
        self.host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
        self.port = os.environ.get("POSTGRES_PORT", "5432")
        self.conn = None
        self.max_retries = max_retries
        self.wait_seconds = wait_seconds

    def __enter__(self):
        for attempt in range(1, self.max_retries + 1):
            try:
                #print(f"[Attempt {attempt}] Connecting to Postgres at {self.host}:{self.port} ...")
                self.conn = psycopg2.connect(
                    dbname=self.db_name,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                #print("Connected successfully!")
                return self.conn
            except psycopg2.OperationalError as e:
                print(f"Connection failed: {e}")
                if attempt == self.max_retries:
                    raise
                print(f"Retrying in {self.wait_seconds} seconds...")
                time.sleep(self.wait_seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type:
                print("Rolling back transaction due to exception...")
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()


def create_events_table():
    sql_query = """
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
    """
    with DatabaseConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            print("Table 'economic_events' created or already exists.")


def insert_event(event_data):
    sql_query = """
    INSERT INTO economic_events (
        event_datetime, time, country, level, summary
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    with DatabaseConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query, (
                event_data['event_datetime'],
                event_data['time'],
                event_data['country'],
                event_data['level'],
                event_data['summary']
            ))
            print("Inserted event or event already exists:", event_data['summary'])


def insert_events_from_df(df: pd.DataFrame):

    df_filtered = df[['event_datetime', 'time', 'country', 'level', 'summary']].copy()

    data_tuples = list(df_filtered.itertuples(index=False, name=None))
    sql_query = """
    INSERT INTO economic_events (
        event_datetime, time, country, level, summary
    ) VALUES %s
    ON CONFLICT (event_datetime, summary) DO NOTHING;
    """
    total_scraped = len(df)
    with DatabaseConnection() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, sql_query, data_tuples)
                inserted_count = cur.rowcount
                print(f"Successfully inserted {inserted_count} rows.")
            except psycopg2.Error as e:
                print(f"Bulk insertion failed: {e}")
                conn.rollback()
    existing_count = total_scraped - inserted_count
    print(f"""
----------------------------------------
📊 SCRAPE RESULTS:
   Total Scraped:    {total_scraped}
   Existing in DB:   {existing_count}
   New & Inserted:   {inserted_count}
----------------------------------------
    """)



def fetch_recent_events_to_df():
    sql_query = "SELECT * FROM economic_events WHERE event_datetime >= NOW() - INTERVAL '30 days';"

    with DatabaseConnection() as conn:
        df = pd.read_sql_query(sql_query, conn)

    return df

def get_unsynced_events():
    sql_query = """
    SELECT * FROM economic_events 
    WHERE gcal_event_id IS NULL 
    AND (
        level = '3' 
        OR LOWER(summary) LIKE '%%initial jobless claims%%' 
        OR LOWER(summary) LIKE '%%gdp growth rate%%' 
        OR LOWER(summary) LIKE '%%core pce price index mom%%'
    )
    """
    with DatabaseConnection() as conn:
        df = pd.read_sql_query(sql_query, conn)
    return df



def mark_event_as_synced(event_datetime, summary, gcal_event_id):
    sql_query = """
    UPDATE economic_events
    SET gcal_event_id = %s
    WHERE event_datetime = %s AND summary = %s;
    """

    values_tuple = (gcal_event_id, event_datetime, summary)


    with DatabaseConnection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_query, values_tuple)

                conn.commit()
                print(f"Success updating event {summary}")
            
            except Exception as e:
                conn.rollback()
                print(f"Error updating event {summary}: {e}")


if __name__=="__main__":
    print("yo")
    load_dotenv()
    mark_event_as_synced("2025-11-14 00:00:00+00", "Business Inventories MoM AUG",0)
