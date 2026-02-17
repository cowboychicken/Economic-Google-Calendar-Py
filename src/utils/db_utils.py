import psycopg2
import os
import time
from dotenv import load_dotenv

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
                self.conn = psycopg2.connect(
                    dbname=self.db_name,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
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
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
