# src/load.py
#
# WHAT THIS FILE DOES:
# This is the "Load" step of our ETL pipeline.
# It takes the clean DataFrame from transform.py and saves it
# permanently to a SQLite database file.
#
# KEY CONCEPT — Idempotency:
# This loader is "idempotent" — you can run it 10 times with the same
# data and the result is always the same. No duplicates, no errors.
# This is a critical property in data engineering. Pipelines fail and
# get re-run all the time — your loader must handle that gracefully.
#
# KEY CONCEPT — Upsert:
# We use INSERT OR REPLACE which means:
#   - If the row doesn't exist yet → INSERT it
#   - If a row with the same (date, city) already exists → REPLACE it
# The PRIMARY KEY (date, city) is what makes this work.

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path


# Path to our SQLite database file.
# Path() gives us a clean cross-platform way to handle file paths.
# The data/ folder is created automatically if it doesn't exist.
DB_PATH = Path("data/weather.db")


def get_engine():
    """
    Create and return a SQLAlchemy engine connected to our SQLite database.

    SQLAlchemy is an ORM (Object Relational Mapper) — it lets us talk to
    databases using Python instead of raw SQL strings. It also means we
    could swap SQLite for PostgreSQL by changing just this one function.
    """
    # Create the data/ directory if it doesn't exist yet
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # sqlite:/// means "connect to a local SQLite file"
    # Three slashes = relative path, four slashes = absolute path
    return create_engine(f"sqlite:///{DB_PATH}")


def create_tables(engine):
    """
    Create the database tables if they don't already exist.

    We define the schema here explicitly — column names, types, and
    constraints. The PRIMARY KEY (date, city) is what enforces uniqueness
    and enables our upsert logic.

    IF NOT EXISTS means this is safe to call every time the pipeline runs —
    it won't error or wipe data if the table already exists.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS weather (
                date        TEXT    NOT NULL,
                city        TEXT    NOT NULL,
                temp_max    REAL,
                temp_min    REAL,
                precip_mm   REAL,
                temp_avg    REAL,
                temp_range  REAL,
                is_rainy    INTEGER,
                loaded_at   TEXT,
                PRIMARY KEY (date, city)
            )
        """))

        # A separate table to log every pipeline run.
        # This gives us an audit trail — we can see exactly when the
        # pipeline ran, how many rows it processed, and whether it succeeded.
        # This is what separates a script from a production-grade pipeline.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at      TEXT    NOT NULL,
                rows_loaded INTEGER NOT NULL,
                status      TEXT    NOT NULL,
                message     TEXT
            )
        """))


def load_weather(df: pd.DataFrame) -> int:
    """
    Load transformed weather data into the SQLite database.

    Uses INSERT OR REPLACE so re-running the pipeline never creates duplicates.
    Also logs the run to the pipeline_runs table for monitoring.

    Args:
        df: Cleaned and validated DataFrame from transform.py

    Returns:
        Number of rows upserted
    """
    engine = get_engine()

    # Always ensure tables exist before trying to write to them
    create_tables(engine)

    # engine.begin() gives us a transaction — either ALL rows are saved,
    # or NONE are (if something goes wrong halfway through).
    # This prevents partial writes corrupting your database.
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT OR REPLACE INTO weather
                (date, city, temp_max, temp_min, precip_mm,
                 temp_avg, temp_range, is_rainy, loaded_at)
                VALUES
                (:date, :city, :temp_max, :temp_min, :precip_mm,
                 :temp_avg, :temp_range, :is_rainy, :loaded_at)
            """), {
                "date":       str(row["date"].date()),
                "city":       row["city"],
                "temp_max":   row["temp_max"],
                "temp_min":   row["temp_min"],
                "precip_mm":  row["precip_mm"],
                "temp_avg":   row["temp_avg"],
                "temp_range": row["temp_range"],
                # SQLite has no boolean type — store as 1/0
                "is_rainy":   int(row["is_rainy"]),
                "loaded_at":  str(row["loaded_at"]),
            })

        # Log this run to the pipeline_runs table
        conn.execute(text("""
            INSERT INTO pipeline_runs (ran_at, rows_loaded, status, message)
            VALUES (:ran_at, :rows_loaded, :status, :message)
        """), {
            "ran_at":      str(pd.Timestamp.utcnow()),
            "rows_loaded": len(df),
            "status":      "success",
            "message":     f"Loaded {len(df)} rows for cities: {df['city'].unique().tolist()}",
        })

    return len(df)