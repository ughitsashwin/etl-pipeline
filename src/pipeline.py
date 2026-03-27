# src/pipeline.py
#
# WHAT THIS FILE DOES:
# This is the main entrypoint for the entire ETL pipeline.
# It wires together extract → transform → load for multiple cities
# and handles errors gracefully so one failing city doesn't
# crash the whole pipeline.
#
# This is the file that GitHub Actions will run every day on a schedule.
# It also logs failures to the database so the dashboard can show them.
#
# DESIGN PATTERN — Orchestrator:
# pipeline.py doesn't contain any business logic itself.
# It just coordinates the other modules. This is called the
# "orchestrator pattern" — common in tools like Apache Airflow,
# Prefect, and Dagster that manage production data pipelines.

import sys
import pandas as pd
from src.extract import fetch_weather
from src.transform import transform_weather
from src.load import load_weather, get_engine, create_tables
from sqlalchemy import text


# The cities we want to track.
# Each entry has a human-readable name plus the GPS coordinates
# the API needs. You can add any city in the world here.
CITIES = [
    {"name": "Dublin",    "lat": 53.33,   "lon": -6.25},
    {"name": "London",    "lat": 51.51,   "lon": -0.13},
    {"name": "New York",  "lat": 40.71,   "lon": -74.01},
    {"name": "Chennai",   "lat": 13.0843, "lon": 80.2705},
    {"name": "Kozhikode", "lat": 11.2488, "lon": 75.7839},
]


def log_failure(engine, error_message: str):
    """
    Log a pipeline failure to the pipeline_runs table.
    Called when a city's ETL fails so we have a record in the dashboard.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_runs (ran_at, rows_loaded, status, message)
            VALUES (:ran_at, :rows_loaded, :status, :message)
        """), {
            "ran_at":      str(pd.Timestamp.utcnow()),
            "rows_loaded": 0,
            "status":      "failed",
            "message":     error_message,
        })


def run() -> int:
    """
    Run the full ETL pipeline for all configured cities.

    Returns:
        Total number of rows loaded across all cities.
        Returns 0 if all cities failed (causes GitHub Actions to mark the run as failed).
    """
    engine = get_engine()
    create_tables(engine)

    total_rows = 0
    failed_cities = []

    for city in CITIES:
        city_name = city["name"]

        try:
            # --- EXTRACT ---
            print(f"[{city_name}] Extracting...")
            raw_df = fetch_weather(city_name, city["lat"], city["lon"])
            print(f"[{city_name}] Got {len(raw_df)} raw rows")

            # --- TRANSFORM ---
            print(f"[{city_name}] Transforming...")
            clean_df = transform_weather(raw_df)

            # --- VALIDATE ---
            # This will raise a SchemaError if the data doesn't meet our rules.
            # The except block below will catch it and log it as a failed run.
            print(f"[{city_name}] Validating...")
            from src.validate import validate
            clean_df = validate(clean_df)

            # --- LOAD ---
            print(f"[{city_name}] Loading...")
            rows = load_weather(clean_df)
            print(f"[{city_name}] Done — {rows} rows loaded")

            total_rows += rows

        except Exception as e:
            # If one city fails, log it and continue with the next city.
            # We don't want London failing to stop New York from loading.
            # This is called "partial failure handling" — important in
            # production pipelines where individual sources can go down.
            error_msg = f"{city_name} failed: {str(e)}"
            print(f"[{city_name}] ERROR — {error_msg}")
            failed_cities.append(city_name)
            log_failure(engine, error_msg)

    # Print a final summary so it's easy to read in GitHub Actions logs
    print()
    print("=" * 40)
    print("Pipeline complete")
    print(f"  Cities succeeded : {len(CITIES) - len(failed_cities)}/{len(CITIES)}")
    print(f"  Total rows loaded: {total_rows}")
    if failed_cities:
        print(f"  Failed cities    : {', '.join(failed_cities)}")
    print("=" * 40)

    return total_rows


if __name__ == "__main__":
    # When run directly (python src/pipeline.py), execute the pipeline.
    # sys.exit(1) tells the shell — and GitHub Actions — that something
    # went wrong. A non-zero exit code marks the CI run as failed,
    # which triggers our Slack alert.
    rows = run()
    sys.exit(0 if rows > 0 else 1)