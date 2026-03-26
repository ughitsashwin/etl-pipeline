# src/transform.py
#
# WHAT THIS FILE DOES:
# This is the "Transform" step of our ETL pipeline.
# It takes the raw DataFrame from extract.py and:
#   1. Cleans bad/missing values
#   2. Ensures correct data types
#   3. Adds new derived columns that make the data more useful
#
# DESIGN PRINCIPLE — Pure Functions:
# Notice this file has NO imports of requests, sqlalchemy, or any I/O.
# It only takes a DataFrame in and returns a DataFrame out.
# This makes it extremely easy to unit test — no API calls, no database needed.
# You can test every transformation with just plain Python dictionaries.

import pandas as pd


def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich raw weather data from the extract step.

    What this does:
    - Drops rows where BOTH temp columns are null (unusable data)
    - Fills missing precipitation with 0.0 (no reading = no rain)
    - Adds temp_avg: average of max and min temperature
    - Adds temp_range: how much the temperature varied that day
    - Adds is_rainy: a boolean flag for days with meaningful rainfall
    - Adds loaded_at: a timestamp recording when this row was processed

    Args:
        df: Raw DataFrame from fetch_weather()

    Returns:
        Cleaned and enriched DataFrame ready for validation and loading
    """

    # Always work on a copy, never mutate the input.
    # This is a best practice — the caller shouldn't be surprised
    # to find their original DataFrame has changed.
    df = df.copy()

    # --- CLEANING ---

    # Drop rows where BOTH temp_max AND temp_min are null.
    # If we have no temperature data at all, the row is useless.
    # "how=all" means: only drop if ALL of the listed columns are null.
    # If just one is null, we keep the row — we can still compute something.
    df = df.dropna(subset=["temp_max", "temp_min"], how="all")

    # Fill missing precipitation values with 0.0.
    # The API sometimes omits this field for days with no recorded rainfall.
    # 0.0 is the correct assumption — no reading means no rain.
    df["precip_mm"] = df["precip_mm"].fillna(0.0)

    # --- DERIVED COLUMNS ---

    # Average temperature: a single number summarising the day's temperature.
    # Useful for charting trends over time.
    df["temp_avg"] = (df["temp_max"] + df["temp_min"]) / 2

    # Temperature range: how much the temperature swung that day.
    # A large range (e.g. 20°C) means a big difference between night and day.
    # A small range means stable weather.
    df["temp_range"] = df["temp_max"] - df["temp_min"]

    # Rainy day flag: True if precipitation exceeded 1mm.
    # We use 1mm as the threshold because anything less is basically
    # just dew or mist — not meaningful rainfall.
    df["is_rainy"] = df["precip_mm"] > 1.0

    # Audit timestamp: records exactly when this row was processed.
    # This is standard in data engineering — always know when data was loaded.
    # utcnow() gives us a timezone-consistent timestamp regardless of
    # where the pipeline is running (your Mac, a GitHub Actions server, etc.)
    df["loaded_at"] = pd.Timestamp.utcnow()

    # --- TYPE ENFORCEMENT ---

    # Ensure date is always a proper datetime, not a string.
    # This matters because SQLite stores dates as text — when we read
    # data back out, we always want pandas to treat it as a date.
    df["date"] = pd.to_datetime(df["date"])

    # Ensure is_rainy is always a boolean (True/False), not an integer (1/0).
    # Pandas sometimes infers this as int — we're being explicit.
    df["is_rainy"] = df["is_rainy"].astype(bool)

    # Reset the index so rows are numbered 0, 1, 2... cleanly
    # after any rows were dropped in the cleaning step above.
    return df.reset_index(drop=True)