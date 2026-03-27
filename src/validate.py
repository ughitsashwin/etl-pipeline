# src/validate.py
#
# WHAT THIS FILE DOES:
# This file defines the "contract" that our data must satisfy
# before it's allowed into the database.
#
# We use pandera — a library that lets you define schemas for
# pandas DataFrames, just like you'd define a schema for a database table.
#
# WHY THIS MATTERS:
# Imagine the Open-Meteo API changes its response format one day,
# or returns a temperature of 999.0 due to a sensor error.
# Without validation, that bad data silently enters your database
# and corrupts every chart and report downstream.
# With validation, the pipeline immediately raises a loud error,
# GitHub Actions marks the run as failed, and Slack alerts you.
# This is called "failing fast" — a core principle in data engineering.
#
# WHERE IT FITS IN THE PIPELINE:
# extract.py → transform.py → validate.py → load.py
# Validation happens AFTER transform (so derived columns exist)
# but BEFORE load (so bad data never reaches the database).

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check


# Define the cities we expect to see in the data.
# If a new city is added to pipeline.py, it must be added here too.
# This prevents typos or unexpected cities from entering the database.
VALID_CITIES = ["Dublin", "London", "New York", "Chennai", "Kozhikode"]


# This is our schema — a complete description of what a valid
# weather DataFrame must look like.
# Each Column() call defines the rules for one column.
weather_schema = DataFrameSchema(
    columns={
        # date must be a datetime type — not a string, not an integer
        "date": Column(
            pa.DateTime,
            nullable=False,
        ),

        # city must be a string AND must be one of our known cities
        # Check.isin() is like a database foreign key constraint
        "city": Column(
            str,
            checks=Check.isin(VALID_CITIES),
            nullable=False,
        ),

        # Temperatures on Earth range roughly -90°C to 60°C.
        # Anything outside that range is a sensor error or API bug.
        # nullable=True because occasionally one reading may be missing.
        "temp_max": Column(
            float,
            checks=Check.in_range(-90, 60),
            nullable=True,
        ),

        "temp_min": Column(
            float,
            checks=Check.in_range(-90, 60),
            nullable=True,
        ),

        # Precipitation can't be negative — there's no such thing as
        # negative rainfall. 500mm is an extreme but possible daily value
        # (think monsoon or hurricane).
        "precip_mm": Column(
            float,
            checks=Check.in_range(0, 500),
            nullable=False,
        ),

        # temp_avg must be between temp_min and temp_max.
        # We use the same range check as individual temperatures.
        "temp_avg": Column(
            float,
            checks=Check.in_range(-90, 60),
            nullable=False,
        ),

        # temp_range (max - min) must always be >= 0.
        # A negative range would mean max < min which is impossible.
        "temp_range": Column(
            float,
            checks=Check.greater_than_or_equal_to(0),
            nullable=False,
        ),

        # is_rainy must be a boolean True/False
        "is_rainy": Column(
            bool,
            nullable=False,
        ),

        # loaded_at just needs to exist — any timestamp is fine
        "loaded_at": Column(
            nullable=False,
        ),
    },
    # If extra columns appear that aren't in our schema, ignore them.
    # This makes the schema forward-compatible if we add columns later.
    strict=False,
)


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate a transformed weather DataFrame against our schema.

    If the data passes all checks, returns the DataFrame unchanged.
    If any check fails, raises a pandera.SchemaError with a clear
    description of exactly which rows and columns failed — much more
    useful than a generic Python error.

    Args:
        df: Transformed DataFrame from transform_weather()

    Returns:
        The same DataFrame if valid

    Raises:
        pandera.SchemaError: if any validation rule is violated
    """
    return weather_schema.validate(df, lazy=True)
    # lazy=True means: check ALL columns before raising an error,
    # rather than stopping at the first failure.
    # This gives you a complete picture of everything that's wrong.