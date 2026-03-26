# src/extract.py
#
# WHAT THIS FILE DOES:
# This is the "Extract" step of our ETL pipeline.
# Its only job is to reach out to the Open-Meteo weather API,
# pull raw data for a given city, and return it as a pandas DataFrame.
#
# DESIGN PRINCIPLE — Single Responsibility:
# This file does ONE thing: fetch data. It does no cleaning,
# no validation, no saving. That keeps it easy to test and swap out.
# If we ever change our data source, we only touch this file.

import requests
import pandas as pd
from datetime import date, timedelta


# The base URL for the Open-Meteo forecast API.
# All our requests will be built on top of this.
BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(city: str, latitude: float, longitude: float, days_back: int = 7) -> pd.DataFrame:
    """
    Fetch daily weather data for a city from the Open-Meteo API.

    Args:
        city:      A human-readable city name (used as a label in the data)
        latitude:  The city's latitude coordinate
        longitude: The city's longitude coordinate
        days_back: How many days of historical data to fetch (default: 7)

    Returns:
        A pandas DataFrame with one row per day containing raw weather data
    """

    # Calculate the date range we want to fetch
    # We go from N days ago up to today
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    # Build the query parameters for the API request.
    # "daily" tells the API which metrics we want — one value per day per metric.
    # "timezone: auto" makes the API return times in the city's local timezone.
    params = {
        "latitude":   latitude,
        "longitude":  longitude,
        "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "start_date": start_date.isoformat(),   # format: "2024-01-01"
        "end_date":   end_date.isoformat(),
        "timezone":   "auto",
    }

    # Make the HTTP GET request to the API.
    # timeout=30 means: if the API doesn't respond in 30 seconds, raise an error.
    # raise_for_status() means: if the API returns a 4xx or 5xx error code, raise an error.
    # This ensures failures are loud and obvious rather than silently returning bad data.
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    # The API returns JSON. We parse it and pull out the "daily" section,
    # which contains arrays of values — one per day.
    # Example structure:
    # {
    #   "daily": {
    #     "time": ["2024-01-01", "2024-01-02", ...],
    #     "temperature_2m_max": [5.2, 3.1, ...],
    #     ...
    #   }
    # }
    data = response.json()["daily"]

    # Build a DataFrame from the API response arrays.
    # Each key becomes a column, each array becomes the column's values.
    # We also add "city" as a column so we know which city each row belongs to
    # when we combine data from multiple cities later.
    df = pd.DataFrame({
        "date":      data["time"],
        "city":      city,
        "temp_max":  data["temperature_2m_max"],
        "temp_min":  data["temperature_2m_min"],
        "precip_mm": data["precipitation_sum"],
    })

    # Convert the "date" column from a plain string ("2024-01-01")
    # to a proper pandas datetime object. This lets us do date maths later
    # and makes charts render correctly on the x-axis.
    df["date"] = pd.to_datetime(df["date"])

    return df