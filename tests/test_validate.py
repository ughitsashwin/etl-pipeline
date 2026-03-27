import sys
import os
import pytest
import pandas as pd
import pandera

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validate import validate
from src.transform import transform_weather


def make_valid_df():
    """A fully valid transformed DataFrame that should always pass validation."""
    raw = pd.DataFrame({
        "date":      pd.to_datetime(["2024-01-01", "2024-01-02"]),
        "city":      ["Dublin", "London"],
        "temp_max":  [10.0, 8.0],
        "temp_min":  [ 2.0, 1.0],
        "precip_mm": [ 0.0, 3.5],
    })
    return transform_weather(raw)


def test_valid_data_passes():
    """A clean DataFrame should pass validation without raising any errors."""
    df = make_valid_df()
    result = validate(df)
    assert result is not None
    assert len(result) == 2


def test_invalid_city_rejected():
    """A city not in our VALID_CITIES list should cause a SchemaError."""
    df = make_valid_df()
    # Tamper with the city name to something unexpected
    df.loc[0, "city"] = "Atlantis"
    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)


def test_temperature_too_high_rejected():
    """A temperature above 60 degrees is physically impossible — should be rejected."""
    df = make_valid_df()
    df.loc[0, "temp_max"] = 999.0
    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)


def test_temperature_too_low_rejected():
    """A temperature below -90 degrees is physically impossible — should be rejected."""
    df = make_valid_df()
    df.loc[0, "temp_min"] = -999.0
    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)


def test_negative_precipitation_rejected():
    """Negative precipitation is impossible — should be rejected."""
    df = make_valid_df()
    df.loc[0, "precip_mm"] = -5.0
    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)


def test_negative_temp_range_rejected():
    """temp_range below 0 means max < min which is impossible — should be rejected."""
    df = make_valid_df()
    df.loc[0, "temp_range"] = -1.0
    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)


def test_all_valid_cities_accepted():
    """Every city in our pipeline should pass validation."""
    cities = ["Dublin", "London", "New York", "Chennai", "Kozhikode"]
    for city in cities:
        df = pd.DataFrame({
            "date":      pd.to_datetime(["2024-01-01"]),
            "city":      [city],
            "temp_max":  [25.0],
            "temp_min":  [15.0],
            "precip_mm": [0.0],
        })
        clean = transform_weather(df)
        result = validate(clean)
        assert len(result) == 1, f"{city} should pass validation"
