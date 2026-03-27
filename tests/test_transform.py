import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transform import transform_weather


def make_sample_df(overrides=None):
    data = {
        "date":      pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "city":      ["Dublin", "Dublin", "Dublin"],
        "temp_max":  [10.0,  5.0,  3.0],
        "temp_min":  [ 2.0, -1.0, -4.0],
        "precip_mm": [ 0.0,  5.5,  0.8],
    }
    if overrides:
        data.update(overrides)
    return pd.DataFrame(data)


def test_derived_columns_are_added():
    result = transform_weather(make_sample_df())
    assert "temp_avg"   in result.columns
    assert "temp_range" in result.columns
    assert "is_rainy"   in result.columns
    assert "loaded_at"  in result.columns


def test_temp_avg_is_correct():
    result = transform_weather(make_sample_df())
    assert result.loc[0, "temp_avg"] == pytest.approx(6.0)
    assert result.loc[1, "temp_avg"] == pytest.approx(2.0)


def test_temp_range_is_correct():
    result = transform_weather(make_sample_df())
    assert result.loc[0, "temp_range"] == pytest.approx(8.0)
    assert result.loc[2, "temp_range"] == pytest.approx(7.0)


def test_is_rainy_true_above_threshold():
    result = transform_weather(make_sample_df())
    assert result.loc[1, "is_rainy"] is True


def test_is_rainy_false_below_threshold():
    result = transform_weather(make_sample_df())
    assert result.loc[0, "is_rainy"] is False
    assert result.loc[2, "is_rainy"] is False


def test_is_rainy_is_boolean_type():
    result = transform_weather(make_sample_df())
    assert result["is_rainy"].dtype == bool


def test_missing_precip_filled_with_zero():
    df = make_sample_df({"precip_mm": [None, 5.5, 0.8]})
    result = transform_weather(df)
    assert result.loc[0, "precip_mm"] == pytest.approx(0.0)


def test_row_dropped_when_both_temps_null():
    df = make_sample_df({
        "temp_max": [None, 5.0,  3.0],
        "temp_min": [None, -1.0, -4.0],
    })
    result = transform_weather(df)
    assert len(result) == 2


def test_row_kept_when_only_one_temp_null():
    df = make_sample_df({
        "temp_max": [None, 5.0,  3.0],
        "temp_min": [2.0, -1.0, -4.0],
    })
    result = transform_weather(df)
    assert len(result) == 3


def test_original_dataframe_not_mutated():
    original = make_sample_df()
    original_columns = list(original.columns)
    transform_weather(original)
    assert list(original.columns) == original_columns


def test_output_has_correct_number_of_columns():
    result = transform_weather(make_sample_df())
    assert len(result.columns) == 9


def test_date_column_is_datetime():
    result = transform_weather(make_sample_df())
    assert pd.api.types.is_datetime64_any_dtype(result["date"])


def test_index_is_reset_after_dropping_rows():
    df = make_sample_df({
        "temp_max": [None, 5.0,  3.0],
        "temp_min": [None, -1.0, -4.0],
    })
    result = transform_weather(df)
    assert list(result.index) == list(range(len(result)))
