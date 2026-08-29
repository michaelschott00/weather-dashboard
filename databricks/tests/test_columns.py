from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

import transformations._columns as columns


def test_source_timestamp_col_weather_matches_weather_filename(spark):
    src = spark.createDataFrame(
        [(".../weather_2026-08-28_13-05-00.json",)],
        ["_source_file"],
    )
    out = src.select(columns.source_timestamp_col(columns.WEATHER_TIMESTAMP_PATTERN))
    assert out.schema["source_file_timestamp"].dataType == TimestampType()
    row = out.first()
    assert row.source_file_timestamp == datetime(2026, 8, 28, 13, 5, 0)


def test_source_timestamp_col_aq_matches_aq_filename(spark):
    src = spark.createDataFrame(
        [(".../aq_2026-08-28_13-05-00.json",)],
        ["_source_file"],
    )
    out = src.select(columns.source_timestamp_col(columns.AQ_TIMESTAMP_PATTERN))
    assert out.first().source_file_timestamp == datetime(2026, 8, 28, 13, 5, 0)


def test_source_timestamp_col_weather_does_not_match_aq_filename(spark):
    src = spark.createDataFrame(
        [(".../aq_2026-08-28_13-05-00.json",)],
        ["_source_file"],
    )
    out = src.select(columns.source_timestamp_col(columns.WEATHER_TIMESTAMP_PATTERN))
    assert out.first().source_file_timestamp is None


def test_source_timestamp_col_non_matching_is_null(spark):
    src = spark.createDataFrame([(".../other.json",)], ["_source_file"])
    out = src.select(columns.source_timestamp_col(columns.WEATHER_TIMESTAMP_PATTERN))
    assert out.first().source_file_timestamp is None


def _build_latest_per_day_input(spark):
    schema = StructType(
        [
            StructField("_source_file", StringType()),
            StructField("time", TimestampType()),
            StructField("source_file_timestamp", TimestampType()),
        ]
    )
    rows = [
        # 2026-08-28 has two source files; keep the newest (05:00)
        ("weather_2026-08-28_04-00-00.json", datetime(2026, 8, 28, 0, 0, 0), datetime(2026, 8, 28, 4, 0, 0)),
        ("weather_2026-08-28_05-00-00.json", datetime(2026, 8, 28, 0, 0, 0), datetime(2026, 8, 28, 5, 0, 0)),
        # newest file for 2026-08-28 has measurements at multiple hours
        ("weather_2026-08-28_05-00-00.json", datetime(2026, 8, 28, 1, 0, 0), datetime(2026, 8, 28, 5, 0, 0)),
        # another day with a single file
        ("weather_2026-08-27_12-00-00.json", datetime(2026, 8, 27, 0, 0, 0), datetime(2026, 8, 27, 12, 0, 0)),
        # null timestamp must be filtered out
        ("weather_old.json", datetime(2026, 8, 26, 0, 0, 0), None),
    ]
    return spark.createDataFrame(rows, schema)


def test_latest_file_per_day_keeps_newest_file_and_drops_nulls(spark):
    df = _build_latest_per_day_input(spark)
    df.createOrReplaceTempView("_test_latest_table")

    out = columns.latest_file_per_day(spark, "_test_latest_table")
    result = out.collect()
    assert len(result) == 3
    result = sorted((r.source_file_timestamp, r.time) for r in result)
    assert result == sorted(
        [
            (datetime(2026, 8, 28, 5, 0, 0), datetime(2026, 8, 28, 0, 0, 0)),
            (datetime(2026, 8, 28, 5, 0, 0), datetime(2026, 8, 28, 1, 0, 0)),
            (datetime(2026, 8, 27, 12, 0, 0), datetime(2026, 8, 27, 0, 0, 0)),
        ]
    )


def test_column_lists_are_defined():
    assert columns.WEATHER_MEASUREMENT_COLUMNS == [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "wind_speed_10m",
        "surface_pressure",
        "cloud_cover",
        "uv_index",
    ]
    assert columns.AQ_MEASUREMENT_COLUMNS == ["pm2_5", "ozone", "european_aqi"]
