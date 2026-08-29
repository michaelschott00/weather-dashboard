from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from conftest import load_transformations_module

silver_weather_hourly = load_transformations_module(
    "transformations.silver.silver_weather_hourly"
)
silver_aq_hourly = load_transformations_module(
    "transformations.silver.silver_aq_hourly"
)
silver_weather_metadata = load_transformations_module(
    "transformations.silver.silver_weather_metadata"
)
silver_aq_metadata = load_transformations_module(
    "transformations.silver.silver_aq_metadata"
)
silver_weather_hourly_units = load_transformations_module(
    "transformations.silver.silver_weather_hourly_units"
)
silver_aq_hourly_units = load_transformations_module(
    "transformations.silver.silver_aq_hourly_units"
)


WEATHER_MEASUREMENT_FIELDS = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "wind_speed_10m",
    "surface_pressure",
    "cloud_cover",
    "uv_index",
]


def _weather_bronze_df(spark):
    hourly_schema = StructType(
        [
            StructField(name, ArrayType(StringType()))
            for name in WEATHER_MEASUREMENT_FIELDS
        ]
    )
    schema = StructType(
        [
            StructField("_source_file", StringType()),
            StructField("hourly", hourly_schema),
        ]
    )
    return spark.createDataFrame(
        [
            Row(
                _source_file="/path/weather_2026-08-28_13-05-00.json",
                hourly={
                    "time": ["2026-08-28T13:00", "2026-08-28T14:00"],
                    "temperature_2m": ["20.5", "21.0"],
                    "relative_humidity_2m": ["60.0", "61.0"],
                    "apparent_temperature": ["23.1", "24.0"],
                    "wind_speed_10m": ["12.3", "13.1"],
                    "surface_pressure": ["1012.0", "1011.0"],
                    "cloud_cover": ["10", "20"],
                    "uv_index": ["3", "4"],
                },
            )
        ],
        schema,
    )


def test_silver_weather_hourly_explodes_and_parses_timestamp(spark):
    df = _weather_bronze_df(spark)
    out = silver_weather_hourly.compute_silver_weather_hourly(df)

    assert out.count() == 2
    rows = {r.time.hour: r for r in out.collect()}

    assert rows[13].source_file_timestamp == \
        datetime(2026, 8, 28, 13, 5, 0)
    assert rows[13].temperature_2m == "20.5"
    assert rows[13].relative_humidity_2m == "60.0"

    assert rows[14].source_file_timestamp == \
        datetime(2026, 8, 28, 13, 5, 0)
    assert rows[14].temperature_2m == "21.0"
    assert rows[14].surface_pressure == "1011.0"

    # source_file_timestamp is carried from the filename
    assert out.select("source_file_timestamp").first()[0] == \
        datetime(2026, 8, 28, 13, 5, 0)


def _aq_bronze_df(spark):
    hourly_schema = StructType(
        [
            StructField("time", ArrayType(StringType())),
            StructField("pm2_5", ArrayType(StringType())),
            StructField("ozone", ArrayType(StringType())),
            StructField("european_aqi", ArrayType(StringType())),
        ]
    )
    schema = StructType(
        [
            StructField("_source_file", StringType()),
            StructField("hourly", hourly_schema),
        ]
    )
    return spark.createDataFrame(
        [
            Row(
                _source_file="/path/aq_2026-08-28_13-05-00.json",
                hourly={
                    "time": ["2026-08-28T13:00", "2026-08-28T14:00"],
                    "pm2_5": ["10.0", "11.0"],
                    "ozone": ["30.0", "31.0"],
                    "european_aqi": ["20", "21"],
                },
            )
        ],
        schema,
    )


def test_silver_aq_hourly_explodes_and_parses_timestamp(spark):
    df = _aq_bronze_df(spark)
    out = silver_aq_hourly.compute_silver_aq_hourly(df)

    assert out.count() == 2
    rows = {r.time.hour: r for r in out.collect()}
    assert rows[13].pm2_5 == "10.0"
    assert rows[13].ozone == "30.0"
    assert rows[14].european_aqi == "21"


WEATHER_UNITS_FIELDS = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "wind_speed_10m",
    "surface_pressure",
    "cloud_cover",
    "uv_index",
]


def test_silver_weather_hourly_units_selects_unit_columns(spark):
    units_schema = StructType(
        [
            StructField(name, StringType())
            for name in WEATHER_UNITS_FIELDS
        ]
    )
    schema = StructType(
        [
            StructField("_source_file", StringType()),
            StructField("hourly_units", units_schema),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(
                _source_file="/path/weather_2026-08-28_13-05-00.json",
                hourly_units={
                    "time": "iso8601",
                    "temperature_2m": "°C",
                    "relative_humidity_2m": "%",
                    "apparent_temperature": "°C",
                    "wind_speed_10m": "km/h",
                    "surface_pressure": "hPa",
                    "cloud_cover": "%",
                    "uv_index": "",
                },
            )
        ],
        schema,
    )
    out = silver_weather_hourly_units.compute_silver_weather_hourly_units(df)
    row = out.first()
    assert row.time_unit == "iso8601"
    assert row.temperature_2m_unit == "°C"
    assert row.relative_humidity_2m_unit == "%"
    assert row.surface_pressure_unit == "hPa"
    assert row.wind_speed_10m_unit == "km/h"


def test_silver_aq_hourly_units_selects_unit_columns(spark):
    units_schema = StructType(
        [
            StructField("time", StringType()),
            StructField("pm2_5", StringType()),
            StructField("ozone", StringType()),
            StructField("european_aqi", StringType()),
        ]
    )
    schema = StructType(
        [
            StructField("_source_file", StringType()),
            StructField("hourly_units", units_schema),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(
                _source_file="/path/aq_2026-08-28_13-05-00.json",
                hourly_units={
                    "time": "iso8601",
                    "pm2_5": "µg/m³",
                    "ozone": "µg/m³",
                    "european_aqi": "European AQI",
                },
            )
        ],
        schema,
    )
    out = silver_aq_hourly_units.compute_silver_aq_hourly_units(df)
    row = out.first()
    assert row.time_unit == "iso8601"
    assert row.pm2_5_unit == "µg/m³"
    assert row.european_aqi_unit == "European AQI"


def test_silver_weather_metadata_selects_metadata_columns(spark):
    df = spark.createDataFrame(
        [
            (
                "/path/weather_2026-08-28_13-05-00.json",
                52.52,
                13.41,
                42,
                3600,
                "Europe/Berlin",
                "CEST",
                38.0,
            )
        ],
        [
            "_source_file",
            "latitude",
            "longitude",
            "generationtime_ms",
            "utc_offset_seconds",
            "timezone",
            "timezone_abbreviation",
            "elevation",
        ],
    )
    out = silver_weather_metadata.compute_silver_weather_metadata(df)
    row = out.first()
    assert row.latitude == 52.52
    assert row.timezone == "Europe/Berlin"
    assert row.elevation == 38.0
    assert set(out.columns) == {
        "_source_file",
        "latitude",
        "longitude",
        "generationtime_ms",
        "utc_offset_seconds",
        "timezone",
        "timezone_abbreviation",
        "elevation",
    }


def test_silver_aq_metadata_selects_metadata_columns(spark):
    df = spark.createDataFrame(
        [
            (
                "/path/aq_2026-08-28_13-05-00.json",
                52.52,
                13.41,
                42,
                3600,
                "Europe/Berlin",
                "CEST",
                38.0,
            )
        ],
        [
            "_source_file",
            "latitude",
            "longitude",
            "generationtime_ms",
            "utc_offset_seconds",
            "timezone",
            "timezone_abbreviation",
            "elevation",
        ],
    )
    out = silver_aq_metadata.compute_silver_aq_metadata(df)
    row = out.first()
    assert row.latitude == 52.52
    assert row.timezone == "Europe/Berlin"
    assert set(out.columns) == {
        "_source_file",
        "latitude",
        "longitude",
        "generationtime_ms",
        "utc_offset_seconds",
        "timezone",
        "timezone_abbreviation",
        "elevation",
    }
