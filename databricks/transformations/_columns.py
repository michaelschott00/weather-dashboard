from pyspark.sql import functions as F
from pyspark.sql.window import Window

WEATHER_MEASUREMENT_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "wind_speed_10m",
    "surface_pressure",
    "cloud_cover",
    "uv_index",
]

AQ_MEASUREMENT_COLUMNS = ["pm2_5", "ozone", "european_aqi"]

METADATA_COLUMNS = [
    "_source_file",
    "latitude",
    "longitude",
    "generationtime_ms",
    "utc_offset_seconds",
    "timezone",
    "timezone_abbreviation",
    "elevation",
]

WEATHER_TIMESTAMP_PATTERN = r"weather_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json$"
AQ_TIMESTAMP_PATTERN = r"aq_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json$"


def source_timestamp_col(pattern):
    return F.to_timestamp(
        F.regexp_extract("_source_file", pattern, 1),
        "yyyy-MM-dd_HH-mm-ss",
    ).alias("source_file_timestamp")


def latest_file_per_day(spark, table_name):
    latest_file_per_day_window = Window.partitionBy("measurement_day")
    return (
        spark.read.table(table_name)
        .filter(F.col("source_file_timestamp").isNotNull())
        .withColumn("measurement_day", F.to_date("time"))
        .withColumn(
            "latest_source_file_timestamp",
            F.max("source_file_timestamp").over(latest_file_per_day_window),
        )
        .filter(
            F.col("source_file_timestamp") == F.col("latest_source_file_timestamp")
        )
        .drop("measurement_day", "latest_source_file_timestamp")
    )
