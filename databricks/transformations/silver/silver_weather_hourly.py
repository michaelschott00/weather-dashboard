from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations._columns import WEATHER_MEASUREMENT_COLUMNS, WEATHER_TIMESTAMP_PATTERN, source_timestamp_col

# Normalized hourly weather measurements.
# arrays_zip merges the parallel arrays inside the 'hourly' struct into one
# array of structs, then explode produces one row per hour (24 rows per file).
# Times are local Berlin time; apply utc_offset_seconds from silver_weather_metadata
# to convert to UTC when needed.
# source_file_timestamp is parsed from the filename so downstream gold views can
# keep only the newest source file for each measurement day.


@dp.table(
    comment="Normalized hourly weather measurements. One row per hour per source file. "
            "Join to silver_weather_metadata on _source_file.",
)
def silver_weather_hourly():
    return (
        spark.readStream.table("weather.bronze.bronze_weather")
        .select(
            "_source_file",
            F.explode(
                F.arrays_zip(
                    F.col("hourly.time").alias("time"),
                    *[
                        F.col(f"hourly.{c}").alias(c)
                        for c in WEATHER_MEASUREMENT_COLUMNS
                    ],
                )
            ).alias("h"),
        )
        .select(
            "_source_file",
            source_timestamp_col(WEATHER_TIMESTAMP_PATTERN),
            F.col("h.time").cast("timestamp").alias("time"),
            *[F.col(f"h.{c}").alias(c) for c in WEATHER_MEASUREMENT_COLUMNS],
        )
    )
