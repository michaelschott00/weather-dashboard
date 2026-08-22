from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Normalized hourly weather measurements.
# arrays_zip merges the parallel arrays inside the 'hourly' struct into one
# array of structs, then explode produces one row per hour (24 rows per file).
# Times are local Berlin time; apply utc_offset_seconds from silver_weather_metadata
# to convert to UTC when needed.
# source_file_timestamp is parsed from the filename so downstream gold views can
# keep only the newest source file for each measurement day.

source_file_timestamp_pattern = r"weather_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json$"


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
                    F.col("hourly.temperature_2m").alias("temperature_2m"),
                    F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
                    F.col("hourly.apparent_temperature").alias("apparent_temperature"),
                    F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
                    F.col("hourly.surface_pressure").alias("surface_pressure"),
                    F.col("hourly.cloud_cover").alias("cloud_cover"),
                    F.col("hourly.uv_index").alias("uv_index"),
                )
            ).alias("h"),
        )
        .select(
            "_source_file",
            F.to_timestamp(
                F.regexp_extract(
                    F.col("_source_file"),
                    source_file_timestamp_pattern,
                    1,
                ),
                "yyyy-MM-dd_HH-mm-ss",
            ).alias("source_file_timestamp"),
            F.col("h.time").cast("timestamp").alias("time"),
            F.col("h.temperature_2m").alias("temperature_2m"),
            F.col("h.relative_humidity_2m").alias("relative_humidity_2m"),
            F.col("h.apparent_temperature").alias("apparent_temperature"),
            F.col("h.wind_speed_10m").alias("wind_speed_10m"),
            F.col("h.surface_pressure").alias("surface_pressure"),
            F.col("h.cloud_cover").alias("cloud_cover"),
            F.col("h.uv_index").alias("uv_index"),
        )
    )
