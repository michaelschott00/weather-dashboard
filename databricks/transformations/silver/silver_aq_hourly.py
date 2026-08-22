from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Normalized hourly air quality measurements.
# arrays_zip merges the parallel arrays inside the 'hourly' struct into one
# array of structs, then explode produces one row per hour (24 rows per file).
# carbon_dioxide is null in current data but typed as DOUBLE via schemaHints
# on the bronze table so it survives future population without schema evolution.
# source_file_timestamp is parsed from the filename so downstream gold views can
# keep only the newest source file for each measurement day.

source_file_timestamp_pattern = r"aq_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json$"


@dp.table(
    comment="Normalized hourly air quality measurements. One row per hour per source "
            "file. Join to silver_aq_metadata on _source_file.",
)
def silver_aq_hourly():
    return (
        spark.readStream.table("weather.bronze.bronze_air_quality")
        .select(
            "_source_file",
            F.explode(
                F.arrays_zip(
                    F.col("hourly.time").alias("time"),
                    F.col("hourly.pm2_5").alias("pm2_5"),
                    F.col("hourly.carbon_dioxide").alias("carbon_dioxide"),
                    F.col("hourly.ozone").alias("ozone"),
                    F.col("hourly.european_aqi").alias("european_aqi"),
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
            F.col("h.pm2_5").alias("pm2_5"),
            F.col("h.carbon_dioxide").alias("carbon_dioxide"),
            F.col("h.ozone").alias("ozone"),
            F.col("h.european_aqi").alias("european_aqi"),
        )
    )
