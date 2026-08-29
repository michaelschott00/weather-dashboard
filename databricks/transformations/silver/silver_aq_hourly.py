from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from transformations._columns import AQ_MEASUREMENT_COLUMNS, AQ_TIMESTAMP_PATTERN, source_timestamp_col

spark: SparkSession

# Normalized hourly air quality measurements.
# arrays_zip merges the parallel arrays inside the 'hourly' struct into one
# array of structs, then explode produces one row per hour (24 rows per file).
# source_file_timestamp is parsed from the filename so downstream gold views can
# keep only the newest source file for each measurement day.


def compute_silver_aq_hourly(df):
    """Normalize raw bronze air quality rows into one row per hour per source file.

    df: streaming or batch DataFrame of bronze air quality rows (with a `hourly`
    struct of parallel arrays and a `_source_file` column).
    """
    return (
        df.select(
            "_source_file",
            F.explode(
                F.arrays_zip(
                    F.col("hourly.time").alias("time"),
                    *[
                        F.col(f"hourly.{c}").alias(c)
                        for c in AQ_MEASUREMENT_COLUMNS
                    ],
                )
            ).alias("h"),
        )
        .select(
            "_source_file",
            source_timestamp_col(AQ_TIMESTAMP_PATTERN),
            F.col("h.time").cast("timestamp").alias("time"),
            *[F.col(f"h.{c}").alias(c) for c in AQ_MEASUREMENT_COLUMNS],
        )
    )


@dp.table(
    comment="Normalized hourly air quality measurements. One row per hour per source "
            "file. Join to silver_aq_metadata on _source_file.",
)
def silver_aq_hourly():
    return compute_silver_aq_hourly(
        spark.readStream.table("weather.bronze.bronze_air_quality")
    )
