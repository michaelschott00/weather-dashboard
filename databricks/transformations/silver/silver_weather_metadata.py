from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from transformations._columns import METADATA_COLUMNS, WEATHER_TIMESTAMP_PATTERN, source_timestamp_col

spark: SparkSession

# Outer-level weather fields — one row per ingested file.
# Join to silver_weather_hourly / silver_weather_hourly_units on _source_file.
# source_file_timestamp is parsed from the filename so downstream gold views can
# keep only the newest source file for each measurement day.


def compute_silver_weather_metadata(df):
    """Select the outer metadata fields from raw bronze weather rows.

    df: streaming or batch DataFrame of bronze weather rows.
    """
    return df.select(*METADATA_COLUMNS, source_timestamp_col(WEATHER_TIMESTAMP_PATTERN))


@dp.table(
    comment="Weather measurement metadata: all outer fields except 'hourly' and "
            "'hourly_units'. One row per source file.",
)
def silver_weather_metadata():
    return compute_silver_weather_metadata(
        spark.readStream.table("weather.bronze.bronze_weather")
    )
