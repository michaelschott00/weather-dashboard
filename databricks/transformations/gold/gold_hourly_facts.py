from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from transformations._columns import (
    WEATHER_MEASUREMENT_COLUMNS,
    AQ_MEASUREMENT_COLUMNS,
)

spark: SparkSession


def compute_gold_hourly_facts(weather, aq):
    """Join silver weather and air quality rows on time.

    weather, aq: batch DataFrames of hourly silver measurements (already restricted to
    the newest source file per measurement day), each with a 'time' column plus the
    measurement columns defined in _columns.
    """
    # Join on time and drop lineage columns
    df = (
        weather
        .join(aq, on="time", how="inner")
        .select(
            "time",
            *[F.col(c) for c in WEATHER_MEASUREMENT_COLUMNS],
            *[F.col(c) for c in AQ_MEASUREMENT_COLUMNS],
        )
    )

    # Window for lag calculation (ordered by time).
    time_window = Window.orderBy("time")

    return (
        df
        # surface_pressure_delta (difference from previous hour), computed input column
        .withColumn(
            "surface_pressure_delta",
            F.col("surface_pressure") - F.lag("surface_pressure", 1).over(time_window)
        )
        .withColumn("thresholds_id", F.lit("trapezoid"))
        .select(
            "time",
            *[F.col(c) for c in WEATHER_MEASUREMENT_COLUMNS],
            *[F.col(c) for c in AQ_MEASUREMENT_COLUMNS],
            "surface_pressure_delta",
            "thresholds_id",
        )
    )


def _latest_hourly(spark, hourly_table, metadata_table):
    """Return hourly rows filtered to the newest source file per measurement day.

    Joins the hourly table with its metadata table on _source_file to obtain
    source_file_timestamp (now stored in metadata) and keeps only the newest
    file for each measurement day. Rows with null timestamp are dropped.
    """
    hourly = spark.read.table(hourly_table)
    meta = spark.read.table(metadata_table).select(
        "_source_file", "source_file_timestamp"
    )
    df = hourly.join(meta, on="_source_file", how="inner").filter(
        F.col("source_file_timestamp").isNotNull()
    )
    window = Window.partitionBy(F.to_date("time"))
    return (
        df.withColumn(
            "latest_source_file_timestamp",
            F.max("source_file_timestamp").over(window),
        )
        .filter(F.col("source_file_timestamp") == F.col("latest_source_file_timestamp"))
        .drop("latest_source_file_timestamp")
    )


@dp.materialized_view(
    name="weather.gold.gold_hourly_facts",
    comment="Gold layer: fact table with hourly weather and air quality measurements joined on timestamp."
)
def gold_hourly_facts():
    # Batch read from silver streaming/hourly and metadata tables, join to obtain
    # source_file_timestamp (now stored in metadata), and keep only the newest
    # source file for each measurement day before joining weather and air quality.
    # Filter out rows with null source_file_timestamp (old data before timestamp was added).
    weather = _latest_hourly(spark, "silver_weather_hourly", "silver_weather_metadata")
    aq = _latest_hourly(spark, "silver_aq_hourly", "silver_aq_metadata")
    return compute_gold_hourly_facts(weather, aq)
