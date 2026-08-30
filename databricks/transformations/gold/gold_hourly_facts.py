from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from transformations._columns import (
    WEATHER_MEASUREMENT_COLUMNS,
    AQ_MEASUREMENT_COLUMNS,
    latest_file_per_day,
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


@dp.materialized_view(
    name="weather.gold.gold_hourly_facts",
    comment="Gold layer: fact table with hourly weather and air quality measurements joined on timestamp."
)
def gold_hourly_facts():
    # Batch read from silver streaming tables and keep only the newest source file
    # for each measurement day before joining weather and air quality rows.
    # Filter out rows with null source_file_timestamp (old data before timestamp was added).
    weather = latest_file_per_day(spark, "silver_weather_hourly")
    aq = latest_file_per_day(spark, "silver_aq_hourly")
    return compute_gold_hourly_facts(weather, aq)
