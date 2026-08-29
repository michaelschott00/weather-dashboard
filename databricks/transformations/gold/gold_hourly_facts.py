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
    """Join silver weather and air quality rows on time and add KPI trapezoid parameters.

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

    # Window for lag calculation (ordered by time)
    time_window = Window.orderBy("time")

    # Replace KPI scores with the trapezoid input parameters they were computed from,
    # exposed as separate columns (one parameter per column) instead of the score itself.
    return (
        df
        # surface_pressure_delta (difference from previous hour), kept as input to its parameters
        .withColumn(
            "surface_pressure_delta",
            F.col("surface_pressure") - F.lag("surface_pressure", 1).over(time_window)
        )
        # apparent_temperature trapezoid parameters: peak [15, 22], slopes [10->15, 22->26]
        .withColumn("apparent_temperature_pos_slope_start", F.lit(10))
        .withColumn("apparent_temperature_pos_slope_end", F.lit(15))
        .withColumn("apparent_temperature_peak_start", F.lit(15))
        .withColumn("apparent_temperature_peak_end", F.lit(22))
        .withColumn("apparent_temperature_neg_slope_start", F.lit(22))
        .withColumn("apparent_temperature_neg_slope_end", F.lit(26))
        # surface_pressure trapezoid parameters: peak [1020, 1030], slope [1010->1020], no negative slope
        .withColumn("surface_pressure_pos_slope_start", F.lit(1010))
        .withColumn("surface_pressure_pos_slope_end", F.lit(1020))
        .withColumn("surface_pressure_peak_start", F.lit(1020))
        .withColumn("surface_pressure_peak_end", F.lit(1030))
        # surface_pressure_delta trapezoid parameters: peak [3, 5], slope [-5->3], no negative slope
        .withColumn("surface_pressure_delta_pos_slope_start", F.lit(-5))
        .withColumn("surface_pressure_delta_pos_slope_end", F.lit(3))
        .withColumn("surface_pressure_delta_peak_start", F.lit(3))
        .withColumn("surface_pressure_delta_peak_end", F.lit(5))
        # european_aqi trapezoid parameters: peak [0, 25], no positive slope, negative slope [25->100]
        .withColumn("european_aqi_peak_start", F.lit(0))
        .withColumn("european_aqi_peak_end", F.lit(25))
        .withColumn("european_aqi_neg_slope_start", F.lit(25))
        .withColumn("european_aqi_neg_slope_end", F.lit(100))
        # cloud_cover trapezoid parameters: peak [0, 50], no positive slope, negative slope [50->90]
        .withColumn("cloud_cover_peak_start", F.lit(0))
        .withColumn("cloud_cover_peak_end", F.lit(50))
        .withColumn("cloud_cover_neg_slope_start", F.lit(50))
        .withColumn("cloud_cover_neg_slope_end", F.lit(90))
        # relative_humidity_2m trapezoid parameters: peak [40, 60], slopes [30->40, 60->80]
        .withColumn("relative_humidity_2m_pos_slope_start", F.lit(30))
        .withColumn("relative_humidity_2m_pos_slope_end", F.lit(40))
        .withColumn("relative_humidity_2m_peak_start", F.lit(40))
        .withColumn("relative_humidity_2m_peak_end", F.lit(60))
        .withColumn("relative_humidity_2m_neg_slope_start", F.lit(60))
        .withColumn("relative_humidity_2m_neg_slope_end", F.lit(80))
        # wind_speed_10m trapezoid parameters: peak [12, 19], slopes [0->12, 19->39]
        .withColumn("wind_speed_10m_pos_slope_start", F.lit(0))
        .withColumn("wind_speed_10m_pos_slope_end", F.lit(12))
        .withColumn("wind_speed_10m_peak_start", F.lit(12))
        .withColumn("wind_speed_10m_peak_end", F.lit(19))
        .withColumn("wind_speed_10m_neg_slope_start", F.lit(19))
        .withColumn("wind_speed_10m_neg_slope_end", F.lit(39))
        .select(
            "time",
            *[F.col(c) for c in WEATHER_MEASUREMENT_COLUMNS],
            *[F.col(c) for c in AQ_MEASUREMENT_COLUMNS],
            "surface_pressure_delta",
            "apparent_temperature_pos_slope_start",
            "apparent_temperature_pos_slope_end",
            "apparent_temperature_peak_start",
            "apparent_temperature_peak_end",
            "apparent_temperature_neg_slope_start",
            "apparent_temperature_neg_slope_end",
            "surface_pressure_pos_slope_start",
            "surface_pressure_pos_slope_end",
            "surface_pressure_peak_start",
            "surface_pressure_peak_end",
            "surface_pressure_delta_pos_slope_start",
            "surface_pressure_delta_pos_slope_end",
            "surface_pressure_delta_peak_start",
            "surface_pressure_delta_peak_end",
            "european_aqi_peak_start",
            "european_aqi_peak_end",
            "european_aqi_neg_slope_start",
            "european_aqi_neg_slope_end",
            "cloud_cover_peak_start",
            "cloud_cover_peak_end",
            "cloud_cover_neg_slope_start",
            "cloud_cover_neg_slope_end",
            "relative_humidity_2m_pos_slope_start",
            "relative_humidity_2m_pos_slope_end",
            "relative_humidity_2m_peak_start",
            "relative_humidity_2m_peak_end",
            "relative_humidity_2m_neg_slope_start",
            "relative_humidity_2m_neg_slope_end",
            "wind_speed_10m_pos_slope_start",
            "wind_speed_10m_pos_slope_end",
            "wind_speed_10m_peak_start",
            "wind_speed_10m_peak_end",
            "wind_speed_10m_neg_slope_start",
            "wind_speed_10m_neg_slope_end",
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
