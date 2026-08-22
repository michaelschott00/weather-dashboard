from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="weather.gold.gold_hourly_facts",
    comment="Gold layer: fact table with hourly weather and air quality measurements joined on timestamp."
)
def gold_hourly_facts():
    latest_file_per_day = Window.partitionBy("measurement_day")

    # Batch read from silver streaming tables and keep only the newest source file
    # for each measurement day before joining weather and air quality rows.
    # Filter out rows with null source_file_timestamp (old data before timestamp was added).
    weather = (
        spark.read.table("silver_weather_hourly")
        .filter(F.col("source_file_timestamp").isNotNull())
        .withColumn("measurement_day", F.to_date("time"))
        .withColumn(
            "latest_source_file_timestamp",
            F.max("source_file_timestamp").over(latest_file_per_day),
        )
        .filter(F.col("source_file_timestamp") == F.col("latest_source_file_timestamp"))
        .drop("measurement_day", "latest_source_file_timestamp")
    )
    aq = (
        spark.read.table("silver_aq_hourly")
        .filter(F.col("source_file_timestamp").isNotNull())
        .withColumn("measurement_day", F.to_date("time"))
        .withColumn(
            "latest_source_file_timestamp",
            F.max("source_file_timestamp").over(latest_file_per_day),
        )
        .filter(F.col("source_file_timestamp") == F.col("latest_source_file_timestamp"))
        .drop("measurement_day", "latest_source_file_timestamp")
    )
    
    # Join on time, drop carbon_dioxide (all nulls), and drop lineage columns
    df = (
        weather
        .join(aq, on="time", how="inner")
        .select(
            "time",
            # Weather measurements
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "wind_speed_10m",
            "surface_pressure",
            "cloud_cover",
            "uv_index",
            # Air quality measurements (excluding carbon_dioxide)
            "pm2_5",
            "ozone",
            "european_aqi"
        )
    )
    
    # Window for lag calculation (ordered by time)
    time_window = Window.orderBy("time")
    
    # Add KPI score columns
    return (
        df
        # apparent_temperature_score: trapezoid with peak [15, 22], slopes [10->15, 22->26]
        .withColumn(
            "apparent_temperature_score",
            F.when(F.col("apparent_temperature") < 10, F.lit(0))
            .when(F.col("apparent_temperature") < 15, 
                  (F.col("apparent_temperature") - 10) / (15 - 10) * 100)
            .when(F.col("apparent_temperature") <= 22, F.lit(100))
            .when(F.col("apparent_temperature") < 26,
                  100 - (F.col("apparent_temperature") - 22) / (26 - 22) * 100)
            .otherwise(F.lit(0))
        )
        # surface_pressure_score: trapezoid with peak [1020, 1030], slope [1010->1020], no negative slope
        .withColumn(
            "surface_pressure_score",
            F.when(F.col("surface_pressure") < 1010, F.lit(0))
            .when(F.col("surface_pressure") < 1020,
                  (F.col("surface_pressure") - 1010) / (1020 - 1010) * 100)
            .otherwise(F.lit(100))
        )
        # surface_pressure_delta (difference from previous hour)
        .withColumn(
            "surface_pressure_delta",
            F.col("surface_pressure") - F.lag("surface_pressure", 1).over(time_window)
        )
        # surface_pressure_delta_score: trapezoid with peak [3, 5], slope [-5->3], no negative slope
        .withColumn(
            "surface_pressure_delta_score",
            F.when(F.col("surface_pressure_delta").isNull(), F.lit(None))
            .when(F.col("surface_pressure_delta") < -5, F.lit(0))
            .when(F.col("surface_pressure_delta") < 3,
                  (F.col("surface_pressure_delta") + 5) / (3 - (-5)) * 100)
            .otherwise(F.lit(100))
        )
        # european_aqi_score: trapezoid with peak [0, 25], no positive slope, negative slope [25->100]
        .withColumn(
            "european_aqi_score",
            F.when(F.col("european_aqi") <= 25, F.lit(100))
            .when(F.col("european_aqi") < 100,
                  100 - (F.col("european_aqi") - 25) / (100 - 25) * 100)
            .otherwise(F.lit(0))
        )
        # cloud_cover_score: trapezoid with peak [0, 50], no positive slope, negative slope [50->90]
        .withColumn(
            "cloud_cover_score",
            F.when(F.col("cloud_cover") <= 50, F.lit(100))
            .when(F.col("cloud_cover") < 90,
                  100 - (F.col("cloud_cover") - 50) / (90 - 50) * 100)
            .otherwise(F.lit(0))
        )
        # relative_humidity_2m_score: trapezoid with peak [40, 60], slopes [30->40, 60->80]
        .withColumn(
            "relative_humidity_2m_score",
            F.when(F.col("relative_humidity_2m") < 30, F.lit(0))
            .when(F.col("relative_humidity_2m") < 40,
                  (F.col("relative_humidity_2m") - 30) / (40 - 30) * 100)
            .when(F.col("relative_humidity_2m") <= 60, F.lit(100))
            .when(F.col("relative_humidity_2m") < 80,
                  100 - (F.col("relative_humidity_2m") - 60) / (80 - 60) * 100)
            .otherwise(F.lit(0))
        )
        # wind_speed_10m_score: trapezoid with peak [12, 19], slopes [0->12, 19->39]
        .withColumn(
            "wind_speed_10m_score",
            F.when(F.col("wind_speed_10m") < 12,
                  F.col("wind_speed_10m") / 12 * 100)
            .when(F.col("wind_speed_10m") <= 19, F.lit(100))
            .when(F.col("wind_speed_10m") < 39,
                  100 - (F.col("wind_speed_10m") - 19) / (39 - 19) * 100)
            .otherwise(F.lit(0))
        )
        # weather_score: weighted sum of all KPI scores
        .withColumn(
            "weather_score",
            (0.25 * F.col("apparent_temperature_score")) +
            (0.10 * F.col("surface_pressure_score")) +
            (0.10 * F.coalesce(F.col("surface_pressure_delta_score"), F.lit(0))) +
            (0.20 * F.col("european_aqi_score")) +
            (0.20 * F.col("cloud_cover_score")) +
            (0.10 * F.col("relative_humidity_2m_score")) +
            (0.05 * F.col("wind_speed_10m_score"))
        )
        .select(
            "time",
            # Original weather measurements
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "wind_speed_10m",
            "surface_pressure",
            "cloud_cover",
            "uv_index",
            # Original air quality measurements
            "pm2_5",
            "ozone",
            "european_aqi",
            # New KPI scores
            "apparent_temperature_score",
            "surface_pressure_score",
            "surface_pressure_delta_score",
            "european_aqi_score",
            "cloud_cover_score",
            "relative_humidity_2m_score",
            "wind_speed_10m_score",
            # Composite score
            "weather_score"
        )
    )
