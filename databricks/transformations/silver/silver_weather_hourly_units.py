from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Units for each hourly weather field — one row per ingested file.
# Join to silver_weather_hourly on _source_file to attach units to measurements.


@dp.table(
    comment="Units for hourly weather measurements (contents of the 'hourly_units' "
            "object). One row per source file.",
)
def silver_weather_hourly_units():
    return (
        spark.readStream.table("weather.bronze.bronze_weather")
        .select(
            "_source_file",
            F.col("hourly_units.time").alias("time_unit"),
            F.col("hourly_units.temperature_2m").alias("temperature_2m_unit"),
            F.col("hourly_units.relative_humidity_2m").alias("relative_humidity_2m_unit"),
            F.col("hourly_units.apparent_temperature").alias("apparent_temperature_unit"),
            F.col("hourly_units.wind_speed_10m").alias("wind_speed_10m_unit"),
            F.col("hourly_units.surface_pressure").alias("surface_pressure_unit"),
            F.col("hourly_units.cloud_cover").alias("cloud_cover_unit"),
            F.col("hourly_units.uv_index").alias("uv_index_unit"),
        )
    )
