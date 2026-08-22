from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Units for each hourly air quality field — one row per ingested file.
# Join to silver_aq_hourly on _source_file to attach units to measurements.


@dp.table(
    comment="Units for hourly air quality measurements (contents of the 'hourly_units' "
            "object). One row per source file.",
)
def silver_aq_hourly_units():
    return (
        spark.readStream.table("weather.bronze.bronze_air_quality")
        .select(
            "_source_file",
            F.col("hourly_units.time").alias("time_unit"),
            F.col("hourly_units.pm2_5").alias("pm2_5_unit"),
            F.col("hourly_units.carbon_dioxide").alias("carbon_dioxide_unit"),
            F.col("hourly_units.ozone").alias("ozone_unit"),
            F.col("hourly_units.european_aqi").alias("european_aqi_unit"),
        )
    )
