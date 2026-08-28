import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _columns import WEATHER_MEASUREMENT_COLUMNS

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
            *[
                F.col(f"hourly_units.{c}").alias(f"{c}_unit")
                for c in WEATHER_MEASUREMENT_COLUMNS
            ],
        )
    )
