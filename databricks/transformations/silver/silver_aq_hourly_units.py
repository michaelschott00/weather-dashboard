from pyspark import pipelines as dp
from pyspark.sql import functions as F

from .._columns import AQ_MEASUREMENT_COLUMNS

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
            *[
                F.col(f"hourly_units.{c}").alias(f"{c}_unit")
                for c in AQ_MEASUREMENT_COLUMNS
            ],
        )
    )
