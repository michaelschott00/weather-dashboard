from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from transformations._columns import AQ_MEASUREMENT_COLUMNS

spark: SparkSession

# Units for each hourly air quality field — one row per ingested file.
# Join to silver_aq_hourly on _source_file to attach units to measurements.


def compute_silver_aq_hourly_units(df):
    """Select the unit fields from the 'hourly_units' object of raw bronze air quality rows.

    df: streaming or batch DataFrame of bronze air quality rows.
    """
    return df.select(
        "_source_file",
        F.col("hourly_units.time").alias("time_unit"),
        *[
            F.col(f"hourly_units.{c}").alias(f"{c}_unit")
            for c in AQ_MEASUREMENT_COLUMNS
        ],
    )


@dp.table(
    comment="Units for hourly air quality measurements (contents of the 'hourly_units' "
            "object). One row per source file.",
)
def silver_aq_hourly_units():
    return compute_silver_aq_hourly_units(
        spark.readStream.table("weather.bronze.bronze_air_quality")
    )
