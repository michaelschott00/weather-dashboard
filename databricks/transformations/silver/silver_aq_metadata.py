from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from transformations._columns import METADATA_COLUMNS

spark: SparkSession

# Outer-level air quality fields — one row per ingested file.
# Join to silver_aq_hourly / silver_aq_hourly_units on _source_file.


def compute_silver_aq_metadata(df):
    """Select the outer metadata fields from raw bronze air quality rows.

    df: streaming or batch DataFrame of bronze air quality rows.
    """
    return df.select(*METADATA_COLUMNS)


@dp.table(
    comment="Air quality measurement metadata: all outer fields except 'hourly' and "
            "'hourly_units'. One row per source file.",
)
def silver_aq_metadata():
    return compute_silver_aq_metadata(
        spark.readStream.table("weather.bronze.bronze_air_quality")
    )
