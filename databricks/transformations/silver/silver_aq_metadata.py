from pyspark import pipelines as dp

from transformations._columns import METADATA_COLUMNS

# Outer-level air quality fields — one row per ingested file.
# Join to silver_aq_hourly / silver_aq_hourly_units on _source_file.


@dp.table(
    comment="Air quality measurement metadata: all outer fields except 'hourly' and "
            "'hourly_units'. One row per source file.",
)
def silver_aq_metadata():
    return (
        spark.readStream.table("weather.bronze.bronze_air_quality")
        .select(*METADATA_COLUMNS)
    )
