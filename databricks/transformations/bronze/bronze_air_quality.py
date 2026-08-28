from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set as pipeline configuration 'bronze_base_path' (see terraform/azure.tf).
base_path = spark.conf.get("bronze_base_path")


@dp.table(
    name="weather.bronze.bronze_air_quality",
    comment="Raw air quality data ingested from JSON files via Auto Loader. "
            "One row per aq_*.json file. "
)
def bronze_air_quality():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("pathGlobFilter", "aq_*.json")
        .load(base_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
