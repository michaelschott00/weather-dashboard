from pyspark import pipelines as dp
from pyspark.sql import functions as F

base_path = "abfss://main@myamazingdatalake.dfs.core.windows.net/bronze/"


@dp.table(
    name="weather.bronze.bronze_air_quality",
    comment="Raw air quality data ingested from JSON files via Auto Loader. "
            "One row per aq_*.json file. "
            "carbon_dioxide is hinted as ARRAY<DOUBLE> because values are null in early files.",
)
def bronze_air_quality():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("pathGlobFilter", "aq_*.json")
        .option("cloudFiles.schemaHints", "hourly.carbon_dioxide ARRAY<DOUBLE>")
        .load(base_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
