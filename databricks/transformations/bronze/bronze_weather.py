from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark: SparkSession

# Set as pipeline configuration 'bronze_base_path' (see terraform/azure.tf).
base_path = spark.conf.get("bronze_base_path")  # type: ignore


@dp.table(
    name="weather.bronze.bronze_weather",
    comment="Raw weather data ingested from JSON files via Auto Loader. "
            "One row per weather_*.json file.",
)
def bronze_weather():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("pathGlobFilter", "weather_*.json")
        .load(base_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
