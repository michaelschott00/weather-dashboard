from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark: SparkSession

@dp.materialized_view(
    name="weather.gold.gold_time_dim",
    comment="Gold layer: time dimension with coarser-grained attributes (day, week, month, year) for each hourly timestamp."
)
def gold_time_dim():
    # Read fact table to get distinct timestamps
    facts = spark.read.table("gold.gold_hourly_facts")
    
    return (
        facts
        .select("time")
        .distinct()
        .withColumn("day", F.to_date("time"))
        .withColumn("week", F.weekofyear("time"))
        .withColumn("month", F.month("time"))
        .withColumn("year", F.year("time"))
        .select(
            "time",
            "day",
            "week",
            "month",
            "year"
        )
    )
