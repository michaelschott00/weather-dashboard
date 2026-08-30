from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark: SparkSession


def compute_gold_thresholds_dim(spark):
    """Return the thresholds dimension: one row per thresholds configuration.

    Single row with thresholds_id as primary key (user-defined name, e.g.
    "trapezoid"). Each threshold parameter for each metric is its own column
    named {metric}_{param} unless that parameter is None for the metric.
    """
    schema = StructType(
        [
            StructField("thresholds_id", StringType(), nullable=False),
            # apparent_temperature: all thresholds present
            StructField("apparent_temperature_pos_slope_start", IntegerType(), nullable=True),
            StructField("apparent_temperature_pos_slope_end", IntegerType(), nullable=True),
            StructField("apparent_temperature_neg_slope_start", IntegerType(), nullable=True),
            StructField("apparent_temperature_neg_slope_end", IntegerType(), nullable=True),
            StructField("apparent_temperature_peak_start", IntegerType(), nullable=True),
            StructField("apparent_temperature_peak_end", IntegerType(), nullable=True),
            # surface_pressure: no negative slope
            StructField("surface_pressure_pos_slope_start", IntegerType(), nullable=True),
            StructField("surface_pressure_pos_slope_end", IntegerType(), nullable=True),
            StructField("surface_pressure_peak_start", IntegerType(), nullable=True),
            StructField("surface_pressure_peak_end", IntegerType(), nullable=True),
            # surface_pressure_delta: no negative slope
            StructField("surface_pressure_delta_pos_slope_start", IntegerType(), nullable=True),
            StructField("surface_pressure_delta_pos_slope_end", IntegerType(), nullable=True),
            StructField("surface_pressure_delta_peak_start", IntegerType(), nullable=True),
            StructField("surface_pressure_delta_peak_end", IntegerType(), nullable=True),
            # european_aqi: no positive slope
            StructField("european_aqi_neg_slope_start", IntegerType(), nullable=True),
            StructField("european_aqi_neg_slope_end", IntegerType(), nullable=True),
            StructField("european_aqi_peak_start", IntegerType(), nullable=True),
            StructField("european_aqi_peak_end", IntegerType(), nullable=True),
            # cloud_cover: no positive slope
            StructField("cloud_cover_neg_slope_start", IntegerType(), nullable=True),
            StructField("cloud_cover_neg_slope_end", IntegerType(), nullable=True),
            StructField("cloud_cover_peak_start", IntegerType(), nullable=True),
            StructField("cloud_cover_peak_end", IntegerType(), nullable=True),
            # relative_humidity_2m: all thresholds present
            StructField("relative_humidity_2m_pos_slope_start", IntegerType(), nullable=True),
            StructField("relative_humidity_2m_pos_slope_end", IntegerType(), nullable=True),
            StructField("relative_humidity_2m_neg_slope_start", IntegerType(), nullable=True),
            StructField("relative_humidity_2m_neg_slope_end", IntegerType(), nullable=True),
            StructField("relative_humidity_2m_peak_start", IntegerType(), nullable=True),
            StructField("relative_humidity_2m_peak_end", IntegerType(), nullable=True),
            # wind_speed_10m: all thresholds present
            StructField("wind_speed_10m_pos_slope_start", IntegerType(), nullable=True),
            StructField("wind_speed_10m_pos_slope_end", IntegerType(), nullable=True),
            StructField("wind_speed_10m_neg_slope_start", IntegerType(), nullable=True),
            StructField("wind_speed_10m_neg_slope_end", IntegerType(), nullable=True),
            StructField("wind_speed_10m_peak_start", IntegerType(), nullable=True),
            StructField("wind_speed_10m_peak_end", IntegerType(), nullable=True),
        ]
    )
    rows = [
        (
            "trapezoid",
            # apparent_temperature: pos_start, pos_end, neg_start, neg_end, peak_start, peak_end
            10, 15, 22, 26, 15, 22,
            # surface_pressure
            1010, 1020, 1020, 1030,
            # surface_pressure_delta
            -5, 3, 3, 5,
            # european_aqi: neg_start, neg_end, peak_start, peak_end
            25, 100, 0, 25,
            # cloud_cover: neg_start, neg_end, peak_start, peak_end
            50, 90, 0, 50,
            # relative_humidity_2m: pos_start, pos_end, neg_start, neg_end, peak_start, peak_end
            30, 40, 60, 80, 40, 60,
            # wind_speed_10m: pos_start, pos_end, neg_start, neg_end, peak_start, peak_end
            0, 12, 19, 39, 12, 19,
        ),
    ]
    return spark.createDataFrame(rows, schema=schema)


@dp.materialized_view(
    name="weather.gold.gold_thresholds_dim",
    comment="Gold layer: dimension table with the trapezoid threshold parameters used to score each KPI metric."
)
def gold_thresholds_dim():
    return compute_gold_thresholds_dim(spark)
