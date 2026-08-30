from datetime import datetime

from pyspark.sql import functions as F

from conftest import load_transformations_module

gold_hourly_facts = load_transformations_module("transformations.gold.gold_hourly_facts")
gold_time_dim = load_transformations_module("transformations.gold.gold_time_dim")
gold_thresholds_dim = load_transformations_module(
    "transformations.gold.gold_thresholds_dim"
)


def _weather_df(spark):
    rows = [
        (datetime(2026, 8, 28, 13, 0, 0), 20.5, 50.0, 1013.0, 15.0, 1000.0, 0.0, 3.0),
        (datetime(2026, 8, 28, 14, 0, 0), 21.5, 55.0, 1012.0, 16.0, 1001.0, 1.0, 4.0),
        (datetime(2026, 8, 28, 15, 0, 0), 22.0, 60.0, 1011.0, 17.0, 1002.0, 2.0, 5.0),
    ]
    return spark.createDataFrame(
        rows,
        [
            "time",
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "apparent_temperature",
            "wind_speed_10m",
            "cloud_cover",
            "uv_index",
        ],
    )


def _aq_df(spark):
    rows = [
        (datetime(2026, 8, 28, 13, 0, 0), 10.0, 30.0, 12),
        (datetime(2026, 8, 28, 14, 0, 0), 11.0, 31.0, 13),
        (datetime(2026, 8, 28, 15, 0, 0), 12.0, 32.0, 14),
    ]
    return spark.createDataFrame(
        rows,
        ["time", "pm2_5", "ozone", "european_aqi"],
    )


def test_gold_hourly_facts_joins_on_time(spark):
    weather = _weather_df(spark)
    aq = _aq_df(spark)
    out = gold_hourly_facts.compute_gold_hourly_facts(weather, aq)

    assert out.count() == 3
    assert out.select("time").orderBy("time").first().time == datetime(
        2026, 8, 28, 13, 0, 0
    )
    # Joined row has both weather and air quality measurements.
    first = out.orderBy("time").first()
    assert first.temperature_2m == 20.5
    assert first.pm2_5 == 10.0
    assert first.european_aqi == 12


def test_gold_hourly_facts_inner_join_drops_non_matching_times(spark):
    weather = _weather_df(spark)
    # aq missing the 13:00 and 15:00 times
    aq = spark.createDataFrame(
        [(datetime(2026, 8, 28, 14, 0, 0), 11.0, 31.0, 13)],
        ["time", "pm2_5", "ozone", "european_aqi"],
    )
    out = gold_hourly_facts.compute_gold_hourly_facts(weather, aq)
    assert out.count() == 1
    assert out.first().time == datetime(2026, 8, 28, 14, 0, 0)


def test_gold_hourly_facts_surface_pressure_delta_lag(spark):
    weather = _weather_df(spark)
    aq = _aq_df(spark)
    out = gold_hourly_facts.compute_gold_hourly_facts(weather, aq)
    by_time = {r.time: r for r in out.orderBy("time").collect()}

    assert by_time[datetime(2026, 8, 28, 13, 0, 0)].surface_pressure_delta is None
    assert by_time[datetime(2026, 8, 28, 14, 0, 0)].surface_pressure_delta == 1012.0 - 1013.0
    assert by_time[datetime(2026, 8, 28, 15, 0, 0)].surface_pressure_delta == 1011.0 - 1012.0


def test_gold_hourly_facts_has_all_expected_columns(spark):
    weather = _weather_df(spark)
    aq = _aq_df(spark)
    out = gold_hourly_facts.compute_gold_hourly_facts(weather, aq)
    cols = set(out.columns)

    assert "time" in cols
    assert {"temperature_2m", "pm2_5", "european_aqi"} <= cols
    assert "surface_pressure_delta" in cols

    # The trapezoid parameter columns live in the thresholds dimension, not here.
    assert not any(
        c.endswith(
            ("_pos_slope_start", "_pos_slope_end", "_neg_slope_start",
             "_neg_slope_end", "_peak_start", "_peak_end")
        )
        for c in cols
    )


def test_gold_time_dim_derives_attributes(spark):
    facts = spark.createDataFrame(
        [
            (datetime(2026, 8, 28, 13, 0, 0),),
            (datetime(2026, 8, 28, 13, 0, 0),),  # duplicate -> should collapse
            (datetime(2026, 8, 28, 14, 0, 0),),
            (datetime(2026, 1, 1, 0, 0, 0),),
        ],
        ["time"],
    )
    out = gold_time_dim.compute_gold_time_dim(facts)

    assert out.count() == 3
    by_time = {r.time: r for r in out.collect()}

    r = by_time[datetime(2026, 8, 28, 13, 0, 0)]
    assert r.day == datetime(2026, 8, 28).date()
    assert r.month == 8
    assert r.year == 2026

    r = by_time[datetime(2026, 1, 1, 0, 0, 0)]
    assert r.day == datetime(2026, 1, 1).date()
    assert r.month == 1
    assert r.year == 2026
    assert r.week == 1


def test_gold_thresholds_dim_has_one_row(spark):
    out = gold_thresholds_dim.compute_gold_thresholds_dim(spark)

    assert out.count() == 1
    row = out.first()
    assert row.thresholds_id == "trapezoid"

    # wide schema: one column per {metric}_{param}
    expected_cols = {
        "thresholds_id",
        "apparent_temperature_pos_slope_start",
        "apparent_temperature_pos_slope_end",
        "apparent_temperature_neg_slope_start",
        "apparent_temperature_neg_slope_end",
        "apparent_temperature_peak_start",
        "apparent_temperature_peak_end",
        "surface_pressure_pos_slope_start",
        "surface_pressure_pos_slope_end",
        "surface_pressure_peak_start",
        "surface_pressure_peak_end",
        "surface_pressure_delta_pos_slope_start",
        "surface_pressure_delta_pos_slope_end",
        "surface_pressure_delta_peak_start",
        "surface_pressure_delta_peak_end",
        "european_aqi_neg_slope_start",
        "european_aqi_neg_slope_end",
        "european_aqi_peak_start",
        "european_aqi_peak_end",
        "cloud_cover_neg_slope_start",
        "cloud_cover_neg_slope_end",
        "cloud_cover_peak_start",
        "cloud_cover_peak_end",
        "relative_humidity_2m_pos_slope_start",
        "relative_humidity_2m_pos_slope_end",
        "relative_humidity_2m_neg_slope_start",
        "relative_humidity_2m_neg_slope_end",
        "relative_humidity_2m_peak_start",
        "relative_humidity_2m_peak_end",
        "wind_speed_10m_pos_slope_start",
        "wind_speed_10m_pos_slope_end",
        "wind_speed_10m_neg_slope_start",
        "wind_speed_10m_neg_slope_end",
        "wind_speed_10m_peak_start",
        "wind_speed_10m_peak_end",
    }
    assert expected_cols <= set(out.columns)
    # no metric column in wide schema
    assert "metric" not in out.columns


def test_gold_thresholds_dim_contains_trapezoid_parameters(spark):
    out = gold_thresholds_dim.compute_gold_thresholds_dim(spark)
    row = out.first()

    # apparent_temperature: all thresholds present
    assert row.apparent_temperature_pos_slope_start == 10
    assert row.apparent_temperature_pos_slope_end == 15
    assert row.apparent_temperature_peak_start == 15
    assert row.apparent_temperature_peak_end == 22
    assert row.apparent_temperature_neg_slope_start == 22
    assert row.apparent_temperature_neg_slope_end == 26

    # surface_pressure: no negative slope
    assert row.surface_pressure_pos_slope_start == 1010
    assert row.surface_pressure_pos_slope_end == 1020
    assert row.surface_pressure_peak_start == 1020
    assert row.surface_pressure_peak_end == 1030

    # surface_pressure_delta: no negative slope
    assert row.surface_pressure_delta_pos_slope_start == -5
    assert row.surface_pressure_delta_pos_slope_end == 3
    assert row.surface_pressure_delta_peak_start == 3
    assert row.surface_pressure_delta_peak_end == 5

    # european_aqi: no positive slope
    assert row.european_aqi_peak_start == 0
    assert row.european_aqi_peak_end == 25
    assert row.european_aqi_neg_slope_start == 25
    assert row.european_aqi_neg_slope_end == 100

    # cloud_cover: no positive slope
    assert row.cloud_cover_peak_start == 0
    assert row.cloud_cover_peak_end == 50
    assert row.cloud_cover_neg_slope_start == 50
    assert row.cloud_cover_neg_slope_end == 90

    # relative_humidity_2m: all thresholds present
    assert row.relative_humidity_2m_pos_slope_start == 30
    assert row.relative_humidity_2m_pos_slope_end == 40
    assert row.relative_humidity_2m_peak_start == 40
    assert row.relative_humidity_2m_peak_end == 60
    assert row.relative_humidity_2m_neg_slope_start == 60
    assert row.relative_humidity_2m_neg_slope_end == 80

    # wind_speed_10m: all thresholds present
    assert row.wind_speed_10m_pos_slope_start == 0
    assert row.wind_speed_10m_pos_slope_end == 12
    assert row.wind_speed_10m_peak_start == 12
    assert row.wind_speed_10m_peak_end == 19
    assert row.wind_speed_10m_neg_slope_start == 19
    assert row.wind_speed_10m_neg_slope_end == 39
