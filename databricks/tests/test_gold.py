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


def test_gold_thresholds_dim_has_one_row_per_metric(spark):
    out = gold_thresholds_dim.compute_gold_thresholds_dim(spark)

    metrics = [r.metric for r in out.orderBy("metric").collect()]
    assert metrics == [
        "apparent_temperature",
        "cloud_cover",
        "european_aqi",
        "relative_humidity_2m",
        "surface_pressure",
        "surface_pressure_delta",
        "wind_speed_10m",
    ]


def test_gold_thresholds_dim_contains_trapezoid_parameters(spark):
    out = gold_thresholds_dim.compute_gold_thresholds_dim(spark)
    by_metric = {r.metric: r for r in out.collect()}

    temp = by_metric["apparent_temperature"]
    assert temp.pos_slope_start == 10
    assert temp.pos_slope_end == 15
    assert temp.peak_start == 15
    assert temp.peak_end == 22
    assert temp.neg_slope_start == 22
    assert temp.neg_slope_end == 26

    pres = by_metric["surface_pressure"]
    assert pres.pos_slope_start == 1010
    assert pres.pos_slope_end == 1020
    assert pres.peak_start == 1020
    assert pres.peak_end == 1030
    assert pres.neg_slope_start is None
    assert pres.neg_slope_end is None

    aqi = by_metric["european_aqi"]
    assert aqi.pos_slope_start is None
    assert aqi.pos_slope_end is None
    assert aqi.peak_start == 0
    assert aqi.peak_end == 25
    assert aqi.neg_slope_start == 25
    assert aqi.neg_slope_end == 100
