import os
import sys
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.transformations import transform_data, make_summary


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("LendingRiskTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_raw(spark, rows):
    columns = [
        "facility_id", "customer_segment", "metric", "period_end",
        "actual_value", "threshold_value", "source_updated_at",
    ]
    return spark.createDataFrame(rows, columns)


@pytest.fixture
def sample_rows():
    return [
        ("F001", "SME", "PD", "2026-09-30", "3.2", "3.0", "2026-10-01T05:00:00Z"),
        ("F002", "Corporate", "PD", "2026-09-30", "2.7", "3.0", "2026-10-01T05:05:00Z"),
        ("F003", "SME", "PD", "2026-09-30", "not_available", "3.0", "2026-10-01T05:10:00Z"),
        ("F004", "", "PD", "2026-09-30", "3.1", "3.0", "2026-10-01T05:15:00Z"),
        ("F005", "Corporate", "PD", "2026-09-30", "3.1", "3.0", "2026-10-01T05:20:00Z"),
        ("F005", "Corporate", "PD", "2026-09-30", "3.5", "3.0", "2026-10-01T06:20:00Z"),
    ]


def test_sample_rejections_duplicates_and_latest_wins(spark, sample_rows):
    detail, rejected, duplicates = transform_data(make_raw(spark, sample_rows))
    assert detail.count() == 3
    assert rejected.count() == 2
    assert duplicates.count() == 1
    assert detail.filter("facility_id = 'F005'").first()["actual_value"] == 3.5


def test_equal_to_threshold_is_breach(spark):
    rows = [("F1", "SME", "PD", "2026-09-30", "3.0", "3.0", "2026-10-01T05:00:00Z")]
    detail, _, _ = transform_data(make_raw(spark, rows))
    assert detail.first()["is_breach"] is True


def test_rejection_reason_is_useful(spark):
    rows = [("F1", "", "PD", "2026-09-30", "3.1", "3.0", "2026-10-01T05:00:00Z")]
    _, rejected, _ = transform_data(make_raw(spark, rows))
    assert "missing customer_segment" in rejected.first()["rejection_reason"]


def test_empty_input_returns_empty_outputs(spark):
    schema = """
        facility_id string, customer_segment string, metric string,
        period_end string, actual_value string, threshold_value string,
        source_updated_at string
    """
    empty = spark.createDataFrame([], schema)
    detail, rejected, duplicates = transform_data(empty)
    assert detail.count() == 0
    assert rejected.count() == 0
    assert duplicates.count() == 0
    assert make_summary(detail).count() == 0


def test_summary_counts_and_rate(spark):
    rows = [
        ("F1", "SME", "PD", "2026-09-30", "3.0", "3.0", "2026-10-01T05:00:00Z"),
        ("F2", "SME", "PD", "2026-09-30", "2.0", "3.0", "2026-10-01T05:01:00Z"),
    ]
    detail, _, _ = transform_data(make_raw(spark, rows))
    summary = make_summary(detail).first()
    assert summary["facility_count"] == 2
    assert summary["breached_facility_count"] == 1
    assert summary["breach_rate"] == 0.5
