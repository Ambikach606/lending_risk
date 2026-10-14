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



# 1. Duplicate Resolution & Latest Record Wins

def test_duplicates_are_identified_and_latest_record_wins(
    spark,
    sample_rows
):
    detail, rejected, duplicates = transform_data(
        make_raw(spark, sample_rows)
    )

    assert detail.count() == 3, (
        "Expected 3 valid records after removing invalid rows "
        "and resolving duplicates."
    )

    assert rejected.count() == 2, (
        "Expected 2 rejected records."
    )

    assert duplicates.count() == 1, (
        "Expected 1 duplicate record."
    )

    assert (
        detail.filter("facility_id = 'F005'")
        .first()["actual_value"]
        == 3.5
    ), (
        "Latest version of F005 should be retained."
    )



# 2. Threshold Boundary Condition


def test_equal_to_threshold_is_breach(spark):
    rows = [
        ("F1", "SME", "PD", "2026-09-30",
         "3.0", "3.0", "2026-10-01T05:00:00Z")
    ]

    detail, _, _ = transform_data(make_raw(spark, rows))

    assert detail.first()["is_breach"] is True, (
        "Actual value equal to threshold must be treated as breach."
    )



# 3. Invalid Input Rejection


def test_invalid_actual_value_is_rejected(spark):
    rows = [
        ("F1", "SME", "PD", "2026-09-30",
         "not_available", "3.0", "2026-10-01T05:00:00Z")
    ]

    _, rejected, _ = transform_data(make_raw(spark, rows))

    assert rejected.count() == 1, (
        "Invalid numeric values should be rejected."
    )



# 4. Missing Mandatory Field


def test_missing_customer_segment_is_rejected(spark):
    rows = [
        ("F1", "", "PD", "2026-09-30",
         "3.1", "3.0", "2026-10-01T05:00:00Z")
    ]

    _, rejected, _ = transform_data(make_raw(spark, rows))

    assert rejected.count() == 1, (
        "Rows with missing customer segment must be rejected."
    )

    assert (
        "missing customer_segment"
        in rejected.first()["rejection_reason"].lower()
    ), (
        "Rejection reason should clearly explain the failure."
    )



# 5. Empty Input Dataset


def test_empty_input_returns_empty_outputs(spark):
    schema = """
        facility_id string,
        customer_segment string,
        metric string,
        period_end string,
        actual_value string,
        threshold_value string,
        source_updated_at string
    """

    empty = spark.createDataFrame([], schema)

    detail, rejected, duplicates = transform_data(empty)

    assert detail.count() == 0, (
        "Empty input should produce zero detail records."
    )

    assert rejected.count() == 0, (
        "Empty input should produce zero rejected records."
    )

    assert duplicates.count() == 0, (
        "Empty input should produce zero duplicate records."
    )

    assert make_summary(detail).count() == 0, (
        "Summary should be empty for empty input."
    )



# 6. All Records Invalid


def test_all_records_invalid_produces_only_rejections(spark):
    rows = [
        ("F1", "", "PD", "2026-09-30",
         "3.1", "3.0", "2026-10-01T05:00:00Z"),

        ("F2", "SME", "PD", "2026-09-30",
         "not_available", "3.0", "2026-10-01T05:01:00Z")
    ]

    detail, rejected, duplicates = transform_data(
        make_raw(spark, rows)
    )

    assert detail.count() == 0, (
        "No valid detail records should remain when every input row is invalid."
    )

    assert rejected.count() == 2, (
        "All invalid rows should be present in rejected output."
    )

    assert duplicates.count() == 0, (
        "Invalid rows should not create duplicate records."
    )

    assert make_summary(detail).count() == 0, (
        "Summary should be empty when no valid records exist."
    )
