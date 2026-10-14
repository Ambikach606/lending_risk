
from pyspark.sql import functions as F
from pyspark.sql.window import Window

REQUIRED_TEXT_COLUMNS = [
    "facility_id",
    "customer_segment",
    "metric",
]

REQUIRED_COLUMNS = [
    "facility_id",
    "customer_segment",
    "metric",
    "period_end",
    "actual_value",
    "threshold_value",
    "source_updated_at",
]

def _validate_required_columns(df, required_columns=None):
    """Validate the DataFrame and mandatory input columns."""
    if df is None:
        raise ValueError("Input DataFrame cannot be None.")

    if required_columns is None:
        required_columns = REQUIRED_COLUMNS

    try:
        available_columns = df.columns
    except Exception as exc:
        raise RuntimeError(
            "Unable to read input DataFrame columns."
        ) from exc

    missing_columns = set(required_columns) - set(available_columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

def _trim_text_columns(df):
    """Trim whitespace from configured text columns."""
    try:
        for column in REQUIRED_TEXT_COLUMNS:
            df = df.withColumn(column, F.trim(F.col(column)))
        return df
    except Exception as exc:
        raise RuntimeError("Failed to trim text columns.") from exc


def _parse_columns(df):
    """Parse date, numeric and timestamp fields."""
    try:
        return (
            df.withColumn(
                "period_end_parsed",
                F.to_date(F.col("period_end"), "yyyy-MM-dd")
            )
            .withColumn(
                "actual_value_parsed",
                F.expr("try_cast(actual_value AS DOUBLE)")
            )
            .withColumn(
                "threshold_value_parsed",
                F.expr("try_cast(threshold_value AS DOUBLE)")
            )
            .withColumn(
                "source_updated_at_parsed",
                F.to_timestamp(
                    F.col("source_updated_at"),
                    "yyyy-MM-dd'T'HH:mm:ssX"
                )
            )
        )
    except Exception as exc:
        raise RuntimeError(
            "Failed to parse date, numeric or timestamp columns."
        ) from exc


def _is_valid_numeric(column_name):
    """Return a condition for finite, non-null numeric values."""
    column = F.col(column_name)
    return (
        column.isNotNull()
        & (~F.isnan(column))
        & (F.abs(column) != F.lit(float("inf")))
    )

def _build_rejection_reason():
    """Construct descriptive rejection reasons."""
    actual_ok = _is_valid_numeric("actual_value_parsed")
    threshold_ok = _is_valid_numeric("threshold_value_parsed")

    reason = F.concat_ws(
        "; ",
        F.when(
            F.col("facility_id").isNull()
            | (F.col("facility_id") == ""),
            "missing facility_id"
        ),
        F.when(
            F.col("customer_segment").isNull()
            | (F.col("customer_segment") == ""),
            "missing customer_segment"
        ),
        F.when(
            F.col("metric").isNull()
            | (F.col("metric") == ""),
            "missing metric"
        ),
        F.when(
            F.col("period_end_parsed").isNull(),
            "invalid period_end (expected YYYY-MM-DD)"
        ),
        F.when(
            ~actual_ok,
            "invalid actual_value (must be a finite number)"
        ),
        F.when(
            ~threshold_ok,
            "invalid threshold_value (must be a finite number)"
        ),
        F.when(
            F.col("source_updated_at_parsed").isNull(),
            "invalid source_updated_at (expected ISO-8601 timestamp)"
        )
    )

    return F.when(
        reason == "",
        F.lit(None).cast("string")
    ).otherwise(reason)


def prepare_rows(raw_df):
    """Trim text, parse types and attach rejection reasons."""
    _validate_required_columns(raw_df)

    try:
        df = _trim_text_columns(raw_df)
        df = _parse_columns(df)
        return df.withColumn(
            "rejection_reason",
            _build_rejection_reason()
        )
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError("Failed to prepare input rows.") from exc


def _split_valid_and_rejected(prepared_df):
    """Split valid and rejected records."""
    try:
        rejected = prepared_df.filter(
            F.col("rejection_reason").isNotNull()
        )

        valid = (
            prepared_df
            .filter(F.col("rejection_reason").isNull())
            .withColumnRenamed("period_end_parsed", "period_end_typed")
            .withColumnRenamed("actual_value_parsed", "actual_value_typed")
            .withColumnRenamed(
                "threshold_value_parsed",
                "threshold_value_typed"
            )
            .withColumnRenamed(
                "source_updated_at_parsed",
                "source_updated_at_typed"
            )
        )

        return valid, rejected
    except Exception as exc:
        raise RuntimeError(
            "Failed to split valid and rejected records."
        ) from exc


def _deduplicate_records(valid_df):
    """Keep the latest record for each business key."""
    try:
        window = (
            Window.partitionBy(
                "facility_id",
                "metric",
                "period_end_typed"
            )
            .orderBy(F.col("source_updated_at_typed").desc())
        )

        ranked = valid_df.withColumn(
            "duplicate_rank",
            F.row_number().over(window)
        )

        latest = ranked.filter(F.col("duplicate_rank") == 1)
        duplicates = ranked.filter(F.col("duplicate_rank") > 1)

        return latest, duplicates
    except Exception as exc:
        raise RuntimeError("Failed to deduplicate records.") from exc

def transform_data(raw_df):
    """
    Return detail_df, rejected_df and duplicate_df.

    Invalid rows are rejected before deduplication.
    The latest source_updated_at is retained per business key.
    """
    _validate_required_columns(raw_df)

    try:
        prepared = prepare_rows(raw_df)
        valid, rejected = _split_valid_and_rejected(prepared)
        latest, duplicates = _deduplicate_records(valid)

        detail = (
            latest.select(
                "facility_id",
                "customer_segment",
                "metric",
                F.col("period_end_typed").alias("period_end"),
                F.col("actual_value_typed").alias("actual_value"),
                F.col("threshold_value_typed").alias("threshold_value"),
                F.col("source_updated_at_typed").alias("source_updated_at"),
            )
            .withColumn(
                "is_breach",
                F.coalesce(
                    F.col("actual_value") >= F.col("threshold_value"),
                    F.lit(False)
                )
            )
        )

        rejected_output = rejected.select(
            *raw_df.columns,
            "rejection_reason"
        )

        duplicate_output = duplicates.select(
            *raw_df.columns,
            F.lit(
                "filtered by a newer source_updated_at "
                "for the same business key"
            ).alias("duplicate_reason")
        )

        return detail, rejected_output, duplicate_output

    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError("Failed to transform input data.") from exc


def make_summary(detail_df):
    """Aggregate report-ready detail data."""

    try:
        return (
            detail_df.groupBy(
                "period_end",
                "customer_segment",
                "metric"
            )
            .agg(
                F.count("facility_id").alias("facility_count"),
                F.sum(
                    F.col("is_breach").cast("long")
                ).alias("breached_facility_count"),
                F.round(
                    F.avg(F.col("is_breach").cast("double")),
                    4
                ).alias("breach_rate"),
                F.round(
                    F.avg("actual_value"),
                    2
                ).alias("average_actual_value"),
            )
            .orderBy(
                "period_end",
                "customer_segment",
                "metric"
            )
        )
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(
            "Failed to generate summary aggregation."
        ) from exc
