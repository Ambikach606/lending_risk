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


def _validate_required_columns(df):
    """Validate mandatory input columns."""

    missing_columns = (
        set(REQUIRED_COLUMNS) - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )


def _trim_text_columns(df):
    """Trim whitespace from configured text columns."""

    for column in REQUIRED_TEXT_COLUMNS:
        df = df.withColumn(
            column,
            F.trim(F.col(column))
        )

    return df


def _parse_columns(df):
    """Parse date, numeric and timestamp fields."""

    return (
        df.withColumn(
            "period_end_parsed",
            F.to_date(
                "period_end",
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "actual_value_parsed",
            F.expr(
                "try_cast(actual_value AS DOUBLE)"
            )
        )
        .withColumn(
            "threshold_value_parsed",
            F.expr(
                "try_cast(threshold_value AS DOUBLE)"
            )
        )
        .withColumn(
            "source_updated_at_parsed",
            F.to_timestamp(
                "source_updated_at",
                "yyyy-MM-dd'T'HH:mm:ssX"
            )
        )
    )


def _is_valid_numeric(column_name):
    """Validate finite numeric values."""

    return (
        F.col(column_name).isNotNull()
        & (~F.isnan(column_name))
        & (F.abs(F.col(column_name)) != float("inf"))
    )


def _build_rejection_reason():
    """Construct rejection reason expression."""

    actual_ok = _is_valid_numeric(
        "actual_value_parsed"
    )

    threshold_ok = _is_valid_numeric(
        "threshold_value_parsed"
    )

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

    return (
        F.when(
            reason == "",
            F.lit(None).cast("string")
        )
        .otherwise(reason)
    )


def prepare_rows(raw_df):
    """
    Trim text, parse types,
    and attach rejection reasons.
    """

    _validate_required_columns(raw_df)

    df = _trim_text_columns(raw_df)

    df = _parse_columns(df)

    return df.withColumn(
        "rejection_reason",
        _build_rejection_reason()
    )


def _split_valid_and_rejected(prepared_df):
    """Split valid and rejected records."""

    rejected = prepared_df.filter(
        F.col("rejection_reason").isNotNull()
    )

    valid = (
        prepared_df
        .filter(
            F.col("rejection_reason").isNull()
        )
        .withColumnRenamed(
            "period_end_parsed",
            "period_end_typed"
        )
        .withColumnRenamed(
            "actual_value_parsed",
            "actual_value_typed"
        )
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


def _deduplicate_records(valid_df):
    """Retain latest record per business key."""

    window = (
        Window.partitionBy(
            "facility_id",
            "metric",
            "period_end_typed"
        )
        .orderBy(
            F.col(
                "source_updated_at_typed"
            ).desc()
        )
    )

    ranked = valid_df.withColumn(
        "duplicate_rank",
        F.row_number().over(window)
    )

    latest = ranked.filter(
        F.col("duplicate_rank") == 1
    )

    duplicates = ranked.filter(
        F.col("duplicate_rank") > 1
    )

    return latest, duplicates


def transform_data(raw_df):
    """
    Return:
        (detail_df, rejected_df, duplicate_df)

    Invalid rows are rejected before deduplication.
    Latest source_updated_at is kept per business key.
    """

    prepared = prepare_rows(raw_df)

    valid, rejected = (
        _split_valid_and_rejected(prepared)
    )

    latest, duplicates = (
        _deduplicate_records(valid)
    )

    detail = (
        latest.select(
            "facility_id",
            "customer_segment",
            "metric",
            F.col(
                "period_end_typed"
            ).alias(
                "period_end"
            ),
            F.col(
                "actual_value_typed"
            ).alias(
                "actual_value"
            ),
            F.col(
                "threshold_value_typed"
            ).alias(
                "threshold_value"
            ),
            F.col(
                "source_updated_at_typed"
            ).alias(
                "source_updated_at"
            ),
        )
        .withColumn(
            "is_breach",
            F.coalesce(
                F.col("actual_value")
                >= F.col("threshold_value"),
                F.lit(False)
            )
        )
    )

    rejected_output = rejected.select(
        *raw_df.columns,
        "rejection_reason",
    )

    duplicate_output = duplicates.select(
        *raw_df.columns,
        F.lit(
            "filtered by a newer source_updated_at for the same business key"
        ).alias(
            "duplicate_reason"
        ),
    )

    return (
        detail,
        rejected_output,
        duplicate_output,
    )


def make_summary(detail_df):
    """Aggregate report-ready detail data."""

    return (
        detail_df.groupBy(
            "period_end",
            "customer_segment",
            "metric"
        )
        .agg(
            F.count(
                "facility_id"
            ).alias(
                "facility_count"
            ),
            F.sum(
                F.col(
                    "is_breach"
                ).cast("long")
            ).alias(
                "breached_facility_count"
            ),
            F.round(
                F.avg(
                    F.col(
                        "is_breach"
                    ).cast("double")
                ),
                4
            ).alias(
                "breach_rate"
            ),
            F.round(
                F.avg(
                    "actual_value"
                ),
                2
            ).alias(
                "average_actual_value"
            ),
        )
        .orderBy(
            "period_end",
            "customer_segment",
            "metric"
        )
    )
