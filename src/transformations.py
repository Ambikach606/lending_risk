"""Simple PySpark transformations for the lending risk exercise."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


REQUIRED_TEXT_COLUMNS = ["facility_id", "customer_segment", "metric"]

def prepare_rows(raw_df):
    """Trim text, parse types, and add rejection reasons without dropping rows."""

    df = raw_df

    # Trim whitespace from required text columns.
    for column in REQUIRED_TEXT_COLUMNS:
        df = df.withColumn(column, F.trim(F.col(column)))

    # Parse dates, numbers, and timestamps safely.
    # try_cast returns NULL for malformed numeric values.
    df = (
        df.withColumn(
            "period_end_parsed",
            F.to_date("period_end", "yyyy-MM-dd")
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
                "source_updated_at",
                "yyyy-MM-dd'T'HH:mm:ssX"
            )
        )
    )

    # Reject NULL, NaN, and infinite numeric values.
    actual_ok = (
        F.col("actual_value_parsed").isNotNull()
        & (~F.isnan("actual_value_parsed"))
        & (F.abs(F.col("actual_value_parsed")) != float("inf"))
    )

    threshold_ok = (
        F.col("threshold_value_parsed").isNotNull()
        & (~F.isnan("threshold_value_parsed"))
        & (F.abs(F.col("threshold_value_parsed")) != float("inf"))
    )

    # Build all applicable rejection reasons.
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
        ),
    )

    # Keep every row and attach its rejection reason, if any.
    return df.withColumn(
        "rejection_reason",
        F.when(reason == "", F.lit(None).cast("string"))
         .otherwise(reason)
    )


def transform_data(raw_df):
    """
    Return (detail_df, rejected_df, duplicate_df).

    Invalid rows are rejected before deduplication.
    Invalid numeric values such as 'not_available' are safely handled
    using try_cast and recorded in rejection_reason.
    The latest source_updated_at is kept for each business key.
    """

    prepared = prepare_rows(raw_df)

    # Safely convert numeric columns without crashing on invalid strings.
    prepared = (
        prepared
        .withColumn(
            "actual_value_parsed",
            F.expr("try_cast(actual_value AS DOUBLE)")
        )
        .withColumn(
            "threshold_value_parsed",
            F.expr("try_cast(threshold_value AS DOUBLE)")
        )
    )

    # Add numeric validation to any existing rejection reasons.
    invalid_numeric = (
        F.col("actual_value_parsed").isNull()
        | F.col("threshold_value_parsed").isNull()
        | F.isnan("actual_value_parsed")
        | F.isnan("threshold_value_parsed")
        | (F.abs(F.col("actual_value_parsed")) == float("inf"))
        | (F.abs(F.col("threshold_value_parsed")) == float("inf"))
    )

    numeric_reason = (
        F.when(
            invalid_numeric,
            F.lit("invalid or non-finite numeric value")
        )
    )

    prepared = prepared.withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_reason").isNotNull() & numeric_reason.isNotNull(),
            F.concat_ws("; ", F.col("rejection_reason"), numeric_reason)
        )
        .when(
            F.col("rejection_reason").isNull() & numeric_reason.isNotNull(),
            numeric_reason
        )
        .otherwise(F.col("rejection_reason"))
    )

    # Keep all rejected records and their rejection reasons.
    rejected = prepared.filter(F.col("rejection_reason").isNotNull())

    # Continue only with valid records.
    valid = (
        prepared
        .filter(F.col("rejection_reason").isNull())
        .withColumnRenamed("period_end_parsed", "period_end_typed")
        .withColumnRenamed("actual_value_parsed", "actual_value_typed")
        .withColumnRenamed("threshold_value_parsed", "threshold_value_typed")
        .withColumnRenamed("source_updated_at_parsed", "source_updated_at_typed")
    )

    # Deduplicate using the business key, keeping the latest update.
    window = Window.partitionBy(
        "facility_id", "metric", "period_end_typed"
    ).orderBy(
        F.col("source_updated_at_typed").desc()
    )

    ranked = valid.withColumn(
        "duplicate_rank",
        F.row_number().over(window)
    )

    duplicates = ranked.filter(F.col("duplicate_rank") > 1)
    latest = ranked.filter(F.col("duplicate_rank") == 1)

    # Build the final detail output.
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
        # .withColumn(
        #     "is_breach",
        #     F.col("actual_value") >= F.col("threshold_value")
        # )
        .withColumn(
        "is_breach",
        # Use coalesce to treat missing data as non-breaches, or leave as null if preferred
        F.coalesce(F.col("actual_value") >= F.col("threshold_value"), F.lit(False))
        )
    )

    # Preserve original input columns in rejected output.
    rejected_output = rejected.select(
        *raw_df.columns,
        "rejection_reason",
    )

    # Preserve original input columns in superseded duplicate output.
    duplicate_output = duplicates.select(
        *raw_df.columns,
        F.lit(
            "filtered by a newer source_updated_at for the same business key"
        ).alias("duplicate_reason"),
    )

    return detail, rejected_output, duplicate_output


# def make_summary(detail_df):
#     """Aggregate the report-ready detail data."""
#     return (
#         detail_df.groupBy("period_end", "customer_segment", "metric")
#         .agg(
#             F.count("*").alias("facility_count"),
#             F.sum(F.col("is_breach").cast("long")).alias("breached_facility_count"),
#             F.avg(F.col("is_breach").cast("double")).alias("breach_rate"),
#             F.avg("actual_value").alias("average_actual_value"),
#         )
#         .orderBy("period_end", "customer_segment", "metric")
#     )

def make_summary(detail_df):
    """Aggregate the report-ready detail data."""
    return (
        detail_df.groupBy("period_end", "customer_segment", "metric")
        .agg(
            F.count("facility_id").alias("facility_count"),
            F.sum(F.col("is_breach").cast("long")).alias("breached_facility_count"),
            # Rounding ensures clean presentations in business intelligence tools
            F.round(F.avg(F.col("is_breach").cast("double")), 4).alias("breach_rate"),
            F.round(F.avg("actual_value"), 2).alias("average_actual_value"),
        )
        .orderBy("period_end", "customer_segment", "metric")
    )