from datetime import date

from pyspark.sql import functions as F

from spark_apps.silver.common.bronze_reader import (
    read_bronze_topic,
)
from spark_apps.silver.config.iceberg import (
    build_iceberg_spark,
)
from spark_apps.silver.config.tables import (
    DIM_DATE,
    TOPIC_ORDERS,
)
from spark_apps.silver.dimensions.dim_date import (
    build_dim_date,
)


DEFAULT_FUTURE_END_DATE = date(
    2030,
    12,
    31,
)


def main() -> None:

    spark = build_iceberg_spark(
        "silver-load-dim-date"
    )

    try:

        print("=" * 100)
        print("BUILDING DIM_DATE")
        print("=" * 100)

        # -----------------------------------------------------
        # Read order date boundaries dynamically
        # -----------------------------------------------------

        orders_df = read_bronze_topic(
            spark,
            TOPIC_ORDERS,
        )

        bounds = (
            orders_df
            .select(
                F.min(
                    F.to_date(
                        F.col("timestamp")
                    )
                ).alias(
                    "min_order_date"
                ),

                F.max(
                    F.to_date(
                        F.col("timestamp")
                    )
                ).alias(
                    "max_order_date"
                ),
            )
            .first()
        )

        min_order_date = (
            bounds[
                "min_order_date"
            ]
        )

        max_order_date = (
            bounds[
                "max_order_date"
            ]
        )

        if (
            min_order_date is None
            or max_order_date is None
        ):
            raise RuntimeError(
                "Cannot determine "
                "order date range."
            )

        # Keep future dates available
        # while dynamically supporting
        # all historical order dates.

        end_date = max(
            max_order_date,
            DEFAULT_FUTURE_END_DATE,
        )

        start_date_str = (
            min_order_date.isoformat()
        )

        end_date_str = (
            end_date.isoformat()
        )

        print(
            f"Minimum order date: "
            f"{start_date_str}"
        )

        print(
            f"Maximum order date: "
            f"{max_order_date}"
        )

        print(
            f"DIM_DATE range: "
            f"{start_date_str} "
            f"to "
            f"{end_date_str}"
        )

        # -----------------------------------------------------
        # Build complete date dimension
        # -----------------------------------------------------

        dim_date_df = build_dim_date(
            spark=spark,
            start_date=start_date_str,
            end_date=end_date_str,
        ).cache()

        generated_count = (
            dim_date_df.count()
        )

        print(
            f"Generated date rows: "
            f"{generated_count:,}"
        )

        # -----------------------------------------------------
        # Ensure Iceberg table exists
        # -----------------------------------------------------

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS
            {DIM_DATE}
            (
                date_sk INT,
                full_date DATE,
                year INT,
                quarter INT,
                month INT,
                month_name STRING,
                week_of_year INT,
                day INT,
                day_of_week INT,
                day_name STRING,
                is_weekend BOOLEAN
            )
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2'
            )
            """
        )

        # -----------------------------------------------------
        # Idempotent MERGE
        #
        # Existing dates remain untouched.
        # Missing historical dates are inserted.
        # -----------------------------------------------------

        dim_date_df.createOrReplaceTempView(
            "staged_dim_date"
        )

        spark.sql(
            f"""
            MERGE INTO
                {DIM_DATE} AS target

            USING
                staged_dim_date AS source

            ON
                target.date_sk =
                source.date_sk

            WHEN NOT MATCHED THEN

                INSERT (
                    date_sk,
                    full_date,
                    year,
                    quarter,
                    month,
                    month_name,
                    week_of_year,
                    day,
                    day_of_week,
                    day_name,
                    is_weekend
                )

                VALUES (
                    source.date_sk,
                    source.full_date,
                    source.year,
                    source.quarter,
                    source.month,
                    source.month_name,
                    source.week_of_year,
                    source.day,
                    source.day_of_week,
                    source.day_name,
                    source.is_weekend
                )
            """
        )

        print(
            "[PASS] DIM_DATE MERGE completed."
        )

        # -----------------------------------------------------
        # Final audit
        # -----------------------------------------------------

        final_df = spark.table(
            DIM_DATE
        )

        final_count = (
            final_df.count()
        )

        final_bounds = (
            final_df
            .select(
                F.min(
                    "full_date"
                ).alias(
                    "min_date"
                ),
                F.max(
                    "full_date"
                ).alias(
                    "max_date"
                ),
            )
            .first()
        )

        print()
        print("DIM_DATE AUDIT")
        print("-" * 100)

        print(
            f"Total rows: "
            f"{final_count:,}"
        )

        print(
            f"Minimum date: "
            f"{final_bounds['min_date']}"
        )

        print(
            f"Maximum date: "
            f"{final_bounds['max_date']}"
        )

        print()
        print(
            "[PASS] DIM_DATE LOAD COMPLETED"
        )

        dim_date_df.unpersist()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
