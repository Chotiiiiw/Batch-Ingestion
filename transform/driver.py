from pyspark.sql.functions import *

if __package__:
    from transform.common import (
        create_spark,
        optional_text,
        read_mysql_table,
    )
else:
    from common import (
        create_spark,
        optional_text,
        read_mysql_table,
    )


def transform_drivers(drivers):
    # 1. Clean and cast source columns
    cleaned_drivers = drivers \
        .withColumn(
            "driver_id",
            col("driver_id").cast("long"),
        ) \
        .withColumn(
            "driver_name",
            trim(col("driver_name")),
        ) \
        .withColumn(
            "number_plate",
            upper(optional_text("number_plate")),
        ) \
        .withColumn(
            "driver_status",
            upper(trim(col("driver_status"))),
        ) \
        .withColumn(
            "created_at",
            to_timestamp(col("created_at")),
        ) \
        .withColumn(
            "updated_at",
            to_timestamp(col("updated_at")),
        )


    # 2. Assign the first applicable rejection reason
    checked_drivers = cleaned_drivers.withColumn(
        "rejection_reason",
        when(
            col("driver_id").isNull()
            | (col("driver_id") <= 0),
            lit("INVALID_DRIVER_ID"),
        )
        .when(
            col("driver_name").isNull()
            | (length(col("driver_name")) == 0),
            lit("MISSING_DRIVER_NAME"),
        )
        .when(
            col("number_plate").isNull()
            | (length(col("number_plate")) == 0),
            lit("MISSING_NUMBER_PLATE"),
        )
        .when(
            ~col("driver_status").isin("ACTIVE", "INACTIVE"),
            lit("INVALID_DRIVER_STATUS"),
        )
        .when(
            col("created_at").isNull(),
            lit("INVALID_CREATED_AT"),
        )
        .when(
            col("updated_at").isNull(),
            lit("INVALID_UPDATED_AT"),
        )
        .when(
            col("updated_at") < col("created_at"),
            lit("UPDATED_BEFORE_CREATED"),
        ),
    )


    # 3. Separate valid and rejected rows
    rejected_drivers = checked_drivers \
        .filter(col("rejection_reason").isNotNull())

    valid_drivers = checked_drivers \
        .filter(col("rejection_reason").isNull()) \
        .dropDuplicates(["driver_id"])


    # 4. Calculate the SCD hash
    valid_drivers = valid_drivers.withColumn(
        "hash_diff",
        sha2(
            concat_ws(
                "||",
                col("driver_name"),
                col("number_plate"),
                col("driver_status"),
            ),
            256,
        ),
    )


    # 5. Prepare warehouse column names
    warehouse_drivers = valid_drivers.select(
        "driver_id",
        "driver_name",
        "number_plate",
        "driver_status",
        col("created_at").alias("valid_from"),
        "hash_diff",
        col("updated_at").alias("source_updated_at"),
    )


    return warehouse_drivers, rejected_drivers


def main():
    # Start session
    spark = create_spark("transform-drivers")


    # 1. Read drivers from MySQL
    drivers = read_mysql_table(spark, "drivers")


    # 2. Transform drivers
    warehouse_drivers, rejected_drivers = \
        transform_drivers(drivers)


    # 3. Check the results
    print("Raw drivers:", drivers.count())
    print("Valid drivers:", warehouse_drivers.count())
    print("Rejected drivers:", rejected_drivers.count())

    warehouse_drivers.show(10, truncate=False)
    rejected_drivers.select(
        "driver_id",
        "rejection_reason",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
