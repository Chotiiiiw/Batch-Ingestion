from pyspark.sql.functions import *
from common import (
    bangkok_date_key,
    create_spark,
    read_mysql_table,
)


def transform_deliveries(deliveries, orders):
    # 1. Clean and cast delivery columns
    cleaned_deliveries = deliveries \
        .withColumn(
            "delivery_id",
            col("delivery_id").cast("long"),
        ) \
        .withColumn(
            "order_id",
            col("order_id").cast("long"),
        ) \
        .withColumn(
            "driver_id",
            col("driver_id").cast("long"),
        ) \
        .withColumn(
            "delivery_status",
            upper(trim(col("delivery_status"))),
        ) \
        .withColumn(
            "distance_km",
            col("distance_km").cast("decimal(8,2)"),
        ) \
        .withColumn(
            "assigned_at",
            to_timestamp(col("assigned_at")),
        ) \
        .withColumn(
            "picked_up_at",
            to_timestamp(col("picked_up_at")),
        ) \
        .withColumn(
            "delivered_at",
            to_timestamp(col("delivered_at")),
        ) \
        .withColumn(
            "source_updated_at",
            to_timestamp(col("updated_at")),
        )


    # 2. Prepare the parent-order lookup
    order_lookup = orders.select(
        col("order_id")
        .cast("long")
        .alias("order_id"),
        col("customer_id")
        .cast("long")
        .alias("customer_id"),
        col("restaurant_id")
        .cast("long")
        .alias("restaurant_id"),
    )


    # 3. Join deliveries with their parent orders
    joined_deliveries = cleaned_deliveries.join(
        order_lookup,
        on="order_id",
        how="left",
    )


    # 4. Assign the first applicable rejection reason
    checked_deliveries = joined_deliveries.withColumn(
        "rejection_reason",
        when(
            col("delivery_id").isNull()
            | (col("delivery_id") <= 0),
            lit("INVALID_DELIVERY_ID"),
        )
        .when(
            col("order_id").isNull()
            | col("customer_id").isNull()
            | col("restaurant_id").isNull(),
            lit("ORDER_NOT_FOUND"),
        )
        .when(
            col("driver_id").isNull()
            | (col("driver_id") <= 0),
            lit("INVALID_DRIVER_ID"),
        )
        .when(
            ~col("delivery_status").isin(
                "ASSIGNED",
                "DELIVERED",
            ),
            lit("INVALID_DELIVERY_STATUS"),
        )
        .when(
            col("distance_km").isNull()
            | (col("distance_km") <= 0)
            | (col("distance_km") > 200),
            lit("INVALID_DISTANCE"),
        )
        .when(
            col("assigned_at").isNull(),
            lit("INVALID_ASSIGNED_AT"),
        )
        .when(
            col("source_updated_at").isNull(),
            lit("INVALID_UPDATED_AT"),
        )
        .when(
            col("picked_up_at").isNotNull()
            & (
                col("picked_up_at")
                < col("assigned_at")
            ),
            lit("PICKED_UP_BEFORE_ASSIGNED"),
        )
        .when(
            col("delivered_at").isNotNull()
            & col("picked_up_at").isNull(),
            lit("DELIVERED_WITHOUT_PICKUP"),
        )
        .when(
            col("delivered_at").isNotNull()
            & (
                col("delivered_at")
                < col("picked_up_at")
            ),
            lit("DELIVERED_BEFORE_PICKUP"),
        )
        .when(
            (col("delivery_status") == "DELIVERED")
            & col("delivered_at").isNull(),
            lit("MISSING_DELIVERED_AT"),
        ),
    )


    # 5. Separate valid and rejected rows
    rejected_deliveries = checked_deliveries \
        .filter(col("rejection_reason").isNotNull())

    valid_deliveries = checked_deliveries \
        .filter(col("rejection_reason").isNull()) \
        .dropDuplicates(["delivery_id"]) \
        .withColumn(
            "assigned_date_key",
            bangkok_date_key("assigned_at"),
        )


    # 6. Prepare warehouse column names
    warehouse_deliveries = valid_deliveries.select(
        "delivery_id",
        "order_id",
        "customer_id",
        "restaurant_id",
        "driver_id",
        "assigned_date_key",
        "delivery_status",
        "distance_km",
        "assigned_at",
        "picked_up_at",
        "delivered_at",
        "source_updated_at",
    )


    return warehouse_deliveries, rejected_deliveries


def main():
    # Start session
    spark = create_spark("transform-deliveries")


    # 1. Read deliveries and orders from MySQL
    deliveries = read_mysql_table(
        spark,
        "deliveries",
    )

    orders = read_mysql_table(
        spark,
        "orders",
    )


    # 2. Transform deliveries
    warehouse_deliveries, rejected_deliveries = \
        transform_deliveries(
            deliveries,
            orders,
        )


    # 3. Check the results
    print("Raw deliveries:", deliveries.count())
    print("Valid deliveries:", warehouse_deliveries.count())
    print("Rejected deliveries:", rejected_deliveries.count())

    warehouse_deliveries.show(10, truncate=False)
    rejected_deliveries.select(
        "delivery_id",
        "order_id",
        "rejection_reason",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
