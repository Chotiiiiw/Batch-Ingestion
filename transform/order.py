from pyspark.sql.functions import *

if __package__:
    from transform.common import bangkok_date_key, create_spark, read_mysql_table
else:
    from common import bangkok_date_key, create_spark, read_mysql_table


def transform_orders(orders):
    # 1. Clean, cast, and recalculate the order total
    cleaned_orders = orders \
        .withColumn(
            "order_id",
            col("order_id").cast("long"),
        ) \
        .withColumn(
            "customer_id",
            col("customer_id").cast("long"),
        ) \
        .withColumn(
            "restaurant_id",
            col("restaurant_id").cast("long"),
        ) \
        .withColumn(
            "delivery_address",
            trim(col("delivery_address")),
        ) \
        .withColumn(
            "order_status",
            upper(trim(col("order_status"))),
        ) \
        .withColumn(
            "subtotal",
            col("subtotal").cast("decimal(12,2)"),
        ) \
        .withColumn(
            "discount",
            col("discount").cast("decimal(12,2)"),
        ) \
        .withColumn(
            "delivery_fee",
            col("delivery_fee").cast("decimal(12,2)"),
        ) \
        .withColumn(
            "source_total_amount",
            col("total_amount").cast("decimal(12,2)"),
        ) \
        .withColumn(
            "total_amount",
            col("subtotal")
            - col("discount")
            + col("delivery_fee"),
        ) \
        .withColumn(
            "ordered_at",
            to_timestamp(col("ordered_at")),
        ) \
        .withColumn(
            "created_at",
            to_timestamp(col("created_at")),
        ) \
        .withColumn(
            "source_updated_at",
            to_timestamp(col("updated_at")),
        )


    # 2. Assign the first applicable rejection reason
    checked_orders = cleaned_orders.withColumn(
        "rejection_reason",
        when(
            col("order_id").isNull()
            | (col("order_id") <= 0),
            lit("INVALID_ORDER_ID"),
        )
        .when(
            col("customer_id").isNull()
            | (col("customer_id") <= 0),
            lit("INVALID_CUSTOMER_ID"),
        )
        .when(
            col("restaurant_id").isNull()
            | (col("restaurant_id") <= 0),
            lit("INVALID_RESTAURANT_ID"),
        )
        .when(
            col("delivery_address").isNull()
            | (length(col("delivery_address")) == 0),
            lit("MISSING_DELIVERY_ADDRESS"),
        )
        .when(
            ~col("order_status").isin(
                "DELIVERED",
                "CANCELLED",
                "PREPARING",
            ),
            lit("INVALID_ORDER_STATUS"),
        )
        .when(
            col("subtotal").isNull()
            | (col("subtotal") < 0),
            lit("INVALID_SUBTOTAL"),
        )
        .when(
            col("discount").isNull()
            | (col("discount") < 0),
            lit("INVALID_DISCOUNT"),
        )
        .when(
            col("delivery_fee").isNull()
            | (col("delivery_fee") < 0),
            lit("INVALID_DELIVERY_FEE"),
        )
        .when(
            col("total_amount").isNull()
            | (col("total_amount") < 0),
            lit("INVALID_TOTAL_AMOUNT"),
        )
        .when(
            col("ordered_at").isNull(),
            lit("INVALID_ORDERED_AT"),
        )
        .when(
            col("created_at").isNull(),
            lit("INVALID_CREATED_AT"),
        )
        .when(
            col("source_updated_at").isNull(),
            lit("INVALID_UPDATED_AT"),
        )
        .when(
            col("source_updated_at") < col("created_at"),
            lit("UPDATED_BEFORE_CREATED"),
        ),
    )


    # 3. Separate valid and rejected rows
    rejected_orders = checked_orders \
        .filter(col("rejection_reason").isNotNull())

    valid_orders = checked_orders \
        .filter(col("rejection_reason").isNull()) \
        .dropDuplicates(["order_id"]) \
        .withColumn(
            "order_date_key",
            bangkok_date_key("ordered_at"),
        ) \
        .withColumn(
            "total_was_corrected",
            col("source_total_amount")
            != col("total_amount"),
        )


    # 4. Prepare warehouse column names
    warehouse_orders = valid_orders.select(
        "order_id",
        "customer_id",
        "restaurant_id",
        "delivery_address",
        "order_date_key",
        "order_status",
        "subtotal",
        "discount",
        "delivery_fee",
        "total_amount",
        "ordered_at",
        "source_updated_at",
    )


    return warehouse_orders, rejected_orders


def main():
    # Start session
    spark = create_spark("transform-orders")


    # 1. Read orders from MySQL
    orders = read_mysql_table(spark, "orders")


    # 2. Transform orders
    warehouse_orders, rejected_orders = \
        transform_orders(orders)


    # 3. Check the results
    print("Raw orders:", orders.count())
    print("Valid orders:", warehouse_orders.count())
    print("Rejected orders:", rejected_orders.count())

    warehouse_orders.show(10, truncate=False)
    rejected_orders.select(
        "order_id",
        "rejection_reason",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
