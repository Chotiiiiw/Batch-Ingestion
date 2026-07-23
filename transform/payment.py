from pyspark.sql.functions import *
from common import (
    bangkok_date_key,
    create_spark,
    optional_text,
    read_mysql_table,
)


def transform_payments(payments, orders):
    # 1. Clean and cast payment columns
    cleaned_payments = payments \
        .withColumn(
            "payment_id",
            col("payment_id").cast("long"),
        ) \
        .withColumn(
            "order_id",
            col("order_id").cast("long"),
        ) \
        .withColumn(
            "payment_method",
            upper(trim(col("payment_method"))),
        ) \
        .withColumn(
            "payment_status",
            upper(trim(col("payment_status"))),
        ) \
        .withColumn(
            "amount",
            col("amount").cast("decimal(12,2)"),
        ) \
        .withColumn(
            "transaction_ref",
            optional_text("transaction_ref"),
        ) \
        .withColumn(
            "payment_created_at",
            to_timestamp(col("created_at")),
        ) \
        .withColumn(
            "paid_at",
            to_timestamp(col("paid_at")),
        ) \
        .withColumn(
            "source_updated_at",
            to_timestamp(col("updated_at")),
        )


    # 2. Prepare the parent-order lookup
    order_lookup = orders.select(
        col("order_id")
        .cast("long")
        .alias("lookup_order_id"),
        col("customer_id")
        .cast("long")
        .alias("customer_id"),
        col("restaurant_id")
        .cast("long")
        .alias("restaurant_id"),
        col("subtotal")
        .cast("decimal(12,2)")
        .alias("order_subtotal"),
        col("discount")
        .cast("decimal(12,2)")
        .alias("order_discount"),
        col("delivery_fee")
        .cast("decimal(12,2)")
        .alias("order_delivery_fee"),
    )

    order_lookup = order_lookup.withColumn(
        "expected_payment_amount",
        col("order_subtotal")
        - col("order_discount")
        + col("order_delivery_fee"),
    )

    joined_payments = cleaned_payments.join(
        order_lookup,
        cleaned_payments.order_id
        == order_lookup.lookup_order_id,
        "left",
    )


    # 3. Assign the first applicable rejection reason
    checked_payments = joined_payments.withColumn(
        "rejection_reason",
        when(
            col("payment_id").isNull()
            | (col("payment_id") <= 0),
            lit("INVALID_PAYMENT_ID"),
        )
        .when(
            col("lookup_order_id").isNull(),
            lit("ORDER_NOT_FOUND"),
        )
        .when(
            ~col("payment_method").isin(
                "CARD",
                "PROMPTPAY",
                "CASH",
                "WALLET",
            ),
            lit("INVALID_PAYMENT_METHOD"),
        )
        .when(
            ~col("payment_status").isin(
                "PAID",
                "PENDING",
                "REFUNDED",
            ),
            lit("INVALID_PAYMENT_STATUS"),
        )
        .when(
            col("amount").isNull()
            | (col("amount") < 0),
            lit("INVALID_PAYMENT_AMOUNT"),
        )
        .when(
            col("expected_payment_amount").isNull(),
            lit("INVALID_ORDER_AMOUNT"),
        )
        .when(
            col("amount")
            != col("expected_payment_amount"),
            lit("PAYMENT_AMOUNT_MISMATCH"),
        )
        .when(
            (col("payment_method") != "CASH")
            & (
                col("transaction_ref").isNull()
                | (length(col("transaction_ref")) == 0)
            ),
            lit("MISSING_TRANSACTION_REFERENCE"),
        )
        .when(
            col("payment_created_at").isNull(),
            lit("INVALID_PAYMENT_CREATED_AT"),
        )
        .when(
            col("source_updated_at").isNull(),
            lit("INVALID_UPDATED_AT"),
        )
        .when(
            col("source_updated_at")
            < col("payment_created_at"),
            lit("UPDATED_BEFORE_CREATED"),
        )
        .when(
            (col("payment_status") == "PENDING")
            & col("paid_at").isNotNull(),
            lit("PENDING_PAYMENT_HAS_PAID_AT"),
        )
        .when(
            col("payment_status").isin(
                "PAID",
                "REFUNDED",
            )
            & col("paid_at").isNull(),
            lit("MISSING_PAID_AT"),
        )
        .when(
            col("paid_at").isNotNull()
            & (
                col("paid_at")
                < col("payment_created_at")
            ),
            lit("PAID_BEFORE_CREATED"),
        ),
    )


    # 4. Separate valid and rejected rows
    rejected_payments = checked_payments \
        .filter(col("rejection_reason").isNotNull())

    valid_payments = checked_payments \
        .filter(col("rejection_reason").isNull()) \
        .dropDuplicates(["payment_id"]) \
        .withColumn(
            "payment_created_date_key",
            bangkok_date_key("payment_created_at"),
        ) \
        .withColumn(
            "paid_date_key",
            when(
                col("paid_at").isNull(),
                lit(0),
            ).otherwise(
                bangkok_date_key("paid_at")
            ),
        )


    # 5. Prepare warehouse column names
    warehouse_payments = valid_payments.select(
        "payment_id",
        "order_id",
        "customer_id",
        "restaurant_id",
        "payment_method",
        "payment_created_date_key",
        "paid_date_key",
        "payment_status",
        "amount",
        "transaction_ref",
        "payment_created_at",
        "paid_at",
        "source_updated_at",
    )


    return warehouse_payments, rejected_payments


def main():
    # Start session
    spark = create_spark("transform-payments")


    # 1. Read payments and orders from MySQL
    payments = read_mysql_table(
        spark,
        "payments",
    )

    orders = read_mysql_table(
        spark,
        "orders",
    )


    # 2. Transform payments
    warehouse_payments, rejected_payments = \
        transform_payments(
            payments,
            orders,
        )


    # 3. Check the results
    print("Raw payments:", payments.count())
    print("Valid payments:", warehouse_payments.count())
    print("Rejected payments:", rejected_payments.count())

    warehouse_payments.show(10, truncate=False)
    rejected_payments.select(
        "payment_id",
        "order_id",
        "rejection_reason",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
