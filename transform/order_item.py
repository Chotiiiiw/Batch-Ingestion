from pyspark.sql.functions import *
from common import bangkok_date_key, create_spark

#Create SparkSession
spark = create_spark("transform-order-items")


# 1. Read source and lookup data.
order_items = spark.read\
    .option("header", True)\
    .csv("mock-data/output/order_items.csv")


orders = spark.read\
    .option("header", True)\
    .csv("mock-data/output/orders.csv")

menu_items = spark.read.json("mock-data/output/menu_items.jsonl")


# 2. Clean and cast order-item fields.
cleaned_order_items = order_items\
    .withColumn("order_item_id", col("order_item_id").cast("long"))\
    .withColumn("order_id", col("order_id").cast("long"))\
    .withColumn("menu_item_id", trim(col("menu_item_id")))\
    .withColumn("menu_item_name", trim(col("menu_item_name")))\
    .withColumn("quantity", col("quantity").cast("integer"))\
    .withColumn("unit_price", col("unit_price").cast("decimal(12,2)"))\
    .withColumn(
        "source_total_price",
        col("total_price").cast("decimal(12,2)"),
    )\
    .withColumn(
        "total_price",
        col("quantity") * col("unit_price"),
    )\
    .withColumn("created_at", to_timestamp(col("created_at")))\
    .withColumn(
        "source_updated_at",
        to_timestamp(col("updated_at")),
    )


# 3. Prepare the order and menu lookups required by the fact.
order_lookup = orders.select(
    col("order_id").cast("long").alias("lookup_order_id"),
    col("customer_id").cast("long").alias("customer_id"),
    col("restaurant_id").cast("long").alias("restaurant_id"),
    to_timestamp(col("ordered_at")).alias("ordered_at"),
)

menu_lookup = menu_items\
    .select(
        trim(col("_id")).alias("lookup_menu_item_id"),
        col("restaurantId").cast("long").alias("menu_restaurant_id"),
    )\
    .dropDuplicates(["lookup_menu_item_id"])


# 4. Join lookup data without joining one warehouse fact to another.
joined_order_items = cleaned_order_items\
    .join(
        order_lookup,
        cleaned_order_items.order_id == order_lookup.lookup_order_id,
        "left",
    )\
    .join(
        menu_lookup,
        cleaned_order_items.menu_item_id
        == menu_lookup.lookup_menu_item_id,
        "left",
    )


# 5. Assign the first applicable rejection reason.
checked_order_items = joined_order_items.withColumn(
    "rejection_reason",
    when(
        col("order_item_id").isNull() | (col("order_item_id") <= 0),
        lit("INVALID_ORDER_ITEM_ID"),
    )
    .when(
        col("lookup_order_id").isNull(),
        lit("ORDER_NOT_FOUND"),
    )
    .when(
        col("menu_item_id").isNull() | (length(col("menu_item_id")) == 0),
        lit("INVALID_MENU_ITEM_ID"),
    )
    .when(
        col("lookup_menu_item_id").isNull(),
        lit("MENU_ITEM_NOT_FOUND"),
    )
    .when(
        col("restaurant_id") != col("menu_restaurant_id"),
        lit("MENU_RESTAURANT_MISMATCH"),
    )
    .when(
        col("menu_item_name").isNull()
        | (length(col("menu_item_name")) == 0),
        lit("MISSING_MENU_ITEM_NAME"),
    )
    .when(
        col("quantity").isNull() | (col("quantity") <= 0),
        lit("INVALID_QUANTITY"),
    )
    .when(
        col("unit_price").isNull() | (col("unit_price") < 0),
        lit("INVALID_UNIT_PRICE"),
    )
    .when(
        col("total_price").isNull() | (col("total_price") < 0),
        lit("INVALID_TOTAL_PRICE"),
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


# 6. Separate valid and rejected rows.
rejected_order_items = checked_order_items.filter(
    col("rejection_reason").isNotNull()
)

valid_order_items = (
    checked_order_items
    .filter(col("rejection_reason").isNull())
    .dropDuplicates(["order_item_id"])
    .withColumn("order_date_key", bangkok_date_key("ordered_at"))
    .withColumn(
        "total_was_corrected",
        col("source_total_price") != col("total_price"),
    )
)


# 7. Prepare warehouse-ready natural-key columns.
warehouse_order_items = valid_order_items.select(
    "order_item_id",
    "order_id",
    "customer_id",
    "restaurant_id",
    "menu_item_id",
    "order_date_key",
    "quantity",
    "unit_price",
    "total_price",
    "ordered_at",
    "source_updated_at",
)


print("Raw order items:", order_items.count())
print("Valid order items:", warehouse_order_items.count())
print("Rejected order items:", rejected_order_items.count())
print(
    "Corrected totals:",
    valid_order_items.filter(col("total_was_corrected")).count(),
)

warehouse_order_items.show(10, truncate=False)
rejected_order_items.select(
    "order_item_id",
    "order_id",
    "rejection_reason",
).show(20, truncate=False)

spark.stop()
