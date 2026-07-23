from pyspark.sql.functions import *
from common import create_spark, optional_text

#Create SparkSession
spark = create_spark("transform-restaurants")


# 1. Read source data.
restaurants = (
    spark.read
    .option("header", True)
    .csv("mock-data/output/restaurants.csv")
)


# 2. Clean and cast source columns.

cleaned_restaurants = restaurants\
    .withColumn("restaurant_id", col("restaurant_id").cast("long"))\
    .withColumn("restaurant_name", trim(col("restaurant_name")))\
    .withColumn("category", initcap(lower(trim(col("category")))))\
    .withColumn("city", initcap(lower(trim(col("city")))))\
    .withColumn("address", optional_text("address"))\
    .withColumn("status", upper(trim(col("status"))))\
    .withColumn("created_at", to_timestamp(col("created_at")))\
    .withColumn("updated_at", to_timestamp(col("updated_at")))\


# 3. Assign the first applicable rejection reason.
checked_restaurants = cleaned_restaurants.withColumn(
    "rejection_reason",
    when(
        col("restaurant_id").isNull() | (col("restaurant_id") <= 0),
        lit("INVALID_RESTAURANT_ID"),
    )
    .when(
        col("restaurant_name").isNull()
        | (length(col("restaurant_name")) == 0),
        lit("MISSING_RESTAURANT_NAME"),
    )
    .when(
        col("category").isNull() | (length(col("category")) == 0),
        lit("MISSING_CATEGORY"),
    )
    .when(
        col("city").isNull() | (length(col("city")) == 0),
        lit("MISSING_CITY"),
    )
    .when(
        ~col("status").isin("ACTIVE", "INACTIVE"),
        lit("INVALID_RESTAURANT_STATUS"),
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


# 4. Separate valid and rejected rows.
rejected_restaurants = checked_restaurants.filter(
    col("rejection_reason").isNotNull()
)

valid_restaurants = (
    checked_restaurants
    .filter(col("rejection_reason").isNull())
    .dropDuplicates(["restaurant_id"])
)


# 5. Hash the cleaned business attributes for SCD Type 2.
valid_restaurants = valid_restaurants.withColumn(
    "hash_diff",
    sha2(
        concat_ws(
            "||",
            col("restaurant_name"),
            col("category"),
            col("city"),
            coalesce(col("address"), lit("<NULL>")),
            col("status"),
        ),
        256,
    ),
)


# 6. Prepare warehouse-ready natural-key columns.
warehouse_restaurants = valid_restaurants.select(
    "restaurant_id",
    "restaurant_name",
    "category",
    "city",
    "address",
    "status",
    col("created_at").alias("valid_from"),
    "hash_diff",
    col("updated_at").alias("source_updated_at"),
)


print("Raw restaurants:", restaurants.count())
print("Valid restaurants:", warehouse_restaurants.count())
print("Rejected restaurants:", rejected_restaurants.count())

warehouse_restaurants.show(10, truncate=False)
rejected_restaurants.select(
    "restaurant_id",
    "rejection_reason",
).show(20, truncate=False)

spark.stop()
