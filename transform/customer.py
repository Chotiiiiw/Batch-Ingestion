from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# start session
spark = SparkSession.builder \
    .appName("transform-customers") \
    .master("local[*]") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()


# 1. Read the CSV
customers = spark.read \
    .option("header", True) \
    .csv("mock-data/output/customers.csv")


# 2. Clean the columns
cleaned_customers = customers \
    .withColumn(
        "customer_id",
        col("customer_id").cast("long"),
    ) \
    .withColumn(
        "full_name",
        trim(col("full_name")),
    ) \
    .withColumn(
        "email",
        lower(trim(col("email"))),
    ) \
    .withColumn(
        "phone",
        when(
            trim(col("phone")) == "",
            lit(None).cast("string"),
        ).otherwise(
            trim(col("phone"))
        ),
    ) \
    .withColumn(
        "city",
        initcap(lower(trim(col("city")))),
    ) \
    .withColumn(
        "created_at",
        to_timestamp(col("created_at")),
    ) \
    .withColumn(
        "updated_at",
        to_timestamp(col("updated_at")),
    )


# 3. Define invalid customers
invalid_condition = \
    col("customer_id").isNull() \
    | (col("customer_id") <= 0) \
    | col("full_name").isNull() \
    | (col("full_name") == "") \
    | col("email").isNull() \
    | (col("email") == "") \
    | col("city").isNull() \
    | (col("city") == "") \
    | col("created_at").isNull() \
    | col("updated_at").isNull() \
    | (col("updated_at") < col("created_at")) \


# 4. Separate valid and rejected customers
rejected_customers = cleaned_customers.filter(invalid_condition)

valid_customers = cleaned_customers\
                .filter(~invalid_condition) \
                .dropDuplicates(["customer_id"]) \


# 5. Calculate the SCD hash
valid_customers = valid_customers.withColumn(
    "hash_diff",
    sha2(
        concat_ws(
            "||",
            col("full_name"),
            col("email"),
            coalesce(
                col("phone"),
                lit("<NULL>"),
            ),
            col("city"),
        ),
        256,
    ),
)


# 6. Prepare warehouse column names
warehouse_customers = valid_customers.select(
    "customer_id",
    "full_name",
    "email",
    "phone",
    "city",
    col("created_at").alias("valid_from"),
    "hash_diff",
    col("updated_at").alias("source_updated_at"),
)


# 7. Check the results
print("Raw customers:", customers.count())
print("Valid customers:", warehouse_customers.count())
print("Rejected customers:", rejected_customers.count())

warehouse_customers.show(10, truncate=False)
rejected_customers.show(10, truncate=False)

spark.stop()