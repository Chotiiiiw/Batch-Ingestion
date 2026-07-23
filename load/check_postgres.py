from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("check-postgres") \
    .master("local[*]") \
    .getOrCreate()


dim_customer = spark.read \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:postgresql://localhost:5433/food_delivery_warehouse",
    ) \
    .option(
        "dbtable",
        "warehouse.dim_customer",
    ) \
    .option("user", "warehouse_user") \
    .option("password", "warehouse_password") \
    .option("driver", "org.postgresql.Driver") \
    .load()


dim_customer.printSchema()
dim_customer.show(truncate=False)

spark.stop()