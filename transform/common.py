import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


MYSQL_URL = os.getenv(
    "MYSQL_URL",
    "jdbc:mysql://localhost:3307/food_delivery_source",
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27018",
)

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "local[*]",
)


def create_spark(app_name):
    # Create the local Spark session
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def read_mysql_table(spark, table_name):
    # Read one source table from MySQL
    return spark.read \
        .format("jdbc") \
        .option(
            "url",
            MYSQL_URL,
        ) \
        .option("dbtable", table_name) \
        .option("user", "etl_user") \
        .option("password", "etl_password") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()


def read_mongo_collection(spark, collection_name):
    # Read one source collection from MongoDB
    return spark.read \
        .format("mongodb") \
        .option(
            "connection.uri",
            MONGO_URI,
        ) \
        .option(
            "database",
            "food_delivery_source",
        ) \
        .option(
            "collection",
            collection_name,
        ) \
        .load()


def optional_text(column_name):
    # Trim optional text and turn blank values into NULL
    return when(
        col(column_name).isNull() | (length(trim(col(column_name))) == 0),
        lit(None).cast("string"),
    ).otherwise(trim(col(column_name)))


def bangkok_date_key(column_name):
    # Convert a UTC timestamp to an integer YYYYMMDD Bangkok date key
    return date_format(
        from_utc_timestamp(col(column_name), "Asia/Bangkok"),
        "yyyyMMdd",
    ).cast("integer")
