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


def _validate_mysql_identifier(identifier):
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"Invalid MySQL identifier: {identifier}")


def _format_mysql_watermark(watermark):
    if watermark.tzinfo is not None:
        watermark = watermark.replace(tzinfo=None)

    return watermark.strftime("%Y-%m-%d %H:%M:%S.%f")


def get_mysql_watermark_to(spark, table_name, watermark_column):
    _validate_mysql_identifier(table_name)
    _validate_mysql_identifier(watermark_column)

    watermark_query = (
        f"(SELECT MAX({watermark_column}) AS watermark_to "
        f"FROM {table_name}) AS source_watermark"
    )

    watermark_row = read_mysql_table(spark, watermark_query).first()

    return watermark_row["watermark_to"]


def read_mysql_incremental_table(spark, table_name, watermark_column, watermark_from, watermark_to):
    _validate_mysql_identifier(table_name)
    _validate_mysql_identifier(watermark_column)

    if watermark_from is None:
        raise ValueError("watermark_from is required")

    if watermark_to is None:
        incremental_query = (
            f"(SELECT * FROM {table_name} "
            "WHERE 1 = 0) AS incremental_source"
        )
    else:
        formatted_watermark_from = _format_mysql_watermark(watermark_from)
        formatted_watermark_to = _format_mysql_watermark(watermark_to)

        incremental_query = (
            f"(SELECT * FROM {table_name} "
            f"WHERE {watermark_column} > '{formatted_watermark_from}' "
            f"AND {watermark_column} <= '{formatted_watermark_to}') "
            "AS incremental_source"
        )

    return read_mysql_table(spark, incremental_query)


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
