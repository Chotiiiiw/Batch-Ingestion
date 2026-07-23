from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark(app_name):
    #Create the local Spark session.
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def optional_text(column_name):
    #Trim optional text and turn blank values into NULL.
    return when(
        col(column_name).isNull() | (length(trim(col(column_name))) == 0),
        lit(None).cast("string"),
    ).otherwise(trim(col(column_name)))


def bangkok_date_key(column_name):
    #Convert a UTC timestamp to an integer YYYYMMDD Bangkok date key.
    return date_format(
        from_utc_timestamp(col(column_name), "Asia/Bangkok"),
        "yyyyMMdd",
    ).cast("integer")
