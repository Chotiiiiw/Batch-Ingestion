"""Check the Spark-to-Snowflake connector path."""

from pyspark.sql import SparkSession

from load.snowflake_config import build_snowflake_spark_options


SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def check_snowflake_spark_connection():
    spark = SparkSession.builder \
        .appName("check-snowflake-spark") \
        .master("local[*]") \
        .getOrCreate()

    snowflake_options = build_snowflake_spark_options()

    try:
        connection_result = spark.read \
            .format(SNOWFLAKE_SOURCE) \
            .options(**snowflake_options) \
            .option(
                "query",
                """
                SELECT
                    CURRENT_USER() AS current_user,
                    CURRENT_ROLE() AS current_role,
                    CURRENT_WAREHOUSE() AS current_warehouse
                """,
            ) \
            .load()

        connection_result.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    check_snowflake_spark_connection()
