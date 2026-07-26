from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import snowflake.connector

from load.snowflake_config import build_snowflake_spark_options, load_snowflake_config


SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
DEFAULT_MYSQL_WATERMARK = datetime(1900,1,1,tzinfo=ZoneInfo("Asia/Bangkok"))


def connect_snowflake():
    config = load_snowflake_config()

    return snowflake.connector.connect(
        account=config["SNOWFLAKE_ACCOUNT"],
        user=config["SNOWFLAKE_USER"],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(config["SNOWFLAKE_PRIVATE_KEY_PATH"]),
        private_key_file_pwd=config["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
        role=config["SNOWFLAKE_ROLE"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"],
        autocommit=False,
    )


def get_last_mysql_watermark(pipeline_name):
    if not pipeline_name:
        raise ValueError("pipeline_name is required")

    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MYSQL_WATERMARK_TO
                FROM WAREHOUSE.ETL_BATCH
                WHERE PIPELINE_NAME = %s
                  AND BATCH_STATUS IN ('SUCCEEDED', 'PARTIAL')
                  AND MYSQL_WATERMARK_TO IS NOT NULL
                ORDER BY
                    COMPLETED_AT DESC,
                    STARTED_AT DESC
                LIMIT 1
                """,
                (pipeline_name,),
            )

            row = cursor.fetchone()

    if row is None:
        return DEFAULT_MYSQL_WATERMARK

    return row[0]


def start_batch(pipeline_name, input_row_count, rejected_row_count, mysql_watermark_from=None, mysql_watermark_to=None):
    batch_id = str(uuid4())

    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO WAREHOUSE.ETL_BATCH (
                    BATCH_ID,
                    PIPELINE_NAME,
                    STARTED_AT,
                    BATCH_STATUS,
                    MYSQL_WATERMARK_FROM,
                    MYSQL_WATERMARK_TO,
                    INPUT_ROW_COUNT,
                    REJECTED_ROW_COUNT
                )
                VALUES (
                    %s,
                    %s,
                    CURRENT_TIMESTAMP(),
                    'RUNNING',
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    batch_id,
                    pipeline_name,
                    mysql_watermark_from,
                    mysql_watermark_to,
                    input_row_count,
                    rejected_row_count,
                ),
            )

    return batch_id


def complete_batch(batch_id, loaded_row_count):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE WAREHOUSE.ETL_BATCH
                SET
                    COMPLETED_AT = CURRENT_TIMESTAMP(),
                    BATCH_STATUS = CASE
                        WHEN REJECTED_ROW_COUNT > 0
                        THEN 'PARTIAL'
                        ELSE 'SUCCEEDED'
                    END,
                    LOADED_ROW_COUNT = %s,
                    ERROR_MESSAGE = NULL
                WHERE BATCH_ID = %s
                """,
                (
                    loaded_row_count,
                    batch_id,
                ),
            )


def fail_batch(batch_id, error_message):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE WAREHOUSE.ETL_BATCH
                SET
                    COMPLETED_AT = CURRENT_TIMESTAMP(),
                    BATCH_STATUS = 'FAILED',
                    ERROR_MESSAGE = %s
                WHERE BATCH_ID = %s
                """,
                (
                    str(error_message),
                    batch_id,
                ),
            )


def write_snowflake_staging(dataframe, table_name):
    snowflake_options = build_snowflake_spark_options()
    snowflake_options["sfSchema"] = "STAGING"

    dataframe.write \
        .format(SNOWFLAKE_SOURCE) \
        .options(**snowflake_options) \
        .option("dbtable", table_name.upper()) \
        .mode("append") \
        .save()
