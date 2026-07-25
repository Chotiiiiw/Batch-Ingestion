from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake,write_snowflake_staging



def stage_customers(warehouse_customers, batch_id):
    staging_customers = warehouse_customers \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "customer_id",
            "full_name",
            "email",
            "phone",
            "city",
            "valid_from",
            "hash_diff",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_customers,
        "CUSTOMER",
    )


def merge_customers(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            # 1. Close current versions whose tracked values changed.
            cursor.execute(
                """
                UPDATE WAREHOUSE.DIM_CUSTOMER AS TARGET
                SET
                    VALID_TO = SOURCE.SOURCE_UPDATED_AT,
                    IS_CURRENT = FALSE
                FROM STAGING.CUSTOMER AS SOURCE
                WHERE SOURCE.BATCH_ID = %s
                  AND TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID
                  AND TARGET.IS_CURRENT = TRUE
                  AND TARGET.HASH_DIFF <> SOURCE.HASH_DIFF
                  AND SOURCE.SOURCE_UPDATED_AT
                      > TARGET.SOURCE_UPDATED_AT
                  AND SOURCE.SOURCE_UPDATED_AT
                      > TARGET.VALID_FROM
                """,
                (batch_id,),
            )

            changed_row_count = cursor.rowcount


            # 2. Insert first versions and replacements for closed versions.
            cursor.execute(
                """
                INSERT INTO WAREHOUSE.DIM_CUSTOMER (
                    CUSTOMER_ID,
                    FULL_NAME,
                    EMAIL,
                    PHONE,
                    CITY,
                    VALID_FROM,
                    VALID_TO,
                    IS_CURRENT,
                    HASH_DIFF,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.CUSTOMER_ID,
                    SOURCE.FULL_NAME,
                    SOURCE.EMAIL,
                    SOURCE.PHONE,
                    SOURCE.CITY,
                    CASE
                        WHEN HISTORY.CUSTOMER_ID IS NOT NULL
                        THEN SOURCE.SOURCE_UPDATED_AT
                        ELSE SOURCE.VALID_FROM
                    END,
                    NULL,
                    TRUE,
                    SOURCE.HASH_DIFF,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM STAGING.CUSTOMER AS SOURCE
                LEFT JOIN (
                    SELECT CUSTOMER_ID
                    FROM WAREHOUSE.DIM_CUSTOMER
                    GROUP BY CUSTOMER_ID
                ) AS HISTORY
                    ON HISTORY.CUSTOMER_ID = SOURCE.CUSTOMER_ID
                LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CURRENT_VERSION
                    ON CURRENT_VERSION.CUSTOMER_ID
                        = SOURCE.CUSTOMER_ID
                   AND CURRENT_VERSION.IS_CURRENT = TRUE
                WHERE SOURCE.BATCH_ID = %s
                  AND CURRENT_VERSION.CUSTOMER_ID IS NULL
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount


            if changed_row_count > inserted_row_count:
                raise RuntimeError(
                    "Closed customer versions exceed inserted versions"
                )


            # 3. Remove only the staging rows merged by this batch.
            cursor.execute(
                """
                DELETE FROM STAGING.CUSTOMER
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return inserted_row_count
