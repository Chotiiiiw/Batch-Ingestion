from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_restaurants(warehouse_restaurants, batch_id):
    staging_restaurants = warehouse_restaurants \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "restaurant_id",
            "restaurant_name",
            "category",
            "city",
            "address",
            "status",
            "valid_from",
            "hash_diff",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_restaurants,
        "RESTAURANT",
    )


def merge_restaurants(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE WAREHOUSE.DIM_RESTAURANT AS TARGET
                SET
                    VALID_TO = SOURCE.SOURCE_UPDATED_AT,
                    IS_CURRENT = FALSE
                FROM STAGING.RESTAURANT AS SOURCE
                WHERE SOURCE.BATCH_ID = %s
                  AND TARGET.RESTAURANT_ID = SOURCE.RESTAURANT_ID
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


            cursor.execute(
                """
                INSERT INTO WAREHOUSE.DIM_RESTAURANT (
                    RESTAURANT_ID,
                    RESTAURANT_NAME,
                    CATEGORY,
                    CITY,
                    ADDRESS,
                    STATUS,
                    VALID_FROM,
                    VALID_TO,
                    IS_CURRENT,
                    HASH_DIFF,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.RESTAURANT_ID,
                    SOURCE.RESTAURANT_NAME,
                    SOURCE.CATEGORY,
                    SOURCE.CITY,
                    SOURCE.ADDRESS,
                    SOURCE.STATUS,
                    CASE
                        WHEN HISTORY.RESTAURANT_ID IS NOT NULL
                        THEN SOURCE.SOURCE_UPDATED_AT
                        ELSE SOURCE.VALID_FROM
                    END,
                    NULL,
                    TRUE,
                    SOURCE.HASH_DIFF,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM STAGING.RESTAURANT AS SOURCE
                LEFT JOIN (
                    SELECT RESTAURANT_ID
                    FROM WAREHOUSE.DIM_RESTAURANT
                    GROUP BY RESTAURANT_ID
                ) AS HISTORY
                    ON HISTORY.RESTAURANT_ID = SOURCE.RESTAURANT_ID
                LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS CURRENT_VERSION
                    ON CURRENT_VERSION.RESTAURANT_ID
                        = SOURCE.RESTAURANT_ID
                   AND CURRENT_VERSION.IS_CURRENT = TRUE
                WHERE SOURCE.BATCH_ID = %s
                  AND CURRENT_VERSION.RESTAURANT_ID IS NULL
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount

            if changed_row_count > inserted_row_count:
                raise RuntimeError(
                    "Closed restaurant versions exceed inserted versions"
                )


            cursor.execute(
                """
                DELETE FROM STAGING.RESTAURANT
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return inserted_row_count
