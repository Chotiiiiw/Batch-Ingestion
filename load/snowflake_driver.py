from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_drivers(warehouse_drivers, batch_id):
    staging_drivers = warehouse_drivers \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "driver_id",
            "driver_name",
            "phone",
            "number_plate",
            "driver_status",
            "valid_from",
            "hash_diff",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_drivers,
        "DRIVER",
    )


def merge_drivers(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            # Fail before mutating the dimension when normalized plates
            # collide inside the batch or with another current driver.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT UPPER(TRIM(NUMBER_PLATE))
                        AS NORMALIZED_NUMBER_PLATE
                    FROM STAGING.DRIVER
                    WHERE BATCH_ID = %s
                    GROUP BY UPPER(TRIM(NUMBER_PLATE))
                    HAVING COUNT(DISTINCT DRIVER_ID) > 1

                    UNION ALL

                    SELECT UPPER(TRIM(SOURCE.NUMBER_PLATE))
                        AS NORMALIZED_NUMBER_PLATE
                    FROM STAGING.DRIVER AS SOURCE
                    JOIN WAREHOUSE.DIM_DRIVER AS CURRENT_DRIVER
                      ON UPPER(TRIM(CURRENT_DRIVER.NUMBER_PLATE))
                         = UPPER(TRIM(SOURCE.NUMBER_PLATE))
                     AND CURRENT_DRIVER.IS_CURRENT = TRUE
                     AND CURRENT_DRIVER.DRIVER_ID <> SOURCE.DRIVER_ID
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY UPPER(TRIM(SOURCE.NUMBER_PLATE))
                ) AS NUMBER_PLATE_CONFLICTS
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            number_plate_conflict_count = cursor.fetchone()[0]

            if number_plate_conflict_count > 0:
                raise RuntimeError(
                    "Driver batch contains duplicate current number plates"
                )

            cursor.execute(
                """
                UPDATE WAREHOUSE.DIM_DRIVER AS TARGET
                SET
                    VALID_TO = SOURCE.SOURCE_UPDATED_AT,
                    IS_CURRENT = FALSE
                FROM STAGING.DRIVER AS SOURCE
                WHERE SOURCE.BATCH_ID = %s
                  AND TARGET.DRIVER_ID = SOURCE.DRIVER_ID
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
                INSERT INTO WAREHOUSE.DIM_DRIVER (
                    DRIVER_ID,
                    DRIVER_NAME,
                    PHONE,
                    NUMBER_PLATE,
                    DRIVER_STATUS,
                    VALID_FROM,
                    VALID_TO,
                    IS_CURRENT,
                    HASH_DIFF,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.DRIVER_ID,
                    SOURCE.DRIVER_NAME,
                    SOURCE.PHONE,
                    SOURCE.NUMBER_PLATE,
                    SOURCE.DRIVER_STATUS,
                    CASE
                        WHEN HISTORY.DRIVER_ID IS NOT NULL
                        THEN SOURCE.SOURCE_UPDATED_AT
                        ELSE SOURCE.VALID_FROM
                    END,
                    NULL,
                    TRUE,
                    SOURCE.HASH_DIFF,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM STAGING.DRIVER AS SOURCE
                LEFT JOIN (
                    SELECT DRIVER_ID
                    FROM WAREHOUSE.DIM_DRIVER
                    GROUP BY DRIVER_ID
                ) AS HISTORY
                    ON HISTORY.DRIVER_ID = SOURCE.DRIVER_ID
                LEFT JOIN WAREHOUSE.DIM_DRIVER AS CURRENT_VERSION
                    ON CURRENT_VERSION.DRIVER_ID = SOURCE.DRIVER_ID
                   AND CURRENT_VERSION.IS_CURRENT = TRUE
                WHERE SOURCE.BATCH_ID = %s
                  AND CURRENT_VERSION.DRIVER_ID IS NULL
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount

            if changed_row_count > inserted_row_count:
                raise RuntimeError(
                    "Closed driver versions exceed inserted versions"
                )


            cursor.execute(
                """
                DELETE FROM STAGING.DRIVER
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return inserted_row_count
