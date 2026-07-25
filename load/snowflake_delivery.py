from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_deliveries(warehouse_deliveries, batch_id):
    staging_deliveries = warehouse_deliveries \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "delivery_id",
            "order_id",
            "customer_id",
            "restaurant_id",
            "driver_id",
            "assigned_date_key",
            "delivery_status",
            "distance_km",
            "assigned_at",
            "picked_up_at",
            "delivered_at",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_deliveries,
        "DELIVERY",
    )


def merge_deliveries(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            # 1. Stop if any delivery matches multiple dimension rows.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT SOURCE.DELIVERY_ID
                    FROM STAGING.DELIVERY AS SOURCE
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = SOURCE.CUSTOMER_ID
                     AND SOURCE.ASSIGNED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR SOURCE.ASSIGNED_AT < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = SOURCE.RESTAURANT_ID
                     AND SOURCE.ASSIGNED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR SOURCE.ASSIGNED_AT < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DRIVER AS DRIVER
                      ON DRIVER.DRIVER_ID = SOURCE.DRIVER_ID
                     AND SOURCE.ASSIGNED_AT >= DRIVER.VALID_FROM
                     AND (
                            DRIVER.VALID_TO IS NULL
                            OR SOURCE.ASSIGNED_AT < DRIVER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = SOURCE.ASSIGNED_DATE_KEY
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.DELIVERY_ID
                    HAVING COUNT(*) > 1
                )
                """,
                (batch_id,),
            )

            ambiguous_delivery_count = cursor.fetchone()[0]

            if ambiguous_delivery_count > 0:
                raise RuntimeError(
                    "Ambiguous dimension rows found for delivery facts"
                )


            # 2. Update newer lifecycle states and repair unknown keys.
            cursor.execute(
                """
                UPDATE WAREHOUSE.FACT_DELIVERY_ATTEMPT AS TARGET
                SET
                    ORDER_ID = SOURCE.ORDER_ID,
                    CUSTOMER_KEY = SOURCE.CUSTOMER_KEY,
                    RESTAURANT_KEY = SOURCE.RESTAURANT_KEY,
                    DRIVER_KEY = SOURCE.DRIVER_KEY,
                    ASSIGNED_DATE_KEY
                        = SOURCE.RESOLVED_ASSIGNED_DATE_KEY,
                    DELIVERY_STATUS = SOURCE.DELIVERY_STATUS,
                    DISTANCE_KM = SOURCE.DISTANCE_KM,
                    ASSIGNED_AT = SOURCE.ASSIGNED_AT,
                    PICKED_UP_AT = SOURCE.PICKED_UP_AT,
                    DELIVERED_AT = SOURCE.DELIVERED_AT,
                    SOURCE_UPDATED_AT = SOURCE.SOURCE_UPDATED_AT,
                    BATCH_ID = SOURCE.BATCH_ID,
                    LOADED_AT = CURRENT_TIMESTAMP()
                FROM (
                    SELECT
                        STAGED.*,
                        COALESCE(CUSTOMER.CUSTOMER_KEY, 0)
                            AS CUSTOMER_KEY,
                        COALESCE(RESTAURANT.RESTAURANT_KEY, 0)
                            AS RESTAURANT_KEY,
                        COALESCE(DRIVER.DRIVER_KEY, 0)
                            AS DRIVER_KEY,
                        COALESCE(DATE_DIM.DATE_KEY, 0)
                            AS RESOLVED_ASSIGNED_DATE_KEY
                    FROM STAGING.DELIVERY AS STAGED
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = STAGED.CUSTOMER_ID
                     AND STAGED.ASSIGNED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR STAGED.ASSIGNED_AT < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = STAGED.RESTAURANT_ID
                     AND STAGED.ASSIGNED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR STAGED.ASSIGNED_AT < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DRIVER AS DRIVER
                      ON DRIVER.DRIVER_ID = STAGED.DRIVER_ID
                     AND STAGED.ASSIGNED_AT >= DRIVER.VALID_FROM
                     AND (
                            DRIVER.VALID_TO IS NULL
                            OR STAGED.ASSIGNED_AT < DRIVER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = STAGED.ASSIGNED_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE TARGET.DELIVERY_ID = SOURCE.DELIVERY_ID
                  AND SOURCE.SOURCE_UPDATED_AT
                      >= TARGET.SOURCE_UPDATED_AT
                  AND (
                        SOURCE.SOURCE_UPDATED_AT
                            > TARGET.SOURCE_UPDATED_AT
                        OR SOURCE.CUSTOMER_KEY
                            <> TARGET.CUSTOMER_KEY
                        OR SOURCE.RESTAURANT_KEY
                            <> TARGET.RESTAURANT_KEY
                        OR SOURCE.DRIVER_KEY
                            <> TARGET.DRIVER_KEY
                        OR SOURCE.RESOLVED_ASSIGNED_DATE_KEY
                            <> TARGET.ASSIGNED_DATE_KEY
                      )
                """,
                (batch_id,),
            )

            updated_row_count = cursor.rowcount


            # 3. Insert delivery attempts that have never been loaded.
            cursor.execute(
                """
                INSERT INTO WAREHOUSE.FACT_DELIVERY_ATTEMPT (
                    DELIVERY_ID,
                    ORDER_ID,
                    CUSTOMER_KEY,
                    RESTAURANT_KEY,
                    DRIVER_KEY,
                    ASSIGNED_DATE_KEY,
                    DELIVERY_STATUS,
                    DISTANCE_KM,
                    ASSIGNED_AT,
                    PICKED_UP_AT,
                    DELIVERED_AT,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.DELIVERY_ID,
                    SOURCE.ORDER_ID,
                    SOURCE.CUSTOMER_KEY,
                    SOURCE.RESTAURANT_KEY,
                    SOURCE.DRIVER_KEY,
                    SOURCE.RESOLVED_ASSIGNED_DATE_KEY,
                    SOURCE.DELIVERY_STATUS,
                    SOURCE.DISTANCE_KM,
                    SOURCE.ASSIGNED_AT,
                    SOURCE.PICKED_UP_AT,
                    SOURCE.DELIVERED_AT,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM (
                    SELECT
                        STAGED.*,
                        COALESCE(CUSTOMER.CUSTOMER_KEY, 0)
                            AS CUSTOMER_KEY,
                        COALESCE(RESTAURANT.RESTAURANT_KEY, 0)
                            AS RESTAURANT_KEY,
                        COALESCE(DRIVER.DRIVER_KEY, 0)
                            AS DRIVER_KEY,
                        COALESCE(DATE_DIM.DATE_KEY, 0)
                            AS RESOLVED_ASSIGNED_DATE_KEY
                    FROM STAGING.DELIVERY AS STAGED
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = STAGED.CUSTOMER_ID
                     AND STAGED.ASSIGNED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR STAGED.ASSIGNED_AT < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = STAGED.RESTAURANT_ID
                     AND STAGED.ASSIGNED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR STAGED.ASSIGNED_AT < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DRIVER AS DRIVER
                      ON DRIVER.DRIVER_ID = STAGED.DRIVER_ID
                     AND STAGED.ASSIGNED_AT >= DRIVER.VALID_FROM
                     AND (
                            DRIVER.VALID_TO IS NULL
                            OR STAGED.ASSIGNED_AT < DRIVER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = STAGED.ASSIGNED_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM WAREHOUSE.FACT_DELIVERY_ATTEMPT AS TARGET
                    WHERE TARGET.DELIVERY_ID = SOURCE.DELIVERY_ID
                )
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount


            # 4. Clean only the successfully merged staging batch.
            cursor.execute(
                """
                DELETE FROM STAGING.DELIVERY
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return updated_row_count + inserted_row_count
