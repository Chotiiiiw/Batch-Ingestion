from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_orders(warehouse_orders, batch_id):
    staging_orders = warehouse_orders \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "order_id",
            "customer_id",
            "restaurant_id",
            "delivery_address",
            "order_date_key",
            "order_status",
            "subtotal",
            "discount",
            "delivery_fee",
            "total_amount",
            "ordered_at",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_orders,
        "ORDERS",
    )


def merge_orders(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            # 1. Reject ambiguous SCD lookups before changing the fact table.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT SOURCE.ORDER_ID
                    FROM STAGING.ORDERS AS SOURCE
                    JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = SOURCE.CUSTOMER_ID
                     AND SOURCE.ORDERED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR SOURCE.ORDERED_AT < CUSTOMER.VALID_TO
                         )
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.ORDER_ID
                    HAVING COUNT(*) > 1

                    UNION ALL

                    SELECT SOURCE.ORDER_ID
                    FROM STAGING.ORDERS AS SOURCE
                    JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = SOURCE.RESTAURANT_ID
                     AND SOURCE.ORDERED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR SOURCE.ORDERED_AT < RESTAURANT.VALID_TO
                         )
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.ORDER_ID
                    HAVING COUNT(*) > 1
                )
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            ambiguous_order_count = cursor.fetchone()[0]

            if ambiguous_order_count > 0:
                raise RuntimeError(
                    "Ambiguous dimension versions found for order facts"
                )


            # 2. Update facts when the source is newer or an unknown key
            # can now be resolved.
            cursor.execute(
                """
                UPDATE WAREHOUSE.FACT_ORDER AS TARGET
                SET
                    CUSTOMER_KEY = SOURCE.CUSTOMER_KEY,
                    RESTAURANT_KEY = SOURCE.RESTAURANT_KEY,
                    DELIVERY_ADDRESS = SOURCE.DELIVERY_ADDRESS,
                    ORDER_DATE_KEY = SOURCE.RESOLVED_ORDER_DATE_KEY,
                    ORDER_STATUS = SOURCE.ORDER_STATUS,
                    SUBTOTAL = SOURCE.SUBTOTAL,
                    DISCOUNT = SOURCE.DISCOUNT,
                    DELIVERY_FEE = SOURCE.DELIVERY_FEE,
                    TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
                    ORDERED_AT = SOURCE.ORDERED_AT,
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
                        COALESCE(DATE_DIM.DATE_KEY, 0)
                            AS RESOLVED_ORDER_DATE_KEY
                    FROM STAGING.ORDERS AS STAGED
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = STAGED.CUSTOMER_ID
                     AND STAGED.ORDERED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR STAGED.ORDERED_AT < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = STAGED.RESTAURANT_ID
                     AND STAGED.ORDERED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR STAGED.ORDERED_AT < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = STAGED.ORDER_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE TARGET.ORDER_ID = SOURCE.ORDER_ID
                  AND SOURCE.SOURCE_UPDATED_AT
                      >= TARGET.SOURCE_UPDATED_AT
                  AND (
                        SOURCE.SOURCE_UPDATED_AT
                            > TARGET.SOURCE_UPDATED_AT
                        OR SOURCE.CUSTOMER_KEY
                            <> TARGET.CUSTOMER_KEY
                        OR SOURCE.RESTAURANT_KEY
                            <> TARGET.RESTAURANT_KEY
                        OR SOURCE.RESOLVED_ORDER_DATE_KEY
                            <> TARGET.ORDER_DATE_KEY
                      )
                """,
                (batch_id,),
            )

            updated_row_count = cursor.rowcount


            # 3. Insert orders that have never been loaded.
            cursor.execute(
                """
                INSERT INTO WAREHOUSE.FACT_ORDER (
                    ORDER_ID,
                    CUSTOMER_KEY,
                    RESTAURANT_KEY,
                    DELIVERY_ADDRESS,
                    ORDER_DATE_KEY,
                    ORDER_STATUS,
                    SUBTOTAL,
                    DISCOUNT,
                    DELIVERY_FEE,
                    TOTAL_AMOUNT,
                    ORDERED_AT,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.ORDER_ID,
                    SOURCE.CUSTOMER_KEY,
                    SOURCE.RESTAURANT_KEY,
                    SOURCE.DELIVERY_ADDRESS,
                    SOURCE.RESOLVED_ORDER_DATE_KEY,
                    SOURCE.ORDER_STATUS,
                    SOURCE.SUBTOTAL,
                    SOURCE.DISCOUNT,
                    SOURCE.DELIVERY_FEE,
                    SOURCE.TOTAL_AMOUNT,
                    SOURCE.ORDERED_AT,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM (
                    SELECT
                        STAGED.*,
                        COALESCE(CUSTOMER.CUSTOMER_KEY, 0)
                            AS CUSTOMER_KEY,
                        COALESCE(RESTAURANT.RESTAURANT_KEY, 0)
                            AS RESTAURANT_KEY,
                        COALESCE(DATE_DIM.DATE_KEY, 0)
                            AS RESOLVED_ORDER_DATE_KEY
                    FROM STAGING.ORDERS AS STAGED
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = STAGED.CUSTOMER_ID
                     AND STAGED.ORDERED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR STAGED.ORDERED_AT < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = STAGED.RESTAURANT_ID
                     AND STAGED.ORDERED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR STAGED.ORDERED_AT < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = STAGED.ORDER_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM WAREHOUSE.FACT_ORDER AS TARGET
                    WHERE TARGET.ORDER_ID = SOURCE.ORDER_ID
                )
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount


            # 4. Clean only this successfully merged batch from staging.
            cursor.execute(
                """
                DELETE FROM STAGING.ORDERS
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return updated_row_count + inserted_row_count
