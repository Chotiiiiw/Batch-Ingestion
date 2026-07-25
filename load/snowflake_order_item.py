from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_order_items(warehouse_order_items, batch_id):
    staging_order_items = warehouse_order_items \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "order_item_id",
            "order_id",
            "customer_id",
            "restaurant_id",
            "menu_item_id",
            "order_date_key",
            "quantity",
            "unit_price",
            "total_price",
            "ordered_at",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_order_items,
        "ORDER_ITEM",
    )


def merge_order_items(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            # 1. Stop if an event matches multiple SCD versions.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT SOURCE.ORDER_ITEM_ID
                    FROM STAGING.ORDER_ITEM AS SOURCE
                    JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = SOURCE.CUSTOMER_ID
                     AND SOURCE.ORDERED_AT >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR SOURCE.ORDERED_AT < CUSTOMER.VALID_TO
                         )
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.ORDER_ITEM_ID
                    HAVING COUNT(*) > 1

                    UNION ALL

                    SELECT SOURCE.ORDER_ITEM_ID
                    FROM STAGING.ORDER_ITEM AS SOURCE
                    JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = SOURCE.RESTAURANT_ID
                     AND SOURCE.ORDERED_AT >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR SOURCE.ORDERED_AT < RESTAURANT.VALID_TO
                         )
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.ORDER_ITEM_ID
                    HAVING COUNT(*) > 1

                    UNION ALL

                    SELECT SOURCE.ORDER_ITEM_ID
                    FROM STAGING.ORDER_ITEM AS SOURCE
                    JOIN WAREHOUSE.DIM_MENU_ITEM AS MENU_ITEM
                      ON MENU_ITEM.MENU_ITEM_ID = SOURCE.MENU_ITEM_ID
                     AND SOURCE.ORDERED_AT >= MENU_ITEM.VALID_FROM
                     AND (
                            MENU_ITEM.VALID_TO IS NULL
                            OR SOURCE.ORDERED_AT < MENU_ITEM.VALID_TO
                         )
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.ORDER_ITEM_ID
                    HAVING COUNT(*) > 1
                )
                """,
                (
                    batch_id,
                    batch_id,
                    batch_id,
                ),
            )

            ambiguous_item_count = cursor.fetchone()[0]

            if ambiguous_item_count > 0:
                raise RuntimeError(
                    "Ambiguous dimension versions found for order-item facts"
                )


            # 2. Update newer source rows and repair resolvable unknown keys.
            cursor.execute(
                """
                UPDATE WAREHOUSE.FACT_ORDER_ITEM AS TARGET
                SET
                    ORDER_ID = SOURCE.ORDER_ID,
                    CUSTOMER_KEY = SOURCE.CUSTOMER_KEY,
                    RESTAURANT_KEY = SOURCE.RESTAURANT_KEY,
                    MENU_ITEM_KEY = SOURCE.MENU_ITEM_KEY,
                    ORDER_DATE_KEY = SOURCE.RESOLVED_ORDER_DATE_KEY,
                    QUANTITY = SOURCE.QUANTITY,
                    UNIT_PRICE = SOURCE.UNIT_PRICE,
                    TOTAL_PRICE = SOURCE.TOTAL_PRICE,
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
                        COALESCE(MENU_ITEM.MENU_ITEM_KEY, 0)
                            AS MENU_ITEM_KEY,
                        COALESCE(DATE_DIM.DATE_KEY, 0)
                            AS RESOLVED_ORDER_DATE_KEY
                    FROM STAGING.ORDER_ITEM AS STAGED
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
                    LEFT JOIN WAREHOUSE.DIM_MENU_ITEM AS MENU_ITEM
                      ON MENU_ITEM.MENU_ITEM_ID = STAGED.MENU_ITEM_ID
                     AND STAGED.ORDERED_AT >= MENU_ITEM.VALID_FROM
                     AND (
                            MENU_ITEM.VALID_TO IS NULL
                            OR STAGED.ORDERED_AT < MENU_ITEM.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = STAGED.ORDER_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE TARGET.ORDER_ITEM_ID = SOURCE.ORDER_ITEM_ID
                  AND SOURCE.SOURCE_UPDATED_AT
                      >= TARGET.SOURCE_UPDATED_AT
                  AND (
                        SOURCE.SOURCE_UPDATED_AT
                            > TARGET.SOURCE_UPDATED_AT
                        OR SOURCE.CUSTOMER_KEY
                            <> TARGET.CUSTOMER_KEY
                        OR SOURCE.RESTAURANT_KEY
                            <> TARGET.RESTAURANT_KEY
                        OR SOURCE.MENU_ITEM_KEY
                            <> TARGET.MENU_ITEM_KEY
                        OR SOURCE.RESOLVED_ORDER_DATE_KEY
                            <> TARGET.ORDER_DATE_KEY
                      )
                """,
                (batch_id,),
            )

            updated_row_count = cursor.rowcount


            # 3. Insert order items that have never been loaded.
            cursor.execute(
                """
                INSERT INTO WAREHOUSE.FACT_ORDER_ITEM (
                    ORDER_ITEM_ID,
                    ORDER_ID,
                    CUSTOMER_KEY,
                    RESTAURANT_KEY,
                    MENU_ITEM_KEY,
                    ORDER_DATE_KEY,
                    QUANTITY,
                    UNIT_PRICE,
                    TOTAL_PRICE,
                    ORDERED_AT,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.ORDER_ITEM_ID,
                    SOURCE.ORDER_ID,
                    SOURCE.CUSTOMER_KEY,
                    SOURCE.RESTAURANT_KEY,
                    SOURCE.MENU_ITEM_KEY,
                    SOURCE.RESOLVED_ORDER_DATE_KEY,
                    SOURCE.QUANTITY,
                    SOURCE.UNIT_PRICE,
                    SOURCE.TOTAL_PRICE,
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
                        COALESCE(MENU_ITEM.MENU_ITEM_KEY, 0)
                            AS MENU_ITEM_KEY,
                        COALESCE(DATE_DIM.DATE_KEY, 0)
                            AS RESOLVED_ORDER_DATE_KEY
                    FROM STAGING.ORDER_ITEM AS STAGED
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
                    LEFT JOIN WAREHOUSE.DIM_MENU_ITEM AS MENU_ITEM
                      ON MENU_ITEM.MENU_ITEM_ID = STAGED.MENU_ITEM_ID
                     AND STAGED.ORDERED_AT >= MENU_ITEM.VALID_FROM
                     AND (
                            MENU_ITEM.VALID_TO IS NULL
                            OR STAGED.ORDERED_AT < MENU_ITEM.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_DATE AS DATE_DIM
                      ON DATE_DIM.DATE_KEY = STAGED.ORDER_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM WAREHOUSE.FACT_ORDER_ITEM AS TARGET
                    WHERE TARGET.ORDER_ITEM_ID = SOURCE.ORDER_ITEM_ID
                )
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount


            # 4. Clean only the successfully merged staging batch.
            cursor.execute(
                """
                DELETE FROM STAGING.ORDER_ITEM
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return updated_row_count + inserted_row_count
