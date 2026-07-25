from pyspark.sql.functions import lit, to_json

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_menu_items(warehouse_menu_items, batch_id):
    staging_menu_items = warehouse_menu_items \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .withColumn(
            "tags_json",
            to_json("tags"),
        ) \
        .select(
            "batch_id",
            "menu_item_id",
            "restaurant_id",
            "menu_item_name",
            "category",
            "base_price",
            "available",
            "tags_json",
            "valid_from",
            "hash_diff",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_menu_items,
        "MENU_ITEM",
    )


def merge_menu_items(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE WAREHOUSE.DIM_MENU_ITEM AS TARGET
                SET
                    VALID_TO = SOURCE.SOURCE_UPDATED_AT,
                    IS_CURRENT = FALSE
                FROM STAGING.MENU_ITEM AS SOURCE
                WHERE SOURCE.BATCH_ID = %s
                  AND TARGET.MENU_ITEM_ID = SOURCE.MENU_ITEM_ID
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
                INSERT INTO WAREHOUSE.DIM_MENU_ITEM (
                    MENU_ITEM_ID,
                    RESTAURANT_ID,
                    MENU_ITEM_NAME,
                    CATEGORY,
                    BASE_PRICE,
                    AVAILABLE,
                    TAGS,
                    VALID_FROM,
                    VALID_TO,
                    IS_CURRENT,
                    HASH_DIFF,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.MENU_ITEM_ID,
                    SOURCE.RESTAURANT_ID,
                    SOURCE.MENU_ITEM_NAME,
                    SOURCE.CATEGORY,
                    SOURCE.BASE_PRICE,
                    SOURCE.AVAILABLE,
                    PARSE_JSON(SOURCE.TAGS_JSON)::ARRAY,
                    CASE
                        WHEN HISTORY.MENU_ITEM_ID IS NOT NULL
                        THEN SOURCE.SOURCE_UPDATED_AT
                        ELSE SOURCE.VALID_FROM
                    END,
                    NULL,
                    TRUE,
                    SOURCE.HASH_DIFF,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM STAGING.MENU_ITEM AS SOURCE
                LEFT JOIN (
                    SELECT MENU_ITEM_ID
                    FROM WAREHOUSE.DIM_MENU_ITEM
                    GROUP BY MENU_ITEM_ID
                ) AS HISTORY
                    ON HISTORY.MENU_ITEM_ID = SOURCE.MENU_ITEM_ID
                LEFT JOIN WAREHOUSE.DIM_MENU_ITEM AS CURRENT_VERSION
                    ON CURRENT_VERSION.MENU_ITEM_ID
                        = SOURCE.MENU_ITEM_ID
                   AND CURRENT_VERSION.IS_CURRENT = TRUE
                WHERE SOURCE.BATCH_ID = %s
                  AND CURRENT_VERSION.MENU_ITEM_ID IS NULL
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount

            if changed_row_count > inserted_row_count:
                raise RuntimeError(
                    "Closed menu-item versions exceed inserted versions"
                )


            cursor.execute(
                """
                DELETE FROM STAGING.MENU_ITEM
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return inserted_row_count
