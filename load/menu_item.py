from pyspark.sql.functions import lit

from load.common import *


def stage_menu_items(
    menu_items,
    warehouse_menu_items,
    rejected_menu_items,
):
    # 1. Count the transformation results
    input_row_count = menu_items.count()
    staged_row_count = warehouse_menu_items.count()
    rejected_row_count = rejected_menu_items.count()


    # 2. Start one ETL batch
    batch_id = start_batch(
        input_row_count,
        rejected_row_count,
    )


    # 3. Prepare the staging columns
    staging_menu_items = warehouse_menu_items \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "menu_item_id",
            "restaurant_id",
            "menu_item_name",
            "category",
            "base_price",
            "available",
            "tags",
            "valid_from",
            "hash_diff",
            "source_updated_at",
        )


    # 4. Write this batch to PostgreSQL staging
    write_postgres_table(
        staging_menu_items,
        "staging.menu_item",
    )


    return batch_id, staged_row_count


def merge_menu_items(batch_id):
    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            # 1. Close changed menu-item versions and insert new versions
            cursor.execute(
                """
                WITH changed_menu_items AS (
                    UPDATE warehouse.dim_menu_item AS target
                    SET
                        valid_to = source.source_updated_at,
                        is_current = FALSE
                    FROM staging.menu_item AS source
                    WHERE source.batch_id = %s
                      AND target.menu_item_id = source.menu_item_id
                      AND target.is_current = TRUE
                      AND target.hash_diff <> source.hash_diff
                      AND source.source_updated_at > target.source_updated_at
                      AND source.source_updated_at > target.valid_from
                    RETURNING
                        source.menu_item_id,
                        source.restaurant_id,
                        source.menu_item_name,
                        source.category,
                        source.base_price,
                        source.available,
                        source.tags,
                        source.hash_diff,
                        source.source_updated_at
                )
                INSERT INTO warehouse.dim_menu_item (
                    menu_item_id,
                    restaurant_id,
                    menu_item_name,
                    category,
                    base_price,
                    available,
                    tags,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    menu_item_id,
                    restaurant_id,
                    menu_item_name,
                    category,
                    base_price,
                    available,
                    tags,
                    source_updated_at,
                    NULL,
                    TRUE,
                    hash_diff,
                    source_updated_at,
                    %s::UUID
                FROM changed_menu_items
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            changed_row_count = cursor.rowcount


            # 2. Insert menu items that have never existed
            cursor.execute(
                """
                INSERT INTO warehouse.dim_menu_item (
                    menu_item_id,
                    restaurant_id,
                    menu_item_name,
                    category,
                    base_price,
                    available,
                    tags,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    source.menu_item_id,
                    source.restaurant_id,
                    source.menu_item_name,
                    source.category,
                    source.base_price,
                    source.available,
                    source.tags,
                    source.valid_from,
                    NULL,
                    TRUE,
                    source.hash_diff,
                    source.source_updated_at,
                    %s::UUID
                FROM staging.menu_item AS source
                WHERE source.batch_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM warehouse.dim_menu_item AS target
                      WHERE target.menu_item_id = source.menu_item_id
                  )
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            new_row_count = cursor.rowcount


            # 3. Remove the successfully merged staging rows
            cursor.execute(
                """
                DELETE FROM staging.menu_item
                WHERE batch_id = %s
                """,
                (batch_id,),
            )


    return changed_row_count + new_row_count
