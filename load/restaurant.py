from pyspark.sql.functions import lit

from load.common import *


def stage_restaurants(
    restaurants,
    warehouse_restaurants,
    rejected_restaurants,
):
    # 1. Count the transformation results
    input_row_count = restaurants.count()
    staged_row_count = warehouse_restaurants.count()
    rejected_row_count = rejected_restaurants.count()


    # 2. Start one ETL batch
    batch_id = start_batch(
        input_row_count,
        rejected_row_count,
    )


    # 3. Prepare the staging columns
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


    # 4. Write this batch to PostgreSQL staging
    write_postgres_table(
        staging_restaurants,
        "staging.restaurant",
    )


    return batch_id, staged_row_count


def merge_restaurants(batch_id):
    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            # 1. Close changed restaurant versions and insert new versions
            cursor.execute(
                """
                WITH changed_restaurants AS (
                    UPDATE warehouse.dim_restaurant AS target
                    SET
                        valid_to = source.source_updated_at,
                        is_current = FALSE
                    FROM staging.restaurant AS source
                    WHERE source.batch_id = %s
                      AND target.restaurant_id = source.restaurant_id
                      AND target.is_current = TRUE
                      AND target.hash_diff <> source.hash_diff
                      AND source.source_updated_at > target.source_updated_at
                      AND source.source_updated_at > target.valid_from
                    RETURNING
                        source.restaurant_id,
                        source.restaurant_name,
                        source.category,
                        source.city,
                        source.address,
                        source.status,
                        source.hash_diff,
                        source.source_updated_at
                )
                INSERT INTO warehouse.dim_restaurant (
                    restaurant_id,
                    restaurant_name,
                    category,
                    city,
                    address,
                    status,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    restaurant_id,
                    restaurant_name,
                    category,
                    city,
                    address,
                    status,
                    source_updated_at,
                    NULL,
                    TRUE,
                    hash_diff,
                    source_updated_at,
                    %s::UUID
                FROM changed_restaurants
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            changed_row_count = cursor.rowcount


            # 2. Insert restaurants that have never existed
            cursor.execute(
                """
                INSERT INTO warehouse.dim_restaurant (
                    restaurant_id,
                    restaurant_name,
                    category,
                    city,
                    address,
                    status,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    source.restaurant_id,
                    source.restaurant_name,
                    source.category,
                    source.city,
                    source.address,
                    source.status,
                    source.valid_from,
                    NULL,
                    TRUE,
                    source.hash_diff,
                    source.source_updated_at,
                    %s::UUID
                FROM staging.restaurant AS source
                WHERE source.batch_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM warehouse.dim_restaurant AS target
                      WHERE target.restaurant_id = source.restaurant_id
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
                DELETE FROM staging.restaurant
                WHERE batch_id = %s
                """,
                (batch_id,),
            )


    return changed_row_count + new_row_count
