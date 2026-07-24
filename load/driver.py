from pyspark.sql.functions import lit

from load.common import *


def stage_drivers(
    drivers,
    warehouse_drivers,
    rejected_drivers,
):
    # 1. Count the transformation results
    input_row_count = drivers.count()
    staged_row_count = warehouse_drivers.count()
    rejected_row_count = rejected_drivers.count()


    # 2. Start one ETL batch
    batch_id = start_batch(
        input_row_count,
        rejected_row_count,
    )


    # 3. Prepare the staging columns
    staging_drivers = warehouse_drivers \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "driver_id",
            "driver_name",
            "number_plate",
            "driver_status",
            "valid_from",
            "hash_diff",
            "source_updated_at",
        )


    # 4. Write this batch to PostgreSQL staging
    write_postgres_table(
        staging_drivers,
        "staging.driver",
    )


    return batch_id, staged_row_count


def merge_drivers(batch_id):
    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            # 1. Close changed driver versions and insert new versions
            cursor.execute(
                """
                WITH changed_drivers AS (
                    UPDATE warehouse.dim_driver AS target
                    SET
                        valid_to = source.source_updated_at,
                        is_current = FALSE
                    FROM staging.driver AS source
                    WHERE source.batch_id = %s
                      AND target.driver_id = source.driver_id
                      AND target.is_current = TRUE
                      AND target.hash_diff <> source.hash_diff
                      AND source.source_updated_at > target.source_updated_at
                      AND source.source_updated_at > target.valid_from
                    RETURNING
                        source.driver_id,
                        source.driver_name,
                        source.number_plate,
                        source.driver_status,
                        source.hash_diff,
                        source.source_updated_at
                )
                INSERT INTO warehouse.dim_driver (
                    driver_id,
                    driver_name,
                    number_plate,
                    driver_status,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    driver_id,
                    driver_name,
                    number_plate,
                    driver_status,
                    source_updated_at,
                    NULL,
                    TRUE,
                    hash_diff,
                    source_updated_at,
                    %s::UUID
                FROM changed_drivers
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            changed_row_count = cursor.rowcount


            # 2. Insert drivers that have never existed
            cursor.execute(
                """
                INSERT INTO warehouse.dim_driver (
                    driver_id,
                    driver_name,
                    number_plate,
                    driver_status,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    source.driver_id,
                    source.driver_name,
                    source.number_plate,
                    source.driver_status,
                    source.valid_from,
                    NULL,
                    TRUE,
                    source.hash_diff,
                    source.source_updated_at,
                    %s::UUID
                FROM staging.driver AS source
                WHERE source.batch_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM warehouse.dim_driver AS target
                      WHERE target.driver_id = source.driver_id
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
                DELETE FROM staging.driver
                WHERE batch_id = %s
                """,
                (batch_id,),
            )


    return changed_row_count + new_row_count
