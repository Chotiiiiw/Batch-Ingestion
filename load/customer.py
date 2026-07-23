from pyspark.sql.functions import lit

from load.common import *


def stage_customers(customers, warehouse_customers, rejected_customers):
    # 1. Count the transformation results
    input_row_count = customers.count()
    staged_row_count = warehouse_customers.count()
    rejected_row_count = rejected_customers.count()


    # 2. Start one ETL batch
    batch_id = start_batch(
        input_row_count,
        rejected_row_count,
    )


    # 3. Prepare the staging columns
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


    # 4. Write this batch to PostgreSQL staging
    write_postgres_table(
        staging_customers,
        "staging.customer",
    )


    return batch_id, staged_row_count


def merge_customers(batch_id):
    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            # 1. Close changed customer versions and insert new versions
            cursor.execute(
                """
                WITH changed_customers AS (
                    UPDATE warehouse.dim_customer AS target
                    SET
                        valid_to = source.source_updated_at,
                        is_current = FALSE
                    FROM staging.customer AS source
                    WHERE source.batch_id = %s
                      AND target.customer_id = source.customer_id
                      AND target.is_current = TRUE
                      AND target.hash_diff <> source.hash_diff
                      AND source.source_updated_at > target.source_updated_at
                      AND source.source_updated_at > target.valid_from
                    RETURNING
                        source.customer_id,
                        source.full_name,
                        source.email,
                        source.phone,
                        source.city,
                        source.hash_diff,
                        source.source_updated_at
                )
                INSERT INTO warehouse.dim_customer (
                    customer_id,
                    full_name,
                    email,
                    phone,
                    city,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    customer_id,
                    full_name,
                    email,
                    phone,
                    city,
                    source_updated_at,
                    NULL,
                    TRUE,
                    hash_diff,
                    source_updated_at,
                    %s::UUID
                FROM changed_customers
                """,
                (
                    batch_id,
                    batch_id,
                ),
            )

            changed_row_count = cursor.rowcount


            # 2. Insert customers that have never existed
            cursor.execute(
                """
                INSERT INTO warehouse.dim_customer (
                    customer_id,
                    full_name,
                    email,
                    phone,
                    city,
                    valid_from,
                    valid_to,
                    is_current,
                    hash_diff,
                    source_updated_at,
                    batch_id
                )
                SELECT
                    source.customer_id,
                    source.full_name,
                    source.email,
                    source.phone,
                    source.city,
                    source.valid_from,
                    NULL,
                    TRUE,
                    source.hash_diff,
                    source.source_updated_at,
                    %s::UUID
                FROM staging.customer AS source
                WHERE source.batch_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM warehouse.dim_customer AS target
                      WHERE target.customer_id = source.customer_id
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
                DELETE FROM staging.customer
                WHERE batch_id = %s
                """,
                (batch_id,),
            )


    return changed_row_count + new_row_count
