from uuid import uuid4

import psycopg


POSTGRES_DSN = (
    "host=localhost "
    "port=5433 "
    "dbname=food_delivery_warehouse "
    "user=warehouse_user "
    "password=warehouse_password"
)


def start_batch(input_row_count, rejected_row_count):
    batch_id = str(uuid4())

    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO warehouse.etl_batch (
                    batch_id,
                    started_at,
                    batch_status,
                    input_row_count,
                    rejected_row_count
                )
                VALUES (
                    %s,
                    CURRENT_TIMESTAMP,
                    'RUNNING',
                    %s,
                    %s
                )
                """,
                (
                    batch_id,
                    input_row_count,
                    rejected_row_count,
                ),
            )

    return batch_id


def complete_batch(batch_id, loaded_row_count):
    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE warehouse.etl_batch
                SET
                    completed_at = CURRENT_TIMESTAMP,
                    batch_status = 'SUCCEEDED',
                    loaded_row_count = %s,
                    error_message = NULL
                WHERE batch_id = %s
                """,
                (
                    loaded_row_count,
                    batch_id,
                ),
            )


def fail_batch(batch_id, error_message):
    with psycopg.connect(POSTGRES_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE warehouse.etl_batch
                SET
                    completed_at = CURRENT_TIMESTAMP,
                    batch_status = 'FAILED',
                    error_message = %s
                WHERE batch_id = %s
                """,
                (
                    str(error_message),
                    batch_id,
                ),
            )


def write_postgres_table(dataframe, table_name):
    dataframe.write \
        .format("jdbc") \
        .mode("append") \
        .option(
            "url",
            "jdbc:postgresql://localhost:5433/food_delivery_warehouse",
        ) \
        .option("dbtable", table_name) \
        .option("user", "warehouse_user") \
        .option("password", "warehouse_password") \
        .option("driver", "org.postgresql.Driver") \
        .save()
