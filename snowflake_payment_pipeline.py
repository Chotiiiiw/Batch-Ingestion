from load.snowflake_common import complete_batch, fail_batch, start_batch
from load.snowflake_payment import merge_payments, stage_payments
from transform.common import create_spark, read_mysql_table
from transform.payment import transform_payments


def main():
    spark = create_spark("snowflake-payment-pipeline")
    batch_id = None

    try:
        payments = read_mysql_table(
            spark,
            "payments",
        )

        orders = read_mysql_table(
            spark,
            "orders",
        )

        warehouse_payments, rejected_payments = \
            transform_payments(
                payments,
                orders,
            )

        input_row_count = payments.count()
        staged_row_count = warehouse_payments.count()
        rejected_row_count = rejected_payments.count()

        batch_id = start_batch("payment", input_row_count, rejected_row_count)

        stage_payments(
            warehouse_payments,
            batch_id,
        )

        loaded_row_count = merge_payments(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

        print("Batch ID:", batch_id)
        print("Staged payments:", staged_row_count)
        print("Loaded fact payments:", loaded_row_count)

    except Exception as error:
        if batch_id is not None:
            fail_batch(
                batch_id,
                error,
            )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
