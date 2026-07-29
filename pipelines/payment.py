"""Run the incremental payment pipeline from MySQL to Snowflake."""

from load.snowflake_common import *
from load.snowflake_payment import merge_payments, stage_payments
from transform.common import *
from transform.payment import transform_payments

def main():
    spark = create_spark("snowflake-payment-pipeline")
    batch_id = None

    try:
        mysql_watermark_from = get_last_mysql_watermark(
            "payment",
        )
        mysql_watermark_to = get_mysql_watermark_to(
            spark,
            "payments",
            "updated_at",
        )

        payments = read_mysql_incremental_table(
            spark,
            "payments",
            "updated_at",
            mysql_watermark_from,
            mysql_watermark_to,
        )

        # The parent-order lookup remains a full read so every incremental
        # payment can still be validated and enriched.
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

        batch_id = start_batch("payment", input_row_count, rejected_row_count, mysql_watermark_from, mysql_watermark_to)

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
