from load.snowflake_common import complete_batch, fail_batch, start_batch
from load.snowflake_order import merge_orders, stage_orders
from transform.common import create_spark, read_mysql_table
from transform.order import transform_orders


def main():
    spark = create_spark("snowflake-order-pipeline")
    batch_id = None

    try:
        orders = read_mysql_table(
            spark,
            "orders",
        )

        warehouse_orders, rejected_orders = \
            transform_orders(orders)

        input_row_count = orders.count()
        staged_row_count = warehouse_orders.count()
        rejected_row_count = rejected_orders.count()

        batch_id = start_batch(
            input_row_count,
            rejected_row_count,
        )

        stage_orders(
            warehouse_orders,
            batch_id,
        )

        loaded_row_count = merge_orders(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

        print("Batch ID:", batch_id)
        print("Staged orders:", staged_row_count)
        print("Loaded fact orders:", loaded_row_count)

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
