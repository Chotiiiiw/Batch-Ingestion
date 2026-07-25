from load.snowflake_common import complete_batch, fail_batch, start_batch
from load.snowflake_delivery import merge_deliveries, stage_deliveries
from transform.common import create_spark, read_mysql_table
from transform.delivery import transform_deliveries


def main():
    spark = create_spark("snowflake-delivery-pipeline")
    batch_id = None

    try:
        deliveries = read_mysql_table(
            spark,
            "deliveries",
        )

        orders = read_mysql_table(
            spark,
            "orders",
        )

        warehouse_deliveries, rejected_deliveries = \
            transform_deliveries(
                deliveries,
                orders,
            )

        input_row_count = deliveries.count()
        staged_row_count = warehouse_deliveries.count()
        rejected_row_count = rejected_deliveries.count()

        batch_id = start_batch(
            input_row_count,
            rejected_row_count,
        )

        stage_deliveries(
            warehouse_deliveries,
            batch_id,
        )

        loaded_row_count = merge_deliveries(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

        print("Batch ID:", batch_id)
        print("Staged deliveries:", staged_row_count)
        print("Loaded fact delivery attempts:", loaded_row_count)

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
