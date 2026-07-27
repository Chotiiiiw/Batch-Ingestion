from load.snowflake_common import (
    complete_batch,
    fail_batch,
    get_last_mysql_watermark,
    start_batch,
)
from load.snowflake_order_item import merge_order_items, stage_order_items
from transform.common import (
    create_spark,
    get_mysql_watermark_to,
    read_mongo_collection,
    read_mysql_incremental_table,
    read_mysql_table,
)
from transform.order_item import transform_order_items


def main():
    spark = create_spark("snowflake-order-item-pipeline")
    batch_id = None

    try:
        mysql_watermark_from = get_last_mysql_watermark(
            "order_item",
        )
        mysql_watermark_to = get_mysql_watermark_to(
            spark,
            "order_items",
            "updated_at",
        )

        order_items = read_mysql_incremental_table(
            spark,
            "order_items",
            "updated_at",
            mysql_watermark_from,
            mysql_watermark_to,
        )

        # Lookup sources remain full reads because an incremental order item
        # still needs its parent order and menu-item context.
        orders = read_mysql_table(
            spark,
            "orders",
        )

        menu_items = read_mongo_collection(
            spark,
            "menu_items",
        )

        warehouse_order_items, rejected_order_items = \
            transform_order_items(
                order_items,
                orders,
                menu_items,
            )

        input_row_count = order_items.count()
        staged_row_count = warehouse_order_items.count()
        rejected_row_count = rejected_order_items.count()

        batch_id = start_batch("order_item", input_row_count, rejected_row_count, mysql_watermark_from, mysql_watermark_to)

        stage_order_items(
            warehouse_order_items,
            batch_id,
        )

        loaded_row_count = merge_order_items(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

        print("Batch ID:", batch_id)
        print("Staged order items:", staged_row_count)
        print("Loaded fact order items:", loaded_row_count)

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
