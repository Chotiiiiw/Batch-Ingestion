from load.snowflake_common import complete_batch, fail_batch, start_batch
from load.snowflake_menu_item import merge_menu_items, stage_menu_items
from transform.common import create_spark, read_mongo_collection, read_mysql_table
from transform.menu_item import transform_menu_items


def main():
    spark = create_spark("snowflake-menu-item-pipeline")
    batch_id = None

    try:
        menu_items = read_mongo_collection(
            spark,
            "menu_items",
        )

        restaurants = read_mysql_table(
            spark,
            "restaurants",
        )

        warehouse_menu_items, rejected_menu_items = \
            transform_menu_items(
                menu_items,
                restaurants,
            )

        input_row_count = menu_items.count()
        staged_row_count = warehouse_menu_items.count()
        rejected_row_count = rejected_menu_items.count()

        batch_id = start_batch("menu_item", input_row_count, rejected_row_count)

        stage_menu_items(
            warehouse_menu_items,
            batch_id,
        )

        loaded_row_count = merge_menu_items(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

        print("Batch ID:", batch_id)
        print("Staged menu items:", staged_row_count)
        print("Loaded menu-item versions:", loaded_row_count)

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
