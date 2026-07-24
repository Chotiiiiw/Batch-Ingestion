from load.common import complete_batch, fail_batch
from load.menu_item import merge_menu_items, stage_menu_items
from transform.common import create_spark, read_mongo_collection, read_mysql_table
from transform.menu_item import transform_menu_items


def main():
    spark = create_spark("menu-item-pipeline")
    batch_id = None

    try:
        # 1. Extract menu items from MongoDB
        menu_items = read_mongo_collection(spark, "menu_items")


        # 2. Read the restaurant lookup from MySQL
        restaurants = read_mysql_table(spark, "restaurants")


        # 3. Transform and validate menu items
        warehouse_menu_items, rejected_menu_items = transform_menu_items(menu_items, restaurants)


        # 4. Write the valid menu items to staging
        batch_id, staged_row_count = stage_menu_items(menu_items, warehouse_menu_items, rejected_menu_items)


        # 5. Merge staging into the menu-item dimension
        loaded_row_count = merge_menu_items(batch_id)


        # 6. Complete the ETL batch
        complete_batch(batch_id, loaded_row_count)


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
