from load.common import complete_batch, fail_batch
from load.restaurant import merge_restaurants, stage_restaurants
from transform.common import create_spark, read_mysql_table
from transform.restaurant import transform_restaurants


def main():
    spark = create_spark("restaurant-pipeline")
    batch_id = None

    try:
        # 1. Extract restaurants from MySQL
        restaurants = read_mysql_table(spark, "restaurants")


        # 2. Transform and validate restaurants
        warehouse_restaurants, rejected_restaurants = transform_restaurants(restaurants)


        # 3. Write the valid restaurants to staging
        batch_id, staged_row_count = stage_restaurants(restaurants, warehouse_restaurants, rejected_restaurants)


        # 4. Merge staging into the restaurant dimension
        loaded_row_count = merge_restaurants(batch_id)


        # 5. Complete the ETL batch
        complete_batch(batch_id, loaded_row_count)


        print("Batch ID:", batch_id)
        print("Staged restaurants:", staged_row_count)
        print("Loaded restaurant versions:", loaded_row_count)

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
