from load.snowflake_common import complete_batch, fail_batch, get_last_mysql_watermark, start_batch
from load.snowflake_restaurant import merge_restaurants, stage_restaurants
from transform.common import create_spark, get_mysql_watermark_to, read_mysql_incremental_table
from transform.restaurant import transform_restaurants


def main():
    spark = create_spark("snowflake-restaurant-pipeline")
    batch_id = None

    try:
        mysql_watermark_from = get_last_mysql_watermark("restaurant")
        mysql_watermark_to = get_mysql_watermark_to(
            spark,
            "restaurants",
            "updated_at",
        )

        restaurants = read_mysql_incremental_table(
            spark,
            "restaurants",
            "updated_at",
            mysql_watermark_from,
            mysql_watermark_to,
        )

        warehouse_restaurants, rejected_restaurants = \
            transform_restaurants(restaurants)

        input_row_count = restaurants.count()
        staged_row_count = warehouse_restaurants.count()
        rejected_row_count = rejected_restaurants.count()

        batch_id = start_batch("restaurant", input_row_count, rejected_row_count, mysql_watermark_from, mysql_watermark_to)

        stage_restaurants(
            warehouse_restaurants,
            batch_id,
        )

        loaded_row_count = merge_restaurants(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

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
