from load.snowflake_common import complete_batch, fail_batch, get_last_mysql_watermark, start_batch
from load.snowflake_driver import merge_drivers, stage_drivers
from transform.common import create_spark, get_mysql_watermark_to, read_mysql_incremental_table
from transform.driver import transform_drivers

def main():
    spark = create_spark("snowflake-driver-pipeline")
    batch_id = None

    try:
        mysql_watermark_from = get_last_mysql_watermark("driver")
        mysql_watermark_to = get_mysql_watermark_to(
            spark,
            "drivers",
            "updated_at",
        )

        drivers = read_mysql_incremental_table(
            spark,
            "drivers",
            "updated_at",
            mysql_watermark_from,
            mysql_watermark_to,
        )

        warehouse_drivers, rejected_drivers = \
            transform_drivers(drivers)

        input_row_count = drivers.count()
        staged_row_count = warehouse_drivers.count()
        rejected_row_count = rejected_drivers.count()

        batch_id = start_batch("driver", input_row_count, rejected_row_count, mysql_watermark_from, mysql_watermark_to)

        stage_drivers(
            warehouse_drivers,
            batch_id,
        )

        loaded_row_count = merge_drivers(batch_id)

        complete_batch(
            batch_id,
            loaded_row_count,
        )

        print("Batch ID:", batch_id)
        print("Staged drivers:", staged_row_count)
        print("Loaded driver versions:", loaded_row_count)

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
