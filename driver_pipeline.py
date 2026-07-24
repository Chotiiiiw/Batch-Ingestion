from load.common import complete_batch, fail_batch
from load.driver import merge_drivers,stage_drivers
from transform.common import create_spark,read_mysql_table
from transform.driver import transform_drivers


def main():
    spark = create_spark("driver-pipeline")
    batch_id = None

    try:
        # 1. Extract drivers from MySQL
        drivers = read_mysql_table(spark, "drivers")


        # 2. Transform and validate drivers
        warehouse_drivers, rejected_drivers = transform_drivers(drivers)


        # 3. Write the valid drivers to staging
        batch_id, staged_row_count = stage_drivers(drivers, warehouse_drivers, rejected_drivers)


        # 4. Merge staging into the driver dimension
        loaded_row_count = merge_drivers(batch_id)


        # 5. Complete the ETL batch
        complete_batch(batch_id,loaded_row_count)


        print("Batch ID:", batch_id)
        print("Staged drivers:", staged_row_count)
        print("Loaded driver versions:", loaded_row_count)

    except Exception as error:
        if batch_id is not None:
            fail_batch(batch_id, error)

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
