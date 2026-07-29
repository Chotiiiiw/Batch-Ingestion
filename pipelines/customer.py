"""Run the incremental customer pipeline from MySQL to Snowflake."""

from load.snowflake_common import complete_batch, fail_batch, get_last_mysql_watermark, start_batch
from load.snowflake_customer import merge_customers, stage_customers
from transform.common import create_spark, get_mysql_watermark_to, read_mysql_incremental_table
from transform.customer import transform_customers

def main():
    spark = create_spark("snowflake-customer-pipeline")
    batch_id = None

    try:
        # 1. Define the source window for this incremental batch.
        mysql_watermark_from = get_last_mysql_watermark("customer")
        mysql_watermark_to = get_mysql_watermark_to(
            spark,
            "customers",
            "updated_at",
        )


        # 2. Extract only customers inside the source window.
        customers = read_mysql_incremental_table(
            spark,
            "customers",
            "updated_at",
            mysql_watermark_from,
            mysql_watermark_to,
        )


        # 3. Transform and validate customers.
        warehouse_customers, rejected_customers = \
            transform_customers(customers)

        input_row_count = customers.count()
        staged_row_count = warehouse_customers.count()
        rejected_row_count = rejected_customers.count()


        # 4. Create the audit batch before writing to staging.
        batch_id = start_batch("customer", input_row_count, rejected_row_count, mysql_watermark_from, mysql_watermark_to)


        # 5. Write the valid DataFrame to Snowflake staging.
        stage_customers(
            warehouse_customers,
            batch_id,
        )


        # 6. Apply SCD Type 2 changes in Snowflake.
        loaded_row_count = merge_customers(batch_id)


        # 7. Mark the batch as successful.
        complete_batch(
            batch_id,
            loaded_row_count,
        )


        print("Batch ID:", batch_id)
        print("Staged customers:", staged_row_count)
        print("Loaded customer versions:", loaded_row_count)

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
