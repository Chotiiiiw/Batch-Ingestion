from load.common import (
    complete_batch,
    fail_batch,
)
from load.customer import (
    merge_customers,
    stage_customers,
)
from transform.common import (
    create_spark,
    read_mysql_table,
)
from transform.customer import transform_customers


def main():
    spark = create_spark("customer-pipeline")
    batch_id = None

    try:
        # 1. Extract customers from MySQL
        customers = read_mysql_table(
            spark,
            "customers",
        )


        # 2. Transform and validate customers
        warehouse_customers, rejected_customers = \
            transform_customers(customers)


        # 3. Write the valid customers to staging
        batch_id, staged_row_count = stage_customers(
            customers,
            warehouse_customers,
            rejected_customers,
        )


        # 4. Merge staging into the customer dimension
        loaded_row_count = merge_customers(batch_id)


        # 5. Complete the ETL batch
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
