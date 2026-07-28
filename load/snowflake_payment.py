from pyspark.sql.functions import lit

from load.snowflake_common import connect_snowflake, write_snowflake_staging


def stage_payments(warehouse_payments, batch_id):
    staging_payments = warehouse_payments \
        .withColumn(
            "batch_id",
            lit(batch_id),
        ) \
        .select(
            "batch_id",
            "payment_id",
            "order_id",
            "customer_id",
            "restaurant_id",
            "payment_method",
            "payment_created_date_key",
            "paid_date_key",
            "payment_status",
            "amount",
            "transaction_ref",
            "payment_created_at",
            "paid_at",
            "source_updated_at",
        )

    write_snowflake_staging(
        staging_payments,
        "PAYMENT",
    )


def merge_payments(batch_id):
    with connect_snowflake() as connection:
        with connection.cursor() as cursor:
            # 1. Stop if any payment matches multiple dimension rows.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT SOURCE.PAYMENT_ID
                    FROM STAGING.PAYMENT AS SOURCE
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = SOURCE.CUSTOMER_ID
                     AND SOURCE.PAYMENT_CREATED_AT
                         >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR SOURCE.PAYMENT_CREATED_AT
                                < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = SOURCE.RESTAURANT_ID
                     AND SOURCE.PAYMENT_CREATED_AT
                         >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR SOURCE.PAYMENT_CREATED_AT
                                < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_PAYMENT_METHOD AS METHOD
                      ON METHOD.PAYMENT_METHOD_CODE
                         = SOURCE.PAYMENT_METHOD
                    LEFT JOIN WAREHOUSE.DIM_DATE AS CREATED_DATE
                      ON CREATED_DATE.DATE_KEY
                         = SOURCE.PAYMENT_CREATED_DATE_KEY
                    LEFT JOIN WAREHOUSE.DIM_DATE AS PAID_DATE
                      ON PAID_DATE.DATE_KEY = SOURCE.PAID_DATE_KEY
                    WHERE SOURCE.BATCH_ID = %s
                    GROUP BY SOURCE.PAYMENT_ID
                    HAVING COUNT(*) > 1
                )
                """,
                (batch_id,),
            )

            ambiguous_payment_count = cursor.fetchone()[0]

            if ambiguous_payment_count > 0:
                raise RuntimeError(
                    "Ambiguous dimension rows found for payment facts"
                )


            # 2. Update newer lifecycle states and repair unknown keys.
            cursor.execute(
                """
                UPDATE WAREHOUSE.FACT_PAYMENT AS TARGET
                SET
                    ORDER_ID = SOURCE.ORDER_ID,
                    CUSTOMER_KEY = SOURCE.CUSTOMER_KEY,
                    RESTAURANT_KEY = SOURCE.RESTAURANT_KEY,
                    PAYMENT_METHOD_KEY = SOURCE.PAYMENT_METHOD_KEY,
                    PAYMENT_CREATED_DATE_KEY
                        = SOURCE.RESOLVED_CREATED_DATE_KEY,
                    PAID_DATE_KEY = SOURCE.RESOLVED_PAID_DATE_KEY,
                    PAYMENT_STATUS = SOURCE.PAYMENT_STATUS,
                    AMOUNT = SOURCE.AMOUNT,
                    TRANSACTION_REF = SOURCE.TRANSACTION_REF,
                    PAYMENT_CREATED_AT = SOURCE.PAYMENT_CREATED_AT,
                    PAID_AT = SOURCE.PAID_AT,
                    SOURCE_UPDATED_AT = SOURCE.SOURCE_UPDATED_AT,
                    BATCH_ID = SOURCE.BATCH_ID,
                    LOADED_AT = CURRENT_TIMESTAMP()
                FROM (
                    SELECT
                        STAGED.*,
                        COALESCE(CUSTOMER.CUSTOMER_KEY, 0)
                            AS CUSTOMER_KEY,
                        COALESCE(RESTAURANT.RESTAURANT_KEY, 0)
                            AS RESTAURANT_KEY,
                        COALESCE(METHOD.PAYMENT_METHOD_KEY, 0)
                            AS PAYMENT_METHOD_KEY,
                        COALESCE(CREATED_DATE.DATE_KEY, 0)
                            AS RESOLVED_CREATED_DATE_KEY,
                        COALESCE(PAID_DATE.DATE_KEY, 0)
                            AS RESOLVED_PAID_DATE_KEY
                    FROM STAGING.PAYMENT AS STAGED
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = STAGED.CUSTOMER_ID
                     AND STAGED.PAYMENT_CREATED_AT
                         >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR STAGED.PAYMENT_CREATED_AT
                                < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = STAGED.RESTAURANT_ID
                     AND STAGED.PAYMENT_CREATED_AT
                         >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR STAGED.PAYMENT_CREATED_AT
                                < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_PAYMENT_METHOD AS METHOD
                      ON METHOD.PAYMENT_METHOD_CODE
                         = STAGED.PAYMENT_METHOD
                    LEFT JOIN WAREHOUSE.DIM_DATE AS CREATED_DATE
                      ON CREATED_DATE.DATE_KEY
                         = STAGED.PAYMENT_CREATED_DATE_KEY
                    LEFT JOIN WAREHOUSE.DIM_DATE AS PAID_DATE
                      ON PAID_DATE.DATE_KEY = STAGED.PAID_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE TARGET.PAYMENT_ID = SOURCE.PAYMENT_ID
                  AND SOURCE.SOURCE_UPDATED_AT
                      >= TARGET.SOURCE_UPDATED_AT
                  AND (
                        SOURCE.SOURCE_UPDATED_AT
                            > TARGET.SOURCE_UPDATED_AT
                        OR SOURCE.CUSTOMER_KEY
                            <> TARGET.CUSTOMER_KEY
                        OR SOURCE.RESTAURANT_KEY
                            <> TARGET.RESTAURANT_KEY
                        OR SOURCE.PAYMENT_METHOD_KEY
                            <> TARGET.PAYMENT_METHOD_KEY
                        OR SOURCE.RESOLVED_CREATED_DATE_KEY
                            <> TARGET.PAYMENT_CREATED_DATE_KEY
                        OR SOURCE.RESOLVED_PAID_DATE_KEY
                            <> TARGET.PAID_DATE_KEY
                      )
                """,
                (batch_id,),
            )

            updated_row_count = cursor.rowcount


            # 3. Insert payment records that have never been loaded.
            cursor.execute(
                """
                INSERT INTO WAREHOUSE.FACT_PAYMENT (
                    PAYMENT_ID,
                    ORDER_ID,
                    CUSTOMER_KEY,
                    RESTAURANT_KEY,
                    PAYMENT_METHOD_KEY,
                    PAYMENT_CREATED_DATE_KEY,
                    PAID_DATE_KEY,
                    PAYMENT_STATUS,
                    AMOUNT,
                    TRANSACTION_REF,
                    PAYMENT_CREATED_AT,
                    PAID_AT,
                    SOURCE_UPDATED_AT,
                    BATCH_ID
                )
                SELECT
                    SOURCE.PAYMENT_ID,
                    SOURCE.ORDER_ID,
                    SOURCE.CUSTOMER_KEY,
                    SOURCE.RESTAURANT_KEY,
                    SOURCE.PAYMENT_METHOD_KEY,
                    SOURCE.RESOLVED_CREATED_DATE_KEY,
                    SOURCE.RESOLVED_PAID_DATE_KEY,
                    SOURCE.PAYMENT_STATUS,
                    SOURCE.AMOUNT,
                    SOURCE.TRANSACTION_REF,
                    SOURCE.PAYMENT_CREATED_AT,
                    SOURCE.PAID_AT,
                    SOURCE.SOURCE_UPDATED_AT,
                    SOURCE.BATCH_ID
                FROM (
                    SELECT
                        STAGED.*,
                        COALESCE(CUSTOMER.CUSTOMER_KEY, 0)
                            AS CUSTOMER_KEY,
                        COALESCE(RESTAURANT.RESTAURANT_KEY, 0)
                            AS RESTAURANT_KEY,
                        COALESCE(METHOD.PAYMENT_METHOD_KEY, 0)
                            AS PAYMENT_METHOD_KEY,
                        COALESCE(CREATED_DATE.DATE_KEY, 0)
                            AS RESOLVED_CREATED_DATE_KEY,
                        COALESCE(PAID_DATE.DATE_KEY, 0)
                            AS RESOLVED_PAID_DATE_KEY
                    FROM STAGING.PAYMENT AS STAGED
                    LEFT JOIN WAREHOUSE.DIM_CUSTOMER AS CUSTOMER
                      ON CUSTOMER.CUSTOMER_ID = STAGED.CUSTOMER_ID
                     AND STAGED.PAYMENT_CREATED_AT
                         >= CUSTOMER.VALID_FROM
                     AND (
                            CUSTOMER.VALID_TO IS NULL
                            OR STAGED.PAYMENT_CREATED_AT
                                < CUSTOMER.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_RESTAURANT AS RESTAURANT
                      ON RESTAURANT.RESTAURANT_ID
                         = STAGED.RESTAURANT_ID
                     AND STAGED.PAYMENT_CREATED_AT
                         >= RESTAURANT.VALID_FROM
                     AND (
                            RESTAURANT.VALID_TO IS NULL
                            OR STAGED.PAYMENT_CREATED_AT
                                < RESTAURANT.VALID_TO
                         )
                    LEFT JOIN WAREHOUSE.DIM_PAYMENT_METHOD AS METHOD
                      ON METHOD.PAYMENT_METHOD_CODE
                         = STAGED.PAYMENT_METHOD
                    LEFT JOIN WAREHOUSE.DIM_DATE AS CREATED_DATE
                      ON CREATED_DATE.DATE_KEY
                         = STAGED.PAYMENT_CREATED_DATE_KEY
                    LEFT JOIN WAREHOUSE.DIM_DATE AS PAID_DATE
                      ON PAID_DATE.DATE_KEY = STAGED.PAID_DATE_KEY
                    WHERE STAGED.BATCH_ID = %s
                ) AS SOURCE
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM WAREHOUSE.FACT_PAYMENT AS TARGET
                    WHERE TARGET.PAYMENT_ID = SOURCE.PAYMENT_ID
                )
                """,
                (batch_id,),
            )

            inserted_row_count = cursor.rowcount


            # 4. Clean only the successfully merged staging batch.
            cursor.execute(
                """
                DELETE FROM STAGING.PAYMENT
                WHERE BATCH_ID = %s
                """,
                (batch_id,),
            )


    return updated_row_count + inserted_row_count
