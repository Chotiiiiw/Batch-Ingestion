import sys
from load.snowflake_common import connect_snowflake

# Data quality check. The rows that are not meet the requirements will be counted. 

EXPECTED_BATCH_COUNT = 8

CHECKS = {
    "staging_tables_are_empty": """
        SELECT
            (SELECT COUNT(*) FROM STAGING.CUSTOMER)
            + (SELECT COUNT(*) FROM STAGING.RESTAURANT)
            + (SELECT COUNT(*) FROM STAGING.DRIVER)
            + (SELECT COUNT(*) FROM STAGING.MENU_ITEM)
            + (SELECT COUNT(*) FROM STAGING.ORDERS)
            + (SELECT COUNT(*) FROM STAGING.ORDER_ITEM)
            + (SELECT COUNT(*) FROM STAGING.PAYMENT)
            + (SELECT COUNT(*) FROM STAGING.DELIVERY)
    """,
    "customer_has_at_most_one_current_version": """
        SELECT COUNT(*)
        FROM (
            SELECT CUSTOMER_ID
            FROM WAREHOUSE.DIM_CUSTOMER
            WHERE IS_CURRENT = TRUE
            GROUP BY CUSTOMER_ID
            HAVING COUNT(*) > 1
        ) AS DUPLICATE_CURRENT_CUSTOMERS
    """,
    "restaurant_has_at_most_one_current_version": """
        SELECT COUNT(*)
        FROM (
            SELECT RESTAURANT_ID
            FROM WAREHOUSE.DIM_RESTAURANT
            WHERE IS_CURRENT = TRUE
            GROUP BY RESTAURANT_ID
            HAVING COUNT(*) > 1
        ) AS DUPLICATE_CURRENT_RESTAURANTS
    """,
    "driver_has_at_most_one_current_version": """
        SELECT COUNT(*)
        FROM (
            SELECT DRIVER_ID
            FROM WAREHOUSE.DIM_DRIVER
            WHERE IS_CURRENT = TRUE
            GROUP BY DRIVER_ID
            HAVING COUNT(*) > 1
        ) AS DUPLICATE_CURRENT_DRIVERS
    """,
    "current_customer_email_is_unique": """
        SELECT COUNT(*)
        FROM (
            SELECT LOWER(TRIM(EMAIL)) AS NORMALIZED_EMAIL
            FROM WAREHOUSE.DIM_CUSTOMER
            WHERE IS_CURRENT = TRUE
            GROUP BY LOWER(TRIM(EMAIL))
            HAVING COUNT(DISTINCT CUSTOMER_ID) > 1
        ) AS DUPLICATE_CURRENT_CUSTOMER_EMAILS
    """,
    "current_driver_number_plate_is_unique": """
        SELECT COUNT(*)
        FROM (
            SELECT UPPER(TRIM(NUMBER_PLATE)) AS NORMALIZED_NUMBER_PLATE
            FROM WAREHOUSE.DIM_DRIVER
            WHERE IS_CURRENT = TRUE
            GROUP BY UPPER(TRIM(NUMBER_PLATE))
            HAVING COUNT(DISTINCT DRIVER_ID) > 1
        ) AS DUPLICATE_CURRENT_DRIVER_NUMBER_PLATES
    """,
    "customer_contacts_are_present": """
        SELECT COUNT(*)
        FROM WAREHOUSE.DIM_CUSTOMER
        WHERE EMAIL IS NULL
           OR LENGTH(TRIM(EMAIL)) = 0
           OR PHONE IS NULL
           OR LENGTH(TRIM(PHONE)) = 0
    """,
    "driver_identity_fields_are_present": """
        SELECT COUNT(*)
        FROM WAREHOUSE.DIM_DRIVER
        WHERE PHONE IS NULL
           OR LENGTH(TRIM(PHONE)) = 0
           OR NUMBER_PLATE IS NULL
           OR LENGTH(TRIM(NUMBER_PLATE)) = 0
    """,
    "menu_item_has_at_most_one_current_version": """
        SELECT COUNT(*)
        FROM (
            SELECT MENU_ITEM_ID
            FROM WAREHOUSE.DIM_MENU_ITEM
            WHERE IS_CURRENT = TRUE
            GROUP BY MENU_ITEM_ID
            HAVING COUNT(*) > 1
        ) AS DUPLICATE_CURRENT_MENU_ITEMS
    """,
    "current_customer_has_null_valid_to": """
        SELECT COUNT(*)
        FROM WAREHOUSE.DIM_CUSTOMER
        WHERE IS_CURRENT = TRUE
          AND VALID_TO IS NOT NULL
    """,
    "current_restaurant_has_null_valid_to": """
        SELECT COUNT(*)
        FROM WAREHOUSE.DIM_RESTAURANT
        WHERE IS_CURRENT = TRUE
          AND VALID_TO IS NOT NULL
    """,
    "current_driver_has_null_valid_to": """
        SELECT COUNT(*)
        FROM WAREHOUSE.DIM_DRIVER
        WHERE IS_CURRENT = TRUE
          AND VALID_TO IS NOT NULL
    """,
    "current_menu_item_has_null_valid_to": """
        SELECT COUNT(*)
        FROM WAREHOUSE.DIM_MENU_ITEM
        WHERE IS_CURRENT = TRUE
          AND VALID_TO IS NOT NULL
    """,
    "fact_order_foreign_keys_are_not_null": """
        SELECT COUNT(*)
        FROM WAREHOUSE.FACT_ORDER
        WHERE CUSTOMER_KEY IS NULL
           OR RESTAURANT_KEY IS NULL
           OR ORDER_DATE_KEY IS NULL
           OR BATCH_ID IS NULL
    """,
    "fact_order_delivery_address_is_present": """
        SELECT COUNT(*)
        FROM WAREHOUSE.FACT_ORDER
        WHERE DELIVERY_ADDRESS IS NULL
           OR LENGTH(TRIM(DELIVERY_ADDRESS)) = 0
    """,
    "fact_order_item_foreign_keys_are_not_null": """
        SELECT COUNT(*)
        FROM WAREHOUSE.FACT_ORDER_ITEM
        WHERE CUSTOMER_KEY IS NULL
           OR RESTAURANT_KEY IS NULL
           OR MENU_ITEM_KEY IS NULL
           OR ORDER_DATE_KEY IS NULL
           OR BATCH_ID IS NULL
    """,
    "fact_payment_foreign_keys_are_not_null": """
        SELECT COUNT(*)
        FROM WAREHOUSE.FACT_PAYMENT
        WHERE CUSTOMER_KEY IS NULL
           OR RESTAURANT_KEY IS NULL
           OR PAYMENT_METHOD_KEY IS NULL
           OR PAYMENT_CREATED_DATE_KEY IS NULL
           OR PAID_DATE_KEY IS NULL
           OR BATCH_ID IS NULL
    """,
    "fact_delivery_foreign_keys_are_not_null": """
        SELECT COUNT(*)
        FROM WAREHOUSE.FACT_DELIVERY_ATTEMPT
        WHERE CUSTOMER_KEY IS NULL
           OR RESTAURANT_KEY IS NULL
           OR DRIVER_KEY IS NULL
           OR ASSIGNED_DATE_KEY IS NULL
           OR BATCH_ID IS NULL
    """,
    "latest_etl_batches_succeeded": f"""
        SELECT
            COALESCE(
                SUM(
                    CASE
                        WHEN BATCH_STATUS IN ('SUCCEEDED', 'PARTIAL') THEN 0
                        ELSE 1
                    END
                ),
                0
            )
            + GREATEST(
                {EXPECTED_BATCH_COUNT} - COUNT(*),
                0
            )
        FROM (
            SELECT BATCH_STATUS
            FROM WAREHOUSE.ETL_BATCH
            ORDER BY STARTED_AT DESC
            LIMIT {EXPECTED_BATCH_COUNT}
        ) AS LATEST_BATCHES
    """,
}


def main():
    failed_checks = []

    try:
        with connect_snowflake() as connection:
            with connection.cursor() as cursor:
                for check_name, sql in CHECKS.items():
                    cursor.execute(sql)
                    violation_count = cursor.fetchone()[0]

                    if violation_count == 0:
                        print(f"[PASS] {check_name}")
                    else:
                        print(
                            f"[FAIL] {check_name}: "
                            f"{violation_count} violation(s)"
                        )
                        failed_checks.append(check_name)

    except Exception as error:
        print(f"[ERROR] Quality checks could not run: {error}")
        return 1

    if failed_checks:
        print("Failed checks:", ", ".join(failed_checks))
        return 1

    print("All data-quality checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
