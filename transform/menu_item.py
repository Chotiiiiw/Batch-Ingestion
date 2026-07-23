from pyspark.sql.functions import *
from common import (
    create_spark,
    read_mongo_collection,
    read_mysql_table,
)


def transform_menu_items(menu_items, restaurants):
    # 1. Support both current and legacy price names
    current_price = \
        col("basePrice") \
        if "basePrice" in menu_items.columns \
        else lit(None)

    legacy_price = \
        col("base_price") \
        if "base_price" in menu_items.columns \
        else lit(None)


    # 2. Clean and cast source columns
    cleaned_menu_items = menu_items \
        .withColumn(
            "menu_item_id",
            trim(col("_id")),
        ) \
        .withColumn(
            "restaurant_id",
            col("restaurantId").cast("long"),
        ) \
        .withColumn(
            "menu_item_name",
            trim(col("name")),
        ) \
        .withColumn(
            "category",
            initcap(lower(trim(col("category")))),
        ) \
        .withColumn(
            "base_price",
            coalesce(
                current_price.cast("decimal(12,2)"),
                legacy_price.cast("decimal(12,2)"),
            ),
        ) \
        .withColumn(
            "available",
            col("available").cast("boolean"),
        ) \
        .withColumn(
            "tags",
            array_sort(
                coalesce(
                    col("tags"),
                    array().cast("array<string>"),
                )
            ),
        ) \
        .withColumn(
            "created_at",
            to_timestamp(col("createdAt")),
        ) \
        .withColumn(
            "updated_at",
            to_timestamp(col("updatedAt")),
        )


    # 3. Validate that each menu belongs to a source restaurant
    restaurant_lookup = restaurants \
        .select(
            col("restaurant_id")
            .cast("long")
            .alias("known_restaurant_id")
        ) \
        .dropDuplicates(["known_restaurant_id"])

    joined_menu_items = cleaned_menu_items.join(
        restaurant_lookup,
        cleaned_menu_items.restaurant_id
        == restaurant_lookup.known_restaurant_id,
        "left",
    )


    # 4. Assign the first applicable rejection reason
    checked_menu_items = joined_menu_items.withColumn(
        "rejection_reason",
        when(
            col("menu_item_id").isNull()
            | (length(col("menu_item_id")) == 0),
            lit("INVALID_MENU_ITEM_ID"),
        )
        .when(
            col("restaurant_id").isNull()
            | (col("restaurant_id") <= 0),
            lit("INVALID_RESTAURANT_ID"),
        )
        .when(
            col("known_restaurant_id").isNull(),
            lit("RESTAURANT_NOT_FOUND"),
        )
        .when(
            col("menu_item_name").isNull()
            | (length(col("menu_item_name")) == 0),
            lit("MISSING_MENU_ITEM_NAME"),
        )
        .when(
            col("category").isNull()
            | (length(col("category")) == 0),
            lit("MISSING_CATEGORY"),
        )
        .when(
            col("base_price").isNull()
            | (col("base_price") < 0),
            lit("INVALID_BASE_PRICE"),
        )
        .when(
            col("available").isNull(),
            lit("INVALID_AVAILABLE_FLAG"),
        )
        .when(
            col("created_at").isNull(),
            lit("INVALID_CREATED_AT"),
        )
        .when(
            col("updated_at").isNull(),
            lit("INVALID_UPDATED_AT"),
        )
        .when(
            col("updated_at") < col("created_at"),
            lit("UPDATED_BEFORE_CREATED"),
        ),
    )


    # 5. Separate valid and rejected rows
    rejected_menu_items = checked_menu_items \
        .filter(col("rejection_reason").isNotNull())

    valid_menu_items = checked_menu_items \
        .filter(col("rejection_reason").isNull()) \
        .dropDuplicates(["menu_item_id"])


    # 6. Calculate the SCD hash
    valid_menu_items = valid_menu_items.withColumn(
        "hash_diff",
        sha2(
            concat_ws(
                "||",
                col("restaurant_id").cast("string"),
                col("menu_item_name"),
                col("category"),
                col("base_price").cast("string"),
                col("available").cast("string"),
                array_join(col("tags"), ","),
            ),
            256,
        ),
    )


    # 7. Prepare warehouse column names
    warehouse_menu_items = valid_menu_items.select(
        "menu_item_id",
        "restaurant_id",
        "menu_item_name",
        "category",
        "base_price",
        "available",
        "tags",
        col("created_at").alias("valid_from"),
        "hash_diff",
        col("updated_at").alias("source_updated_at"),
    )


    return warehouse_menu_items, rejected_menu_items


def main():
    # Start session
    spark = create_spark("transform-menu-items")


    # 1. Read menu items from MongoDB
    menu_items = read_mongo_collection(
        spark,
        "menu_items",
    )


    # 2. Read restaurant lookup from MySQL
    restaurants = read_mysql_table(
        spark,
        "restaurants",
    )


    # 3. Transform menu items
    warehouse_menu_items, rejected_menu_items = \
        transform_menu_items(
            menu_items,
            restaurants,
        )


    # 4. Check the results
    print("Raw menu items:", menu_items.count())
    print("Valid menu items:", warehouse_menu_items.count())
    print("Rejected menu items:", rejected_menu_items.count())

    warehouse_menu_items.show(10, truncate=False)
    rejected_menu_items.select(
        "menu_item_id",
        "rejection_reason",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
