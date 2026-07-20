USE food_delivery_source;

START TRANSACTION;


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/customers.csv'
INTO TABLE customers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @customer_id,
    @full_name,
    @email,
    @phone,
    @city,
    @created_at,
    @updated_at
)
SET
    customer_id = @customer_id,
    full_name = @full_name,
    email = @email,
    phone = NULLIF(@phone, ''),
    city = @city,
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/restaurants.csv'
INTO TABLE restaurants
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @restaurant_id,
    @restaurant_name,
    @category,
    @city,
    @address,
    @status,
    @created_at,
    @updated_at
)
SET
    restaurant_id = @restaurant_id,
    restaurant_name = @restaurant_name,
    category = @category,
    city = @city,
    address = NULLIF(@address, ''),
    status = @status,
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/drivers.csv'
INTO TABLE drivers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @driver_id,
    @driver_name,
    @number_plate,
    @driver_status,
    @created_at,
    @updated_at
)
SET
    driver_id = @driver_id,
    driver_name = @driver_name,
    number_plate = NULLIF(@number_plate, ''),
    driver_status = @driver_status,
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/orders.csv'
INTO TABLE orders
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @order_id,
    @customer_id,
    @restaurant_id,
    @order_status,
    @subtotal,
    @discount,
    @delivery_fee,
    @total_amount,
    @ordered_at,
    @created_at,
    @updated_at
)
SET
    order_id = @order_id,
    customer_id = @customer_id,
    restaurant_id = @restaurant_id,
    order_status = @order_status,
    subtotal = @subtotal,
    discount = @discount,
    delivery_fee = @delivery_fee,
    total_amount = @total_amount,
    ordered_at = STR_TO_DATE(
        @ordered_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/order_items.csv'
INTO TABLE order_items
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @order_item_id,
    @order_id,
    @menu_item_id,
    @menu_item_name,
    @quantity,
    @unit_price,
    @total_price,
    @created_at,
    @updated_at
)
SET
    order_item_id = @order_item_id,
    order_id = @order_id,
    menu_item_id = @menu_item_id,
    menu_item_name = @menu_item_name,
    quantity = @quantity,
    unit_price = @unit_price,
    total_price = @total_price,
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/payments.csv'
INTO TABLE payments
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @payment_id,
    @order_id,
    @payment_method,
    @payment_status,
    @amount,
    @transaction_ref,
    @paid_at,
    @created_at,
    @updated_at
)
SET
    payment_id = @payment_id,
    order_id = @order_id,
    payment_method = @payment_method,
    payment_status = @payment_status,
    amount = @amount,
    transaction_ref = NULLIF(@transaction_ref, ''),
    paid_at = STR_TO_DATE(
        NULLIF(@paid_at, ''),
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


LOAD DATA INFILE '/var/lib/mysql-files/mock-data/deliveries.csv'
INTO TABLE deliveries
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @delivery_id,
    @order_id,
    @driver_id,
    @delivery_status,
    @distance_km,
    @assigned_at,
    @picked_up_at,
    @delivered_at,
    @created_at,
    @updated_at
)
SET
    delivery_id = @delivery_id,
    order_id = @order_id,
    driver_id = @driver_id,
    delivery_status = @delivery_status,
    distance_km = @distance_km,
    assigned_at = STR_TO_DATE(
        @assigned_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    picked_up_at = STR_TO_DATE(
        NULLIF(@picked_up_at, ''),
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    delivered_at = STR_TO_DATE(
        NULLIF(@delivered_at, ''),
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    created_at = STR_TO_DATE(
        @created_at,
        '%Y-%m-%dT%H:%i:%sZ'
    ),
    updated_at = STR_TO_DATE(
        @updated_at,
        '%Y-%m-%dT%H:%i:%sZ'
    );


COMMIT;