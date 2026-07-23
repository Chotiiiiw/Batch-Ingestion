CREATE DATABASE IF NOT EXISTS food_delivery_source
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE food_delivery_source;


CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    INDEX idx_customers_updated_at (updated_at)
) ENGINE = InnoDB;


CREATE TABLE restaurants (
    restaurant_id BIGINT PRIMARY KEY,
    restaurant_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    address VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    INDEX idx_restaurants_updated_at (updated_at)
) ENGINE = InnoDB;


CREATE TABLE drivers (
    driver_id BIGINT PRIMARY KEY,
    driver_name VARCHAR(255) NOT NULL,
    number_plate VARCHAR(50) NOT NULL,
    driver_status VARCHAR(50) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    INDEX idx_drivers_updated_at (updated_at)
) ENGINE = InnoDB;


CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    restaurant_id BIGINT NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    subtotal DECIMAL(12, 2) NOT NULL,
    discount DECIMAL(12, 2) NOT NULL,
    delivery_fee DECIMAL(12, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    ordered_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id),

    CONSTRAINT fk_orders_restaurant
        FOREIGN KEY (restaurant_id)
        REFERENCES restaurants (restaurant_id),

    INDEX idx_orders_customer (customer_id),
    INDEX idx_orders_restaurant (restaurant_id),
    INDEX idx_orders_ordered_at (ordered_at),
    INDEX idx_orders_updated_at (updated_at)
) ENGINE = InnoDB;


CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    menu_item_id VARCHAR(50) NOT NULL,
    menu_item_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    total_price DECIMAL(12, 2) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),

    INDEX idx_order_items_order (order_id),
    INDEX idx_order_items_menu (menu_item_id),
    INDEX idx_order_items_updated_at (updated_at)
) ENGINE = InnoDB;


CREATE TABLE payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    payment_status VARCHAR(50) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    transaction_ref VARCHAR(100),
    paid_at DATETIME,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),

    INDEX idx_payments_order (order_id),
    INDEX idx_payments_transaction_ref (transaction_ref),
    INDEX idx_payments_updated_at (updated_at)
) ENGINE = InnoDB;


CREATE TABLE deliveries (
    delivery_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    driver_id BIGINT NOT NULL,
    delivery_status VARCHAR(50) NOT NULL,
    distance_km DECIMAL(8, 2) NOT NULL,
    assigned_at DATETIME NOT NULL,
    picked_up_at DATETIME,
    delivered_at DATETIME,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,

    CONSTRAINT fk_deliveries_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),

    CONSTRAINT fk_deliveries_driver
        FOREIGN KEY (driver_id)
        REFERENCES drivers (driver_id),

    INDEX idx_deliveries_order (order_id),
    INDEX idx_deliveries_driver (driver_id),
    INDEX idx_deliveries_updated_at (updated_at)
) ENGINE = InnoDB;