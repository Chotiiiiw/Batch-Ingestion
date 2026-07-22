BEGIN;

SET search_path TO warehouse, public;


-- Bootstap 

INSERT INTO etl_batch (
    batch_id,
    started_at,
    completed_at,
    batch_status
)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    'SUCCEEDED'
)
ON CONFLICT (batch_id) DO NOTHING;


-- Unknown SCD

INSERT INTO dim_customer (
    customer_key,
    customer_id,
    full_name,
    email,
    city,
    valid_from,
    hash_diff,
    source_updated_at,
    batch_id
)
VALUES (
    0,
    0,
    'Unknown',
    'unknown@invalid',
    'Unknown',
    '-infinity',
    REPEAT('0', 64),
    '-infinity',
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (customer_key) DO NOTHING;


INSERT INTO dim_restaurant (
    restaurant_key,
    restaurant_id,
    restaurant_name,
    category,
    city,
    status,
    valid_from,
    hash_diff,
    source_updated_at,
    batch_id
)
VALUES (
    0,
    0,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    '-infinity',
    REPEAT('0', 64),
    '-infinity',
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (restaurant_key) DO NOTHING;


INSERT INTO dim_menu_item (
    menu_item_key,
    menu_item_id,
    restaurant_id,
    menu_item_name,
    category,
    base_price,
    available,
    valid_from,
    hash_diff,
    source_updated_at,
    batch_id
)
VALUES (
    0,
    'UNKNOWN',
    0,
    'Unknown',
    'Unknown',
    0.00,
    FALSE,
    '-infinity',
    REPEAT('0', 64),
    '-infinity',
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (menu_item_key) DO NOTHING;


INSERT INTO dim_driver (
    driver_key,
    driver_id,
    driver_name,
    driver_status,
    valid_from,
    hash_diff,
    source_updated_at,
    batch_id
)
VALUES (
    0,
    0,
    'Unknown',
    'Unknown',
    '-infinity',
    REPEAT('0', 64),
    '-infinity',
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (driver_key) DO NOTHING;


-- Payment methods


-- The unknown payment method must explicitly use surrogate key 0.
INSERT INTO dim_payment_method (
    payment_method_key,
    payment_method_code,
    payment_method_name
)
VALUES (
    0,
    'UNKNOWN',
    'Unknown'
)
ON CONFLICT (payment_method_key) DO NOTHING;


-- Normal payment methods use the identity default for payment_method_key.
INSERT INTO dim_payment_method (
    payment_method_code,
    payment_method_name
)
VALUES
    ('CARD', 'Card'),
    ('PROMPTPAY', 'PromptPay'),
    ('CASH', 'Cash'),
    ('WALLET', 'Wallet')
ON CONFLICT (payment_method_code) DO NOTHING;


-- Date dimension

-- Key 0 represents a missing or unknown date. Other fields remain NULL.
INSERT INTO dim_date (
    date_key
)
VALUES (
    0
)
ON CONFLICT (date_key) DO NOTHING;


-- Generate one calendar row per day.
INSERT INTO dim_date (
    date_key,
    full_date,
    day_of_month,
    day_of_week,
    day_name,
    week_of_year,
    month_number,
    month_name,
    quarter_number,
    year_number,
    is_weekend
)
SELECT
    TO_CHAR(calendar_day, 'YYYYMMDD')::INTEGER,
    calendar_day,
    EXTRACT(DAY FROM calendar_day)::SMALLINT,
    EXTRACT(ISODOW FROM calendar_day)::SMALLINT,
    TO_CHAR(calendar_day, 'FMDay'),
    EXTRACT(WEEK FROM calendar_day)::SMALLINT,
    EXTRACT(MONTH FROM calendar_day)::SMALLINT,
    TO_CHAR(calendar_day, 'FMMonth'),
    EXTRACT(QUARTER FROM calendar_day)::SMALLINT,
    EXTRACT(YEAR FROM calendar_day)::INTEGER,
    EXTRACT(ISODOW FROM calendar_day) IN (6, 7)
FROM GENERATE_SERIES(
    DATE '2020-01-01',
    DATE '2035-12-31',
    INTERVAL '1 day'
) AS generated_dates(calendar_timestamp)
CROSS JOIN LATERAL (
    SELECT calendar_timestamp::DATE AS calendar_day
) AS calendar
ON CONFLICT (date_key) DO NOTHING;


COMMIT;