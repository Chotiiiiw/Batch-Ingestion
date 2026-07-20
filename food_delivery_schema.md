# Food Delivery Batch ETL — Example Database Schema

## Data stack

| Stage | Technology | Responsibility |
|---|---|---|
| Extract | MySQL | Transactional data: customers, restaurants, orders, payments, deliveries |
| Extract | MongoDB | Semi-structured menu catalog and menu options |
| Transform | Apache Spark | Validation, cleansing, flattening, joining, deduplication |
| Load | PostgreSQL | Analytical warehouse for reporting and BI |

```mermaid
flowchart LR
    MYSQL[(MySQL)] --> SPARK[Apache Spark]
    MONGO[(MongoDB)] --> SPARK
    SPARK --> PG[(PostgreSQL)]
```

## 1. MySQL source schema

MySQL stores transactional data. Every mutable table includes `created_at` and
`updated_at` so Spark can perform incremental extraction using a watermark.

### Entity relationship diagram

```mermaid
erDiagram
    CUSTOMERS ||--o{ ORDERS : places
    RESTAURANTS ||--o{ ORDERS : receives
    ORDERS ||--|{ ORDER_ITEMS : contains
    ORDERS ||--o{ PAYMENTS : has
    ORDERS ||--o{ DELIVERIES : has_attempts
    DRIVERS ||--o{ DELIVERIES : handles

    CUSTOMERS {
        bigint customer_id PK
        varchar full_name
        varchar email UK
        varchar phone
        varchar city
        datetime created_at
        datetime updated_at
    }

    RESTAURANTS {
        bigint restaurant_id PK
        varchar restaurant_name
        varchar category
        varchar city
        varchar address
        varchar status
        datetime created_at
        datetime updated_at
    }

    ORDERS {
        bigint order_id PK
        bigint customer_id FK
        bigint restaurant_id FK
        varchar order_status
        decimal subtotal
        decimal discount
        decimal delivery_fee
        decimal total_amount
        datetime ordered_at
        datetime created_at
        datetime updated_at
    }

    ORDER_ITEMS {
        bigint order_item_id PK
        bigint order_id FK
        varchar menu_item_id
        varchar menu_item_name
        int quantity
        decimal unit_price
        decimal total_price
        datetime created_at
        datetime updated_at
    }

    PAYMENTS {
        bigint payment_id PK
        bigint order_id FK
        varchar payment_method
        varchar payment_status
        decimal amount
        varchar transaction_ref
        datetime paid_at
        datetime created_at
        datetime updated_at
    }

    DRIVERS {
        bigint driver_id PK
        varchar driver_name
        varchar number_plate
        varchar driver_status
        datetime created_at
        datetime updated_at
    }

    DELIVERIES {
        bigint delivery_id PK
        bigint order_id FK
        bigint driver_id FK
        varchar delivery_status
        decimal distance_km
        datetime assigned_at
        datetime picked_up_at
        datetime delivered_at
        datetime created_at
        datetime updated_at
    }
```

### Cross-source identifier

- `order_items.menu_item_id` references a menu document in MongoDB.
- MySQL cannot enforce a foreign key for this reference. Spark must validate it
  during transformation.
- `order_items.menu_item_name` and `unit_price` are snapshots of values at the
  time of purchase.

## 2. MongoDB source schema

Collection: `menu_items`

```json
{
  "_id": "MENU-1001",
  "restaurantId": 101,
  "name": "Chicken Burger",
  "description": "Grilled chicken with cheese",
  "category": "Burger",
  "basePrice": 159.00,
  "available": true,
  "tags": ["popular", "chicken"],
  "options": [
    {
      "name": "Size",
      "required": true,
      "choices": [
        {"name": "Regular", "extraPrice": 0},
        {"name": "Large", "extraPrice": 40}
      ]
    },
    {
      "name": "Extra topping",
      "required": false,
      "choices": [
        {"name": "Cheese", "extraPrice": 20},
        {"name": "Egg", "extraPrice": 15}
      ]
    }
  ],
  "createdAt": "2026-07-01T08:00:00Z",
  "updatedAt": "2026-07-19T10:30:00Z"
}
```

Spark flattens the nested document into these logical datasets:

```mermaid
erDiagram
    MENU_ITEMS ||--o{ MENU_OPTIONS : has
    MENU_OPTIONS ||--o{ MENU_OPTION_CHOICES : contains

    MENU_ITEMS {
        varchar menu_item_id PK
        bigint restaurant_id
        varchar name
        varchar category
        decimal base_price
        boolean available
        timestamp updated_at
    }

    MENU_OPTIONS {
        varchar menu_option_id PK
        varchar menu_item_id FK
        varchar option_name
        boolean required
    }

    MENU_OPTION_CHOICES {
        varchar choice_id PK
        varchar menu_option_id FK
        varchar choice_name
        decimal extra_price
    }
```

## 3. PostgreSQL warehouse schema

The target uses a star schema. Surrogate keys are used for dimensions while
source identifiers are retained for traceability.

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_ORDERS : groups
    DIM_CUSTOMER ||--o{ FACT_ORDERS : places
    DIM_RESTAURANT ||--o{ FACT_ORDERS : receives
    FACT_ORDERS ||--|{ FACT_ORDER_ITEMS : contains
    DIM_MENU_ITEM ||--o{ FACT_ORDER_ITEMS : identifies
    FACT_ORDERS ||--o{ FACT_PAYMENTS : has
    FACT_ORDERS ||--o{ FACT_DELIVERIES : has_attempts
    DIM_DRIVER ||--o{ FACT_DELIVERIES : handles

    DIM_DATE {
        int date_key PK
        date full_date
        int day
        int month
        int quarter
        int year
        boolean is_weekend
    }

    DIM_CUSTOMER {
        bigint customer_key PK
        bigint customer_id UK
        varchar full_name
        varchar city
        timestamp effective_from
        timestamp effective_to
        boolean is_current
    }

    DIM_RESTAURANT {
        bigint restaurant_key PK
        bigint restaurant_id
        varchar restaurant_name
        varchar category
        varchar city
        varchar address
        varchar status
        timestamp effective_from
        timestamp effective_to
        boolean is_current
    }

    DIM_MENU_ITEM {
        bigint menu_item_key PK
        varchar menu_item_id
        bigint restaurant_id
        varchar menu_item_name
        varchar category
        decimal base_price
        boolean available
    }

    DIM_DRIVER {
        bigint driver_key PK
        bigint driver_id UK
        varchar driver_name
        varchar number_plate
    }

    FACT_ORDERS {
        bigint order_id PK
        int date_key FK
        bigint customer_key FK
        bigint restaurant_key FK
        varchar order_status
        decimal subtotal
        decimal discount
        decimal delivery_fee
        decimal total_amount
        timestamp ordered_at
        varchar batch_id
        timestamp loaded_at
    }

    FACT_ORDER_ITEMS {
        bigint order_item_id PK
        bigint order_id FK
        bigint menu_item_key FK
        int quantity
        decimal unit_price
        decimal total_price
    }

    FACT_PAYMENTS {
        bigint payment_id PK
        bigint order_id FK
        varchar payment_method
        varchar payment_status
        decimal amount
        timestamp paid_at
    }

    FACT_DELIVERIES {
        bigint delivery_id PK
        bigint order_id FK
        bigint driver_key FK
        varchar delivery_status
        decimal distance_km
        timestamp assigned_at
        timestamp picked_up_at
        timestamp delivered_at
        int delivery_minutes
    }
```

### Fact table grain

| Fact table | One row represents |
|---|---|
| `fact_orders` | One food order |
| `fact_order_items` | One menu item line within an order |
| `fact_payments` | One payment or refund transaction |
| `fact_deliveries` | One delivery attempt for an order |

## 4. Incremental extraction fields

| Source | Incremental field | Suggested extraction |
|---|---|---|
| MySQL | `updated_at` | `last_watermark < updated_at <= current_watermark` |
| MongoDB | `updatedAt` | The same half-open watermark pattern |

Each Spark run should create a `batch_id`, store its watermark range, and record
its result. Reprocessing the same batch must not create duplicate warehouse
rows.
