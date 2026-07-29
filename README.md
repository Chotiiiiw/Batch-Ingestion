# Food Delivery Batch Data Engineering Pipeline

A batch data platform built with MySQL, MongoDB, PySpark, Snowflake,
Airflow, and Docker.

## Repository layout

- `pipelines/`: executable incremental pipeline entry points
- `transform/`: Spark cleansing, validation, and warehouse projections
- `load/`: Snowflake staging and merge logic
- `quality/`: post-load warehouse checks
- `mysql/`: MySQL source schema and fixture loading
- `snowflake/`: Snowflake warehouse DDL and seed data
- `scripts/`: mock-data generation and connection diagnostics
- `airflow/dags/`: orchestration
- `docs/`: [schema design](docs/food_delivery_schema.md) and
  [data-quality contract](docs/DATA_QUALITY.md)

## Local entry points

Run commands from the repository root. Spark entry points require the project
root on `PYTHONPATH`:

```bash
PYTHONPATH=. spark-submit \
  --packages com.mysql:mysql-connector-j:8.4.0,org.mongodb.spark:mongo-spark-connector_2.12:10.7.0,net.snowflake:spark-snowflake_2.12:3.1.9,net.snowflake:snowflake-jdbc:3.28.0 \
  pipelines/customer.py
```

Generate deterministic source fixtures with:

```bash
python scripts/mock_data_generator.py --profile demo
```

Run post-load checks with:

```bash
python -m quality.check_warehouse
```
