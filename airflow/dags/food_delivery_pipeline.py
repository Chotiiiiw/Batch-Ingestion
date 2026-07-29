from airflow.sdk import dag, task
from pendulum import datetime, duration


PROJECT_DIRECTORY = "/opt/airflow/project"

SPARK_PACKAGES = ",".join(
    [
        "com.mysql:mysql-connector-j:8.4.0",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.7.0",
        "net.snowflake:spark-snowflake_2.12:3.1.9",
        "net.snowflake:snowflake-jdbc:3.28.0",
    ]
)


def build_spark_submit_command(script_name):
    return (
        f"cd {PROJECT_DIRECTORY} && "
        f"spark-submit --packages {SPARK_PACKAGES} "
        f"{script_name}"
    )


@dag(
    dag_id="food_delivery_batch_pipeline",
    start_date=datetime(2026, 7, 1, tz="Asia/Bangkok"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
    },
    tags=[
        "spark",
        "snowflake",
        "batch",
    ],
)
def food_delivery_batch_pipeline():

    @task.bash
    def customer_pipeline():
        return build_spark_submit_command(
            "snowflake_customer_pipeline.py"
        )

    @task.bash
    def restaurant_pipeline():
        return build_spark_submit_command(
            "snowflake_restaurant_pipeline.py"
        )

    @task.bash
    def driver_pipeline():
        return build_spark_submit_command(
            "snowflake_driver_pipeline.py"
        )

    @task.bash
    def menu_item_pipeline():
        return build_spark_submit_command(
            "snowflake_menu_item_pipeline.py"
        )

    @task
    def dimensions_complete():
        pass

    @task.bash
    def order_pipeline():
        return build_spark_submit_command(
            "snowflake_order_pipeline.py"
        )

    @task.bash
    def order_item_pipeline():
        return build_spark_submit_command(
            "snowflake_order_item_pipeline.py"
        )

    @task.bash
    def payment_pipeline():
        return build_spark_submit_command(
            "snowflake_payment_pipeline.py"
        )

    @task.bash
    def delivery_pipeline():
        return build_spark_submit_command(
            "snowflake_delivery_pipeline.py"
        )

    @task.bash
    def data_quality_checks():
        return (
            f"cd {PROJECT_DIRECTORY} && "
            "python -m quality.check_warehouse"
        )

    customer = customer_pipeline()
    restaurant = restaurant_pipeline()
    driver = driver_pipeline()
    menu_item = menu_item_pipeline()

    dimensions_done = dimensions_complete()

    order = order_pipeline()
    order_item = order_item_pipeline()
    payment = payment_pipeline()
    delivery = delivery_pipeline()

    quality = data_quality_checks()

    [customer, restaurant, driver, menu_item] >> dimensions_done
    dimensions_done >> [order, order_item, payment, delivery]
    [order, order_item, payment, delivery] >> quality


food_delivery_batch_pipeline()
