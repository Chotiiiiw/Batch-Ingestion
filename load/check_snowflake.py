import snowflake.connector

from load.snowflake_config import load_snowflake_config


def check_snowflake_connection():
    config = load_snowflake_config()

    connection = snowflake.connector.connect(
        account=config["SNOWFLAKE_ACCOUNT"],
        user=config["SNOWFLAKE_USER"],
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(config["SNOWFLAKE_PRIVATE_KEY_PATH"]),
        private_key_file_pwd=config[
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        ],
        role=config["SNOWFLAKE_ROLE"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"],
    )

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA()
                """
            )

            user, role, warehouse, database, schema = cursor.fetchone()

    print("Connected to Snowflake")
    print("User:", user)
    print("Role:", role)
    print("Warehouse:", warehouse)
    print("Database:", database)
    print("Schema:", schema)


if __name__ == "__main__":
    check_snowflake_connection()
