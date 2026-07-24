import os
from pathlib import Path

from dotenv import load_dotenv


REQUIRED_VARIABLES = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
)


def load_snowflake_config():
    load_dotenv()

    config = {
        variable: os.getenv(variable)
        for variable in REQUIRED_VARIABLES
    }

    missing_variables = []

    for variable in REQUIRED_VARIABLES:
        if not config[variable]:
            missing_variables.append(variable)

    if missing_variables:
        missing = ", ".join(missing_variables)
        raise ValueError(f"Missing environment variables: {missing}")

    private_key_path = Path(
        config["SNOWFLAKE_PRIVATE_KEY_PATH"]
    ).expanduser()

    if not private_key_path.is_file():
        raise FileNotFoundError(
            f"Snowflake private key not found: {private_key_path}"
        )

    config["SNOWFLAKE_PRIVATE_KEY_PATH"] = private_key_path

    return config
