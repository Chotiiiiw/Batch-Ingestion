import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
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


def load_private_key_for_spark(config):
    private_key_path = config["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = config["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]

    encrypted_key = private_key_path.read_bytes()

    private_key = serialization.load_pem_private_key(
        encrypted_key,
        password=passphrase.encode("utf-8"),
    )

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    pem_lines = private_key_pem.decode("utf-8").splitlines()
    key_lines = []

    for line in pem_lines:
        if "PRIVATE KEY" not in line:
            key_lines.append(line)

    return "".join(key_lines)


def build_snowflake_spark_options():
    config = load_snowflake_config()
    private_key = load_private_key_for_spark(config)

    return {
        "sfURL": (
            f"{config['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com"
        ),
        "sfUser": config["SNOWFLAKE_USER"],
        "sfRole": config["SNOWFLAKE_ROLE"],
        "sfWarehouse": config["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": config["SNOWFLAKE_DATABASE"],
        "sfSchema": config["SNOWFLAKE_SCHEMA"],
        "pem_private_key": private_key,
    }
