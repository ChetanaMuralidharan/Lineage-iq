from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


load_dotenv()

os.environ.setdefault("SF_USE_OPENSSL_ONLY", "false")


def get_snowflake_connection(
    *,
    database: str | None = None,
    schema: str | None = None,
) -> SnowflakeConnection:
    """
    Create a Snowflake connection using environment variables.

    Optional database and schema values override the environment defaults.
    """

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]

    missing = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing)
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database or os.environ["SNOWFLAKE_DATABASE"],
        schema=schema or os.getenv("SNOWFLAKE_SCHEMA", "OBSERVABILITY"),
        autocommit=False,
    )


@contextmanager
def snowflake_cursor(
    *,
    database: str | None = None,
    schema: str | None = None,
) -> Generator[
    tuple[SnowflakeConnection, SnowflakeCursor],
    None,
    None,
]:
    """
    Provide a Snowflake connection and cursor with automatic commit,
    rollback, and cleanup.
    """

    connection = get_snowflake_connection(
        database=database,
        schema=schema,
    )

    cursor = connection.cursor()

    try:
        yield connection, cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()