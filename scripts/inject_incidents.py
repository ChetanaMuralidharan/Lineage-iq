from __future__ import annotations

import argparse
import json
from datetime import timedelta
from typing import Any

from observability.incident_repository import (
    create_incident,
    get_incident,
    mark_resolved,
    utc_now,
)
from observability.snowflake_client import snowflake_cursor


DATABASE = "PIPELINE_PLATFORM"
DBT_SCHEMA = "DBT_DEV"
OBSERVABILITY_SCHEMA = "OBSERVABILITY"

FACT_LINEITEM = f"{DATABASE}.{DBT_SCHEMA}.FACT_LINEITEM"
FACT_LINEITEM_BACKUP = (
    f"{DATABASE}.{OBSERVABILITY_SCHEMA}.F3_BACKUP_FACT_LINEITEM"
)

FACT_ORDERS = f"{DATABASE}.{DBT_SCHEMA}.FACT_ORDERS"
FACT_ORDERS_BACKUP = (
    f"{DATABASE}.{OBSERVABILITY_SCHEMA}.F3_BACKUP_FACT_ORDERS"
)

DIM_CUSTOMER = f"{DATABASE}.{DBT_SCHEMA}.DIM_CUSTOMER"
DIM_CUSTOMER_BACKUP = (
    f"{DATABASE}.{OBSERVABILITY_SCHEMA}.F3_BACKUP_DIM_CUSTOMER"
)

SCHEMA_RENAME_OLD_COLUMN = "MARKET_SEGMENT"
SCHEMA_RENAME_NEW_COLUMN = "MKTSEGMENT"

TYPE_CHANGE_TABLE = FACT_ORDERS
TYPE_CHANGE_BACKUP = (
    f"{DATABASE}.{OBSERVABILITY_SCHEMA}.F3_BACKUP_FACT_ORDERS_TYPE_CHANGE"
)
TYPE_CHANGE_COLUMN = "TOTAL_PRICE"
TYPE_CHANGE_TARGET_TYPE = "VARCHAR(100)"

FRESHNESS_OVERRIDE_TABLE = (
    f"{DATABASE}.{OBSERVABILITY_SCHEMA}.FRESHNESS_SIMULATION_OVERRIDES"
)
SLA_MODEL_NAME = "FACT_ORDERS"
SLA_HOURS = 24


def query_scalar(
    sql: str,
    parameters: tuple[Any, ...] | None = None,
) -> Any:
    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(sql, parameters or ())
        row = cursor.fetchone()

    return row[0] if row else None


def split_table_name(
    fully_qualified_table: str,
) -> tuple[str, str, str]:
    parts = fully_qualified_table.split(".")

    if len(parts) != 3:
        raise ValueError(
            "Table name must use the format DATABASE.SCHEMA.TABLE."
        )

    return (
        parts[0].upper(),
        parts[1].upper(),
        parts[2].upper(),
    )


def table_exists(
    fully_qualified_table: str,
) -> bool:
    database, schema, table = split_table_name(
        fully_qualified_table
    )

    count = query_scalar(
        f"""
        SELECT COUNT(*)
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """,
        (schema, table),
    )

    return bool(count)


def column_exists(
    fully_qualified_table: str,
    column_name: str,
) -> bool:
    database, schema, table = split_table_name(
        fully_qualified_table
    )

    count = query_scalar(
        f"""
        SELECT COUNT(*)
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (
            schema,
            table,
            column_name.upper(),
        ),
    )

    return bool(count)


def get_column_names(
    fully_qualified_table: str,
) -> list[str]:
    database, schema, table = split_table_name(
        fully_qualified_table
    )

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table),
        )
        rows = cursor.fetchall()

    return [str(row[0]) for row in rows]


def get_column_type(
    fully_qualified_table: str,
    column_name: str,
) -> dict[str, Any]:
    """
    Return Snowflake metadata for one table column.
    """

    database, schema, table = split_table_name(
        fully_qualified_table
    )

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            SELECT
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (
                schema.upper(),
                table.upper(),
                column_name.upper(),
            ),
        )

        row = cursor.fetchone()

    if row is None:
        raise ValueError(
            f"Column '{column_name}' was not found in "
            f"{fully_qualified_table}."
        )

    return {
        "data_type": row[0],
        "character_maximum_length": row[1],
        "numeric_precision": row[2],
        "numeric_scale": row[3],
        "is_nullable": row[4],
    }

def format_column_type(
    column_type: dict[str, Any],
) -> str:
    """
    Format Snowflake column metadata as a readable type.
    """

    data_type = str(
        column_type.get("data_type", "")
    ).upper()

    if data_type == "NUMBER":
        precision = column_type.get(
            "numeric_precision"
        )

        scale = column_type.get(
            "numeric_scale"
        )

        if precision is not None and scale is not None:
            return f"NUMBER({precision},{scale})"

    if data_type in {"TEXT", "VARCHAR"}:
        length = column_type.get(
            "character_maximum_length"
        )

        if length is not None:
            return f"VARCHAR({length})"

    return data_type

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            SELECT
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (
                schema,
                table,
                column_name.upper(),
            ),
        )
        row = cursor.fetchone()

    if row is None:
        raise ValueError(
            f"Column {column_name} was not found in "
            f"{fully_qualified_table}."
        )

    return {
        "data_type": row[0],
        "character_maximum_length": row[1],
        "numeric_precision": row[2],
        "numeric_scale": row[3],
        "is_nullable": row[4],
    }


def get_row_count(
    fully_qualified_table: str,
) -> int:
    value = query_scalar(
        f"SELECT COUNT(*) FROM {fully_qualified_table}"
    )
    return int(value or 0)


def get_null_count(
    fully_qualified_table: str,
    column_name: str,
) -> int:
    value = query_scalar(
        f"""
        SELECT COUNT(*)
        FROM {fully_qualified_table}
        WHERE {column_name} IS NULL
        """
    )
    return int(value or 0)


def create_zero_copy_backup(
    source_table: str,
    backup_table: str,
) -> None:
    if table_exists(backup_table):
        raise RuntimeError(
            f"Backup table already exists: {backup_table}. "
            "Restore the previous incident first."
        )

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            CREATE TRANSIENT TABLE {backup_table}
            CLONE {source_table}
            """
        )


def restore_from_backup(
    target_table: str,
    backup_table: str,
) -> None:
    if not table_exists(backup_table):
        raise RuntimeError(
            f"Backup table does not exist: {backup_table}"
        )

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            CREATE OR REPLACE TRANSIENT TABLE {target_table}
            CLONE {backup_table}
            """
        )


def drop_backup(
    backup_table: str,
) -> None:
    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"DROP TABLE IF EXISTS {backup_table}"
        )


def ensure_freshness_override_table() -> None:
    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FRESHNESS_OVERRIDE_TABLE} (
                MODEL_NAME VARCHAR NOT NULL,
                SIMULATED_LAST_UPDATED_AT TIMESTAMP_NTZ NOT NULL,
                SLA_HOURS INTEGER NOT NULL,
                ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
                INCIDENT_ID VARCHAR,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )


def inject_volume_drop(
    *,
    drop_fraction: float = 0.25,
) -> str:
    if not 0 < drop_fraction < 1:
        raise ValueError(
            "drop_fraction must be between 0 and 1."
        )

    original_count = get_row_count(FACT_LINEITEM)

    if original_count == 0:
        raise RuntimeError(
            f"{FACT_LINEITEM} is empty."
        )

    create_zero_copy_backup(
        FACT_LINEITEM,
        FACT_LINEITEM_BACKUP,
    )

    divisor = max(round(1 / drop_fraction), 2)

    try:
        with snowflake_cursor(
            database=DATABASE,
            schema=DBT_SCHEMA,
        ) as (_, cursor):
            cursor.execute(
                f"""
                DELETE FROM {FACT_LINEITEM}
                WHERE MOD(ORDER_KEY, %s) = 0
                """,
                (divisor,),
            )
            deleted_rows = int(cursor.rowcount or 0)

        current_count = get_row_count(FACT_LINEITEM)
        actual_drop_fraction = (
            original_count - current_count
        ) / original_count

        incident_id = create_incident(
            incident_type="volume_anomaly",
            affected_model="FACT_LINEITEM",
            severity="HIGH",
            incident_details={
                "scenario": "volume_drop",
                "target_table": FACT_LINEITEM,
                "backup_table": FACT_LINEITEM_BACKUP,
                "requested_drop_fraction": drop_fraction,
                "actual_drop_fraction": actual_drop_fraction,
                "original_row_count": original_count,
                "current_row_count": current_count,
                "deleted_rows": deleted_rows,
                "restore_required": True,
            },
        )

    except Exception:
        restore_from_backup(
            FACT_LINEITEM,
            FACT_LINEITEM_BACKUP,
        )
        drop_backup(FACT_LINEITEM_BACKUP)
        raise

    print("=" * 72)
    print("VOLUME DROP INJECTED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Original rows     : {original_count:,}")
    print(f"Deleted rows      : {deleted_rows:,}")
    print(f"Remaining rows    : {current_count:,}")
    print(f"Actual drop       : {actual_drop_fraction:.2%}")
    return incident_id


def restore_volume_drop(
    incident_id: str,
) -> None:
    incident = get_incident(incident_id)

    if incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    if incident.incident_type != "volume_anomaly":
        raise ValueError(
            "Incident is not a volume anomaly."
        )

    if incident.detected_at is None:
        raise ValueError(
            "Mark the incident DETECTED before restoring."
        )

    details = incident.incident_details or {}
    original_count = int(
        details.get("original_row_count", 0)
    )

    restore_from_backup(
        FACT_LINEITEM,
        FACT_LINEITEM_BACKUP,
    )

    restored_count = get_row_count(FACT_LINEITEM)

    if restored_count != original_count:
        raise RuntimeError(
            "Volume restoration validation failed."
        )

    mark_resolved(
        incident_id,
        resolution_details={
            "restoration_method": "snowflake_zero_copy_clone",
            "restored_row_count": restored_count,
            "restore_validated": True,
            "restore_required": False,
        },
    )
    drop_backup(FACT_LINEITEM_BACKUP)

    print("=" * 72)
    print("VOLUME DROP RESTORED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Restored rows     : {restored_count:,}")


def inject_null_surge(
    *,
    null_fraction: float = 0.30,
) -> str:
    if not 0 < null_fraction < 1:
        raise ValueError(
            "null_fraction must be between 0 and 1."
        )

    original_count = get_row_count(FACT_ORDERS)
    original_null_count = get_null_count(
        FACT_ORDERS,
        "CUSTOMER_KEY",
    )

    create_zero_copy_backup(
        FACT_ORDERS,
        FACT_ORDERS_BACKUP,
    )

    divisor = max(round(1 / null_fraction), 2)

    try:
        with snowflake_cursor(
            database=DATABASE,
            schema=DBT_SCHEMA,
        ) as (_, cursor):
            cursor.execute(
                f"""
                UPDATE {FACT_ORDERS}
                SET CUSTOMER_KEY = NULL
                WHERE MOD(ORDER_KEY, %s) = 0
                """,
                (divisor,),
            )
            updated_rows = int(cursor.rowcount or 0)

        current_null_count = get_null_count(
            FACT_ORDERS,
            "CUSTOMER_KEY",
        )
        newly_created_nulls = (
            current_null_count - original_null_count
        )
        actual_fraction = (
            current_null_count / original_count
        )

        incident_id = create_incident(
            incident_type="null_surge",
            affected_model="FACT_ORDERS",
            severity="HIGH",
            incident_details={
                "scenario": "null_surge",
                "target_table": FACT_ORDERS,
                "target_column": "CUSTOMER_KEY",
                "backup_table": FACT_ORDERS_BACKUP,
                "requested_null_fraction": null_fraction,
                "actual_total_null_fraction": actual_fraction,
                "original_row_count": original_count,
                "original_null_count": original_null_count,
                "current_null_count": current_null_count,
                "newly_created_nulls": newly_created_nulls,
                "updated_rows": updated_rows,
                "restore_required": True,
            },
        )

    except Exception:
        restore_from_backup(
            FACT_ORDERS,
            FACT_ORDERS_BACKUP,
        )
        drop_backup(FACT_ORDERS_BACKUP)
        raise

    print("=" * 72)
    print("NULL SURGE INJECTED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Updated rows      : {updated_rows:,}")
    print(f"Current nulls     : {current_null_count:,}")
    print(f"Null rate         : {actual_fraction:.2%}")
    return incident_id


def restore_null_surge(
    incident_id: str,
) -> None:
    incident = get_incident(incident_id)

    if incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    if incident.incident_type != "null_surge":
        raise ValueError(
            "Incident is not a null surge."
        )

    if incident.detected_at is None:
        raise ValueError(
            "Mark the incident DETECTED before restoring."
        )

    details = incident.incident_details or {}
    original_count = int(
        details.get("original_row_count", 0)
    )
    original_null_count = int(
        details.get("original_null_count", 0)
    )

    restore_from_backup(
        FACT_ORDERS,
        FACT_ORDERS_BACKUP,
    )

    restored_count = get_row_count(FACT_ORDERS)
    restored_null_count = get_null_count(
        FACT_ORDERS,
        "CUSTOMER_KEY",
    )

    if restored_count != original_count:
        raise RuntimeError(
            "Null-surge row-count restoration failed."
        )

    if restored_null_count != original_null_count:
        raise RuntimeError(
            "Null-surge null-count restoration failed."
        )

    mark_resolved(
        incident_id,
        resolution_details={
            "restoration_method": "snowflake_zero_copy_clone",
            "restored_row_count": restored_count,
            "restored_null_count": restored_null_count,
            "restore_validated": True,
            "restore_required": False,
        },
    )
    drop_backup(FACT_ORDERS_BACKUP)

    print("=" * 72)
    print("NULL SURGE RESTORED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Restored rows     : {restored_count:,}")
    print(f"Restored nulls    : {restored_null_count:,}")


def inject_schema_rename() -> str:
    if not column_exists(
        DIM_CUSTOMER,
        SCHEMA_RENAME_OLD_COLUMN,
    ):
        raise RuntimeError(
            f"{SCHEMA_RENAME_OLD_COLUMN} was not found."
        )

    if column_exists(
        DIM_CUSTOMER,
        SCHEMA_RENAME_NEW_COLUMN,
    ):
        raise RuntimeError(
            f"{SCHEMA_RENAME_NEW_COLUMN} already exists."
        )

    original_row_count = get_row_count(DIM_CUSTOMER)
    original_columns = get_column_names(DIM_CUSTOMER)

    create_zero_copy_backup(
        DIM_CUSTOMER,
        DIM_CUSTOMER_BACKUP,
    )

    try:
        with snowflake_cursor(
            database=DATABASE,
            schema=DBT_SCHEMA,
        ) as (_, cursor):
            cursor.execute(
                f"""
                ALTER TABLE {DIM_CUSTOMER}
                RENAME COLUMN {SCHEMA_RENAME_OLD_COLUMN}
                TO {SCHEMA_RENAME_NEW_COLUMN}
                """
            )

        current_columns = get_column_names(DIM_CUSTOMER)

        incident_id = create_incident(
            incident_type="schema_drift",
            affected_model="DIM_CUSTOMER",
            severity="CRITICAL",
            incident_details={
                "scenario": "schema_column_rename",
                "change_type": "column_rename",
                "target_table": DIM_CUSTOMER,
                "backup_table": DIM_CUSTOMER_BACKUP,
                "old_column_name": SCHEMA_RENAME_OLD_COLUMN,
                "new_column_name": SCHEMA_RENAME_NEW_COLUMN,
                "original_row_count": original_row_count,
                "original_columns": original_columns,
                "current_columns": current_columns,
                "restore_required": True,
            },
        )

    except Exception:
        restore_from_backup(
            DIM_CUSTOMER,
            DIM_CUSTOMER_BACKUP,
        )
        drop_backup(DIM_CUSTOMER_BACKUP)
        raise

    print("=" * 72)
    print("SCHEMA RENAME INJECTED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(
        f"Column            : {SCHEMA_RENAME_OLD_COLUMN} "
        f"-> {SCHEMA_RENAME_NEW_COLUMN}"
    )
    return incident_id


def restore_schema_rename(
    incident_id: str,
) -> None:
    incident = get_incident(incident_id)

    if incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    details = incident.incident_details or {}

    if details.get("scenario") != "schema_column_rename":
        raise ValueError(
            "Incident is not the schema rename scenario."
        )

    if incident.detected_at is None:
        raise ValueError(
            "Mark the incident DETECTED before restoring."
        )

    original_row_count = int(
        details.get("original_row_count", 0)
    )
    original_columns = [
        str(value).upper()
        for value in details.get("original_columns", [])
    ]

    restore_from_backup(
        DIM_CUSTOMER,
        DIM_CUSTOMER_BACKUP,
    )

    restored_count = get_row_count(DIM_CUSTOMER)
    restored_columns = get_column_names(DIM_CUSTOMER)

    if restored_count != original_row_count:
        raise RuntimeError(
            "Schema-rename row-count restoration failed."
        )

    if [x.upper() for x in restored_columns] != original_columns:
        raise RuntimeError(
            "Schema-rename column restoration failed."
        )

    mark_resolved(
        incident_id,
        resolution_details={
            "restoration_method": "snowflake_zero_copy_clone",
            "restored_row_count": restored_count,
            "restored_columns": restored_columns,
            "restore_validated": True,
            "restore_required": False,
        },
    )
    drop_backup(DIM_CUSTOMER_BACKUP)

    print("=" * 72)
    print("SCHEMA RENAME RESTORED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Restored rows     : {restored_count:,}")


def inject_type_change() -> str:
    """
    Recreate FACT_ORDERS with TOTAL_PRICE cast from NUMBER
    to VARCHAR.

    Snowflake does not support every direct ALTER COLUMN
    conversion from NUMBER to VARCHAR, so this scenario
    safely rebuilds the table from a zero-copy backup.
    """

    if not table_exists(TYPE_CHANGE_TABLE):
        raise RuntimeError(
            f"Target table does not exist: {TYPE_CHANGE_TABLE}"
        )

    if not column_exists(
        TYPE_CHANGE_TABLE,
        TYPE_CHANGE_COLUMN,
    ):
        raise RuntimeError(
            f"{TYPE_CHANGE_COLUMN} was not found in "
            f"{TYPE_CHANGE_TABLE}."
        )

    original_type = get_column_type(
        TYPE_CHANGE_TABLE,
        TYPE_CHANGE_COLUMN,
    )

    original_row_count = get_row_count(
        TYPE_CHANGE_TABLE
    )

    if str(original_type["data_type"]).upper() in {
        "TEXT",
        "VARCHAR",
    }:
        raise RuntimeError(
            f"{TYPE_CHANGE_COLUMN} is already a text type."
        )

    create_zero_copy_backup(
        TYPE_CHANGE_TABLE,
        TYPE_CHANGE_BACKUP,
    )

    try:
        with snowflake_cursor(
            database=DATABASE,
            schema=DBT_SCHEMA,
        ) as (_, cursor):
            cursor.execute(
                f"""
                CREATE OR REPLACE TRANSIENT TABLE
                    {TYPE_CHANGE_TABLE}
                AS
                SELECT
                    ORDER_KEY,
                    CUSTOMER_KEY,
                    ORDER_STATUS,
                    TO_VARCHAR(TOTAL_PRICE) AS TOTAL_PRICE,
                    ORDER_DATE,
                    ORDER_PRIORITY,
                    SHIP_PRIORITY
                FROM {TYPE_CHANGE_BACKUP}
                """
            )

        changed_type = get_column_type(
            TYPE_CHANGE_TABLE,
            TYPE_CHANGE_COLUMN,
        )

        current_row_count = get_row_count(
            TYPE_CHANGE_TABLE
        )

        if str(changed_type["data_type"]).upper() not in {
            "TEXT",
            "VARCHAR",
        }:
            raise RuntimeError(
                "Type-change validation failed. "
                f"Expected VARCHAR but found "
                f"{changed_type['data_type']}."
            )

        if current_row_count != original_row_count:
            raise RuntimeError(
                "Type-change row-count validation failed. "
                f"Expected {original_row_count:,} rows but "
                f"found {current_row_count:,}."
            )

        incident_id = create_incident(
            incident_type="schema_drift",
            affected_model="FACT_ORDERS",
            severity="CRITICAL",
            incident_details={
                "scenario": "schema_type_change",
                "change_type": "column_type_change",
                "target_table": TYPE_CHANGE_TABLE,
                "target_column": TYPE_CHANGE_COLUMN,
                "original_type": original_type,
                "changed_type": changed_type,
                "target_type_expression": (
                    TYPE_CHANGE_TARGET_TYPE
                ),
                "original_row_count": original_row_count,
                "current_row_count": current_row_count,
                "backup_table": TYPE_CHANGE_BACKUP,
                "implementation_method": (
                    "create_or_replace_table_as_select"
                ),
                "restore_required": True,
            },
        )

    except Exception:
        restore_from_backup(
            TYPE_CHANGE_TABLE,
            TYPE_CHANGE_BACKUP,
        )

        drop_backup(
            TYPE_CHANGE_BACKUP
        )

        raise

    print("=" * 72)
    print("SCHEMA TYPE CHANGE INJECTED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Target table      : {TYPE_CHANGE_TABLE}")
    print(f"Target column     : {TYPE_CHANGE_COLUMN}")
    print(
        f"Original type     : "
        f"{format_column_type(original_type)}"
    )
    print(
        f"Changed type      : "
        f"{format_column_type(changed_type)}"
    )
    print(f"Row count         : {current_row_count:,}")
    print(f"Backup table      : {TYPE_CHANGE_BACKUP}")
    print()
    print(
        "The incident is currently INJECTED, "
        "not DETECTED."
    )

    return incident_id


def restore_type_change(
    incident_id: str,
) -> None:
    incident = get_incident(incident_id)

    if incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    details = incident.incident_details or {}

    if details.get("scenario") != "schema_type_change":
        raise ValueError(
            "Incident is not the schema type-change scenario."
        )

    if incident.detected_at is None:
        raise ValueError(
            "Mark the incident DETECTED before restoring."
        )

    original_row_count = int(
        details.get("original_row_count", 0)
    )
    original_type = details.get("original_type", {})

    restore_from_backup(
        TYPE_CHANGE_TABLE,
        TYPE_CHANGE_BACKUP,
    )

    restored_count = get_row_count(TYPE_CHANGE_TABLE)
    restored_type = get_column_type(
        TYPE_CHANGE_TABLE,
        TYPE_CHANGE_COLUMN,
    )

    if restored_count != original_row_count:
        raise RuntimeError(
            "Type-change row-count restoration failed."
        )

    if (
        str(restored_type["data_type"]).upper()
        != str(original_type.get("data_type")).upper()
    ):
        raise RuntimeError(
            "Type-change schema restoration failed."
        )

    mark_resolved(
        incident_id,
        resolution_details={
            "restoration_method": "snowflake_zero_copy_clone",
            "restored_row_count": restored_count,
            "restored_type": restored_type,
            "restore_validated": True,
            "restore_required": False,
        },
    )
    drop_backup(TYPE_CHANGE_BACKUP)

    print("=" * 72)
    print("SCHEMA TYPE CHANGE RESTORED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(
        f"Restored type     : {restored_type['data_type']}"
    )
    print(f"Restored rows     : {restored_count:,}")


def inject_sla_breach(
    *,
    stale_hours: int = 48,
) -> str:
    if stale_hours <= SLA_HOURS:
        raise ValueError(
            f"stale_hours must be greater than the "
            f"{SLA_HOURS}-hour SLA."
        )

    ensure_freshness_override_table()

    active_override_count = int(
        query_scalar(
            f"""
            SELECT COUNT(*)
            FROM {FRESHNESS_OVERRIDE_TABLE}
            WHERE MODEL_NAME = %s
              AND ACTIVE = TRUE
            """,
            (SLA_MODEL_NAME,),
        )
        or 0
    )

    if active_override_count > 0:
        raise RuntimeError(
            f"An active freshness override already exists for "
            f"{SLA_MODEL_NAME}."
        )

    simulated_last_updated_at = (
        utc_now() - timedelta(hours=stale_hours)
    )

    incident_id = create_incident(
        incident_type="sla_breach",
        affected_model=SLA_MODEL_NAME,
        severity="HIGH",
        incident_details={
            "scenario": "freshness_sla_breach",
            "target_model": SLA_MODEL_NAME,
            "sla_hours": SLA_HOURS,
            "simulated_stale_hours": stale_hours,
            "simulated_last_updated_at": (
                simulated_last_updated_at.isoformat()
            ),
            "override_table": FRESHNESS_OVERRIDE_TABLE,
            "restore_required": True,
            "simulation_note": (
                "Phase 1 freshness_monitor.py must use the active "
                "override timestamp when present; otherwise it uses "
                "Snowflake metadata."
            ),
        },
    )

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            INSERT INTO {FRESHNESS_OVERRIDE_TABLE} (
                MODEL_NAME,
                SIMULATED_LAST_UPDATED_AT,
                SLA_HOURS,
                ACTIVE,
                INCIDENT_ID,
                CREATED_AT,
                UPDATED_AT
            )
            VALUES (
                %s,
                %s,
                %s,
                TRUE,
                %s,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
            """,
            (
                SLA_MODEL_NAME,
                simulated_last_updated_at,
                SLA_HOURS,
                incident_id,
            ),
        )

    print("=" * 72)
    print("SLA BREACH INJECTED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Target model      : {SLA_MODEL_NAME}")
    print(f"SLA               : {SLA_HOURS} hours")
    print(f"Simulated age     : {stale_hours} hours")
    print(
        f"Simulated update  : {simulated_last_updated_at}"
    )
    print(
        "This uses a controlled freshness override. The Phase 1 "
        "freshness monitor will read it and mark the incident detected."
    )
    return incident_id


def restore_sla_breach(
    incident_id: str,
) -> None:
    incident = get_incident(incident_id)

    if incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    details = incident.incident_details or {}

    if details.get("scenario") != "freshness_sla_breach":
        raise ValueError(
            "Incident is not the SLA-breach scenario."
        )

    if incident.detected_at is None:
        raise ValueError(
            "Mark the incident DETECTED before restoring."
        )

    ensure_freshness_override_table()

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            UPDATE {FRESHNESS_OVERRIDE_TABLE}
            SET
                ACTIVE = FALSE,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE INCIDENT_ID = %s
              AND ACTIVE = TRUE
            """,
            (incident_id,),
        )

        if cursor.rowcount == 0:
            raise RuntimeError(
                "No active SLA simulation override was found."
            )

    active_count = int(
        query_scalar(
            f"""
            SELECT COUNT(*)
            FROM {FRESHNESS_OVERRIDE_TABLE}
            WHERE INCIDENT_ID = %s
              AND ACTIVE = TRUE
            """,
            (incident_id,),
        )
        or 0
    )

    if active_count != 0:
        raise RuntimeError(
            "SLA simulation restoration validation failed."
        )

    mark_resolved(
        incident_id,
        resolution_details={
            "restoration_method": "deactivate_freshness_override",
            "override_active": False,
            "restore_validated": True,
            "restore_required": False,
        },
    )

    print("=" * 72)
    print("SLA BREACH RESTORED")
    print("=" * 72)
    print(f"Incident ID       : {incident_id}")
    print(f"Target model      : {SLA_MODEL_NAME}")
    print("Override active   : no")


def show_status() -> None:
    ensure_freshness_override_table()

    type_info = (
        get_column_type(
            TYPE_CHANGE_TABLE,
            TYPE_CHANGE_COLUMN,
        )
        if table_exists(TYPE_CHANGE_TABLE)
        else None
    )

    sla_override = None

    with snowflake_cursor(
        database=DATABASE,
        schema=OBSERVABILITY_SCHEMA,
    ) as (_, cursor):
        cursor.execute(
            f"""
            SELECT
                INCIDENT_ID,
                SIMULATED_LAST_UPDATED_AT,
                SLA_HOURS,
                ACTIVE
            FROM {FRESHNESS_OVERRIDE_TABLE}
            WHERE MODEL_NAME = %s
            ORDER BY CREATED_AT DESC
            LIMIT 1
            """,
            (SLA_MODEL_NAME,),
        )
        row = cursor.fetchone()

    if row:
        sla_override = {
            "incident_id": row[0],
            "simulated_last_updated_at": str(row[1]),
            "sla_hours": row[2],
            "active": bool(row[3]),
        }

    result = {
        "volume_drop": {
            "target_row_count": get_row_count(FACT_LINEITEM),
            "backup_exists": table_exists(
                FACT_LINEITEM_BACKUP
            ),
        },
        "null_surge": {
            "target_row_count": get_row_count(FACT_ORDERS),
            "customer_key_null_count": get_null_count(
                FACT_ORDERS,
                "CUSTOMER_KEY",
            ),
            "backup_exists": table_exists(
                FACT_ORDERS_BACKUP
            ),
        },
        "schema_rename": {
            "original_column_exists": column_exists(
                DIM_CUSTOMER,
                SCHEMA_RENAME_OLD_COLUMN,
            ),
            "renamed_column_exists": column_exists(
                DIM_CUSTOMER,
                SCHEMA_RENAME_NEW_COLUMN,
            ),
            "backup_exists": table_exists(
                DIM_CUSTOMER_BACKUP
            ),
        },
        "schema_type_change": {
            "target_table": TYPE_CHANGE_TABLE,
            "target_column": TYPE_CHANGE_COLUMN,
            "current_type": type_info,
            "backup_exists": table_exists(
                TYPE_CHANGE_BACKUP
            ),
        },
        "sla_breach": {
            "target_model": SLA_MODEL_NAME,
            "sla_hours": SLA_HOURS,
            "latest_override": sla_override,
        },
    }

    print(json.dumps(result, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inject and restore controlled LineageIQ incidents."
        )
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    volume_parser = subparsers.add_parser(
        "inject-volume"
    )
    volume_parser.add_argument(
        "--fraction",
        type=float,
        default=0.25,
    )

    restore_volume_parser = subparsers.add_parser(
        "restore-volume"
    )
    restore_volume_parser.add_argument("incident_id")

    null_parser = subparsers.add_parser(
        "inject-null"
    )
    null_parser.add_argument(
        "--fraction",
        type=float,
        default=0.30,
    )

    restore_null_parser = subparsers.add_parser(
        "restore-null"
    )
    restore_null_parser.add_argument("incident_id")

    subparsers.add_parser(
        "inject-schema-rename"
    )

    restore_schema_parser = subparsers.add_parser(
        "restore-schema-rename"
    )
    restore_schema_parser.add_argument("incident_id")

    subparsers.add_parser(
        "inject-type-change"
    )

    restore_type_parser = subparsers.add_parser(
        "restore-type-change"
    )
    restore_type_parser.add_argument("incident_id")

    sla_parser = subparsers.add_parser(
        "inject-sla-breach"
    )
    sla_parser.add_argument(
        "--stale-hours",
        type=int,
        default=48,
    )

    restore_sla_parser = subparsers.add_parser(
        "restore-sla-breach"
    )
    restore_sla_parser.add_argument("incident_id")

    subparsers.add_parser("status")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "inject-volume":
        inject_volume_drop(
            drop_fraction=args.fraction,
        )
    elif args.command == "restore-volume":
        restore_volume_drop(args.incident_id)
    elif args.command == "inject-null":
        inject_null_surge(
            null_fraction=args.fraction,
        )
    elif args.command == "restore-null":
        restore_null_surge(args.incident_id)
    elif args.command == "inject-schema-rename":
        inject_schema_rename()
    elif args.command == "restore-schema-rename":
        restore_schema_rename(args.incident_id)
    elif args.command == "inject-type-change":
        inject_type_change()
    elif args.command == "restore-type-change":
        restore_type_change(args.incident_id)
    elif args.command == "inject-sla-breach":
        inject_sla_breach(
            stale_hours=args.stale_hours,
        )
    elif args.command == "restore-sla-breach":
        restore_sla_breach(args.incident_id)
    elif args.command == "status":
        show_status()


if __name__ == "__main__":
    main()