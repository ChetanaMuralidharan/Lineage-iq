from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from observability.incident_repository import create_incident, mark_detected
from observability.snowflake_client import snowflake_cursor
from uuid import uuid4

DATABASE = "PIPELINE_PLATFORM"
MODEL_SCHEMA = "DBT_DEV"
OBSERVABILITY_SCHEMA = "OBSERVABILITY"
FRESHNESS_CHECKS_TABLE = f"{DATABASE}.{OBSERVABILITY_SCHEMA}.FRESHNESS_CHECKS"
FRESHNESS_OVERRIDE_TABLE = (
    f"{DATABASE}.{OBSERVABILITY_SCHEMA}.FRESHNESS_SIMULATION_OVERRIDES"
)
INCIDENTS_TABLE = f"{DATABASE}.{OBSERVABILITY_SCHEMA}.INCIDENTS"

# SLA values are intentionally explicit and version-controlled.
MODEL_SLAS_HOURS: dict[str, int] = {
    "STG_ORDERS": 24,
    "STG_LINEITEM": 24,
    "STG_CUSTOMER": 48,
    "STG_SUPPLIER": 48,
    "STG_PART": 48,
    "DIM_CUSTOMER": 48,
    "DIM_SUPPLIER": 48,
    "DIM_PART": 48,
    "FACT_ORDERS": 24,
    "FACT_LINEITEM": 24,
}


@dataclass(frozen=True)
class FreshnessResult:
    model_name: str
    last_updated_at: datetime
    sla_hours: int
    age_hours: float
    is_breached: bool
    source: str
    incident_id: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def normalize_to_utc(value: datetime) -> datetime:
    """
    Convert a datetime to timezone-aware UTC.

    Snowflake may return TIMESTAMP_NTZ values as naive datetimes and
    metadata timestamps such as LAST_ALTERED as timezone-aware datetimes.
    This normalizes both forms before comparison.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)

def create_freshness_tables(cursor: Any) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FRESHNESS_CHECKS_TABLE} (
            CHECK_ID VARCHAR DEFAULT UUID_STRING(),
            MODEL_NAME VARCHAR NOT NULL,
            LAST_UPDATED_AT TIMESTAMP_NTZ NOT NULL,
            SLA_HOURS INTEGER NOT NULL,
            AGE_HOURS FLOAT NOT NULL,
            IS_BREACHED BOOLEAN NOT NULL,
            FRESHNESS_SOURCE VARCHAR NOT NULL,
            INCIDENT_ID VARCHAR,
            CHECKED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_FRESHNESS_CHECKS PRIMARY KEY (CHECK_ID)
        )
        """
    )

    # The incident harness creates this too; keeping it here makes the monitor
    # independently deployable and idempotent.
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


def _get_last_updated(cursor: Any, model_name: str) -> tuple[datetime, int, str, str | None]:
    """Return effective timestamp, SLA, source, and optional injected incident."""
    configured_sla = MODEL_SLAS_HOURS[model_name]

    cursor.execute(
        f"""
        SELECT
            SIMULATED_LAST_UPDATED_AT,
            SLA_HOURS,
            INCIDENT_ID
        FROM {FRESHNESS_OVERRIDE_TABLE}
        WHERE UPPER(MODEL_NAME) = %s
          AND ACTIVE = TRUE
        QUALIFY ROW_NUMBER() OVER (ORDER BY UPDATED_AT DESC, CREATED_AT DESC) = 1
        """,
        (model_name,),
    )
    override = cursor.fetchone()
    if override:
        return override[0], int(override[1]), "TEST_OVERRIDE", override[2]

    # LAST_ALTERED is available in INFORMATION_SCHEMA.TABLES. It is more
    # appropriate than TABLE_STORAGE_METRICS for this metadata check.
    cursor.execute(
        f"""
        SELECT LAST_ALTERED
        FROM {DATABASE}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """,
        (MODEL_SCHEMA, model_name),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise RuntimeError(f"Could not find metadata for {MODEL_SCHEMA}.{model_name}.")

    return row[0], configured_sla, "INFORMATION_SCHEMA.TABLES", None


def _find_open_incident(cursor: Any, model_name: str) -> str | None:
    cursor.execute(
        f"""
        SELECT INCIDENT_ID
        FROM {INCIDENTS_TABLE}
        WHERE INCIDENT_TYPE = 'sla_breach'
          AND UPPER(AFFECTED_MODEL) = %s
          AND DETECTED_AT IS NULL
        ORDER BY INJECTED_AT DESC
        LIMIT 1
        """,
        (model_name,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def collect_freshness_checks() -> list[FreshnessResult]:
    checked_at = utc_now()
    pending_detections: list[tuple[str, dict[str, Any]]] = []
    results: list[FreshnessResult] = []

    with snowflake_cursor(database=DATABASE, schema=OBSERVABILITY_SCHEMA) as (_, cursor):
        create_freshness_tables(cursor)

        for model_name in MODEL_SLAS_HOURS:
            last_updated_at, sla_hours, source, override_incident_id = _get_last_updated(
                cursor, model_name
            )

            last_updated_at = normalize_to_utc(last_updated_at)
            age_hours = max(
                (checked_at - last_updated_at).total_seconds() / 3600.0,
                0.0,
            )
            is_breached = age_hours > sla_hours
            incident_id: str | None = None

            if is_breached:
                incident_id = override_incident_id or _find_open_incident(cursor, model_name)
                if incident_id is None:
                    incident_id = create_incident(
                        incident_type="sla_breach",
                        affected_model=model_name,
                        severity="HIGH",
                        injected_at=checked_at,
                        incident_details={
                            "source": "freshness_monitor",
                            "last_updated_at": last_updated_at.isoformat(),
                            "sla_hours": sla_hours,
                            "age_hours": round(age_hours, 4),
                        },
                    )
                pending_detections.append(
                    (
                        incident_id,
                        {
                            "monitor": "freshness_monitor",
                            "last_updated_at": last_updated_at.isoformat(),
                            "sla_hours": sla_hours,
                            "age_hours": round(age_hours, 4),
                            "freshness_source": source,
                        },
                    )
                )

            check_id = f"FRESH-{uuid4().hex[:16].upper()}"

            last_updated_at_ntz = last_updated_at.replace(tzinfo=None)
            checked_at_ntz = checked_at.replace(tzinfo=None)

            cursor.execute(
                f"""
                INSERT INTO {FRESHNESS_CHECKS_TABLE} (
                    CHECK_ID,
                    MODEL_NAME,
                    LAST_UPDATED_AT,
                    SLA_HOURS,
                    AGE_HOURS,
                    IS_BREACHED,
                    CHECKED_AT,
                    FRESHNESS_SOURCE,
                    INCIDENT_ID
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    check_id,
                    model_name,
                    last_updated_at_ntz,
                    sla_hours,
                    age_hours,
                    is_breached,
                    checked_at_ntz,
                    source,
                    incident_id,
                ),
            )

            results.append(
                FreshnessResult(
                    model_name=model_name,
                    last_updated_at=last_updated_at,
                    sla_hours=sla_hours,
                    age_hours=age_hours,
                    is_breached=is_breached,
                    source=source,
                    incident_id=incident_id,
                )
            )

    # Call repository functions after the collection transaction closes to
    # avoid nested Snowflake connections while a cursor is active.
    for incident_id, details in pending_detections:
        try:
            mark_detected(
                incident_id,
                severity="HIGH",
                detection_details=details,
            )
        except ValueError as exc:
            if "already been detected" not in str(exc):
                raise

    return results


def main() -> None:
    results = collect_freshness_checks()
    for result in results:
        state = "BREACHED" if result.is_breached else "OK"
        print(
            f"{result.model_name}: {state} | age={result.age_hours:.2f}h "
            f"| SLA={result.sla_hours}h | source={result.source}"
        )


if __name__ == "__main__":
    main()
