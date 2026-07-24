from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from observability.incident_repository import create_incident, mark_detected
from observability.snowflake_client import snowflake_cursor

DATABASE = "PIPELINE_PLATFORM"
MODEL_SCHEMA = "DBT_DEV"
OBSERVABILITY_SCHEMA = "OBSERVABILITY"
QUALITY_TABLE = f"{DATABASE}.{OBSERVABILITY_SCHEMA}.QUALITY_METRICS"
COLUMN_QUALITY_TABLE = f"{DATABASE}.{OBSERVABILITY_SCHEMA}.QUALITY_COLUMN_METRICS"
INCIDENTS_TABLE = f"{DATABASE}.{OBSERVABILITY_SCHEMA}.INCIDENTS"

BASELINE_WINDOW = 30
MIN_BASELINE_POINTS = 10
VOLUME_Z_THRESHOLD = 2.5
NULL_Z_THRESHOLD = 2.0

MODELS_TO_CHECK = [
    ("STG_ORDERS", "ORDER_KEY"),
    ("STG_LINEITEM", "ORDER_KEY"),
    ("STG_CUSTOMER", "CUSTOMER_KEY"),
    ("STG_SUPPLIER", "SUPPLIER_KEY"),
    ("STG_PART", "PART_KEY"),
    ("DIM_CUSTOMER", "CUSTOMER_KEY"),
    ("DIM_SUPPLIER", "SUPPLIER_KEY"),
    ("DIM_PART", "PART_KEY"),
    ("FACT_ORDERS", "ORDER_KEY"),
    ("FACT_LINEITEM", "ORDER_KEY"),
]


@dataclass(frozen=True)
class Baseline:
    count: int
    mean: float | None
    stddev: float | None


@dataclass(frozen=True)
class QualityResult:
    model_name: str
    row_count: int
    null_pk_count: int
    null_pk_rate: float
    max_null_rate: float
    max_null_column: str | None
    volume_z_score: float | None
    volume_anomaly: bool
    null_z_score: float | None
    null_anomaly: bool


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_quality_table(cursor: Any) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_TABLE} (
            METRIC_ID VARCHAR DEFAULT UUID_STRING(),
            MODEL_NAME VARCHAR NOT NULL,
            COLLECTED_AT TIMESTAMP_NTZ NOT NULL,
            ROW_COUNT BIGINT NOT NULL,
            NULL_PK_COUNT BIGINT NOT NULL,
            NULL_PK_RATE FLOAT NOT NULL,
            MAX_NULL_RATE FLOAT NOT NULL DEFAULT 0,
            MAX_NULL_COLUMN VARCHAR,
            BASELINE_SAMPLE_COUNT INTEGER,
            ROLLING_MEAN FLOAT,
            ROLLING_STDDEV FLOAT,
            Z_SCORE FLOAT,
            EXPECTED_RANGE_LOW FLOAT,
            EXPECTED_RANGE_HIGH FLOAT,
            IS_VOLUME_ANOMALY BOOLEAN NOT NULL DEFAULT FALSE,
            MAX_NULL_BASELINE_MEAN FLOAT,
            MAX_NULL_BASELINE_STDDEV FLOAT,
            NULL_Z_SCORE FLOAT,
            IS_NULL_ANOMALY BOOLEAN NOT NULL DEFAULT FALSE,
            VOLUME_INCIDENT_ID VARCHAR,
            NULL_INCIDENT_ID VARCHAR,
            CONSTRAINT PK_QUALITY_METRICS PRIMARY KEY (METRIC_ID)
        )
        """
    )

    # Safe migration for repositories that already have the original table.
    migrations = [
        "METRIC_ID VARCHAR DEFAULT UUID_STRING()",
        "MAX_NULL_RATE FLOAT DEFAULT 0",
        "MAX_NULL_COLUMN VARCHAR",
        "BASELINE_SAMPLE_COUNT INTEGER",
        "ROLLING_MEAN FLOAT",
        "ROLLING_STDDEV FLOAT",
        "Z_SCORE FLOAT",
        "EXPECTED_RANGE_LOW FLOAT",
        "EXPECTED_RANGE_HIGH FLOAT",
        "IS_VOLUME_ANOMALY BOOLEAN DEFAULT FALSE",
        "MAX_NULL_BASELINE_MEAN FLOAT",
        "MAX_NULL_BASELINE_STDDEV FLOAT",
        "NULL_Z_SCORE FLOAT",
        "IS_NULL_ANOMALY BOOLEAN DEFAULT FALSE",
        "VOLUME_INCIDENT_ID VARCHAR",
        "NULL_INCIDENT_ID VARCHAR",
    ]
    for definition in migrations:
        cursor.execute(f"ALTER TABLE {QUALITY_TABLE} ADD COLUMN IF NOT EXISTS {definition}")

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {COLUMN_QUALITY_TABLE} (
            METRIC_ID VARCHAR DEFAULT UUID_STRING(),
            MODEL_NAME VARCHAR NOT NULL,
            COLUMN_NAME VARCHAR NOT NULL,
            NULL_COUNT BIGINT NOT NULL,
            NULL_RATE FLOAT NOT NULL,
            COLLECTED_AT TIMESTAMP_NTZ NOT NULL,
            CONSTRAINT PK_COLUMN_QUALITY_METRICS PRIMARY KEY (METRIC_ID)
        )
        """
    )


def _baseline(cursor: Any, model_name: str, metric_column: str) -> Baseline:
    allowed = {"ROW_COUNT", "MAX_NULL_RATE"}
    if metric_column not in allowed:
        raise ValueError(f"Unsupported baseline metric: {metric_column}")

    cursor.execute(
        f"""
        SELECT
            COUNT(*),
            AVG({metric_column}),
            STDDEV_SAMP({metric_column})
        FROM (
            SELECT {metric_column}
            FROM {QUALITY_TABLE}
            WHERE UPPER(MODEL_NAME) = %s
            ORDER BY COLLECTED_AT DESC
            LIMIT {BASELINE_WINDOW}
        )
        """,
        (model_name,),
    )
    row = cursor.fetchone()
    return Baseline(
        count=int(row[0] or 0),
        mean=float(row[1]) if row[1] is not None else None,
        stddev=float(row[2]) if row[2] is not None else None,
    )


def _z_score(current: float, baseline: Baseline) -> float | None:
    if (
        baseline.count < MIN_BASELINE_POINTS
        or baseline.mean is None
        or baseline.stddev is None
        or math.isclose(baseline.stddev, 0.0, abs_tol=1e-12)
    ):
        return None
    return (current - baseline.mean) / baseline.stddev


def _get_columns(cursor: Any, model_name: str) -> list[str]:
    cursor.execute(
        f"""
        SELECT COLUMN_NAME
        FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (MODEL_SCHEMA, model_name),
    )
    return [str(row[0]).upper() for row in cursor.fetchall()]


def _collect_model_stats(cursor: Any, model_name: str, pk_col: str) -> tuple[int, int, dict[str, tuple[int, float]]]:
    columns = _get_columns(cursor, model_name)
    if not columns:
        raise RuntimeError(f"No columns found for {MODEL_SCHEMA}.{model_name}.")

    expressions = ["COUNT(*) AS ROW_COUNT"]
    expressions.extend(
        f'SUM(IFF("{column}" IS NULL, 1, 0)) AS "NULL_{column}"' for column in columns
    )
    cursor.execute(
        f'SELECT {", ".join(expressions)} FROM {DATABASE}.{MODEL_SCHEMA}."{model_name}"'
    )
    row = cursor.fetchone()
    row_count = int(row[0] or 0)

    column_stats: dict[str, tuple[int, float]] = {}
    for index, column in enumerate(columns, start=1):
        null_count = int(row[index] or 0)
        null_rate = null_count / row_count if row_count else 0.0
        column_stats[column] = (null_count, null_rate)

    null_pk_count = column_stats[pk_col][0]
    return row_count, null_pk_count, column_stats


def _find_open_incident(cursor: Any, incident_type: str, model_name: str) -> str | None:
    cursor.execute(
        f"""
        SELECT INCIDENT_ID
        FROM {INCIDENTS_TABLE}
        WHERE INCIDENT_TYPE = %s
          AND UPPER(AFFECTED_MODEL) = %s
          AND DETECTED_AT IS NULL
        ORDER BY INJECTED_AT DESC
        LIMIT 1
        """,
        (incident_type, model_name),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _incident_for_detection(
    cursor: Any,
    *,
    incident_type: str,
    model_name: str,
    severity: str,
    detected_at: datetime,
    details: dict[str, Any],
) -> str:
    existing = _find_open_incident(cursor, incident_type, model_name)
    if existing:
        return existing
    return create_incident(
        incident_type=incident_type,
        affected_model=model_name,
        severity=severity,
        injected_at=detected_at,
        incident_details={"source": "quality_collector", **details},
    )


def collect_metrics(cursor: Any | None = None) -> list[QualityResult]:
    """Collect metrics. A cursor is accepted for backward-compatible Airflow usage."""
    if cursor is not None:
        return _collect_metrics(cursor)

    with snowflake_cursor(database=DATABASE, schema=OBSERVABILITY_SCHEMA) as (_, managed_cursor):
        return _collect_metrics(managed_cursor)


def _collect_metrics(cursor: Any) -> list[QualityResult]:
    create_quality_table(cursor)
    collected_at = utc_now()
    pending_detections: list[tuple[str, dict[str, Any], float | None, str]] = []
    results: list[QualityResult] = []

    for model_name, pk_col in MODELS_TO_CHECK:
        row_count, null_pk_count, column_stats = _collect_model_stats(cursor, model_name, pk_col)
        null_pk_rate = null_pk_count / row_count if row_count else 0.0

        non_pk_stats = {name: stats for name, stats in column_stats.items() if name != pk_col}
        max_null_column = max(non_pk_stats, key=lambda name: non_pk_stats[name][1], default=None)
        max_null_rate = non_pk_stats[max_null_column][1] if max_null_column else 0.0

        volume_baseline = _baseline(cursor, model_name, "ROW_COUNT")
        null_baseline = _baseline(cursor, model_name, "MAX_NULL_RATE")
        volume_z = _z_score(float(row_count), volume_baseline)
        null_z = _z_score(max_null_rate, null_baseline)
        volume_zero_variance_change = (
            volume_baseline.count >= MIN_BASELINE_POINTS
            and volume_baseline.mean is not None
            and volume_baseline.stddev is not None
            and math.isclose(volume_baseline.stddev, 0.0, abs_tol=1e-12)
            and not math.isclose(float(row_count), volume_baseline.mean, abs_tol=1e-12)
        )
        null_zero_variance_change = (
            null_baseline.count >= MIN_BASELINE_POINTS
            and null_baseline.mean is not None
            and null_baseline.stddev is not None
            and math.isclose(null_baseline.stddev, 0.0, abs_tol=1e-12)
            and max_null_rate > null_baseline.mean
        )
        volume_anomaly = (
            volume_z is not None and abs(volume_z) > VOLUME_Z_THRESHOLD
        ) or volume_zero_variance_change
        null_anomaly = (
            null_z is not None and null_z > NULL_Z_THRESHOLD
        ) or null_zero_variance_change

        expected_low = (
            volume_baseline.mean - VOLUME_Z_THRESHOLD * volume_baseline.stddev
            if volume_baseline.mean is not None and volume_baseline.stddev is not None
            else None
        )
        expected_high = (
            volume_baseline.mean + VOLUME_Z_THRESHOLD * volume_baseline.stddev
            if volume_baseline.mean is not None and volume_baseline.stddev is not None
            else None
        )

        volume_incident_id = None
        null_incident_id = None
        if volume_anomaly:
            volume_incident_id = _incident_for_detection(
                cursor,
                incident_type="volume_anomaly",
                model_name=model_name,
                severity="HIGH",
                detected_at=collected_at,
                details={"row_count": row_count, "z_score": volume_z},
            )
            pending_detections.append(
                (
                    volume_incident_id,
                    {
                        "monitor": "quality_collector",
                        "metric": "row_count",
                        "row_count": row_count,
                        "rolling_mean": volume_baseline.mean,
                        "rolling_stddev": volume_baseline.stddev,
                        "z_score": volume_z,
                        "threshold": VOLUME_Z_THRESHOLD,
                        "zero_variance_change": volume_zero_variance_change,
                    },
                    volume_z,
                    "HIGH",
                )
            )

        if null_anomaly:
            null_incident_id = _incident_for_detection(
                cursor,
                incident_type="null_surge",
                model_name=model_name,
                severity="HIGH",
                detected_at=collected_at,
                details={
                    "column": max_null_column,
                    "null_rate": max_null_rate,
                    "z_score": null_z,
                },
            )
            pending_detections.append(
                (
                    null_incident_id,
                    {
                        "monitor": "quality_collector",
                        "metric": "column_null_rate",
                        "column": max_null_column,
                        "null_rate": max_null_rate,
                        "rolling_mean": null_baseline.mean,
                        "rolling_stddev": null_baseline.stddev,
                        "z_score": null_z,
                        "threshold": NULL_Z_THRESHOLD,
                        "zero_variance_change": null_zero_variance_change,
                    },
                    null_z,
                    "HIGH",
                )
            )

        cursor.execute(
            f"""
            INSERT INTO {QUALITY_TABLE} (
                MODEL_NAME, COLLECTED_AT, ROW_COUNT, NULL_PK_COUNT, NULL_PK_RATE,
                MAX_NULL_RATE, MAX_NULL_COLUMN, BASELINE_SAMPLE_COUNT,
                ROLLING_MEAN, ROLLING_STDDEV, Z_SCORE,
                EXPECTED_RANGE_LOW, EXPECTED_RANGE_HIGH, IS_VOLUME_ANOMALY,
                MAX_NULL_BASELINE_MEAN, MAX_NULL_BASELINE_STDDEV, NULL_Z_SCORE,
                IS_NULL_ANOMALY, VOLUME_INCIDENT_ID, NULL_INCIDENT_ID
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                model_name,
                collected_at,
                row_count,
                null_pk_count,
                null_pk_rate,
                max_null_rate,
                max_null_column,
                volume_baseline.count,
                volume_baseline.mean,
                volume_baseline.stddev,
                volume_z,
                expected_low,
                expected_high,
                volume_anomaly,
                null_baseline.mean,
                null_baseline.stddev,
                null_z,
                null_anomaly,
                volume_incident_id,
                null_incident_id,
            ),
        )

        for column_name, (null_count, null_rate) in column_stats.items():
            cursor.execute(
                f"""
                INSERT INTO {COLUMN_QUALITY_TABLE} (
                    MODEL_NAME, COLUMN_NAME, NULL_COUNT, NULL_RATE, COLLECTED_AT
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (model_name, column_name, null_count, null_rate, collected_at),
            )

        results.append(
            QualityResult(
                model_name=model_name,
                row_count=row_count,
                null_pk_count=null_pk_count,
                null_pk_rate=null_pk_rate,
                max_null_rate=max_null_rate,
                max_null_column=max_null_column,
                volume_z_score=volume_z,
                volume_anomaly=volume_anomaly,
                null_z_score=null_z,
                null_anomaly=null_anomaly,
            )
        )

    # Merge detector evidence with the injection metadata needed by the
    # restoration commands, then mark each matching incident detected.
    import json
    for incident_id, details, z_score, severity in pending_detections:
        cursor.execute(
            f"SELECT INCIDENT_DETAILS FROM {INCIDENTS_TABLE} WHERE INCIDENT_ID = %s",
            (incident_id,),
        )
        row = cursor.fetchone()
        existing_details = row[0] if row else {}
        if isinstance(existing_details, str):
            existing_details = json.loads(existing_details)
        merged_details = {**(existing_details or {}), **details}

        cursor.execute(
            f"""
            UPDATE {INCIDENTS_TABLE}
            SET DETECTED_AT = COALESCE(DETECTED_AT, %s),
                STATUS = IFF(DETECTED_AT IS NULL, 'DETECTED', STATUS),
                Z_SCORE = COALESCE(Z_SCORE, %s),
                SEVERITY = COALESCE(SEVERITY, %s),
                INCIDENT_DETAILS = PARSE_JSON(%s)
            WHERE INCIDENT_ID = %s
            """,
            (
                collected_at,
                z_score,
                severity,
                json.dumps(merged_details, default=str),
                incident_id,
            ),
        )

    return results


def main() -> None:
    results = collect_metrics()
    for result in results:
        print(
            f"{result.model_name}: rows={result.row_count:,} | "
            f"pk_null_rate={result.null_pk_rate:.4f} | "
            f"max_null={result.max_null_column}:{result.max_null_rate:.4f} | "
            f"volume_z={result.volume_z_score} | null_z={result.null_z_score}"
        )


if __name__ == "__main__":
    main()
