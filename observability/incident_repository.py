from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from observability.snowflake_client import snowflake_cursor


INCIDENTS_TABLE = (
    "PIPELINE_PLATFORM.OBSERVABILITY.INCIDENTS"
)

VALID_INCIDENT_TYPES = {
    "schema_drift",
    "volume_anomaly",
    "sla_breach",
    "null_surge",
}

VALID_SEVERITIES = {
    "CRITICAL",
    "HIGH",
    "MEDIUM",
    "LOW",
}

VALID_STATUSES = {
    "INJECTED",
    "DETECTED",
    "RESOLVED",
}


@dataclass
class Incident:
    incident_id: str
    incident_type: str
    affected_model: str
    injected_at: datetime | None
    detected_at: datetime | None
    resolved_at: datetime | None
    status: str
    severity: str
    z_score: float | None
    blast_radius_rank: int | None
    actual_rank: int | None
    incident_details: dict[str, Any] | None
    mttd_seconds: float | None = None
    mttr_seconds: float | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def validate_incident_type(incident_type: str) -> str:
    normalized = incident_type.strip().lower()

    if normalized not in VALID_INCIDENT_TYPES:
        raise ValueError(
            f"Unsupported incident type '{incident_type}'. "
            f"Expected one of: {sorted(VALID_INCIDENT_TYPES)}"
        )

    return normalized


def validate_severity(severity: str) -> str:
    normalized = severity.strip().upper()

    if normalized not in VALID_SEVERITIES:
        raise ValueError(
            f"Unsupported severity '{severity}'. "
            f"Expected one of: {sorted(VALID_SEVERITIES)}"
        )

    return normalized


def create_incident(
    *,
    incident_type: str,
    affected_model: str,
    severity: str,
    incident_details: dict[str, Any] | None = None,
    injected_at: datetime | None = None,
    z_score: float | None = None,
    actual_rank: int | None = None,
) -> str:
    """
    Create a new injected incident and return its incident ID.
    """

    incident_type = validate_incident_type(incident_type)
    severity = validate_severity(severity)

    incident_id = f"INC-{uuid.uuid4().hex[:12].upper()}"
    injected_at = injected_at or utc_now()

    details_json = json.dumps(
        incident_details or {},
        default=str,
    )

    sql = f"""
        INSERT INTO {INCIDENTS_TABLE} (
            INCIDENT_ID,
            INCIDENT_TYPE,
            AFFECTED_MODEL,
            INJECTED_AT,
            DETECTED_AT,
            RESOLVED_AT,
            STATUS,
            SEVERITY,
            Z_SCORE,
            BLAST_RADIUS_RANK,
            ACTUAL_RANK,
            INCIDENT_DETAILS,
            CREATED_AT
        )
        SELECT
            %s,
            %s,
            %s,
            %s,
            NULL,
            NULL,
            'INJECTED',
            %s,
            %s,
            NULL,
            %s,
            PARSE_JSON(%s),
            CURRENT_TIMESTAMP()
    """

    parameters = (
        incident_id,
        incident_type,
        affected_model.upper(),
        injected_at,
        severity,
        z_score,
        actual_rank,
        details_json,
    )

    with snowflake_cursor(
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
    ) as (_, cursor):
        cursor.execute(sql, parameters)

    return incident_id


def mark_detected(
    incident_id: str,
    *,
    detected_at: datetime | None = None,
    blast_radius_rank: int | None = None,
    z_score: float | None = None,
    severity: str | None = None,
    detection_details: dict[str, Any] | None = None,
) -> None:
    """
    Mark an injected incident as detected.
    """

    detected_at = detected_at or utc_now()

    if severity is not None:
        severity = validate_severity(severity)

    existing_incident = get_incident(incident_id)

    if existing_incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    if existing_incident.detected_at is not None:
        raise ValueError(
            f"Incident '{incident_id}' has already been detected."
        )

    merged_details = dict(
        existing_incident.incident_details or {}
    )

    merged_details.update(
        detection_details or {}
    )

    details_json = json.dumps(
        merged_details,
        default=str,
    )

    sql = f"""
        UPDATE {INCIDENTS_TABLE}
        SET
            DETECTED_AT = %s,
            STATUS = 'DETECTED',
            BLAST_RADIUS_RANK =
                COALESCE(%s, BLAST_RADIUS_RANK),
            Z_SCORE =
                COALESCE(%s, Z_SCORE),
            SEVERITY =
                COALESCE(%s, SEVERITY),
            INCIDENT_DETAILS =
                PARSE_JSON(%s)
        WHERE INCIDENT_ID = %s
          AND DETECTED_AT IS NULL
    """

    parameters = (
        detected_at,
        blast_radius_rank,
        z_score,
        severity,
        details_json,
        incident_id,
    )

    with snowflake_cursor(
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
    ) as (_, cursor):
        cursor.execute(sql, parameters)

        if cursor.rowcount == 0:
            raise ValueError(
                f"Incident '{incident_id}' was not found "
                "or has already been detected."
            )
        
def mark_resolved(
    incident_id: str,
    *,
    resolved_at: datetime | None = None,
    resolution_details: dict[str, Any] | None = None,
) -> None:
    """
    Mark a detected incident as resolved.
    """

    resolved_at = resolved_at or utc_now()

    existing_incident = get_incident(incident_id)

    if existing_incident is None:
        raise ValueError(
            f"Incident '{incident_id}' was not found."
        )

    if existing_incident.detected_at is None:
        raise ValueError(
            f"Incident '{incident_id}' has not been detected."
        )

    if existing_incident.resolved_at is not None:
        raise ValueError(
            f"Incident '{incident_id}' is already resolved."
        )

    merged_details = dict(
        existing_incident.incident_details or {}
    )

    merged_details.update(
        resolution_details or {}
    )

    details_json = json.dumps(
        merged_details,
        default=str,
    )

    sql = f"""
        UPDATE {INCIDENTS_TABLE}
        SET
            RESOLVED_AT = %s,
            STATUS = 'RESOLVED',
            INCIDENT_DETAILS =
                PARSE_JSON(%s)
        WHERE INCIDENT_ID = %s
          AND DETECTED_AT IS NOT NULL
          AND RESOLVED_AT IS NULL
    """

    parameters = (
        resolved_at,
        details_json,
        incident_id,
    )

    with snowflake_cursor(
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
    ) as (_, cursor):
        cursor.execute(sql, parameters)

        if cursor.rowcount == 0:
            raise ValueError(
                f"Incident '{incident_id}' was not found, "
                "has not been detected, or is already resolved."
            )


def update_actual_rank(
    incident_id: str,
    actual_rank: int,
) -> None:
    """
    Store the manually verified correct ranking.
    """

    if actual_rank < 1:
        raise ValueError("actual_rank must be at least 1.")

    sql = f"""
        UPDATE {INCIDENTS_TABLE}
        SET ACTUAL_RANK = %s
        WHERE INCIDENT_ID = %s
    """

    with snowflake_cursor(
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
    ) as (_, cursor):
        cursor.execute(
            sql,
            (
                actual_rank,
                incident_id,
            ),
        )

        if cursor.rowcount == 0:
            raise ValueError(
                f"Incident '{incident_id}' was not found."
            )


def get_incident(
    incident_id: str,
) -> Incident | None:
    """
    Retrieve one incident with computed MTTD and MTTR.
    """

    sql = f"""
        SELECT
            INCIDENT_ID,
            INCIDENT_TYPE,
            AFFECTED_MODEL,
            INJECTED_AT,
            DETECTED_AT,
            RESOLVED_AT,
            STATUS,
            SEVERITY,
            Z_SCORE,
            BLAST_RADIUS_RANK,
            ACTUAL_RANK,
            INCIDENT_DETAILS,
            CASE
                WHEN DETECTED_AT IS NOT NULL
                THEN DATEDIFF(
                    'millisecond',
                    INJECTED_AT,
                    DETECTED_AT
                ) / 1000.0
            END AS MTTD_SECONDS,
            CASE
                WHEN RESOLVED_AT IS NOT NULL
                THEN DATEDIFF(
                    'millisecond',
                    DETECTED_AT,
                    RESOLVED_AT
                ) / 1000.0
            END AS MTTR_SECONDS
        FROM {INCIDENTS_TABLE}
        WHERE INCIDENT_ID = %s
    """

    with snowflake_cursor(
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
    ) as (_, cursor):
        cursor.execute(
            sql,
            (incident_id,),
        )

        row = cursor.fetchone()

    if row is None:
        return None

    details = row[11]

    if isinstance(details, str):
        details = json.loads(details)

    return Incident(
        incident_id=row[0],
        incident_type=row[1],
        affected_model=row[2],
        injected_at=row[3],
        detected_at=row[4],
        resolved_at=row[5],
        status=row[6],
        severity=row[7],
        z_score=row[8],
        blast_radius_rank=row[9],
        actual_rank=row[10],
        incident_details=details,
        mttd_seconds=float(row[12]) if row[12] is not None else None,
        mttr_seconds=float(row[13]) if row[13] is not None else None,
    )


def list_incidents(
    *,
    status: str | None = None,
    limit: int = 100,
) -> list[Incident]:
    """
    List recent incidents, optionally filtered by status.
    """

    if limit < 1:
        raise ValueError("limit must be at least 1.")

    where_clause = ""
    parameters: list[Any] = []

    if status is not None:
        normalized_status = status.strip().upper()

        if normalized_status not in VALID_STATUSES:
            raise ValueError(
                f"Unsupported status '{status}'. "
                f"Expected one of: {sorted(VALID_STATUSES)}"
            )

        where_clause = "WHERE STATUS = %s"
        parameters.append(normalized_status)

    parameters.append(limit)

    sql = f"""
        SELECT
            INCIDENT_ID
        FROM {INCIDENTS_TABLE}
        {where_clause}
        ORDER BY INJECTED_AT DESC
        LIMIT %s
    """

    with snowflake_cursor(
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
    ) as (_, cursor):
        cursor.execute(
            sql,
            tuple(parameters),
        )

        incident_ids = [
            row[0]
            for row in cursor.fetchall()
        ]

    incidents: list[Incident] = []

    for incident_id in incident_ids:
        incident = get_incident(incident_id)

        if incident is not None:
            incidents.append(incident)

    return incidents


def incident_to_json(incident: Incident) -> str:
    return json.dumps(
        asdict(incident),
        indent=2,
        default=str,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage LineageIQ incidents."
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    create_parser = subparsers.add_parser(
        "create",
        help="Create an injected incident.",
    )

    create_parser.add_argument(
        "--type",
        required=True,
        choices=sorted(VALID_INCIDENT_TYPES),
    )

    create_parser.add_argument(
        "--model",
        required=True,
    )

    create_parser.add_argument(
        "--severity",
        required=True,
        choices=sorted(VALID_SEVERITIES),
    )

    create_parser.add_argument(
        "--details",
        default="{}",
        help="Incident details as JSON.",
    )

    detect_parser = subparsers.add_parser(
        "detect",
        help="Mark an incident as detected.",
    )

    detect_parser.add_argument(
        "incident_id",
    )

    detect_parser.add_argument(
        "--rank",
        type=int,
    )

    detect_parser.add_argument(
        "--z-score",
        type=float,
    )

    resolve_parser = subparsers.add_parser(
        "resolve",
        help="Mark an incident as resolved.",
    )

    resolve_parser.add_argument(
        "incident_id",
    )

    show_parser = subparsers.add_parser(
        "show",
        help="Display one incident.",
    )

    show_parser.add_argument(
        "incident_id",
    )

    list_parser = subparsers.add_parser(
        "list",
        help="List recent incidents.",
    )

    list_parser.add_argument(
        "--status",
        choices=sorted(VALID_STATUSES),
    )

    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "create":
        try:
            details = json.loads(args.details)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "--details must contain valid JSON."
            ) from exc

        incident_id = create_incident(
            incident_type=args.type,
            affected_model=args.model,
            severity=args.severity,
            incident_details=details,
        )

        print(f"Incident created: {incident_id}")

    elif args.command == "detect":
        mark_detected(
            args.incident_id,
            blast_radius_rank=args.rank,
            z_score=args.z_score,
        )

        print(
            f"Incident detected: {args.incident_id}"
        )

    elif args.command == "resolve":
        mark_resolved(
            args.incident_id,
        )

        print(
            f"Incident resolved: {args.incident_id}"
        )

    elif args.command == "show":
        incident = get_incident(
            args.incident_id
        )

        if incident is None:
            raise ValueError(
                f"Incident '{args.incident_id}' was not found."
            )

        print(incident_to_json(incident))

    elif args.command == "list":
        incidents = list_incidents(
            status=args.status,
            limit=args.limit,
        )

        for incident in incidents:
            print(incident_to_json(incident))


if __name__ == "__main__":
    main()