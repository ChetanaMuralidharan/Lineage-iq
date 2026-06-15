import os
os.environ["SF_USE_OPENSSL_ONLY"] = "false"
import json
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
        warehouse="COMPUTE_WH"
    )


def get_baseline(model_name: str, cur) -> dict:
    cur.execute(f"""
        SELECT schema_snapshot
        FROM PIPELINE_PLATFORM.OBSERVABILITY.SCHEMA_REGISTRY
        WHERE model_name = '{model_name}'
        AND is_baseline = TRUE
        ORDER BY snapshot_at DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return {}
    columns = json.loads(row[0])
    return {col['column']: col for col in columns}


def get_latest(model_name: str, cur) -> dict:
    cur.execute(f"""
        SELECT schema_snapshot
        FROM PIPELINE_PLATFORM.OBSERVABILITY.SCHEMA_REGISTRY
        WHERE model_name = '{model_name}'
        AND is_baseline = FALSE
        ORDER BY snapshot_at DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return {}
    columns = json.loads(row[0])
    return {col['column']: col for col in columns}


def detect_drift(model_name: str, cur) -> list:
    baseline = get_baseline(model_name, cur)
    latest   = get_latest(model_name, cur)

    if not baseline or not latest:
        return []

    drifts = []

    for col in baseline:
        if col not in latest:
            drifts.append({
                'type':     'column_dropped',
                'column':   col,
                'severity': 'HIGH'
            })
        elif baseline[col]['data_type'] != latest[col]['data_type']:
            drifts.append({
                'type':     'type_changed',
                'column':   col,
                'from':     baseline[col]['data_type'],
                'to':       latest[col]['data_type'],
                'severity': 'HIGH'
            })

    for col in latest:
        if col not in baseline:
            drifts.append({
                'type':     'column_added',
                'column':   col,
                'severity': 'LOW'
            })

    return drifts


if __name__ == "__main__":
    sf = get_snowflake_connection()
    cur = sf.cursor()

    models = [
        'stg_orders', 'stg_lineitem', 'stg_customer', 'stg_supplier',
        'dim_customer', 'dim_supplier', 'dim_part',
        'fact_orders', 'fact_lineitem'
    ]

    for model in models:
        drifts = detect_drift(model, cur)
        if drifts:
            print(f"{model}: {len(drifts)} drift(s) detected")
            for d in drifts:
                print(f"  {d}")
        else:
            print(f"{model}: clean")

    sf.close()