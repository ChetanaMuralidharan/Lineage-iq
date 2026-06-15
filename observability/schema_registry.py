import os
os.environ["SF_USE_OPENSSL_ONLY"] = "false"
import json
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

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


def create_registry_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS PIPELINE_PLATFORM.OBSERVABILITY.SCHEMA_REGISTRY (
            model_name VARCHAR,
            snapshot_at TIMESTAMP,
            schema_snapshot VARIANT,
            is_baseline BOOLEAN
        )
    """)


def snapshot_schema(model_name: str, cur):
    # Query Snowflake's built-in metadata to get column structure
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM PIPELINE_PLATFORM.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = 'DBT_DEV'
        AND table_name = UPPER('{model_name}')
        ORDER BY ordinal_position
    """)

    columns = [
        {
            'column': row[0],
            'data_type': row[1],
            'nullable': row[2]
        }
        for row in cur.fetchall()
    ]

    if not columns:
        print(f"WARNING: No columns found for {model_name}. Skipping.")
        return

    # Check if a baseline already exists for this model
    cur.execute(f"""
        SELECT COUNT(*) FROM PIPELINE_PLATFORM.OBSERVABILITY.SCHEMA_REGISTRY
        WHERE model_name = '{model_name}'
        AND is_baseline = TRUE
    """)
    baseline_exists = cur.fetchone()[0] > 0
    is_baseline = not baseline_exists

    cur.execute(
        """INSERT INTO PIPELINE_PLATFORM.OBSERVABILITY.SCHEMA_REGISTRY
           SELECT %s, %s, PARSE_JSON(%s), %s""",
        (model_name, datetime.utcnow(), json.dumps(columns), is_baseline)
    )

    label = "BASELINE" if is_baseline else "snapshot"
    print(f"{model_name}: {len(columns)} columns captured ({label})")


if __name__ == "__main__":
    models = [
        'stg_orders', 'stg_lineitem', 'stg_customer', 'stg_supplier',
        'dim_customer', 'dim_supplier', 'dim_part',
        'fact_orders', 'fact_lineitem'
    ]

    sf = get_snowflake_connection()
    cur = sf.cursor()

    create_registry_table(cur)

    for model in models:
        snapshot_schema(model, cur)

    sf.commit()
    sf.close()
    print("Schema registry complete.")