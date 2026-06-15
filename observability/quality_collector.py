import os
os.environ["SF_USE_OPENSSL_ONLY"] = "false"
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


MODELS_TO_CHECK = [
    ('stg_orders',      'order_key'),
    ('stg_lineitem',    'order_key'),
    ('stg_customer',    'customer_key'),
    ('stg_supplier',    'supplier_key'),
    ('dim_customer',    'customer_key'),
    ('dim_supplier',    'supplier_key'),
    ('dim_part',        'part_key'),
    ('fact_orders',     'order_key'),
    ('fact_lineitem',   'order_key'),
]


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
        warehouse="COMPUTE_WH"
    )


def create_quality_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS PIPELINE_PLATFORM.OBSERVABILITY.QUALITY_METRICS (
            model_name      VARCHAR,
            collected_at    TIMESTAMP,
            row_count       BIGINT,
            null_pk_count   BIGINT,
            null_pk_rate    FLOAT
        )
    """)


def collect_metrics(cur):
    for model_name, pk_col in MODELS_TO_CHECK:
        cur.execute(f"""
            SELECT
                COUNT(*)                                                    AS row_count,
                SUM(CASE WHEN {pk_col} IS NULL THEN 1 ELSE 0 END)          AS null_pk_count
            FROM PIPELINE_PLATFORM.DBT_DEV.{model_name}
        """)

        row = cur.fetchone()
        row_count       = row[0]
        null_pk_count   = row[1]
        null_pk_rate    = null_pk_count / row_count if row_count > 0 else 0

        cur.execute(
            """INSERT INTO PIPELINE_PLATFORM.OBSERVABILITY.QUALITY_METRICS
               SELECT %s, %s, %s, %s, %s""",
            (model_name, datetime.utcnow(), row_count, null_pk_count, null_pk_rate)
        )

        print(f"{model_name}: {row_count} rows | null PK count: {null_pk_count} | null PK rate: {null_pk_rate:.4f}")


if __name__ == "__main__":
    sf = get_snowflake_connection()
    cur = sf.cursor()

    create_quality_table(cur)
    collect_metrics(cur)

    sf.commit()
    sf.close()
    print("Quality metrics collected.")