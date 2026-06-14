import os
import pandas as pd
import tempfile
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

STAGE_NAME = "PIPELINE_PLATFORM.RAW.lineageiq_stage"
DATABASE = "PIPELINE_PLATFORM"
SCHEMA = "RAW"

# Columns to exclude from MERGE SET clause — audit-only,
# should not be overwritten on UPDATE (only set on INSERT).
# This means MAX(_LOADED_AT) per table reliably shows when a row first arrived.
AUDIT_COLUMNS = ['_LOADED_AT', '_SOURCE']

# Primary keys per table — used for MERGE
PRIMARY_KEYS = {
    'CUSTOMER':  'C_CUSTKEY',
    'ORDERS':    'O_ORDERKEY',
    'LINEITEM':  ['L_ORDERKEY', 'L_LINENUMBER'],  # composite key
    'SUPPLIER':  'S_SUPPLIERKEY',
    'PART':      'P_PARTKEY',
}

# Column mappings: Postgres column → Snowflake column
COLUMN_MAPS = {
    'CUSTOMER': {
        'c_custkey':   'C_CUSTKEY',
        'c_name':      'C_NAME',
        'c_address':   'C_ADDRESS',
        'c_nationkey': 'C_NATIONKEY',
        'c_phone':     'C_PHONE',
        'c_acctbal':   'C_ACCOUNTBALANCE',
        'c_mktsegment':'C_MARKETSEGMENT',
        'c_comment':   'C_COMMENT'
    },
    'ORDERS': {
        'o_orderkey':    'O_ORDERKEY',
        'o_custkey':     'O_CUSTKEY',
        'o_orderstatus': 'O_ORDERSTATUS',
        'o_totalprice':  'O_TOTALPRICE',
        'o_orderdate':   'O_ORDERDATE',
        'o_orderpriority':'O_ORDERPRIORITY',
        'o_clerk':       'O_CLERK',
        'o_shippriority':'O_SHIPPRIORITY',
        'o_comment':     'O_COMMENT'
    },
    'LINEITEM': {
        'l_orderkey':    'L_ORDERKEY',
        'l_partkey':     'L_PARTKEY',
        'l_suppkey':     'L_SUPPLIERKEY',
        'l_linenumber':  'L_LINENUMBER',
        'l_quantity':    'L_QUANTITY',
        'l_extendedprice':'L_EXTENDEDPRICE',
        'l_discount':    'L_DISCOUNT',
        'l_tax':         'L_TAX',
        'l_returnflag':  'L_RETURNFLAG',
        'l_linestatus':  'L_LINESTATUS',
        'l_shipdate':    'L_SHIPDATE',
        'l_commitdate':  'L_COMMITDATE',
        'l_receiptdate': 'L_RECEIPTDATE',
        'l_shipinstruct':'L_SHIPINSTRUCT',
        'l_shipmode':    'L_SHIPMODE',
        'l_comment':     'L_COMMENT'
    },
    'SUPPLIER': {
        's_suppkey':   'S_SUPPLIERKEY',
        's_name':      'S_NAME',
        's_address':   'S_ADDRESS',
        's_nationkey': 'S_NATIONKEY',
        's_phone':     'S_PHONE',
        's_acctbal':   'S_ACCOUNTBALANCE',
        's_comment':   'S_COMMENT'
    },
    'PART': {
        'p_partkey':    'P_PARTKEY',
        'p_name':       'P_NAME',
        'p_mfgr':       'P_MANUFACTURER',
        'p_brand':      'P_BRAND',
        'p_type':       'P_TYPE',
        'p_size':       'P_SIZE',
        'p_container':  'P_CONTAINER',
        'p_retailprice':'P_RETAILPRICE',
        'p_comment':    'P_COMMENT'
    }
}

# Load order matters — parent tables before child tables
LOAD_ORDER = ['CUSTOMER', 'SUPPLIER', 'PART', 'ORDERS', 'LINEITEM']

# ─────────────────────────────────────────
# CONNECTIONS
# ─────────────────────────────────────────

def get_pg_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )

def get_sf_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=DATABASE,
        schema=SCHEMA
    )

# ─────────────────────────────────────────
# PRE-FLIGHT CHECKS
# ─────────────────────────────────────────

def ensure_stage_exists(sf_conn):
    """
    Verify the Snowflake stage exists before any PUT commands.
    Raises a clear error immediately rather than hanging for hours.
    """
    cur = sf_conn.cursor()
    try:
        cur.execute(f"SHOW STAGES LIKE 'lineageiq_stage' IN SCHEMA {DATABASE}.{SCHEMA}")
        rows = cur.fetchall()
        if not rows:
            raise RuntimeError(
                f"Stage '{STAGE_NAME}' does not exist. "
                f"Run this in Snowflake first:\n"
                f"  CREATE STAGE {STAGE_NAME} FILE_FORMAT = (TYPE = 'PARQUET');"
            )
        logger.info(f"[STAGE] Stage '{STAGE_NAME}' verified — exists.")
    finally:
        cur.close()

def ensure_audit_columns(table_name: str, sf_conn):
    """
    Ensure _LOADED_AT and _SOURCE exist on the final table.
    Safe to run every time — IF NOT EXISTS is a no-op if columns already exist.
    Prevents MERGE failures when audit columns are added after a table was created.
    """
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            ALTER TABLE {DATABASE}.{SCHEMA}.{table_name}
            ADD COLUMN IF NOT EXISTS _LOADED_AT TIMESTAMP
        """)
        cur.execute(f"""
            ALTER TABLE {DATABASE}.{SCHEMA}.{table_name}
            ADD COLUMN IF NOT EXISTS _SOURCE VARCHAR
        """)
        logger.info(f"[SCHEMA] Audit columns verified on {table_name}")
    finally:
        cur.close()

# ─────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────

def extract_from_postgres(table_name: str, pg_engine) -> pd.DataFrame:
    pg_table = table_name.lower()
    logger.info(f"[EXTRACT] Reading {pg_table} from Postgres...")
    df = pd.read_sql(f"SELECT * FROM {pg_table}", pg_engine)
    logger.info(f"[EXTRACT] {len(df)} rows extracted from {pg_table}")
    return df

# ─────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────

def transform(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    logger.info(f"[TRANSFORM] Mapping columns for {table_name}...")
    df = df.rename(columns=COLUMN_MAPS[table_name])

    # _LOADED_AT is set on INSERT only and never overwritten on UPDATE.
    # This means MAX(_LOADED_AT) per table reliably tells you when a row
    # first arrived — use it to verify the pipeline ran:
    #   SELECT MAX(_LOADED_AT) FROM PIPELINE_PLATFORM.RAW.CUSTOMER;
    df['_LOADED_AT'] = datetime.utcnow()
    df['_SOURCE'] = 'postgres'

    logger.info(f"[TRANSFORM] Final columns: {list(df.columns)}")
    return df

# ─────────────────────────────────────────
# STAGE
# ─────────────────────────────────────────

def stage_parquet(df: pd.DataFrame, table_name: str, sf_conn, run_timestamp: str) -> str:
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        local_path = tmp.name
        df.to_parquet(local_path, index=False, engine='pyarrow')
        logger.info(f"[STAGE] Written to temp file: {local_path}")

    cur = sf_conn.cursor()
    put_cmd = f"PUT file://{local_path} @{STAGE_NAME}/{table_name.lower()}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    logger.info(f"[STAGE] Executing: {put_cmd}")
    cur.execute(put_cmd)

    result = cur.fetchone()
    staged_filename = result[1]
    logger.info(f"[STAGE] PUT result: {result}")
    logger.info(f"[STAGE] File staged as: {staged_filename}")

    os.unlink(local_path)
    cur.close()

    return staged_filename

# ─────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────

def create_staging_table(table_name: str, df: pd.DataFrame, sf_conn):
    cur = sf_conn.cursor()
    staging_table = f"{table_name}_STAGING"

    # All VARCHAR staging table — avoids type casting issues during COPY INTO.
    # The MERGE INTO final table will cast to correct types.
    col_definitions = ', '.join([f"{col} VARCHAR" for col in df.columns])

    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {DATABASE}.{SCHEMA}.{staging_table} (
            {col_definitions}
        )
    """)
    logger.info(f"[LOAD] Created staging table: {staging_table}")
    cur.close()
    return staging_table

def copy_into_staging(table_name: str, staging_table: str,
                      df: pd.DataFrame, staged_filename: str, sf_conn):
    cur = sf_conn.cursor()

    columns = list(df.columns)
    col_list = ', '.join(columns)
    parquet_cols = ', '.join([f'$1:{col}::VARCHAR' for col in columns])

    copy_cmd = f"""
        COPY INTO {DATABASE}.{SCHEMA}.{staging_table} ({col_list})
        FROM (
            SELECT {parquet_cols}
            FROM @{STAGE_NAME}/{table_name.lower()}/{staged_filename}
        )
        FILE_FORMAT = (TYPE = 'PARQUET')
        FORCE = TRUE
    """
    logger.info(f"[LOAD] Executing COPY INTO {staging_table}...")
    logger.info(f"[LOAD] SQL: {copy_cmd}")
    cur.execute(copy_cmd)
    result = cur.fetchone()
    logger.info(f"[LOAD] COPY INTO result: {result}")
    cur.close()

def merge_into_final(table_name: str, staging_table: str,
                     df: pd.DataFrame, sf_conn):
    """
    MERGE from staging into the final table.
    - Matched on primary key → UPDATE business columns only.
      _LOADED_AT and _SOURCE are intentionally excluded from UPDATE —
      they preserve the original insert time and source.
    - Not matched → INSERT all columns including audit columns.
    This makes the pipeline fully idempotent.
    """
    cur = sf_conn.cursor()
    pk = PRIMARY_KEYS[table_name]
    all_columns = list(df.columns)

    # Columns to skip in UPDATE: primary key(s) + audit columns
    pk_set = set(pk) if isinstance(pk, list) else {pk}
    skip_in_update = pk_set | set(AUDIT_COLUMNS)

    # Build the ON clause
    if isinstance(pk, list):
        on_clause = ' AND '.join([f"target.{k} = source.{k}" for k in pk])
    else:
        on_clause = f"target.{pk} = source.{pk}"

    # SET clause: business columns only — not audit columns, not PK
    update_cols = [c for c in all_columns if c not in skip_in_update]
    set_clause = ', '.join([f"target.{c} = source.{c}" for c in update_cols])

    # INSERT clause: all columns including _LOADED_AT
    insert_cols = ', '.join(all_columns)
    insert_vals = ', '.join([f"source.{c}" for c in all_columns])

    merge_cmd = f"""
        MERGE INTO {DATABASE}.{SCHEMA}.{table_name} AS target
        USING {DATABASE}.{SCHEMA}.{staging_table} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    logger.info(f"[MERGE] Merging {staging_table} → {table_name}...")
    cur.execute(merge_cmd)
    result = cur.fetchone()
    logger.info(f"[MERGE] Rows inserted/updated: {result}")
    cur.close()

# ─────────────────────────────────────────
# ORCHESTRATE
# ─────────────────────────────────────────

def run_etl_for_table(table_name: str, pg_engine, sf_conn, run_timestamp: str):
    logger.info(f"{'='*50}")
    logger.info(f"Starting ETL for table: {table_name}")
    logger.info(f"{'='*50}")

    df = extract_from_postgres(table_name, pg_engine)
    df = transform(df, table_name)
    staged_filename = stage_parquet(df, table_name, sf_conn, run_timestamp)
    staging_table = create_staging_table(table_name, df, sf_conn)
    copy_into_staging(table_name, staging_table, df, staged_filename, sf_conn)
    ensure_audit_columns(table_name, sf_conn)  # no-op if columns already exist
    merge_into_final(table_name, staging_table, df, sf_conn)

    logger.info(f"[DONE] {table_name} complete.")

def run_full_etl():
    run_timestamp = datetime.utcnow().strftime('%Y_%m_%d_%H%M%S')
    logger.info(f"ETL run started | timestamp: {run_timestamp}")

    pg_engine = get_pg_engine()
    sf_conn = get_sf_conn()

    try:
        # Fail fast — verify the stage exists before touching any table.
        # Without this, a missing stage causes PUT to hang indefinitely.
        ensure_stage_exists(sf_conn)

        for table in LOAD_ORDER:
            run_etl_for_table(table, pg_engine, sf_conn, run_timestamp)

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        sf_conn.close()
        logger.info("Snowflake connection closed.")

    logger.info("Full ETL run complete.")

if __name__ == "__main__":
    run_full_etl()