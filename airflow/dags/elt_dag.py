import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow/project')


def run_dbt():
    import subprocess
    result = subprocess.run(
        [
            '/home/airflow/.local/bin/dbt', 'build',
            '--project-dir', '/opt/airflow/project/dbt_project/pipeline_platform',
            '--profiles-dir', '/opt/airflow/project/dbt_project'
        ],
        capture_output=True,
        text=True,
        env={**os.environ}
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("dbt build failed. See logs above.")


def run_lineage_parse():
    from lineage.manifest_parser import parse_manifest, store_lineage_in_snowflake
    manifest_path = '/opt/airflow/project/dbt_project/pipeline_platform/target/manifest.json'
    parsed = parse_manifest(manifest_path)
    store_lineage_in_snowflake(parsed)


def run_schema_snapshot():
    import snowflake.connector
    from observability.schema_registry import snapshot_schema, create_registry_table

    sf = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
        warehouse="COMPUTE_WH"
    )
    cur = sf.cursor()
    create_registry_table(cur)
    models = [
        'stg_orders', 'stg_lineitem', 'stg_customer', 'stg_supplier', 'stg_part',
        'dim_customer', 'dim_supplier', 'dim_part',
        'fact_orders', 'fact_lineitem'
    ]
    for model in models:
        snapshot_schema(model, cur)
    sf.commit()
    sf.close()


def run_quality_collect():
    import snowflake.connector
    from observability.quality_collector import collect_metrics, create_quality_table

    sf = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
        warehouse="COMPUTE_WH"
    )
    cur = sf.cursor()
    create_quality_table(cur)
    collect_metrics(cur)
    sf.commit()
    sf.close()


def run_drift_and_rank():
    import snowflake.connector
    from observability.drift_detector import detect_drift
    from ai_ranker.blast_radius import rank_blast_radius

    sf = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="OBSERVABILITY",
        warehouse="COMPUTE_WH"
    )
    cur = sf.cursor()

    models = [
        'stg_orders', 'stg_lineitem', 'stg_customer', 'stg_supplier', 'stg_part',
        'dim_customer', 'dim_supplier', 'dim_part',
        'fact_orders', 'fact_lineitem'
    ]
    for model in models:
        drifts = detect_drift(model, cur)
        if drifts:
            print(f"\nDrift detected in {model}: {drifts}")
            report = rank_blast_radius(model, drifts)
            print(f"\n=== BLAST RADIUS REPORT: {model} ===")
            print(report)
        else:
            print(f"{model}: no drift detected")

    sf.close()


with DAG(
    dag_id='lineageiq_elt',
    description='dbt build, lineage parse, schema snapshot, quality collect, drift and rank',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'lineageiq',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
    },
    tags=['lineageiq', 'elt', 'observability']
) as dag:

    t1 = PythonOperator(task_id='dbt_build', python_callable=run_dbt)
    t2 = PythonOperator(task_id='lineage_parse', python_callable=run_lineage_parse)
    t3 = PythonOperator(task_id='schema_snapshot', python_callable=run_schema_snapshot)
    t4 = PythonOperator(task_id='quality_collect', python_callable=run_quality_collect)
    t5 = PythonOperator(task_id='drift_and_rank', python_callable=run_drift_and_rank)

    t1 >> [t2, t3, t4] >> t5