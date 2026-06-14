import json
import os
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def parse_manifest(manifest_path: str) -> dict:
    with open(manifest_path) as f:
        manifest = json.load(f)

    nodes = {}
    edges = []

    for node_id, node_data in manifest['nodes'].items():
        if node_data['resource_type'] not in ('model', 'snapshot'):
            continue

        nodes[node_id] = {
            'node_id': node_id,
            'name': node_data['name'],
            'resource_type': node_data['resource_type'],
            'schema': node_data.get('schema', ''),
            'database': node_data.get('database', ''),
            'columns': list(node_data.get('columns', {}).keys())
        }

        for upstream in node_data['depends_on']['nodes']:
            edges.append({
                'upstream_node': upstream,
                'downstream_node': node_id,
                'parsed_at': datetime.utcnow().isoformat()
            })

    return {'nodes': nodes, 'edges': edges}


def store_lineage_in_snowflake(parsed: dict):
    sf = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="LINEAGE",
        warehouse="COMPUTE_WH"
    )
    cur = sf.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS LINEAGE_NODES (
            node_id VARCHAR,
            name VARCHAR,
            resource_type VARCHAR,
            schema_name VARCHAR,
            database_name VARCHAR,
            columns VARIANT,
            captured_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS LINEAGE_EDGES (
            upstream_node VARCHAR,
            downstream_node VARCHAR,
            parsed_at TIMESTAMP
        )
    """)

    cur.execute("DELETE FROM LINEAGE_NODES")
    cur.execute("DELETE FROM LINEAGE_EDGES")

    for node in parsed['nodes'].values():
       cur.execute(
    "INSERT INTO LINEAGE_NODES SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s",
    (
        node['node_id'],
        node['name'],
        node['resource_type'],
        node['schema'],
        node['database'],
        json.dumps(node['columns']),
        datetime.utcnow()
    )
)

    for edge in parsed['edges']:
        cur.execute(
            "INSERT INTO LINEAGE_EDGES VALUES (%s, %s, %s)",
            (
                edge['upstream_node'],
                edge['downstream_node'],
                edge['parsed_at']
            )
        )

    sf.commit()
    print(f"Stored {len(parsed['nodes'])} nodes and {len(parsed['edges'])} edges in Snowflake.")
    sf.close()


if __name__ == "__main__":
    manifest_path = "dbt_project/pipeline_platform/target/manifest.json"
    parsed = parse_manifest(manifest_path)
    print(f"Parsed {len(parsed['nodes'])} nodes and {len(parsed['edges'])} edges")
    store_lineage_in_snowflake(parsed)