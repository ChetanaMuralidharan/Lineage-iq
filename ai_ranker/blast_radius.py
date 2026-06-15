import os
os.environ["SF_USE_OPENSSL_ONLY"] = "false"
import json
import requests
import snowflake.connector
from dotenv import load_dotenv

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lineage.graph_analytics import build_lineage_graph, get_downstream_assets

load_dotenv()


def get_quality_scores(sf_conn) -> dict:
    """
    Fetches the most recent quality metrics for every model.
    Uses QUALIFY to get the latest row per model in a single query.
    """
    cur = sf_conn.cursor()
    cur.execute("""
        SELECT model_name, row_count, null_pk_rate, collected_at
        FROM PIPELINE_PLATFORM.OBSERVABILITY.QUALITY_METRICS
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY model_name ORDER BY collected_at DESC
        ) = 1
    """)
    rows = cur.fetchall()
    cur.close()

    return {
        row[0]: {
            'row_count': row[1],
            'null_pk_rate': row[2]
        }
        for row in rows
    }


def call_ollama(prompt: str) -> str:
    """
    Sends a prompt to the local Ollama server and returns the response.
    Uses OLLAMA_HOST env var so it works both locally and inside Docker.
    """
    ollama_host = os.getenv("OLLAMA_HOST", "localhost")
    url = f"http://{ollama_host}:11434/api/generate"

    payload = {
        "model": "mistral",
        "prompt": prompt,
        "stream": False
    }

    response = requests.post(url, json=payload, timeout=120)

    if response.status_code != 200:
        raise Exception(f"Ollama error {response.status_code}: {response.text}")

    return response.json().get('response', '').strip()


def rank_blast_radius(changed_model: str, drifts: list) -> str:
    """
    Main function. Given a changed model name and a list of drift events,
    fetches lineage and quality data, builds a prompt, calls Mistral via
    Ollama, and returns the ranked blast radius report as a string.
    """
    sf = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="LINEAGE",
        warehouse="COMPUTE_WH"
    )

    graph = build_lineage_graph(sf)

    changed_node_id = f"model.pipeline_platform.{changed_model}"
    downstream = get_downstream_assets(graph, changed_node_id)

    quality = get_quality_scores(sf)
    sf.close()

    if not downstream:
        return f"No downstream assets found for {changed_model}. No blast radius to rank."

    # Build the structured payload for the prompt
    downstream_detail = []
    for node_id, info in downstream.items():
        model_name = node_id.split('.')[-1]
        q = quality.get(model_name, {})
        downstream_detail.append({
            'model': model_name,
            'lineage_depth': info['depth'],
            'row_count': q.get('row_count', 'unknown'),
            'null_pk_rate': q.get('null_pk_rate', 'unknown')
        })

    # Sort by depth so the prompt presents shallower models first
    downstream_detail.sort(key=lambda x: x['lineage_depth'])

    prompt = f"""You are a data reliability engineer triaging a pipeline incident.

A schema change was detected in the dbt model '{changed_model}'.

Changes detected:
{json.dumps(drifts, indent=2)}

Downstream models affected (with lineage depth and quality data):
{json.dumps(downstream_detail, indent=2)}

Your job:
1. Rank each downstream model as CRITICAL, HIGH, MEDIUM, or LOW risk.
2. Base the ranking on:
   - lineage_depth: shallower means more directly affected
   - null_pk_rate: higher means the model is already fragile
   - row_count: more rows means more data at risk
   - the type of change: column_dropped is most severe, type_changed is severe, column_added is low risk
3. For each model write exactly one sentence explaining why it has that risk level.
4. Return the list ordered from most critical to least critical.

Be direct. This is going to an on-call engineer who needs to triage in minutes."""

    return call_ollama(prompt)


if __name__ == "__main__":
    # Simulate a column drop on stg_orders to test the full flow
    test_drifts = [
        {
            'type': 'column_dropped',
            'column': 'order_priority',
            'severity': 'HIGH'
        }
    ]

    print("Running blast radius ranking for stg_orders...")
    print("=" * 60)
    result = rank_blast_radius('stg_orders', test_drifts)
    print(result)