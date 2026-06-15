import os
import networkx as nx
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def build_lineage_graph(sf_conn) -> nx.DiGraph:
    """
    Reads all edges from LINEAGE_EDGES in Snowflake and builds
    an in-memory directed graph. Nodes are dbt node IDs in the
    format model.pipeline_platform.model_name.
    """
    cur = sf_conn.cursor()
    cur.execute("""
        SELECT UPSTREAM_NODE, DOWNSTREAM_NODE
        FROM PIPELINE_PLATFORM.LINEAGE.LINEAGE_EDGES
    """)
    edges = cur.fetchall()
    cur.close()

    G = nx.DiGraph()
    for upstream, downstream in edges:
        G.add_edge(upstream, downstream)

    return G


def get_downstream_assets(graph: nx.DiGraph, changed_node_id: str) -> dict:
    """
    Returns all nodes downstream of changed_node_id, with the
    minimum hop count from the changed node to each downstream node.

    Returns an empty dict if the node does not exist in the graph.

    Example return value:
    {
        'model.pipeline_platform.fact_orders': {'depth': 1},
        'model.pipeline_platform.fact_lineitem': {'depth': 2}
    }
    """
    if changed_node_id not in graph:
        print(f"WARNING: Node '{changed_node_id}' not found in lineage graph.")
        print(f"Available nodes: {list(graph.nodes())}")
        return {}

    downstream = {}
    for node in nx.descendants(graph, changed_node_id):
        depth = nx.shortest_path_length(graph, changed_node_id, node)
        downstream[node] = {'depth': depth}

    return downstream


if __name__ == "__main__":
    sf = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="PIPELINE_PLATFORM",
        schema="LINEAGE",
        warehouse="COMPUTE_WH"
    )

    graph = build_lineage_graph(sf)
    print(f"Graph has {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges.")
    print()

    # Test with stg_orders — should show fact_orders as depth 1 downstream
    test_node = "model.pipeline_platform.stg_orders"
    downstream = get_downstream_assets(graph, test_node)

    if downstream:
        print(f"Downstream of {test_node}:")
        for node_id, info in sorted(downstream.items(), key=lambda x: x[1]['depth']):
            model_name = node_id.split('.')[-1]
            print(f"  depth {info['depth']} — {model_name}")
    else:
        print("No downstream assets found. Check that LINEAGE_EDGES is populated.")

    sf.close()