# LineageIQ

> A data pipeline observability and lineage tracking platform built from scratch using production-grade open source tools.

LineageIQ ingests operational data from a source database, transforms it inside a cloud data warehouse using dbt, tracks how data flows between every model in the pipeline, monitors data quality over time, and uses an AI layer to assess the blast radius when something breaks or changes.

---

## The Problem

In production data environments, pipelines are deeply interconnected. A single staging model might feed three dimension tables, which feed two fact tables, which feed a dozen BI dashboards and a MetricFlow semantic layer. When something changes upstream — even something small like a column rename — the blast radius can be enormous and invisible.

Data teams typically discover this through angry Slack messages from analysts, not through automated detection.

**LineageIQ makes the impact visible before the break propagates.**

---

## What LineageIQ Does

The platform combines four capabilities into one cohesive system:

**Lineage Tracking** — Parses the dbt manifest to build a directed graph of every upstream and downstream dependency between models, stored in Snowflake so it can be queried and visualized.

**Schema Registry & Drift Detection** — Snapshots the schema of every model after each run and compares it to the established baseline. Dropped columns, type changes, and added columns are detected automatically and classified by severity.

**Data Quality Monitoring** — Collects row counts, null rates, and other metrics on every model after every run, building a historical view of whether data is healthy or degrading.

**AI-Powered Blast Radius Ranking** — When drift is detected, a language model synthesizes the lineage graph, quality history, and nature of the change to produce a ranked list of which downstream assets are most at risk and why — in plain English — so an engineer can triage in minutes instead of hours.

---

## Use Cases

- A source system renames a column. LineageIQ detects the drift, traces every downstream model that depends on it, and ranks them by criticality so the on-call engineer knows exactly what to fix first.

- A nightly pipeline starts producing tables with significantly fewer rows than usual. Quality monitoring flags the anomaly before any analyst notices a dashboard looks wrong.

- A new data engineer joins and needs to understand how data flows through the warehouse. The lineage graph gives them an instant visual map of every dependency.

- A business analyst asks which models feed the revenue dashboard. The lineage layer answers this in a single query against the `LINEAGE` schema in Snowflake.

---

## Tech Stack

| Tool | Purpose | Cost |
|---|---|---|
| Snowflake | Cloud data warehouse — transformation output, lineage storage, observability metrics | Free 30-day trial ($400 credits) |
| Postgres (Docker) | Source operational database — simulates a real transactional system | Free |
| Apache Airflow (Docker) | Pipeline orchestration — schedules and monitors every stage | Free |
| dbt Core | Transformation layer — staging, dimension, fact models, tests, snapshots | Free (not dbt Cloud) |
| Python | ETL scripts, lineage parsing, quality collection, drift detection | Free |
| NetworkX | In-memory graph library for lineage traversal and blast radius computation | Free |
| LLM (Mistral via Hugging Face / Claude API) | AI blast radius ranker | Free tier available |
| Metabase (Docker) | Dashboard and visualization layer | Free open source |
| Docker Desktop | Container runtime for Postgres, Airflow, Metabase | Free personal use |

---

## Architecture Overview

```
Postgres (Docker)
      │
      │  ETL (Python + Parquet + MERGE)
      ▼
Snowflake RAW Schema
      │
      │  ELT (dbt Core)
      ▼
Snowflake DBT_DEV Schema
  ├── Staging Views       (stg_*)
  ├── Dimension Tables    (dim_*)
  ├── Fact Tables         (fact_*)
  └── Snapshots           (snap_*)
      │
      │  Observability Layer (Python)
      ├──► LINEAGE Schema       (nodes + edges from manifest.json)
      ├──► OBSERVABILITY Schema (quality metrics + schema registry)
      │
      │  AI Ranker (NetworkX + LLM)
      ▼
Blast Radius Report (plain-English ranked severity list)
      │
      ▼
Metabase Dashboard (Pipeline Health)
```

All tasks are orchestrated by a single Airflow DAG (`lineageiq_elt`) running on a daily schedule.

---

## Project Structure

```
LineageIQ/
├── etl/                  # Python scripts: Postgres → Snowflake
│   ├── generate_tpch_data.py
│   ├── load_to_postgres.py
│   └── load_to_snowflake.py
├── elt/                  # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/      # stg_* views (rename + clean raw columns)
│   │   └── marts/        # dim_* and fact_* tables
│   ├── snapshots/        # SCD Type 2 (snap_supplier, snap_part)
│   └── tests/            # Custom SQL data quality tests
├── airflow/
│   ├── dags/
│   │   ├── etl_dag.py        # Phase 2: Postgres → Snowflake DAG
│   │   └── lineageiq_elt.py  # Phase 5: Master ELT DAG (all phases)
│   └── docker-compose.yml
├── lineage/
│   ├── manifest_parser.py    # Parses dbt manifest → LINEAGE schema
│   └── graph_analytics.py    # NetworkX graph traversal + depth
├── observability/
│   ├── schema_registry.py    # Schema snapshots + baseline tracking
│   ├── quality_collector.py  # Row counts + null PK rates
│   └── drift_detector.py     # Baseline vs. latest schema comparison
├── ai_ranker/
│   └── blast_radius.py       # Prompt assembly + LLM call + ranked report
├── tpch_data/            # Generated CSV files (gitignored)
└── .env                  # Secrets (gitignored)
```

---

## Setup

### Prerequisites

- Python 3.10 or 3.11 (3.12 has Airflow compatibility issues — avoid it)
- Docker Desktop (running)
- A free Snowflake account (30-day trial at snowflake.com)
- A free GitHub account
- A Hugging Face account (for the AI ranker) or an Anthropic API key

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/LineageIQ.git
cd LineageIQ
```

### 2. Create Your Virtual Environment

```bash
python -m venv venv

# Mac/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install dbt-core dbt-snowflake snowflake-connector-python \
  pandas apache-airflow anthropic python-dotenv psycopg2-binary \
  sqlalchemy pyarrow networkx
```

**Why each package:**
- `dbt-core` + `dbt-snowflake` — dbt transformation framework and Snowflake adapter
- `snowflake-connector-python` — low-level Snowflake connection used by ETL scripts
- `pandas` — DataFrame manipulation for reading from Postgres and writing to Parquet
- `apache-airflow` — orchestration framework (local install for CLI; scheduler runs in Docker)
- `anthropic` — Claude API client (optional; only needed if using Claude as the LLM)
- `python-dotenv` — loads `.env` file into environment variables
- `psycopg2-binary` — Postgres driver
- `sqlalchemy` — used by pandas `read_sql`/`to_sql` for Postgres connections
- `pyarrow` — Parquet file serialization (required by the stage step in ETL)
- `networkx` — graph library for traversing the lineage DAG

### 4. Set Up Your `.env` File

Create `.env` at the project root. This file is gitignored and must never be committed.

```env
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PIPELINE_PLATFORM

ANTHROPIC_API_KEY=sk-ant-...   # optional — only if using Claude

PG_HOST=localhost               # use 'lineageiq-tpch-db' inside Docker
PG_PORT=5432
PG_USER=tpch_user
PG_PASSWORD=tpch_pass
PG_DB=tpch
```

> **Note on `PG_HOST`:** When running scripts locally, use `localhost`. When Airflow runs scripts inside the Docker container, it uses the container name `lineageiq-tpch-db`. The docker-compose file handles this automatically via its own environment block.

### 5. Start Docker Services

```bash
cd airflow
docker compose up -d
```

This starts three containers:
- `lineageiq-tpch-db` — source Postgres database
- `lineageiq-airflow-db` — Postgres for Airflow's internal metadata (kept separate intentionally)
- `lineageiq-airflow` — the Airflow scheduler and webserver

Verify all three are running:

```bash
docker ps
```

All three should show status `Up`.

### 6. Set Up Snowflake

Log into your Snowflake account, open a worksheet, and run:

```sql
CREATE DATABASE IF NOT EXISTS PIPELINE_PLATFORM;
CREATE SCHEMA IF NOT EXISTS PIPELINE_PLATFORM.RAW;
CREATE SCHEMA IF NOT EXISTS PIPELINE_PLATFORM.DBT_DEV;
CREATE SCHEMA IF NOT EXISTS PIPELINE_PLATFORM.OBSERVABILITY;
CREATE SCHEMA IF NOT EXISTS PIPELINE_PLATFORM.LINEAGE;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

**Schema purposes:**
- `RAW` — landing zone for ETL output. Data arrives exactly as extracted from Postgres, plus audit columns.
- `DBT_DEV` — where dbt writes all transformation output: staging models, dimensions, facts, snapshots.
- `OBSERVABILITY` — quality metrics collected after each run, schema snapshots for drift detection.
- `LINEAGE` — the parsed lineage graph: nodes (models) and edges (dependencies).

> `AUTO_SUSPEND = 60` ensures the warehouse shuts down after 60 seconds of inactivity — critical to avoid burning trial credits on idle compute.

---

## Phase-by-Phase Walkthrough

### Phase 1 — Environment Setup

Everything in steps 1–6 above constitutes Phase 1. Before proceeding, verify:

- `docker ps` shows all three containers running
- You can connect to Snowflake from your browser and see the four schemas
- `python -c "import snowflake.connector, pandas, psycopg2, pyarrow; print('ok')"` returns `ok` inside your venv
- Postgres is reachable from inside Airflow:
  ```bash
  docker exec lineageiq-airflow python -c \
    "import psycopg2; psycopg2.connect(host='lineageiq-tpch-db', port=5432, user='tpch_user', password='tpch_pass', dbname='tpch'); print('ok')"
  ```

---

### Phase 2 — Source Layer and ETL Pipeline

#### The Data: TPC-H

LineageIQ uses the TPC-H benchmarking dataset — a standard synthetic supply chain model with customers, orders, line items, suppliers, and parts. It has realistic relational structure, proper foreign key relationships, and no privacy concerns.

**Data model:**
- `CUSTOMER` — master data for customers (PK: `C_CUSTKEY`)
- `SUPPLIER` — master data for suppliers (PK: `S_SUPPLIERKEY`)
- `PART` — catalog of parts (PK: `P_PARTKEY`)
- `ORDERS` — one row per order (PK: `O_ORDERKEY`, FK: `O_CUSTKEY`)
- `LINEITEM` — individual line items (composite PK: `L_ORDERKEY` + `L_LINENUMBER`)

Load order matters: `CUSTOMER`, `SUPPLIER`, and `PART` must exist before `ORDERS`; `ORDERS` must exist before `LINEITEM`.

#### Generate and Load the Data

```bash
# Generate synthetic TPC-H data (fixed seed for reproducibility)
python etl/generate_tpch_data.py

# Load into Postgres
python etl/load_to_postgres.py
```

Expected Postgres row counts: 1,000 customers, 100 suppliers, 500 parts, 3,000 orders, 9,000 line items.

#### Run the ETL to Snowflake

The ETL pipeline uses a four-stage pattern per table: **Extract → Transform → Stage → Load**.

- Data is written to Parquet locally, uploaded to a Snowflake internal stage via `PUT`, then loaded via `COPY INTO` and upserted via `MERGE`.
- `MERGE` makes the pipeline idempotent — safe to re-run after any failure without duplicating data.
- `_LOADED_AT` audit columns are set on first insert and never overwritten, preserving original arrival timestamps.

```bash
python etl/load_to_snowflake.py
```

Verify in Snowflake:
```sql
SELECT 'CUSTOMER' as t, COUNT(*), MAX(_LOADED_AT) FROM PIPELINE_PLATFORM.RAW.CUSTOMER
UNION ALL SELECT 'ORDERS', COUNT(*), MAX(_LOADED_AT) FROM PIPELINE_PLATFORM.RAW.ORDERS
UNION ALL SELECT 'LINEITEM', COUNT(*), MAX(_LOADED_AT) FROM PIPELINE_PLATFORM.RAW.LINEITEM
UNION ALL SELECT 'SUPPLIER', COUNT(*), MAX(_LOADED_AT) FROM PIPELINE_PLATFORM.RAW.SUPPLIER
UNION ALL SELECT 'PART', COUNT(*), MAX(_LOADED_AT) FROM PIPELINE_PLATFORM.RAW.PART;
```

Expected: 1,000 / 3,000 / 9,000 / 100 / 500 rows respectively, all with a recent `_LOADED_AT`.

#### Airflow DAG (Phase 2)

The `lineageiq_etl` DAG in `airflow/dags/etl_dag.py` runs the ETL on a `@daily` schedule with `catchup=False` and `retries=2`.

Access the Airflow UI at [http://localhost:8081](http://localhost:8081) (default credentials: `admin` / `admin`). Toggle the DAG on and click the play button to trigger a manual run.

**Phase 2 checkpoint:**
- All five RAW tables have correct row counts
- `_LOADED_AT` is populated and `_SOURCE` shows `'postgres'` on every row
- The Airflow DAG shows green in Grid view
- Running the DAG a second time produces identical row counts (idempotency check)

---

### Phase 3 — dbt Analytics Engineering Layer

dbt transforms raw operational data into a clean, tested, analytics-ready dimensional model. It also generates `manifest.json` — the machine-readable dependency graph that powers the lineage tracker in Phase 4.

#### Why dbt

Every `{{ ref() }}` call in a dbt model records a dependency edge. dbt resolves all of these and writes the complete graph into `target/manifest.json`. The lineage parser in Phase 4 reads that file — you're not recomputing anything, just extracting what dbt already knows.

#### Three-Layer Architecture

**Staging layer (`stg_*`)** — materialized as **views**. Sits directly on top of raw source tables. Single responsibility: translate source schema into the project's internal naming convention. No business logic, no joins, no aggregations. The only layer that references `source()` — everything else uses `ref()`.

**Dimension layer (`dim_*`)** — materialized as **tables**. Represent business entities: customers, suppliers, parts. Relatively small in row count, wide in descriptive columns, change slowly. Each has a primary key referenced by fact tables.

**Fact layer (`fact_*`)** — materialized as **tables**. Represent events and transactions: orders and line items. Large in row count, contain foreign keys and numeric measures. Inner joins to dimensions enforce referential integrity and act as a data quality gate.

#### Running the Build

```bash
cd elt
dbt debug     # verify Snowflake connection
dbt build     # runs all models, tests, and snapshots in dependency order
```

The full build runs 25 tasks (10 models + 15 tests) and all 25 should pass on clean data.

#### Data Tests

Tests are declared in `models/marts/schema.yml` using four built-in types:

| Test | What It Checks |
|---|---|
| `unique` | No duplicate primary keys |
| `not_null` | No NULL values on PKs or FKs |
| `accepted_values` | `market_segment` only contains valid TPC-H values |
| `relationships` | Every FK value exists as a PK in the referenced table |

Two custom SQL tests are in `tests/`:
- `assert_no_negative_prices.sql` — no row in `fact_lineitem` has a negative price, discount, or tax
- `assert_orders_have_lineitems.sql` — no order exists without at least one line item (uses `LEFT JOIN ... IS NULL` rather than `NOT IN` to avoid three-valued logic pitfalls)

#### Snapshots (SCD Type 2)

Snapshots track how dimension data changes over time, preserving historical versions of records with `dbt_valid_from` / `dbt_valid_to` timestamps.

- `snap_supplier` — tracks changes to `account_balance`, `address`, `phone`
- `snap_part` — tracks changes to `retail_price`, `brand`, `part_type`

Run snapshots separately:
```bash
dbt snapshot
```

#### Lineage Visualization

```bash
dbt docs generate
dbt docs serve
```

The documentation server renders the full lineage graph. Green nodes on the left are raw sources; teal nodes in the middle are dbt-owned models. The edges in this visualization are exactly what the lineage parser extracts into Snowflake.

**Phase 3 checkpoint:**
- `dbt build` runs 25 tasks, all pass
- `dbt docs serve` shows the complete lineage graph
- Snowflake `DBT_DEV` schema contains all staging views, dimension tables, fact tables, and snapshot tables

---

### Phase 4 — Observability Layer

Phase 4 takes the implicit, fragile knowledge in dbt's output and makes it explicit, persistent, and queryable in Snowflake. It is the foundation that makes the AI blast radius analysis possible.

#### Manifest Parser (`lineage/manifest_parser.py`)

Reads `target/manifest.json` and extracts:
- **Nodes** — every model and snapshot, with column metadata stored as `VARIANT` (native JSON) in Snowflake
- **Edges** — every upstream/downstream dependency pair

The `LINEAGE` schema tables are replaced on every run (DELETE before insert) so they always reflect the current manifest state, not accumulated history.

```bash
python lineage/manifest_parser.py
```

#### Schema Registry (`observability/schema_registry.py`)

Snapshots the column structure of each model by querying `INFORMATION_SCHEMA.COLUMNS`. The first snapshot for each model is marked `is_baseline = TRUE` and never overwritten — it represents the intended state. Every subsequent snapshot is `is_baseline = FALSE`.

```bash
python observability/schema_registry.py
```

#### Quality Collector (`observability/quality_collector.py`)

Runs two measurements against each model on every run: total row count and null primary key count. Uses `SUM(CASE WHEN pk IS NULL THEN 1 ELSE 0 END)` to compute both in a single query.

```bash
python observability/quality_collector.py
```

#### Drift Detector (`observability/drift_detector.py`)

Compares the latest non-baseline schema snapshot against the baseline and classifies each change:

| Change Type | Severity | Reason |
|---|---|---|
| Column dropped | HIGH | Downstream SQL referencing that column will fail |
| Type changed | HIGH | Silent data corruption possible (e.g. VARCHAR → INT) |
| Column added | LOW | Additive change — existing downstream models are unaffected |

> **The rename problem:** A renamed column appears as a `column_dropped` (HIGH) + `column_added` (LOW). This is intentional — from a downstream model's perspective, a rename is functionally identical to a drop.

---

### Phase 5 — AI Blast Radius Ranker + Dashboard

Phase 5 is where everything comes together.

#### Graph Analytics (`lineage/graph_analytics.py`)

Builds an in-memory directed graph from `LINEAGE.LINEAGE_EDGES` using NetworkX. Two functions:

- `build_lineage_graph(sf_conn)` — constructs the `DiGraph` from Snowflake edge data
- `get_downstream_assets(graph, changed_node_id)` — returns every downstream node with its minimum hop depth using `nx.descendants()` and `nx.shortest_path_length()`

Depth is computed as shortest path length, so if `fact_orders` depends on `stg_orders` both directly and via `dim_customer`, it correctly reports depth 1, not 2.

```bash
python lineage/graph_analytics.py
```

Expected output:
```
Graph has 17 nodes and 15 edges.

Downstream of model.pipeline_platform.stg_orders:
  depth 1 — fact_orders
```

#### AI Blast Radius Ranker (`ai_ranker/blast_radius.py`)

When drift is detected, the ranker:

1. Fetches quality scores from `OBSERVABILITY.QUALITY_METRICS` using a `QUALIFY` window function to get only the most recent snapshot per model
2. Assembles a structured prompt combining: drift events, downstream assets with lineage depths, and historical null PK rates and row counts
3. Calls the language model with instructions to rank each downstream asset from CRITICAL to LOW
4. Returns a plain-English ranked severity report

**Why an LLM instead of a rules-based scorer:**

Signal interaction is non-linear. A model at depth 2 with a 15% null PK rate and 10 million rows might be more critical than a model at depth 1 with 100 rows and clean data. A rules engine requires explicit weights; the LLM reasons about the combination naturally. The plain-English explanation per asset is also as valuable as the ranking itself — an engineer seeing the reason can act immediately.

**Ranking signals:**

| Signal | Direction |
|---|---|
| Lineage depth | Lower depth = higher risk (closer to the break) |
| Historical null PK rate | Higher rate = higher risk (more fragile) |
| Row count | Higher count = higher downstream impact |
| Change type | Dropped > type changed > added |

**Sample output:**
```
=== BLAST RADIUS REPORT: dim_customer ===

1. CRITICAL — fact_orders: Sits one hop downstream of the renamed column
   with 1.5M rows. Any query referencing MARKET_SEGMENT will break immediately.

2. HIGH — fact_lineitem: Indirectly joins through fact_orders. Secondary
   breakage risk if fact_orders fails to build.

3. LOW — dim_part: No dependency on the changed column. Monitor but no
   immediate action needed.
```

#### Master DAG (`airflow/dags/lineageiq_elt.py`)

The `lineageiq_elt` DAG wires all five phases into a single automated pipeline:

```
dbt_build
    │
    ├──► lineage_parse   ─┐
    ├──► schema_snapshot ─┼──► drift_and_rank
    └──► quality_collect ─┘
```

- `dbt_build` runs first (no dependencies)
- `lineage_parse`, `schema_snapshot`, and `quality_collect` run **in parallel** after `dbt_build` (no inter-dependencies between them)
- `drift_and_rank` runs last, after all three parallel tasks complete — it needs the lineage graph, schema snapshots, and quality scores together

Runs daily. Trigger manually from the Airflow UI at [http://localhost:8081](http://localhost:8081).

#### Pipeline Health Dashboard (Metabase)

Metabase runs in Docker and connects natively to Snowflake. Four charts on the Pipeline Health dashboard:

| Chart | Source Table | What It Shows |
|---|---|---|
| Row Count Over Time | `OBSERVABILITY.QUALITY_METRICS` | Volume per model per day — spots sudden drops or unexpected growth |
| Null PK Rate Over Time | `OBSERVABILITY.QUALITY_METRICS` | Rising rates signal quality degradation before failures |
| Schema Change History | `OBSERVABILITY.SCHEMA_REGISTRY` | Snapshot events by day — when schema activity happened |
| Lineage Depth Distribution | `LINEAGE.LINEAGE_EDGES` | Which source models have the widest blast radius |

Access Metabase at [http://localhost:3000](http://localhost:3000).

---

## End-to-End Demo

Simulate a real breaking schema change to see the full system in action:

**Step 1 — Run the master DAG once** to establish baselines in `SCHEMA_REGISTRY`.

**Step 2 — Simulate a schema change** by renaming a column in Snowflake:
```sql
ALTER TABLE PIPELINE_PLATFORM.DBT_DEV.DIM_CUSTOMER
RENAME COLUMN MARKET_SEGMENT TO MKTSEGMENT;
```

**Step 3 — Re-trigger the DAG.** The `schema_snapshot` task captures the new schema.

**Step 4 — Watch the output.** The `drift_and_rank` task logs a CRITICAL-to-LOW blast radius report identifying `fact_orders` as the highest-risk downstream asset, with the reason explained in plain English.

---

## Key Design Decisions

**ETL uses Parquet + staged COPY INTO + MERGE** — not row-by-row inserts. This is the standard Snowflake bulk loading pattern: faster, type-safe, and idempotent.

**The staging layer is the only layer that references `source()`** — every other layer uses `ref()`. This means a source schema change is fixed in exactly one place.

**Staging models are views, not tables** — they do no heavy computation and are never queried by analysts directly. No reason to store redundant copies.

**Baseline snapshots are never overwritten** — they represent the intended state of the pipeline, not the current state. Drift is always measured against this fixed reference point.

**NetworkX over recursive SQL CTEs** — graph traversal logic is easier to read, test, and extend in Python. Snowflake does the storage; NetworkX does the traversal.

**`PG_HOST` differs between local and Docker contexts** — locally `localhost` works; inside the Airflow container, the Postgres service is reachable only by its container name `lineageiq-tpch-db`. The docker-compose `environment` block handles this automatically.

---

## Extensibility

The architecture is designed to extend cleanly:

- **Additional data sources** — extend the ETL layer and add new dbt staging models for any source system
- **Slack/email alerting** — route the blast radius report to a Slack channel instead of (or in addition to) Airflow logs
- **Custom ranking logic** — tune the LLM prompt to incorporate business priority signals (e.g. models feeding executive dashboards get automatically higher severity)
- **Column-level lineage** — dbt's manifest contains column-level dependencies; the parser can be extended to build field-level blast radius reports
- **OpenLineage integration** — replace the manifest parser with an OpenLineage collector to capture lineage from any tool, not just dbt
- **SLA tracking** — add expected row count ranges per model and alert on anomalies, complementing schema drift detection with volume anomaly detection

---

## Who This Is For

| Persona | What They Get From LineageIQ |
|---|---|
| Data Engineer | Immediate blast radius report in Airflow logs — know exactly what to fix and in what order |
| Analytics Engineer | dbt build failures surfaced early, before downstream consumers are affected |
| Data Team Lead | Pipeline Health dashboard showing trends in data quality and schema stability over time |
| Stakeholder | Fewer surprise dashboard outages because issues are caught and triaged at the pipeline level |

---

## License

MIT

---

## Author

Chetana Muralidharan

Master's Student | Applied Data Intelligence

GitHub: https://github.com/ChetanaMuralidharan
