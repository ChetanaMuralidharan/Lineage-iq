FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "dbt-core==1.7.0" \
    "dbt-snowflake==1.7.0" \
    "snowflake-connector-python==3.6.0" \
    "pyOpenSSL==23.2.0" \
    "cryptography==41.0.7" \
    "networkx==3.2" \
    "python-dotenv==1.0.0" \
    "requests==2.31.0"