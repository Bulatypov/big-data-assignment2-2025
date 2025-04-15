#!/bin/bash
set -euo pipefail

# Configuration
VENV_PATH="./.venv"
OUTPUT_DIR="/bm25_output"
SPARK_CONNECTOR="com.datastax.spark:spark-cassandra-connector_2.12:3.4.0"
VENV_ARCHIVE="/app/.venv.tar.gz#.venv"

# Initialize variables
query_term="$1"

cleanup_output() {
    echo "Cleaning up previous output directory..."
    hdfs dfs -rm -r "$OUTPUT_DIR" || true
}

setup_environment() {
    echo "Setting up Python environment..."
    source "$VENV_PATH/bin/activate"

    # Configure Python paths for Spark
    export PYSPARK_DRIVER_PYTHON=$(which python)
    export PYSPARK_PYTHON=./.venv/bin/python
}

run_spark_query() {
    echo "Executing Spark query for term: $query_term"

    spark-submit \
        --master yarn \
        --packages "$SPARK_CONNECTOR" \
        --archives "$VENV_ARCHIVE" \
        query.py "$query_term"
}

display_results() {
    echo "Query results:"
    hdfs dfs -cat "$OUTPUT_DIR/*.csv"
}

main() {
    cleanup_output
    setup_environment
    run_spark_query
    display_results
}

main "$@"