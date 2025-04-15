#!/bin/bash
set -euo pipefail

# Configuration
DEFAULT_INPUT_PATH="/index/data"
OUTPUT_PATH="/tmp/index/output"
VENV_PATH="./.venv"
CASSANDRA_SCHEMA="/app/cassandra/schema.cql"
HDFS_TEMP_DIR="/local_files"

# Initialize variables
input_path="${1:-$DEFAULT_INPUT_PATH}"
is_custom_input=false

prepare_custom_input() {
    echo "Preparing custom input file from $input_path for HDFS"

    local temp_dir="local_files"
    local filename=$(basename "$input_path")
    local output_file="$temp_dir/$filename"
    local doc_id=$(( RANDOM % 900000 + 100000 ))
    local title=$(basename "$input_path" .txt)
    local text=$(tr '\n' ' ' < "$input_path")

    mkdir -p "$temp_dir"
    echo -e "${doc_id}\t${title}\t${text}" > "$output_file"

    hdfs dfs -rm -r "$HDFS_TEMP_DIR" || true
    hdfs dfs -put "$temp_dir" /
    hdfs dfs -ls "$HDFS_TEMP_DIR"

    rm -rf "$temp_dir"
    input_path="$HDFS_TEMP_DIR/$filename"
    is_custom_input=true
}

run_mapreduce_job() {
    local job_name=$1
    local mapper=$2
    local reducer=$3
    local input=$4
    local output=$5

    echo "Running $job_name..."
    hadoop fs -rm -r "$output" || true

    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -files "/app/mapreduce/$mapper.py,/app/mapreduce/$reducer.py" \
        -archives "/app/.venv.tar.gz#.venv" \
        -mapper ".venv/bin/python $mapper.py" \
        -reducer ".venv/bin/python $reducer.py" \
        -input "$input" \
        -output "$output"
}

main() {
    # Activate Python virtual environment
    source "$VENV_PATH/bin/activate"

    # Handle custom input if provided
    if [[ "$input_path" != "$DEFAULT_INPUT_PATH" ]]; then
        prepare_custom_input
    fi

    # Initialize Cassandra schema
    echo "Setting up Cassandra schema..."
    cqlsh cassandra-server -f "$CASSANDRA_SCHEMA"

    # Run MapReduce jobs
    run_mapreduce_job "MapReduce1" "mapper1" "reducer1" "$input_path" "$OUTPUT_PATH"
    run_mapreduce_job "MapReduce2" "mapper2" "reducer2" "$input_path" "$OUTPUT_PATH"

    echo "Indexing complete. Info inserted into Cassandra."
}

main "$@"