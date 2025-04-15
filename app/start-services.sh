#!/bin/bash
set -euo pipefail

# Configuration
HADOOP_DFS_SCRIPT="$HADOOP_HOME/sbin/start-dfs.sh"
HADOOP_YARN_SCRIPT="$HADOOP_HOME/sbin/start-yarn.sh"
SPARK_JARS_SOURCE="/usr/local/spark/jars/*"
SPARK_JARS_HDFS="/apps/spark/jars"
HDFS_USER_DIR="/user/root"

# Function to start Hadoop services
start_hadoop_services() {
    echo "Starting HDFS daemons..."
    "$HADOOP_DFS_SCRIPT"

    echo "Starting YARN daemons..."
    "$HADOOP_YARN_SCRIPT"

    echo "Starting MapReduce History Server..."
    mapred --daemon start historyserver
}

# Function to verify and configure HDFS
configure_hdfs() {
    echo "Checking HDFS status..."
    hdfs dfsadmin -report

    echo "Ensuring Namenode is out of safemode..."
    hdfs dfsadmin -safemode leave

    echo "Creating Spark jars directory in HDFS..."
    hdfs dfs -mkdir -p "$SPARK_JARS_HDFS"
    hdfs dfs -chmod 744 "$SPARK_JARS_HDFS"

    echo "Copying Spark jars to HDFS..."
    hdfs dfs -put "$SPARK_JARS_SOURCE" "$SPARK_JARS_HDFS/"
    hdfs dfs -chmod +rx "$SPARK_JARS_HDFS/"

    echo "Creating user directory in HDFS..."
    hdfs dfs -mkdir -p "$HDFS_USER_DIR"
}

# Function to display service status
show_service_status() {
    echo "Current running services:"
    jps -lm

    echo "Scala version (Spark):"
    scala -version
}

main() {
    echo "Master node initialization started"

    start_hadoop_services
    configure_hdfs
    show_service_status

    echo "Master node setup completed successfully"
}

main "$@"