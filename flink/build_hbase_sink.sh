#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_HOME="${FLINK_HOME:-$HOME/flink-2.2.1}"
HBASE_HOME="${HBASE_HOME:-$HOME/hbase-2.5.15}"

SOURCE_DIR="$PROJECT_DIR/flink/java"
BUILD_DIR="$PROJECT_DIR/flink/build/hbase-sink"
OUTPUT_JAR="$PROJECT_DIR/flink/lib/hbase-event-sink.jar"
HBASE_CLIENT_JAR="$HBASE_HOME/lib/shaded-clients/hbase-shaded-client-2.5.15.jar"
FLINK_RUNTIME_LIB="$FLINK_HOME/lib"

if [[ ! -d "$FLINK_RUNTIME_LIB" ]]; then
    echo "ERROR: Flink distribution not found: $FLINK_HOME" >&2
    exit 1
fi

if [[ ! -f "$HBASE_CLIENT_JAR" ]]; then
    echo "ERROR: HBase shaded client not found: $HBASE_CLIENT_JAR" >&2
    exit 1
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/classes"

javac \
    -cp "$FLINK_RUNTIME_LIB/*:$HBASE_CLIENT_JAR" \
    -d "$BUILD_DIR/classes" \
    "$SOURCE_DIR/HBaseEventSink.java"

jar cf "$OUTPUT_JAR" \
    -C "$BUILD_DIR/classes" .

# The Python job constructs the Java SinkFunction through Py4J before the
# Flink job is submitted, so the custom sink must be visible to the Flink
# client classloader. The shaded HBase client is installed alongside it.
cp "$OUTPUT_JAR" "$FLINK_RUNTIME_LIB/hbase-event-sink.jar"
cp "$HBASE_CLIENT_JAR" "$FLINK_RUNTIME_LIB/hbase-shaded-client-2.5.15.jar"

echo "Built: $OUTPUT_JAR"
echo "Installed: $FLINK_RUNTIME_LIB/hbase-event-sink.jar"
echo "Installed: $FLINK_RUNTIME_LIB/hbase-shaded-client-2.5.15.jar"
