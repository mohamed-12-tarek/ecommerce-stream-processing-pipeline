#!/usr/bin/env bash
set -euo pipefail

FLINK_HOME="${FLINK_HOME:-$HOME/flink-2.2.1}"
HBASE_HOME="${HBASE_HOME:-$HOME/hbase-2.5.15}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/flink/build/hbase-test"
HBASE_CLIENT_JAR="$HBASE_HOME/lib/shaded-clients/hbase-shaded-client-2.5.15.jar"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/classes"

javac \
    -cp "$FLINK_HOME/lib/*:$HBASE_CLIENT_JAR" \
    -d "$BUILD_DIR/classes" \
    "$PROJECT_DIR/flink/java/HBaseConnectionCheck.java"

java \
    -cp "$BUILD_DIR/classes:$HBASE_CLIENT_JAR" \
    flink.hbase.HBaseConnectionCheck
