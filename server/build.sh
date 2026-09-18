#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p out/main
SOURCES=$(find src -name "*.java")
javac -d out/main -cp "lib/*" $SOURCES
echo "Built. Run with: ./run.sh [port] [dbHost] [dbPort] [dbName] [dbUser] [dbPassword]"
