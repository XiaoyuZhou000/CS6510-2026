#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p out
javac -d out MockServer.java Json.java
echo "Built. Run with: ./run.sh [port] [catalogSize] [stockPerItem] [lowStockThreshold]"
