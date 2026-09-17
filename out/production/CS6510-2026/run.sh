#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
if [ ! -d out ]; then
  echo "Not built yet - running build.sh first..."
  ./build.sh
fi
# Defaults: port=8080 catalogSize=2000 stockPerItem=10000 lowStockThreshold=50
java -cp out MockServer "$@"
