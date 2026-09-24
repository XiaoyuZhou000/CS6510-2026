#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p out/main

# Keep source discovery recursive as classes move into the layered package tree.
SOURCES=()
while IFS= read -r source; do
  SOURCES+=("$source")
done < <(find src -type f -name "*.java" | LC_ALL=C sort)
if [ "${#SOURCES[@]}" -eq 0 ]; then
  echo "No Java sources found under server/src" >&2
  exit 1
fi

MYSQL_CONNECTOR="lib/mysql-connector-j-9.0.0.jar"
javac --release 21 -d out/main -cp "$MYSQL_CONNECTOR" "${SOURCES[@]}"
echo "Built. Run with: ./run.sh [port] [dbHost] [dbPort] [dbName] [dbUser] [dbPassword]"
