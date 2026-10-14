#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

jar=out/transaction.jar
connector=lib/mysql-connector-j-9.0.0.jar
for required in out/common.jar "$jar" "$connector"; do
  if [ ! -f "$required" ]; then
    echo "Missing $required; run ./build.sh transaction first" >&2
    exit 1
  fi
done
case "$(uname -s)" in CYGWIN*|MINGW*|MSYS*) sep=';' ;; *) sep=':' ;; esac
exec java -cp "$jar${sep}out/common.jar${sep}$connector" transaction.TransactionMain "$@"
