#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

jar=out/analytics.jar
connector=lib/mysql-connector-j-9.0.0.jar
for required in out/common.jar out/contracts.jar "$jar" "$connector"; do
  if [ ! -f "$required" ]; then
    echo "Missing $required; run ./build.sh analytics first" >&2
    exit 1
  fi
done
if [ -z "$(ls lib/grpc/*.jar 2>/dev/null)" ]; then
  echo "Missing gRPC runtime in lib/grpc/; run ./build.sh analytics first" >&2
  exit 1
fi
case "$(uname -s)" in CYGWIN*|MINGW*|MSYS*) sep=';' ;; *) sep=':' ;; esac
exec java -cp "$jar${sep}out/common.jar${sep}out/contracts.jar${sep}lib/grpc/*${sep}$connector" analytics.AnalyticsMain "$@"
