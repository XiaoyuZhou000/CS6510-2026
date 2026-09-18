#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
if [ ! -d out/main ]; then
  echo "Not built yet - running build.sh first..."
  ./build.sh
fi
if [ ! -d out/test ]; then
  echo "Tests not compiled yet - running build-tests.sh first..."
  ./build-tests.sh
fi
case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=";" ;;
  *) SEP=":" ;;
esac
# Tests use JVM properties; take defaults from the same environment as the server.
# Explicit -D arguments supplied by the caller override these defaults.
java "-DDB_USER=${DB_USER:-root}" "-DDB_PASS=${DB_PASSWORD:-}" "$@" -cp "lib/*${SEP}out/main${SEP}out/test" org.junit.platform.console.ConsoleLauncher execute --scan-classpath
