#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -f out/common.jar ] || [ ! -f out/transaction.jar ] ||
   [ ! -f out/inventory.jar ] || [ ! -f out/analytics.jar ]; then
  echo "Build artifacts missing - running build.sh first..."
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
JVM_ARGS=("-DDB_USER=${DB_USER:-root}" "-DDB_PASS=${DB_PASSWORD:-}")
STRICT_LIVE=false
if [ -n "${SERVER_BASE_URL:-}" ]; then
  JVM_ARGS+=("-DSERVER_BASE_URL=$SERVER_BASE_URL")
  STRICT_LIVE=true
fi
for arg in "$@"; do
  case "$arg" in
    -DSERVER_BASE_URL=*) STRICT_LIVE=true ;;
  esac
done

REPORT_DIR="out/test-results/run-$(date +%Y%m%d%H%M%S)-$$"
mkdir -p "$REPORT_DIR"
java "${JVM_ARGS[@]}" "$@" \
  -cp "lib/*${SEP}out/common.jar${SEP}out/transaction.jar${SEP}out/inventory.jar${SEP}out/analytics.jar${SEP}out/test" \
  org.junit.platform.console.ConsoleLauncher execute --scan-classpath --reports-dir "$REPORT_DIR"

# JUnit assumptions mark unavailable live tests as skipped. An explicitly
# configured public URL means those skips are a failed validation run.
if [ "$STRICT_LIVE" = true ]; then
  SKIPPED_LIVE="$(awk '
    /<testcase / {
      current = $0
      sub(/^.*classname="/, "", current)
      sub(/".*$/, "", current)
    }
    /<skipped/ && current ~ /^(contract|integration)\./ &&
        current != "integration.ResetBaselineTest" { print current }
  ' "$REPORT_DIR"/TEST-*.xml | LC_ALL=C sort -u)"
  if [ -n "$SKIPPED_LIVE" ]; then
    echo "Live tests skipped despite SERVER_BASE_URL being set:" >&2
    echo "$SKIPPED_LIVE" >&2
    exit 1
  fi
fi
