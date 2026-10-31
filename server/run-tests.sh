#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -f out/common.jar ] || [ ! -f out/contracts.jar ] || [ ! -d lib/messaging ] ||
   [ ! -f out/transaction.jar ] || [ ! -f out/inventory.jar ] || [ ! -f out/analytics.jar ]; then
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
# Live broker suites (006 D14): the listener of deploy/kafka/server.properties by default.
# KAFKA_TEST_BROKER_CONTROL=1 lets broker restart tests stop and start the native broker through
# deploy/kafka/*.ps1, which need KAFKA_HOME.
JVM_ARGS+=("-DKAFKA_TEST_BOOTSTRAP=${KAFKA_TEST_BOOTSTRAP:-127.0.0.1:19092}")
if [ -n "${KAFKA_TEST_BROKER_CONTROL:-}" ]; then
  JVM_ARGS+=("-DKAFKA_TEST_BROKER_CONTROL=$KAFKA_TEST_BROKER_CONTROL")
fi
if [ -n "${KAFKA_HOME:-}" ]; then
  JVM_ARGS+=("-DKAFKA_HOME=$KAFKA_HOME")
fi
if [ -n "${NGINX_BIN:-}" ]; then
  JVM_ARGS+=("-DNGINX_BIN=$NGINX_BIN")
fi
STRICT_LIVE=false
# Packages whose skipped tests fail a strict run.
LIVE_PACKAGES='contract|integration|performance'
if [ -n "${SERVER_BASE_URL:-}" ]; then
  JVM_ARGS+=("-DSERVER_BASE_URL=$SERVER_BASE_URL")
  STRICT_LIVE=true
fi
# Arguments starting with - are JVM options; bare words select test packages, for example
# ./run-tests.sh architecture messaging. Without one, the whole classpath is scanned.
CALLER_JVM_ARGS=()
SELECTORS=()
for arg in "$@"; do
  case "$arg" in
    -DSERVER_BASE_URL=*) STRICT_LIVE=true; CALLER_JVM_ARGS+=("$arg") ;;
    -*) CALLER_JVM_ARGS+=("$arg") ;;
    *) SELECTORS+=(--select-package "$arg") ;;
  esac
done
[ "${#SELECTORS[@]}" -gt 0 ] || SELECTORS=(--scan-classpath)
# STRICT_LIVE_GATES=1 also makes a skipped live broker suite a failure (006 D14).
if [ "${STRICT_LIVE_GATES:-}" = 1 ]; then
  STRICT_LIVE=true
  LIVE_PACKAGES="messaging|$LIVE_PACKAGES"
fi
# PARITY_GATE=US2 or PARITY_GATE=006:US1 (for example) tells CoverageParityLedgerTest which
# section and story gate to enforce.
if [ -n "${PARITY_GATE:-}" ]; then
  JVM_ARGS+=("-Dparity.gate=$PARITY_GATE")
fi
# Direct restoration tests are reported apart from the end-to-end Saga results (SC-016).
RESTORATION_DIRECT_CLASSES="${RESTORATION_DIRECT_CLASSES:-messaging.RestorationEventDirectTest}"

REPORT_DIR="out/test-results/run-$(date +%Y%m%d%H%M%S)-$$"
mkdir -p "$REPORT_DIR"
# lib/* does not reach lib/messaging/, so the messaging runtime is listed separately.
set +e
java "${JVM_ARGS[@]}" ${CALLER_JVM_ARGS[@]+"${CALLER_JVM_ARGS[@]}"} \
  -cp "lib/*${SEP}lib/messaging/*${SEP}out/common.jar${SEP}out/contracts.jar${SEP}out/transaction.jar${SEP}out/inventory.jar${SEP}out/analytics.jar${SEP}out/test" \
  org.junit.platform.console.ConsoleLauncher execute "${SELECTORS[@]}" --reports-dir "$REPORT_DIR"
STATUS=$?
set -e

# Totals from the XML reports, with the direct restoration classes in their own section.
awk -v direct="$RESTORATION_DIRECT_CLASSES" '
  BEGIN { n = split(direct, names, /[ ,]+/); for (i = 1; i <= n; i++) if (names[i] != "") isDirect[names[i]] = 1 }
  /<testcase / {
    current = $0
    sub(/^.*classname="/, "", current)
    sub(/".*$/, "", current)
    group = (current in isDirect) ? "direct" : "main"
    tests[group]++
  }
  /<failure/ { failed[group]++ }
  /<error/ { failed[group]++ }
  /<skipped/ { skipped[group]++ }
  END {
    printf "Summary (excluding direct restoration): %d tests, %d failed, %d skipped\n", tests["main"], failed["main"], skipped["main"]
    printf "Direct restoration (SC-016) [%s]: %d tests, %d failed, %d skipped\n", direct, tests["direct"], failed["direct"], skipped["direct"]
  }
' "$REPORT_DIR"/TEST-*.xml

# JUnit assumptions mark unavailable live tests as skipped. An explicitly
# configured public URL or STRICT_LIVE_GATES=1 means those skips are a failed validation run.
if [ "$STRICT_LIVE" = true ]; then
  SKIPPED_LIVE="$(awk -v live="^($LIVE_PACKAGES)[.]" '
    /<testcase / {
      current = $0
      sub(/^.*classname="/, "", current)
      sub(/".*$/, "", current)
    }
    /<skipped/ && current ~ live &&
        current != "integration.ResetBaselineTest" { print current }
  ' "$REPORT_DIR"/TEST-*.xml | LC_ALL=C sort -u)"
  if [ -n "$SKIPPED_LIVE" ]; then
    echo "Live tests skipped despite SERVER_BASE_URL or STRICT_LIVE_GATES=1 being set:" >&2
    echo "$SKIPPED_LIVE" >&2
    exit 1
  fi
fi
exit "$STATUS"
