#!/usr/bin/env bash
# Compiles src/ and test/ and runs every test of the subscriber test application with JUnit 5.
set -euo pipefail
cd "$(dirname "$0")"

junit=../server/lib/junit-platform-console-standalone-1.11.0.jar
contracts=../server/out/contracts.jar
for required in "$junit" "$contracts"; do
  if [ ! -f "$required" ]; then
    echo "Missing $required; run server/build.sh contracts first" >&2
    exit 1
  fi
done
case "$(uname -s)" in CYGWIN*|MINGW*|MSYS*) sep=';' ;; *) sep=':' ;; esac
classpath="$contracts${sep}../server/lib/messaging/*${sep}$junit"

rm -rf out/test-classes
mkdir -p out/test-classes
javac --release 21 -encoding UTF-8 -d out/test-classes -cp "$classpath" src/*.java test/*.java
report_dir="out/test-results/run-$(date +%Y%m%d%H%M%S)-$$"
mkdir -p "$report_dir"
java -cp "out/test-classes${sep}$classpath" org.junit.platform.console.ConsoleLauncher execute \
  --scan-classpath out/test-classes --reports-dir "$report_dir" --disable-banner "$@"
