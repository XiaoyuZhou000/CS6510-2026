#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p out/test

# Keep test discovery recursive as suites move into layer-specific directories.
SOURCES=()
while IFS= read -r source; do
  SOURCES+=("$source")
done < <(find tests -type f -name "*.java" | LC_ALL=C sort)
if [ "${#SOURCES[@]}" -eq 0 ]; then
  echo "No Java test sources found under server/tests" >&2
  exit 1
fi

# Use ; on Windows (msys/cygwin/mingw), : on Unix
case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=";" ;;
  *) SEP=":" ;;
esac
JUNIT_CONSOLE="lib/junit-platform-console-standalone-1.11.0.jar"
MYSQL_CONNECTOR="lib/mysql-connector-j-9.0.0.jar"
javac --release 21 -d out/test \
  -cp "${JUNIT_CONSOLE}${SEP}${MYSQL_CONNECTOR}${SEP}out/main" \
  "${SOURCES[@]}"
echo "Tests compiled. Run with: ./run-tests.sh"
