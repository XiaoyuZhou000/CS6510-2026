#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p out/test
SOURCES=$(find tests -name "*.java")
# Use ; on Windows (msys/cygwin/mingw), : on Unix
case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=";" ;;
  *) SEP=":" ;;
esac
javac -d out/test -cp "lib/*${SEP}out/main" $SOURCES
echo "Tests compiled. Run with: ./run-tests.sh"
