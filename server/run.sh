#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
if [ ! -d out/main ]; then
  echo "Not built yet - running build.sh first..."
  ./build.sh
fi
case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=";" ;;
  *) SEP=":" ;;
esac
java -cp "out/main${SEP}lib/*" Main "$@"
