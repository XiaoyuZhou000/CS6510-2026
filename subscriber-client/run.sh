#!/usr/bin/env bash
# Runs one main class of the subscriber test application:
#   subscriber-client/run.sh SubscriberMain --out=DIR [--key=value ...]
#   subscriber-client/run.sh ReconcileMain --run-dir=DIR --mode=fixed [--report-out=FILE]
#   subscriber-client/run.sh DecodeSaleEvents --in=FILE --out=FILE
set -euo pipefail
cd "$(dirname "$0")"

if [ $# -lt 1 ]; then
  echo "Usage: subscriber-client/run.sh <SubscriberMain|ReconcileMain|DecodeSaleEvents> [--key=value ...]" >&2
  exit 2
fi
main=$1
shift
if [ ! -f "out/$main.class" ]; then
  echo "out/$main.class not built yet; running build.sh first..." >&2
  ./build.sh
fi
case "$(uname -s)" in CYGWIN*|MINGW*|MSYS*) sep=';' ;; *) sep=':' ;; esac
# Relative paths in the arguments are resolved against the caller's directory, not this one.
cd - > /dev/null
here=$(cd "$(dirname "$0")" && pwd)
exec java -cp "$here/out${sep}$here/../server/out/contracts.jar${sep}$here/../server/lib/messaging/*" "$main" "$@"
