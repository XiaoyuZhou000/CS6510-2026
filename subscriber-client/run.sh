#!/usr/bin/env bash
# Runs one main class of the subscriber test application:
#   subscriber-client/run.sh SubscriberMain --out=DIR [--key=value ...]
#   subscriber-client/run.sh ReconcileMain --run-dir=DIR --mode=fixed [--report-out=FILE]
#   subscriber-client/run.sh DecodeSaleEvents --in=FILE --out=FILE
#   subscriber-client/run.sh IsolationSummary --dir=DIR
set -euo pipefail
cd "$(dirname "$0")"

if [ $# -lt 1 ]; then
  echo "Usage: subscriber-client/run.sh <SubscriberMain|ReconcileMain|ChangeScenarioMain|IsolationSummary|DecodeSaleEvents> [--key=value ...]" >&2
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
# Under Git Bash the ;-separated classpath is not converted for java.exe, so give it Windows paths.
if [ "$sep" = ";" ] && command -v cygpath >/dev/null 2>&1; then here=$(cygpath -m "$here"); fi
exec java -cp "$here/out${sep}$here/../server/out/contracts.jar${sep}$here/../server/lib/messaging/*" "$main" "$@"
