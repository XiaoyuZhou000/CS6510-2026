#!/usr/bin/env bash
# Builds the subscriber test application (007 research N10) into out/.
# It compiles only against the generated event classes and the messaging runtime, never against server code.
set -euo pipefail
cd "$(dirname "$0")"

contracts=../server/out/contracts.jar
if [ ! -f "$contracts" ]; then
  echo "Missing $contracts; run server/build.sh contracts first" >&2
  exit 1
fi
if [ -z "$(ls ../server/lib/messaging/kafka-clients-*.jar 2>/dev/null)" ] || \
   [ -z "$(ls ../server/lib/messaging/protobuf-java-*.jar 2>/dev/null)" ]; then
  echo "Missing the messaging runtime in ../server/lib/messaging/; run server/build.sh contracts first" >&2
  exit 1
fi
case "$(uname -s)" in CYGWIN*|MINGW*|MSYS*) sep=';' ;; *) sep=':' ;; esac

mkdir -p out
rm -f out/*.class
javac --release 21 -encoding UTF-8 -d out -cp "$contracts${sep}../server/lib/messaging/*" src/*.java
echo "Built subscriber-client/out. Run with: subscriber-client/run.sh SubscriberMain --out=DIR [--key=value ...]"
