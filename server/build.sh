#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*) SEP=";" ;;
  *) SEP=":" ;;
esac

MYSQL_CONNECTOR="lib/mysql-connector-j-9.0.0.jar"
if [ ! -f "$MYSQL_CONNECTOR" ]; then
  echo "Missing Connector/J: $MYSQL_CONNECTOR" >&2
  exit 1
fi

sources_in() {
  local root="$1"
  SOURCES=()
  while IFS= read -r source; do
    SOURCES+=("$source")
  done < <(find "$root" -type f -name '*.java' | LC_ALL=C sort)
}

build_common() {
  local classes="out/classes/common-$$"
  mkdir -p "$classes"
  sources_in common/src
  if [ "${#SOURCES[@]}" -gt 0 ]; then
    javac --release 21 -d "$classes" -cp "$MYSQL_CONNECTOR" "${SOURCES[@]}"
  fi
  jar --create --file out/common.jar -C "$classes" .
}

build_service() {
  local service="$1"
  local classes="out/classes/$service-$$"
  local main_class="$service.${service^}Main"
  mkdir -p "$classes"
  sources_in "$service/src"
  if [ "${#SOURCES[@]}" -gt 0 ]; then
    javac --release 21 -d "$classes" \
      -cp "out/common.jar${SEP}${MYSQL_CONNECTOR}" "${SOURCES[@]}"
  fi
  if [ -f "$classes/${service}/${service^}Main.class" ]; then
    jar --create --file "out/$service.jar" --main-class "$main_class" -C "$classes" .
  else
    jar --create --file "out/$service.jar" -C "$classes" .
  fi
  echo "Built out/$service.jar (runtime classpath: out/common.jar${SEP}$MYSQL_CONNECTOR)"
}

case "${1:-all}" in
  all)
    [ "$#" -eq 0 ] || { echo "Usage: ./build.sh [transaction|inventory|analytics]" >&2; exit 2; }
    build_common
    build_service transaction
    build_service inventory
    build_service analytics
    ;;
  transaction|inventory|analytics)
    [ "$#" -eq 1 ] || { echo "Usage: ./build.sh [transaction|inventory|analytics]" >&2; exit 2; }
    build_common
    build_service "$1"
    ;;
  *)
    echo "Usage: ./build.sh [transaction|inventory|analytics]" >&2
    exit 2
    ;;
esac
