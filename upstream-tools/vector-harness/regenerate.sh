#!/usr/bin/env bash
# Regenerates tests/vectors/wire.vectors from the real jgroups-5.2.28.Final jar.
#
# Requires: a JDK (17+), Maven, and network access to Maven Central the first time (to pull
# org.jgroups:jgroups:5.2.28.Final into ~/.m2 — it stays cached after that, so later runs are
# offline). See README.md for details and for what to do if the upstream/
# submodule's JGroups version is ever bumped.
set -euo pipefail

cd "$(dirname "$0")"

mvn -q compile

CP_FILE="$(mktemp)"
trap 'rm -f "$CP_FILE"' EXIT
mvn -q dependency:build-classpath -Dmdep.outputFile="$CP_FILE"

OUT="${1:-../../tests/vectors/wire.vectors}"
mkdir -p "$(dirname "$OUT")"
java -cp "target/classes:$(cat "$CP_FILE")" Main "$OUT"
echo "wrote $OUT"
