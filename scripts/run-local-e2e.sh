#!/usr/bin/env bash
set -euo pipefail

tracking_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
schema_root=${CLASHKING_SCHEMAS_ROOT:-}
if [[ -z $schema_root ]]; then
  echo 'Set CLASHKING_SCHEMAS_ROOT to the clashking_schemas checkout.' >&2
  exit 2
fi
fixture="$schema_root/scripts/with-test-integration.sh"
if [[ ! -x $fixture ]]; then
  echo "Canonical integration fixture is not executable: $fixture" >&2
  exit 2
fi

cd "$tracking_root"
exec "$fixture" --profile retained-api -- env GOCACHE=/tmp/clashking-tracking-e2e-go-cache \
  go test -p=1 -count=1 -tags 'script_internal_tests platform_internal_tests integration local_integration' ./...
