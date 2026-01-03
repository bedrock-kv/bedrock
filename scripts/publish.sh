#!/bin/bash
# Publish an umbrella app to Hex.pm
# Usage: ./scripts/publish.sh <app_name> [hex.publish args...]
#
# Examples:
#   ./scripts/publish.sh bedrock
#   ./scripts/publish.sh bedrock_hca --yes
#   ./scripts/publish.sh bedrock_directory --dry-run
#
# Publish order (dependencies first):
#   1. bedrock
#   2. bedrock_hca
#   3. bedrock_directory

set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <app_name> [hex.publish args...]"
  echo ""
  echo "Apps (publish in order):"
  echo "  1. bedrock"
  echo "  2. bedrock_hca"
  echo "  3. bedrock_directory"
  exit 1
fi

APP=$1
shift

if [ ! -d "apps/$APP" ]; then
  echo "Error: apps/$APP does not exist"
  exit 1
fi

echo "Publishing $APP..."
cd "apps/$APP"
HEX_PUBLISHING=1 mix hex.publish "$@"
