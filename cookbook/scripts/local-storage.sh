#!/bin/sh
set -eu

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cookbook_directory=$(dirname -- "$script_directory")
case "${1:-up}" in
  up)
    docker compose -f "$cookbook_directory/compose.yaml" up --wait rustfs
    docker compose -f "$cookbook_directory/compose.yaml" run --rm bucket-init
    ;;
  down)
    docker compose -f "$cookbook_directory/compose.yaml" down
    ;;
  reset)
    # Explicit destructive reset of this cookbook's private Compose project.
    docker compose -f "$cookbook_directory/compose.yaml" down --volumes
    ;;
  *)
    echo "usage: $0 [up|down|reset]" >&2
    exit 2
    ;;
esac
