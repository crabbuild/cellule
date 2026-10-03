#!/bin/sh
set -eu

application=${1:?usage: run-app.sh APPLICATION [ARGUMENTS]}
shift
case "$application" in
  taskboard|settings|file-vault|work-queue|interval-scheduler|approvals|tenant-workspace|entity-registry|quotas|reservations|webhook-delivery|media-pipeline|report-export|endpoint-monitor|checkout|provisioning|release-pipeline|project-tracker) ;;
  *) echo "unknown runnable application: $application" >&2; exit 2 ;;
esac
script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cookbook_directory=$(dirname -- "$script_directory")
if [ -d "$HOME/Workspace" ]; then
  checkout_key=$(printf '%s' "$cookbook_directory" | shasum -a 256 | cut -c 1-12)
  CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$HOME/Workspace/crabbuild-target/cellule-cookbook-$checkout_key"}
  export CARGO_TARGET_DIR
fi
CELLULE_COOKBOOK_ENDPOINT=${CELLULE_COOKBOOK_ENDPOINT:-"http://127.0.0.1:${CELLULE_COOKBOOK_STORAGE_PORT:-19000}"}
export CELLULE_COOKBOOK_ENDPOINT
case "${1:-demo}" in
  help|--help|prepare|prepare-delete|prepare-run|prepare-data|prepare-seal|prepare-release|prepare-approval|prepare-rollback|prepare-reconcile|prepare-attachment|prepare-target|target-key|target-fault|send|fetch|fetch-target|sample|init|target|target-state|payment-state|provider-state) ;;
  *) sh "$script_directory/local-storage.sh" up ;;
esac
if [ "$#" -eq 0 ]; then
  set -- demo "$cookbook_directory/.state/$application"
fi
exec cargo run --manifest-path "$cookbook_directory/Cargo.toml" \
  -p "cellule-cookbook-$application" --locked -- "$@"
