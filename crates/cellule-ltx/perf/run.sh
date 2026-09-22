#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
target_dir=${CARGO_TARGET_DIR:-${script_dir}/target}
transactions=${LTX_TRANSACTIONS:-128}
payload_bytes=${LTX_PAYLOAD_BYTES:-4096}
rounds=${LTX_ROUNDS:-5}
warmup=${LTX_WARMUP:-1}

common_args=(
  --transactions "$transactions"
  --payload-bytes "$payload_bytes"
  --rounds "$rounds"
  --warmup "$warmup"
)

cellule_report=$(mktemp)
celld_report=$(mktemp)
trap 'rm -f "$cellule_report" "$celld_report"' EXIT

CARGO_TARGET_DIR="$target_dir" cargo run --quiet --release \
  --manifest-path "$script_dir/cellule/Cargo.toml" -- "${common_args[@]}" >"$cellule_report"
CARGO_TARGET_DIR="$target_dir" cargo run --quiet --release \
  --manifest-path "$script_dir/celld/Cargo.toml" -- "${common_args[@]}" >"$celld_report"

printf '%s\n' "=== cellule-ltx ==="
cat "$cellule_report"
printf '%s\n' "=== celld-ltx ==="
cat "$celld_report"
