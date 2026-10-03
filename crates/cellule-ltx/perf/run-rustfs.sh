#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
checkout_id=$(python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:12])' "$script_dir")
target_dir=${CARGO_TARGET_DIR:-${HOME}/Workspace/crabbuild-target/cellule-ltx-rustfs-$checkout_id}
output_dir=${1:-$target_dir/evidence/$(date -u +%Y%m%dT%H%M%SZ)}
test -d "$HOME/Workspace" || { echo "Workspace volume unavailable" >&2; exit 1; }
mkdir -p "$target_dir" "$output_dir"
for side in cellule celld replica-cost; do
  build_dir=$side
  if [[ $side == replica-cost ]]; then build_dir=replica; fi
  CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR="$target_dir/$build_dir" \
    cargo build --release --locked --manifest-path "$script_dir/$side/Cargo.toml"
done

# Use an owned provider and volume, so running the comparison cannot alter an
# application's existing RustFS bucket. Raw measurements survive cleanup.
name=cellule-ltx-perf-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
volume=$name-data
cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT
export AWS_ACCESS_KEY_ID=cellulebench
export AWS_SECRET_ACCESS_KEY=cellulebench
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
docker run -d --name "$name" -p 127.0.0.1::9000 \
  -e RUSTFS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
  -e RUSTFS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
  -e RUSTFS_CONSOLE_ENABLE=false -e RUSTFS_OBS_LOG_DIRECTORY=/data/logs \
  --mount "type=volume,src=$volume,dst=/data" \
  ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858 \
  >/dev/null
endpoint=http://$(docker port "$name" 9000)
ready=false
for _ in {1..60}; do
  if curl --fail --silent "$endpoint/health/ready" >/dev/null; then ready=true; break; fi
  sleep 1
done
[[ $ready == true ]] || { echo "RustFS failed to become ready" >&2; exit 1; }
aws --endpoint-url "$endpoint" s3api create-bucket --bucket cellule-ltx-perf >/dev/null
docker inspect "$name" >"$output_dir/rustfs-inspect.json"
python3 "$script_dir/compare.py" --target-dir "$target_dir" --output "$output_dir" \
  --endpoint "$endpoint" --bucket cellule-ltx-perf
