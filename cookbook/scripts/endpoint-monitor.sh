#!/bin/sh
set -eu
script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec sh "$script_directory/run-app.sh" endpoint-monitor "$@"
