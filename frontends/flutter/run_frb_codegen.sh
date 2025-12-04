#!/bin/bash
# Run flutter_rust_bridge_codegen from the workspace submodule
# This ensures version consistency across environments (CI/CD, local dev)

set -e

# Get the directory where this script is located (frontends/flutter)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to the flutter_rust_bridge submodule
FRB_DIR="${SCRIPT_DIR}/external/flutter_rust_bridge"

# Check if submodule exists
if [ ! -d "${FRB_DIR}" ]; then
    echo "Error: flutter_rust_bridge submodule not found at ${FRB_DIR}" >&2
    echo "Please initialize it with: git submodule update --init --recursive" >&2
    exit 1
fi

# Run flutter_rust_bridge_codegen from the flutter_rust_bridge workspace
# Change to the Flutter directory so it finds flutter_rust_bridge.yaml
cd "${SCRIPT_DIR}"
exec cargo run --manifest-path "${FRB_DIR}/Cargo.toml" --package flutter_rust_bridge_codegen --release -- "$@"
