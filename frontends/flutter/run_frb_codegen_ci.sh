#!/bin/bash
# CI/CD version - uses the same workspace as the local version
# Usage: ./run_frb_codegen_ci.sh generate

set -e

# Get the directory where this script is located (frontends/flutter)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to the flutter_rust_bridge submodule
FRB_DIR="${SCRIPT_DIR}/external/flutter_rust_bridge"

# Ensure submodule is initialized (in case it wasn't cloned with --recursive)
if [ ! -f "${FRB_DIR}/Cargo.toml" ]; then
    echo "Initializing git submodules..."
    cd "${SCRIPT_DIR}/../.."
    git submodule update --init --recursive frontends/flutter/external/flutter_rust_bridge
fi

# Run flutter_rust_bridge_codegen from the flutter_rust_bridge workspace
# Change to the Flutter directory so it finds flutter_rust_bridge.yaml
cd "${SCRIPT_DIR}"
exec cargo run --manifest-path "${FRB_DIR}/Cargo.toml" --package flutter_rust_bridge_codegen --release -- "$@"
