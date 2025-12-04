#!/bin/bash
# Script to run Flutter app with BOTH Rust and Flutter code coverage
# This enables both LLVM source-based coverage for Rust and VM Service for Flutter
#
# Usage:
#   ./scripts/run-with-coverage.sh [flutter-run-args...]
#
# Example:
#   ./scripts/run-with-coverage.sh -d macos
#
# Coverage data:
#   - Rust: frontends/flutter/rust/target/coverage/
#   - Flutter: frontends/flutter/coverage/
#
# Process with:
#   ./scripts/process-rust-coverage.sh
#   ./scripts/process-flutter-coverage.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FLUTTER_DIR="$PROJECT_ROOT/frontends/flutter"
RUST_DIR="$FLUTTER_DIR/rust"
RUST_COVERAGE_DIR="$RUST_DIR/target/coverage"
FLUTTER_COVERAGE_DIR="$FLUTTER_DIR/coverage"

echo "🔍 Setting up BOTH Rust and Flutter code coverage..."
echo ""

# Create coverage directories
mkdir -p "$RUST_COVERAGE_DIR"
mkdir -p "$FLUTTER_COVERAGE_DIR"

# Set environment variables for LLVM coverage instrumentation (Rust)
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="$RUST_COVERAGE_DIR/holon-%p-%m.profraw"

# Clean previous coverage data
echo "🧹 Cleaning previous coverage data..."
rm -f "$RUST_COVERAGE_DIR"/*.profraw 2>/dev/null || true
rm -rf "$FLUTTER_COVERAGE_DIR"/* 2>/dev/null || true

echo "📝 Rust coverage configuration:"
echo "   RUSTFLAGS: $RUSTFLAGS"
echo "   LLVM_PROFILE_FILE: $LLVM_PROFILE_FILE"
echo ""
echo "📝 Flutter coverage configuration:"
echo "   VM Service will be enabled"
echo "   Coverage directory: $FLUTTER_COVERAGE_DIR"
echo ""

# Run Flutter app with both coverage mechanisms enabled
echo "🚀 Running Flutter app with BOTH Rust and Flutter coverage..."
echo ""
echo "   Rust coverage data: $RUST_COVERAGE_DIR"
echo "   Flutter coverage data: $FLUTTER_COVERAGE_DIR"
echo ""
echo "   After running the app, process coverage with:"
echo "   ./scripts/process-rust-coverage.sh"
echo "   ./scripts/process-flutter-coverage.sh"
echo ""

cd "$FLUTTER_DIR"

# Run in debug mode (VM Service is automatically enabled for Flutter coverage)
# RUSTFLAGS is already set above for Rust coverage
# Note: VM Service is automatically enabled in debug mode, no flags needed
flutter run --debug "$@"

