#!/bin/bash
# Grey Window Reproduction Script
# This script attempts to reproduce the grey window issue with RUST_BACKTRACE=full
#
# Created: 2026-01-23
# Purpose: Confirm issue reproduction for investigation task 005

set -e

echo "=================================================="
echo "Grey Window Reproduction Test"
echo "=================================================="
echo ""

# Check Flutter installation
if ! command -v flutter &> /dev/null; then
    echo "ERROR: Flutter SDK not found in PATH"
    echo "Please install Flutter: https://flutter.dev/docs/get-started/install"
    exit 1
fi

echo "Flutter version:"
flutter --version
echo ""

# Navigate to flutter directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLUTTER_DIR="$(dirname "$SCRIPT_DIR")"
cd "$FLUTTER_DIR"

echo "Working directory: $(pwd)"
echo ""

# Create log directory
LOG_DIR="$FLUTTER_DIR/debug_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=================================================="
echo "TEST 1: WITH RUST_BACKTRACE=full (expecting grey window)"
echo "=================================================="
echo ""
echo "Running: RUST_BACKTRACE=full flutter run -d macos --verbose"
echo "Logs will be saved to: $LOG_DIR/test_with_backtrace_$TIMESTAMP.log"
echo ""
echo "Watch for the following diagnostic markers:"
echo "  - [DIAG 1] through [DIAG 43] in Flutter logs"
echo "  - [FFI-DIAG] markers in Rust stderr"
echo ""
echo "EXPECTED BEHAVIOR:"
echo "  - Grey window appears"
echo "  - Last [DIAG] marker indicates blocking point"
echo "  - If [DIAG 36] is not reached, runApp() was never called"
echo ""
echo "Press Ctrl+C to stop the app after observing the grey window."
echo ""

# Run with RUST_BACKTRACE=full
RUST_BACKTRACE=full flutter run -d macos --verbose 2>&1 | tee "$LOG_DIR/test_with_backtrace_$TIMESTAMP.log"

echo ""
echo "Test 1 complete. Log saved to: $LOG_DIR/test_with_backtrace_$TIMESTAMP.log"
echo ""

# Ask to continue
read -p "Run comparison test WITHOUT RUST_BACKTRACE? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "=================================================="
    echo "TEST 2: WITHOUT RUST_BACKTRACE (control test)"
    echo "=================================================="
    echo ""
    echo "Running: flutter run -d macos --verbose"
    echo "Logs will be saved to: $LOG_DIR/test_without_backtrace_$TIMESTAMP.log"
    echo ""
    echo "EXPECTED BEHAVIOR:"
    echo "  - App should start normally with UI visible"
    echo "  - All [DIAG] markers should appear"
    echo "  - [DIAG 43] should be reached"
    echo ""

    unset RUST_BACKTRACE
    flutter run -d macos --verbose 2>&1 | tee "$LOG_DIR/test_without_backtrace_$TIMESTAMP.log"

    echo ""
    echo "Test 2 complete. Log saved to: $LOG_DIR/test_without_backtrace_$TIMESTAMP.log"
fi

echo ""
echo "=================================================="
echo "ANALYSIS"
echo "=================================================="
echo ""
echo "Compare the two log files to identify the blocking point:"
echo "  diff $LOG_DIR/test_with_backtrace_$TIMESTAMP.log $LOG_DIR/test_without_backtrace_$TIMESTAMP.log"
echo ""
echo "Look for:"
echo "  1. Last [DIAG] marker in the grey window test"
echo "  2. Last [FFI-DIAG] marker in stderr"
echo "  3. Any error messages or stack traces"
echo ""
