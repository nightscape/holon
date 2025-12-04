#!/bin/bash
# Comparison Test: WITHOUT RUST_BACKTRACE
# Subtask 1-2: Confirm RUST_BACKTRACE correlation
#
# This script tests the Flutter macOS app WITHOUT RUST_BACKTRACE to establish
# whether the grey window issue is specifically triggered by RUST_BACKTRACE=full
#
# Created: 2026-01-23

set -e

echo "=================================================="
echo "Subtask 1-2: RUST_BACKTRACE Correlation Test"
echo "=================================================="
echo ""
echo "PURPOSE: Run Flutter app WITHOUT RUST_BACKTRACE to compare"
echo "         against the grey window issue seen WITH RUST_BACKTRACE=full"
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
LOGFILE="$LOG_DIR/comparison_no_backtrace_$TIMESTAMP.log"

# Explicitly unset RUST_BACKTRACE
unset RUST_BACKTRACE
unset RUST_LIB_BACKTRACE

echo "=================================================="
echo "Environment Check"
echo "=================================================="
echo "RUST_BACKTRACE: ${RUST_BACKTRACE:-<unset>}"
echo "RUST_LIB_BACKTRACE: ${RUST_LIB_BACKTRACE:-<unset>}"
echo ""

echo "=================================================="
echo "Running: flutter run -d macos --verbose"
echo "Logs saved to: $LOGFILE"
echo "=================================================="
echo ""
echo "EXPECTED OUTCOMES (for correlation confirmation):"
echo ""
echo "  SCENARIO A: App works normally"
echo "    - Window appears with UI content (not grey)"
echo "    - All [DIAG 1-43] markers appear in logs"
echo "    - App is responsive"
echo "    --> CONFIRMS: Issue is specific to RUST_BACKTRACE=full"
echo ""
echo "  SCENARIO B: App shows grey window"
echo "    - Same behavior as RUST_BACKTRACE=full test"
echo "    --> CONTRADICTS: Issue is NOT RUST_BACKTRACE specific"
echo ""
echo "Press Ctrl+C after observing app behavior."
echo ""
echo "--------------------------------------------------" | tee "$LOGFILE"
echo "Test started: $(date)" | tee -a "$LOGFILE"
echo "RUST_BACKTRACE: ${RUST_BACKTRACE:-<unset>}" | tee -a "$LOGFILE"
echo "--------------------------------------------------" | tee -a "$LOGFILE"
echo ""

# Run without RUST_BACKTRACE
flutter run -d macos --verbose 2>&1 | tee -a "$LOGFILE"

echo ""
echo "--------------------------------------------------"
echo "Test completed: $(date)"
echo "Log saved to: $LOGFILE"
echo "--------------------------------------------------"
echo ""

# Post-test analysis prompts
echo "=================================================="
echo "POST-TEST ANALYSIS"
echo "=================================================="
echo ""
echo "After observing the app, answer these questions:"
echo ""
echo "1. Did the app window show UI content? (Y/N): __"
echo "2. Did [DIAG 36] marker appear (runApp called)? (Y/N): __"
echo "3. Did [DIAG 43] marker appear (window ready)? (Y/N): __"
echo "4. Was the app responsive? (Y/N): __"
echo ""
echo "If all answers are Y: CORRELATION CONFIRMED"
echo "  --> RUST_BACKTRACE=full is the trigger for grey window"
echo ""
echo "If any answer is N: CORRELATION NOT CONFIRMED"
echo "  --> Issue may have other causes"
echo ""
echo "To complete the analysis, run with RUST_BACKTRACE variants:"
echo "  RUST_BACKTRACE=1 flutter run -d macos --verbose"
echo "  RUST_BACKTRACE=0 flutter run -d macos --verbose"
echo ""
