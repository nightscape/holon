#!/bin/bash
# OTLP Network Timeout Test Script
# Subtask 2-3: Test if OTLP endpoint unavailability causes blocking
#
# This script tests what happens when:
# 1. OTLP is enabled but endpoint is unreachable
# 2. RUST_BACKTRACE=full is set
#
# Theory: If no OTLP collector is running, the HTTP client may:
# - Block waiting for connection
# - Timeout eventually (but too slow for app startup)
# - Fail and trigger symbol resolution via RUST_BACKTRACE
#
# Created: 2026-01-23

set -e

echo "=================================================="
echo "Subtask 2-3: OTLP Network Timeout Test"
echo "=================================================="
echo ""
echo "PURPOSE: Test behavior when OTLP endpoint is unreachable"
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
echo "Test 1: Default OTLP endpoint (localhost:4318)"
echo "=================================================="
echo ""
echo "Checking if OTLP collector is running on default port..."
if nc -z localhost 4318 2>/dev/null; then
    echo "  [OK] Something is listening on localhost:4318"
    echo "       (This may affect test results - consider stopping collector)"
else
    echo "  [NO] Nothing listening on localhost:4318"
    echo "       (OTLP HTTP connection will fail)"
fi
echo ""

# Test 1: Default endpoint (likely to fail connection)
echo "Running Flutter with default OTLP endpoint..."
echo ""
LOGFILE1="$LOG_DIR/test_otlp_default_endpoint_$TIMESTAMP.log"
echo "Logs: $LOGFILE1"
echo ""
echo "EXPECTATION:"
echo "  If OTLP endpoint is unreachable AND RUST_BACKTRACE=full:"
echo "  - HTTP client may hang during connection attempt"
echo "  - Or may timeout (check if app eventually starts)"
echo ""
echo "Press Ctrl+C after 30 seconds or when behavior is clear."
echo ""
echo "--------------------------------------------------" | tee "$LOGFILE1"
echo "Test: Default OTLP endpoint" | tee -a "$LOGFILE1"
echo "RUST_BACKTRACE=full" | tee -a "$LOGFILE1"
echo "OTEL_EXPORTER_OTLP_ENDPOINT=<default localhost:4318>" | tee -a "$LOGFILE1"
echo "Started: $(date)" | tee -a "$LOGFILE1"
echo "--------------------------------------------------" | tee -a "$LOGFILE1"

export RUST_BACKTRACE=full
# Don't set endpoint - use default
unset OTEL_EXPORTER_OTLP_ENDPOINT
unset OTEL_TRACES_EXPORTER

timeout 60 flutter run -d macos --verbose 2>&1 | tee -a "$LOGFILE1" || true

echo ""
echo "Test 1 complete. Check log for blocking point."
echo ""

read -p "Continue to Test 2 (explicit unreachable endpoint)? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "=================================================="
    echo "Test 2: Explicitly unreachable endpoint"
    echo "=================================================="
    echo ""
    echo "Setting OTLP endpoint to a non-routable address..."
    echo ""
    LOGFILE2="$LOG_DIR/test_otlp_unreachable_$TIMESTAMP.log"
    echo "Logs: $LOGFILE2"
    echo ""
    echo "EXPECTATION:"
    echo "  Connection to 10.255.255.1:4318 will fail immediately"
    echo "  This tests error handling path + RUST_BACKTRACE interaction"
    echo ""
    echo "--------------------------------------------------" | tee "$LOGFILE2"
    echo "Test: Unreachable OTLP endpoint" | tee -a "$LOGFILE2"
    echo "RUST_BACKTRACE=full" | tee -a "$LOGFILE2"
    echo "OTEL_EXPORTER_OTLP_ENDPOINT=http://10.255.255.1:4318" | tee -a "$LOGFILE2"
    echo "Started: $(date)" | tee -a "$LOGFILE2"
    echo "--------------------------------------------------" | tee -a "$LOGFILE2"

    export RUST_BACKTRACE=full
    export OTEL_EXPORTER_OTLP_ENDPOINT="http://10.255.255.1:4318"

    timeout 60 flutter run -d macos --verbose 2>&1 | tee -a "$LOGFILE2" || true

    echo ""
    echo "Test 2 complete."
fi

read -p "Continue to Test 3 (with short timeout)? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "=================================================="
    echo "Test 3: With OTEL_EXPORTER_OTLP_TIMEOUT=1s"
    echo "=================================================="
    echo ""
    echo "Setting very short timeout to see if app eventually starts..."
    echo ""
    LOGFILE3="$LOG_DIR/test_otlp_short_timeout_$TIMESTAMP.log"
    echo "Logs: $LOGFILE3"
    echo ""
    echo "EXPECTATION:"
    echo "  If timeout helps, app might start after brief delay"
    echo "  This would confirm the issue is connection timeout related"
    echo ""
    echo "--------------------------------------------------" | tee "$LOGFILE3"
    echo "Test: Short OTLP timeout" | tee -a "$LOGFILE3"
    echo "RUST_BACKTRACE=full" | tee -a "$LOGFILE3"
    echo "OTEL_EXPORTER_OTLP_TIMEOUT=1000 (1 second)" | tee -a "$LOGFILE3"
    echo "Started: $(date)" | tee -a "$LOGFILE3"
    echo "--------------------------------------------------" | tee -a "$LOGFILE3"

    export RUST_BACKTRACE=full
    export OTEL_EXPORTER_OTLP_TIMEOUT=1000
    unset OTEL_EXPORTER_OTLP_ENDPOINT

    timeout 60 flutter run -d macos --verbose 2>&1 | tee -a "$LOGFILE3" || true

    echo ""
    echo "Test 3 complete."
fi

echo ""
echo "=================================================="
echo "SUMMARY: Network Timeout Test Results"
echo "=================================================="
echo ""
echo "Log files created:"
echo "  Test 1 (default endpoint): $LOGFILE1"
[ -n "$LOGFILE2" ] && echo "  Test 2 (unreachable):      $LOGFILE2"
[ -n "$LOGFILE3" ] && echo "  Test 3 (short timeout):    $LOGFILE3"
echo ""
echo "Analysis commands:"
echo ""
echo "  # Check for connection errors"
echo "  grep -i 'connection\|timeout\|refused' \$LOGFILE"
echo ""
echo "  # Check last FFI diagnostic"
echo "  grep 'FFI-DIAG' \$LOGFILE | tail -5"
echo ""
echo "  # Check if runApp was reached"
echo "  grep 'DIAG 36' \$LOGFILE"
echo ""

echo "=================================================="
echo "INTERPRETATION"
echo "=================================================="
echo ""
echo "If Test 1 (default, unreachable) hangs:"
echo "  -> OTLP HTTP connection blocking is part of the issue"
echo "  -> The HTTP client doesn't timeout quickly enough"
echo ""
echo "If Test 2 (non-routable) hangs similarly:"
echo "  -> Even connection refusal triggers the deadlock"
echo "  -> Issue is in HTTP client initialization, not connection"
echo ""
echo "If Test 3 (short timeout) helps:"
echo "  -> Adding explicit timeout could mitigate the issue"
echo "  -> But still need to understand why RUST_BACKTRACE matters"
echo ""
echo "Compare with OTLP bypass test (test_otlp_bypass.sh):"
echo "  If bypass works but network tests fail:"
echo "  -> Confirms HTTP client is the specific problem"
echo ""
