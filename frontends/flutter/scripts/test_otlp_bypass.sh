#!/bin/bash
# OTLP Bypass Test Script
# Subtask 2-3: Test if OpenTelemetry OTLP HTTP connection causes the hang
#
# HYPOTHESIS: The grey window issue is caused by OTLP HTTP client initialization
#             blocking when RUST_BACKTRACE=full triggers symbol resolution.
#
# TEST: Keep RUST_BACKTRACE=full but bypass OTLP by using stdout exporter
#       If the app works, OTLP HTTP client is confirmed as the issue.
#
# Created: 2026-01-23

set -e

echo "=================================================="
echo "Subtask 2-3: OTLP Bypass Test"
echo "=================================================="
echo ""
echo "PURPOSE: Test if bypassing OpenTelemetry OTLP HTTP fixes the grey window"
echo ""
echo "HYPOTHESIS:"
echo "  The grey window is caused by OTLP HTTP client initialization"
echo "  interacting badly with RUST_BACKTRACE=full symbol resolution."
echo ""
echo "TEST DESIGN:"
echo "  - Keep RUST_BACKTRACE=full (suspected trigger)"
echo "  - Set OTEL_TRACES_EXPORTER=stdout (bypass OTLP HTTP entirely)"
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
LOGFILE="$LOG_DIR/test_otlp_bypass_$TIMESTAMP.log"

echo "=================================================="
echo "Environment Configuration"
echo "=================================================="
echo ""
echo "Setting:"
echo "  RUST_BACKTRACE=full       (keep suspected trigger)"
echo "  OTEL_TRACES_EXPORTER=stdout  (bypass OTLP HTTP)"
echo ""
export RUST_BACKTRACE=full
export OTEL_TRACES_EXPORTER=stdout
echo "Verification:"
echo "  RUST_BACKTRACE=$RUST_BACKTRACE"
echo "  OTEL_TRACES_EXPORTER=$OTEL_TRACES_EXPORTER"
echo ""

echo "=================================================="
echo "How This Test Works"
echo "=================================================="
echo ""
echo "The ffi_bridge.rs init_opentelemetry() function checks OTEL_TRACES_EXPORTER:"
echo ""
echo "  if exporter_type.contains(\"otlp\") {"
echo "      // Creates OTLP HTTP exporters (SUSPECTED DEADLOCK)"
echo "      // Uses reqwest HTTP client which may deadlock with RUST_BACKTRACE=full"
echo "  } else {"
echo "      // Uses stdout exporters (NO HTTP, NO DEADLOCK)"
echo "      // Simple synchronous output, no network"
echo "  }"
echo ""
echo "By setting OTEL_TRACES_EXPORTER=stdout, we bypass the HTTP path entirely."
echo ""

echo "=================================================="
echo "Running: OTLP Bypass Test"
echo "=================================================="
echo ""
echo "Logs saved to: $LOGFILE"
echo ""
echo "EXPECTED OUTCOMES:"
echo ""
echo "  SCENARIO A: App works normally"
echo "    - Window appears with UI content (not grey)"
echo "    - All [DIAG 1-43] markers appear"
echo "    - [FFI-DIAG] shows 'Using stdout exporters (no OTLP)'"
echo "    - App is responsive"
echo "    --> CONFIRMS: OTLP HTTP client is the cause of deadlock"
echo ""
echo "  SCENARIO B: App still shows grey window"
echo "    - Same blocking behavior as RUST_BACKTRACE=full + OTLP"
echo "    --> CONTRADICTS: Issue is NOT in OTLP HTTP client"
echo "    --> Look elsewhere in OpenTelemetry or tracing init"
echo ""
echo "Press Ctrl+C after observing app behavior."
echo ""
echo "--------------------------------------------------" | tee "$LOGFILE"
echo "Test started: $(date)" | tee -a "$LOGFILE"
echo "RUST_BACKTRACE=$RUST_BACKTRACE" | tee -a "$LOGFILE"
echo "OTEL_TRACES_EXPORTER=$OTEL_TRACES_EXPORTER" | tee -a "$LOGFILE"
echo "--------------------------------------------------" | tee -a "$LOGFILE"
echo ""

# Run Flutter app with OTLP bypassed
flutter run -d macos --verbose 2>&1 | tee -a "$LOGFILE"

echo ""
echo "--------------------------------------------------"
echo "Test completed: $(date)"
echo "Log saved to: $LOGFILE"
echo "--------------------------------------------------"
echo ""

# Post-test analysis
echo "=================================================="
echo "POST-TEST ANALYSIS"
echo "=================================================="
echo ""
echo "Check the logs for these key markers:"
echo ""
echo "1. Look for OTLP bypass confirmation:"
echo "   grep 'stdout exporters' $LOGFILE"
echo ""
echo "2. Check if runApp was reached:"
echo "   grep 'DIAG 36' $LOGFILE"
echo ""
echo "3. Check if window initialization completed:"
echo "   grep 'DIAG 43' $LOGFILE"
echo ""
echo "4. Verify Rust initialization sequence:"
echo "   grep 'FFI-DIAG' $LOGFILE | head -20"
echo ""

# Interpretation guide
echo "=================================================="
echo "INTERPRETATION GUIDE"
echo "=================================================="
echo ""
echo "IF THE APP WORKED:"
echo "  - Root cause CONFIRMED: OTLP HTTP client deadlock with RUST_BACKTRACE=full"
echo "  - The reqwest/hyper async HTTP setup triggers symbol resolution"
echo "  - Symbol resolution with RUST_BACKTRACE=full causes re-entrancy/deadlock"
echo ""
echo "IF THE APP STILL HUNG:"
echo "  - OTLP HTTP is NOT the root cause"
echo "  - Check other OpenTelemetry initialization:"
echo "    - SdkTracerProvider creation"
echo "    - SdkLoggerProvider creation"
echo "    - tracing_subscriber initialization"
echo "  - May need to test with OTEL completely disabled"
echo ""

echo "=================================================="
echo "NEXT STEPS (based on outcome)"
echo "=================================================="
echo ""
echo "If OTLP confirmed as cause, recommended fixes:"
echo ""
echo "1. LAZY INITIALIZATION (Recommended)"
echo "   Move OTLP HTTP client creation to background task"
echo "   Use stdout exporter initially, switch to OTLP after app starts"
echo ""
echo "2. DISABLE OTLP IN DEBUG"
echo "   Set OTEL_TRACES_EXPORTER=stdout when RUST_BACKTRACE is set"
echo "   Document this as known limitation"
echo ""
echo "3. TIMEOUT ON HTTP CLIENT"
echo "   Add connect timeout to reqwest client builder"
echo "   Prevents indefinite blocking"
echo ""
echo "4. FIX RUST_BACKTRACE INTERACTION"
echo "   Set RUST_LIB_BACKTRACE=0 during HTTP client init"
echo "   Re-enable after client is created"
echo ""
