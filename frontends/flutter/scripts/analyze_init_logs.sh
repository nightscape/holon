#!/bin/bash
# Diagnostic Log Analyzer
# Subtask 2-1: Analyze diagnostic logs to identify exact blocking point
#
# This script parses the diagnostic output from main.dart and ffi_bridge.rs
# to identify where the initialization sequence blocks.
#
# Created: 2026-01-23

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=================================================="
echo "Diagnostic Log Analyzer"
echo "Subtask 2-1: Identify Blocking Point"
echo "=================================================="
echo ""

# Check if log file provided
if [ -z "$1" ]; then
    echo "Usage: $0 <log_file>"
    echo ""
    echo "Example: $0 debug_logs/test_with_backtrace_20260123_120000.log"
    exit 1
fi

LOGFILE="$1"

if [ ! -f "$LOGFILE" ]; then
    echo "ERROR: Log file not found: $LOGFILE"
    exit 1
fi

echo "Analyzing: $LOGFILE"
echo ""

# Extract Dart diagnostic markers
echo "=================================================="
echo "DART INITIALIZATION SEQUENCE (main.dart)"
echo "=================================================="
echo ""

DART_DIAGS=$(grep -E '\[DIAG [0-9]+' "$LOGFILE" 2>/dev/null | head -50 || echo "")
if [ -n "$DART_DIAGS" ]; then
    echo "$DART_DIAGS"
    echo ""

    # Find last DIAG marker
    LAST_DART_DIAG=$(grep -oE '\[DIAG [0-9]+' "$LOGFILE" | tail -1 | grep -oE '[0-9]+')
    echo -e "${YELLOW}Last Dart DIAG marker: [DIAG $LAST_DART_DIAG]${NC}"

    # Interpret blocking point
    if [ -n "$LAST_DART_DIAG" ]; then
        echo ""
        echo "Interpretation:"
        if [ "$LAST_DART_DIAG" -lt 16 ]; then
            echo -e "${RED}BLOCKED BEFORE: frb.RustLib.init()${NC}"
            echo "  Issue is in early Flutter initialization"
        elif [ "$LAST_DART_DIAG" -eq 16 ]; then
            echo -e "${RED}BLOCKED DURING: frb.RustLib.init()${NC}"
            echo "  Issue is in Rust library FFI initialization"
        elif [ "$LAST_DART_DIAG" -lt 30 ]; then
            echo -e "${YELLOW}BLOCKED DURING: Pre-engine initialization (config/path setup)${NC}"
            echo "  Issue is between frb.RustLib.init() and ffi.initRenderEngine()"
        elif [ "$LAST_DART_DIAG" -eq 30 ]; then
            echo -e "${RED}BLOCKED DURING: ffi.initRenderEngine() - THE CRITICAL RUST CALL${NC}"
            echo "  --> This is the most likely blocking point for RUST_BACKTRACE issues"
            echo "  --> Check [FFI-DIAG] markers in Rust output below"
        elif [ "$LAST_DART_DIAG" -lt 36 ]; then
            echo -e "${YELLOW}BLOCKED DURING: Post-engine initialization (theme loading)${NC}"
        elif [ "$LAST_DART_DIAG" -eq 36 ]; then
            echo -e "${YELLOW}REACHED: runApp() was called${NC}"
            echo "  But [DIAG 37] not seen - runApp may be hanging"
        elif [ "$LAST_DART_DIAG" -ge 43 ]; then
            echo -e "${GREEN}SUCCESS: All initialization steps completed${NC}"
            echo "  App should be displaying normally"
        fi
    fi
else
    echo -e "${RED}No [DIAG] markers found in log file${NC}"
    echo "  Make sure diagnostic code is in main.dart"
fi

echo ""
echo "=================================================="
echo "RUST FFI INITIALIZATION SEQUENCE (ffi_bridge.rs)"
echo "=================================================="
echo ""

FFI_DIAGS=$(grep -E '\[FFI-DIAG\]' "$LOGFILE" 2>/dev/null | head -50 || echo "")
if [ -n "$FFI_DIAGS" ]; then
    echo "$FFI_DIAGS"
    echo ""

    # Find last FFI-DIAG marker
    LAST_FFI_DIAG=$(grep '\[FFI-DIAG\]' "$LOGFILE" | tail -1)
    echo -e "${YELLOW}Last FFI-DIAG marker:${NC}"
    echo "  $LAST_FFI_DIAG"
    echo ""

    # Check for specific blocking patterns
    if echo "$FFI_DIAGS" | grep -q "Creating OTLP trace exporter" && ! echo "$FFI_DIAGS" | grep -q "OTLP trace exporter created"; then
        echo -e "${RED}BLOCKED DURING: OTLP trace exporter creation (.with_http().build())${NC}"
        echo ""
        echo "ROOT CAUSE HYPOTHESIS:"
        echo "  The HTTP client initialization (reqwest/hyper) is blocking."
        echo "  With RUST_BACKTRACE=full, symbol resolution during async HTTP"
        echo "  client setup may cause a deadlock."
        echo ""
        echo "POTENTIAL FIX:"
        echo "  1. Set OTEL_TRACES_EXPORTER=stdout (skip OTLP HTTP)"
        echo "  2. Use timeout for exporter creation"
        echo "  3. Initialize OpenTelemetry in background thread"
    elif echo "$FFI_DIAGS" | grep -q "Creating OTLP log exporter" && ! echo "$FFI_DIAGS" | grep -q "OTLP log exporter created"; then
        echo -e "${RED}BLOCKED DURING: OTLP log exporter creation${NC}"
        echo "  Same issue as trace exporter - HTTP client initialization blocking"
    elif echo "$FFI_DIAGS" | grep -q "Setting global tracer provider" && ! echo "$FFI_DIAGS" | grep -q "Global tracer provider set"; then
        echo -e "${RED}BLOCKED DURING: Global tracer provider setup${NC}"
    elif echo "$FFI_DIAGS" | grep -q "Setting global default subscriber" && ! echo "$FFI_DIAGS" | grep -q "Global default subscriber set"; then
        echo -e "${RED}BLOCKED DURING: Tracing subscriber setup${NC}"
    elif echo "$FFI_DIAGS" | grep -q "init_opentelemetry() EXIT"; then
        echo -e "${GREEN}OpenTelemetry initialization completed successfully${NC}"

        if ! echo "$FFI_DIAGS" | grep -q "create_backend_engine() completed"; then
            echo -e "${RED}BLOCKED DURING: holon::di::create_backend_engine()${NC}"
        fi
    fi
else
    echo -e "${RED}No [FFI-DIAG] markers found in log file${NC}"
    echo "  Make sure diagnostic code is in ffi_bridge.rs"
fi

echo ""
echo "=================================================="
echo "ADDITIONAL CHECKS"
echo "=================================================="
echo ""

# Check for RUST_BACKTRACE setting
BACKTRACE_CHECK=$(grep -i "RUST_BACKTRACE" "$LOGFILE" 2>/dev/null | head -5 || echo "")
if [ -n "$BACKTRACE_CHECK" ]; then
    echo "RUST_BACKTRACE setting found:"
    echo "$BACKTRACE_CHECK"
else
    echo "RUST_BACKTRACE setting not found in logs"
fi
echo ""

# Check for any errors
ERRORS=$(grep -iE '(error|panic|fatal|exception)' "$LOGFILE" 2>/dev/null | head -10 || echo "")
if [ -n "$ERRORS" ]; then
    echo -e "${RED}Errors found:${NC}"
    echo "$ERRORS"
else
    echo -e "${GREEN}No obvious errors found in logs${NC}"
fi
echo ""

echo "=================================================="
echo "SUMMARY"
echo "=================================================="
echo ""
echo "To continue investigation:"
echo "1. If blocked at [DIAG 30] / [FFI-DIAG] OTLP exporter:"
echo "   Try: RUST_BACKTRACE=full OTEL_TRACES_EXPORTER=stdout flutter run -d macos"
echo ""
echo "2. If that works, the issue is HTTP client + RUST_BACKTRACE interaction"
echo ""
echo "3. Next subtask (2-2): Capture stack traces with 'sample' while app is hanging"
echo "   sample \$(pgrep -f 'holon') 5 -file stack_sample.txt"
echo ""
