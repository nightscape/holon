#!/bin/bash
# Stack Trace Capture Script for Grey Window Investigation
# This script captures stack traces of a hanging Flutter/Holon process
# using macOS sample/spindump tools to identify thread deadlock
#
# Created: 2026-01-23
# Purpose: Subtask 2-2 - Capture stack traces to identify thread deadlock
#
# Usage:
#   1. First, reproduce the grey window issue:
#      RUST_BACKTRACE=full flutter run -d macos
#   2. While app is showing grey window, run this script in another terminal:
#      ./capture_stack_traces.sh
#   3. Analyze the output files for deadlock indicators

set -e

echo "=================================================="
echo "Stack Trace Capture for Grey Window Investigation"
echo "=================================================="
echo ""

# Create output directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLUTTER_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$FLUTTER_DIR/debug_logs/stack_traces"
mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "Output directory: $OUTPUT_DIR"
echo ""

# Function to find process
find_holon_process() {
    local pid=""

    # Try to find the holon/flutter process
    # Method 1: Look for holon_app (the actual app binary)
    pid=$(pgrep -f 'holon_app' 2>/dev/null | head -1) || true
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    # Method 2: Look for holon (general match)
    pid=$(pgrep -f 'holon' 2>/dev/null | head -1) || true
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    # Method 3: Look for Flutter Runner
    pid=$(pgrep -f 'Flutter.*Runner' 2>/dev/null | head -1) || true
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    # Method 4: Look for any Flutter process
    pid=$(pgrep -f 'flutter' 2>/dev/null | grep -v 'flutter run' | head -1) || true
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    echo ""
    return 1
}

echo "Searching for hanging Flutter/Holon process..."
echo ""

# List all related processes for visibility
echo "Related processes:"
echo "-----------------"
ps aux | grep -E 'holon|flutter|Flutter' | grep -v grep | grep -v "capture_stack_traces" || echo "No processes found"
echo ""

# Find the target process
TARGET_PID=$(find_holon_process)

if [ -z "$TARGET_PID" ]; then
    echo "ERROR: No Flutter/Holon process found!"
    echo ""
    echo "To capture stack traces:"
    echo "  1. Open a terminal and run:"
    echo "     cd $FLUTTER_DIR"
    echo "     RUST_BACKTRACE=full flutter run -d macos"
    echo ""
    echo "  2. Wait for the grey window to appear"
    echo ""
    echo "  3. In another terminal, run this script:"
    echo "     $0"
    echo ""
    exit 1
fi

echo "Found target process: PID $TARGET_PID"

# Get process details
echo ""
echo "Process details:"
ps -p "$TARGET_PID" -o pid,ppid,user,%cpu,%mem,state,command

echo ""
echo "=================================================="
echo "STEP 1: Capture with 'sample' command"
echo "=================================================="
echo ""

SAMPLE_FILE="$OUTPUT_DIR/sample_${TIMESTAMP}.txt"
echo "Sampling process $TARGET_PID for 5 seconds..."
echo "Output: $SAMPLE_FILE"
echo ""

# sample captures stack traces at regular intervals
# -wait option not available in newer macOS, just use duration
if sample "$TARGET_PID" 5 -file "$SAMPLE_FILE" 2>&1; then
    echo "Sample capture complete!"

    # Show summary
    echo ""
    echo "Sample Summary (first 50 lines):"
    echo "--------------------------------"
    head -50 "$SAMPLE_FILE"
    echo ""
    echo "... (truncated, see full file for complete output)"
else
    echo "WARNING: 'sample' command failed (may require elevated privileges)"
    echo "Try: sudo $0"
fi

echo ""
echo "=================================================="
echo "STEP 2: Capture with 'spindump' command"
echo "=================================================="
echo ""

SPINDUMP_FILE="$OUTPUT_DIR/spindump_${TIMESTAMP}.txt"
echo "Running spindump for process $TARGET_PID..."
echo "Output: $SPINDUMP_FILE"
echo ""

# spindump provides more detailed hang analysis
# Note: May require sudo for full functionality
if sudo spindump "$TARGET_PID" -timeline 10 -file "$SPINDUMP_FILE" 2>&1; then
    echo "Spindump capture complete!"

    # Show summary
    echo ""
    echo "Spindump Summary (first 50 lines):"
    echo "----------------------------------"
    head -50 "$SPINDUMP_FILE"
    echo ""
    echo "... (truncated, see full file for complete output)"
else
    echo "WARNING: 'spindump' failed"
    echo "This is normal if you don't have sudo access."
    echo "The 'sample' output above should still be useful."
fi

echo ""
echo "=================================================="
echo "STEP 3: Quick Thread Analysis"
echo "=================================================="
echo ""

if [ -f "$SAMPLE_FILE" ]; then
    echo "Analyzing sample output for key indicators..."
    echo ""

    # Count threads
    THREAD_COUNT=$(grep -c "Thread_" "$SAMPLE_FILE" 2>/dev/null || echo "0")
    echo "Total threads captured: $THREAD_COUNT"
    echo ""

    # Look for blocking indicators
    echo "Potential blocking indicators:"
    echo "------------------------------"

    # Look for mutex/lock related calls
    if grep -q "pthread_mutex\|pthread_cond\|_lock" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] Mutex/lock operations detected:"
        grep -E "pthread_mutex|pthread_cond|_lock" "$SAMPLE_FILE" | head -10
        echo ""
    fi

    # Look for sleep/wait calls
    if grep -q "__psynch_\|nanosleep\|semaphore_wait\|mach_msg_trap" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] Wait/sleep operations detected:"
        grep -E "__psynch_|nanosleep|semaphore_wait|mach_msg_trap" "$SAMPLE_FILE" | head -10
        echo ""
    fi

    # Look for network-related operations
    if grep -q "socket\|recv\|send\|connect\|network" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] Network operations detected:"
        grep -E "socket|recv|send|connect|network" "$SAMPLE_FILE" | head -10
        echo ""
    fi

    # Look for tokio/async runtime
    if grep -q "tokio\|async\|runtime\|spawn" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] Tokio/async runtime operations:"
        grep -E "tokio|async|runtime|spawn" "$SAMPLE_FILE" | head -10
        echo ""
    fi

    # Look for OpenTelemetry/OTLP
    if grep -q "opentelemetry\|otlp\|otel" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] OpenTelemetry operations:"
        grep -E "opentelemetry|otlp|otel" "$SAMPLE_FILE" | head -10
        echo ""
    fi

    # Look for backtrace/symbol resolution
    if grep -q "backtrace\|symbol\|demangle\|dwarf" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] Backtrace/symbol resolution operations:"
        grep -E "backtrace|symbol|demangle|dwarf" "$SAMPLE_FILE" | head -10
        echo ""
    fi

    # Look for HTTP/TLS
    if grep -q "http\|https\|tls\|ssl\|reqwest\|hyper" "$SAMPLE_FILE" 2>/dev/null; then
        echo "[FOUND] HTTP/TLS operations:"
        grep -E "http|https|tls|ssl|reqwest|hyper" "$SAMPLE_FILE" | head -10
        echo ""
    fi
fi

echo ""
echo "=================================================="
echo "STACK TRACE ANALYSIS GUIDE"
echo "=================================================="
echo ""
echo "Look for these deadlock patterns in the captured files:"
echo ""
echo "1. MAIN THREAD BLOCKED ON MUTEX:"
echo "   - Look for: 'pthread_mutex_lock' or '__psynch_mutexwait'"
echo "   - This indicates the main thread is waiting for a lock"
echo ""
echo "2. ASYNC RUNTIME BLOCKED:"
echo "   - Look for: 'tokio' stack frames stuck in 'park' or 'wait'"
echo "   - This indicates the tokio runtime is waiting for something"
echo ""
echo "3. HTTP CLIENT INITIALIZATION:"
echo "   - Look for: 'reqwest', 'hyper', 'http', 'tls'"
echo "   - Especially if stuck in 'connect' or 'ssl_handshake'"
echo ""
echo "4. BACKTRACE SYMBOL RESOLUTION:"
echo "   - Look for: 'backtrace', 'gimli', 'addr2line', 'symbolic'"
echo "   - If blocked here, RUST_BACKTRACE=full is causing symbol lock"
echo ""
echo "5. CROSS-THREAD DEADLOCK:"
echo "   - Multiple threads each waiting on something the other holds"
echo "   - Thread A: waiting on mutex M"
echo "   - Thread B: waiting on async operation, holding mutex M"
echo ""

echo ""
echo "=================================================="
echo "OUTPUT FILES"
echo "=================================================="
echo ""
echo "1. Sample output:    $SAMPLE_FILE"
if [ -f "$SPINDUMP_FILE" ]; then
    echo "2. Spindump output:  $SPINDUMP_FILE"
fi
echo ""
echo "To view full sample output:"
echo "  cat $SAMPLE_FILE"
echo ""
echo "To search for specific patterns:"
echo "  grep -E 'tokio|mutex|backtrace' $SAMPLE_FILE"
echo ""
echo "=================================================="
echo "NEXT STEPS"
echo "=================================================="
echo ""
echo "1. Review the captured stack traces"
echo "2. Identify which threads are blocked and on what"
echo "3. Document findings in the investigation report"
echo ""
echo "Key questions to answer:"
echo "  - Is main thread blocked waiting for async runtime?"
echo "  - Is async runtime blocked on HTTP/network operation?"
echo "  - Is there evidence of backtrace symbol resolution?"
echo "  - Is there a mutex contention pattern?"
echo ""
