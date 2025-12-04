#!/bin/bash
# Script to process Rust coverage data collected during runtime
# Generates coverage reports using grcov or cargo-llvm-cov
#
# Usage:
#   ./scripts/process-rust-coverage.sh [output-format]
#
# Output formats:
#   - lcov (default): LCOV format for coverage viewers
#   - html: HTML report
#   - json: JSON format
#   - codecov: Codecov format
#
# Prerequisites:
#   - grcov: cargo install grcov
#   - OR cargo-llvm-cov: cargo install cargo-llvm-cov

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FLUTTER_DIR="$PROJECT_ROOT/frontends/flutter"
RUST_DIR="$FLUTTER_DIR/rust"
COVERAGE_DIR="$RUST_DIR/target/coverage"
OUTPUT_FORMAT="${1:-lcov}"

echo "📊 Processing Rust coverage data..."
echo ""

# Check if coverage data exists
if [ ! -d "$COVERAGE_DIR" ] || [ -z "$(ls -A "$COVERAGE_DIR"/*.profraw 2>/dev/null)" ]; then
    echo "❌ No coverage data found in $COVERAGE_DIR"
    echo "   Run the app with: ./scripts/run-with-rust-coverage.sh"
    exit 1
fi

# Count profraw files
PROFRAW_COUNT=$(find "$COVERAGE_DIR" -name "*.profraw" | wc -l | tr -d ' ')
echo "   Found $PROFRAW_COUNT coverage profile file(s)"
echo ""

# Check for grcov or cargo-llvm-cov
if command -v cargo-llvm-cov &> /dev/null; then
    echo "✅ Using cargo-llvm-cov for processing..."
    cd "$RUST_DIR"

    # Merge profraw files first
    MERGED_PROFDATA="$COVERAGE_DIR/merged.profdata"
    echo "📦 Merging coverage profiles..."
    llvm-profdata merge -sparse "$COVERAGE_DIR"/*.profraw -o "$MERGED_PROFDATA" 2>/dev/null || {
        echo "⚠️  Could not merge profraw files - they may be in use or corrupted"
        echo "   Try closing the app and running this script again"
    }

    case "$OUTPUT_FORMAT" in
        lcov)
            OUTPUT_FILE="$COVERAGE_DIR/coverage.lcov"
            # Try to use cargo-llvm-cov with the merged profile
            if [ -f "$MERGED_PROFDATA" ]; then
                # Find binary - Flutter builds to different locations
                BINARY=$(find "$RUST_DIR/target" -name "librust_lib_holon.*" -type f 2>/dev/null | head -1)
                if [ -n "$BINARY" ] && [ -f "$BINARY" ]; then
                    llvm-cov export "$BINARY" \
                        -instr-profile="$MERGED_PROFDATA" \
                        -format=lcov \
                        -ignore-filename-regex='.*/target/.*' \
                        -ignore-filename-regex='.*/external/.*' \
                        > "$OUTPUT_FILE" 2>/dev/null && echo "✅ LCOV report generated: $OUTPUT_FILE" || {
                        echo "⚠️  Direct llvm-cov failed, trying cargo-llvm-cov..."
                        cargo llvm-cov --lcov --output-path "$OUTPUT_FILE" 2>/dev/null || true
                    }
                else
                    echo "⚠️  Could not find Rust library binary"
                    echo "   Try: cargo llvm-cov --lcov --output-path $OUTPUT_FILE"
                fi
            else
                echo "⚠️  No merged profile data - cannot generate report"
            fi
            ;;
        html)
            OUTPUT_DIR="$COVERAGE_DIR/html"
            if [ -f "$MERGED_PROFDATA" ]; then
                BINARY=$(find "$RUST_DIR/target" -name "librust_lib_holon.*" -type f 2>/dev/null | head -1)
                if [ -n "$BINARY" ] && [ -f "$BINARY" ]; then
                    llvm-cov show "$BINARY" \
                        -instr-profile="$MERGED_PROFDATA" \
                        -format=html \
                        -output-dir="$OUTPUT_DIR" \
                        -ignore-filename-regex='.*/target/.*' \
                        -ignore-filename-regex='.*/external/.*' \
                        2>/dev/null && echo "✅ HTML report generated: $OUTPUT_DIR/index.html" || {
                        echo "⚠️  HTML generation failed"
                    }
                fi
            fi
            ;;
        json|codecov)
            OUTPUT_FILE="$COVERAGE_DIR/coverage.json"
            cargo llvm-cov --codecov --output-path "$OUTPUT_FILE" 2>/dev/null || {
                echo "⚠️  Codecov report generation failed"
            }
            if [ -f "$OUTPUT_FILE" ]; then
                echo "✅ Codecov report generated: $OUTPUT_FILE"
            fi
            ;;
        *)
            echo "❌ Unknown output format: $OUTPUT_FORMAT"
            echo "   Supported formats: lcov, html, json, codecov"
            exit 1
            ;;
    esac

elif command -v grcov &> /dev/null; then
    echo "✅ Using grcov for processing..."

    # Find all profraw files
    PROFRAW_FILES=$(find "$COVERAGE_DIR" -name "*.profraw" | tr '\n' ' ')

    # Find binary and source directories
    BINARY_DIR="$RUST_DIR/target/coverage"
    SOURCE_DIR="$RUST_DIR"

    case "$OUTPUT_FORMAT" in
        lcov)
            OUTPUT_FILE="$COVERAGE_DIR/coverage.lcov"
            grcov "$COVERAGE_DIR" \
                --binary-path "$BINARY_DIR" \
                --source-dir "$SOURCE_DIR" \
                --llvm \
                --branch \
                --ignore-not-existing \
                --ignore "**/target/**" \
                --ignore "**/external/**" \
                --output-type lcov \
                --output-path "$OUTPUT_FILE"
            echo "✅ LCOV report generated: $OUTPUT_FILE"
            ;;
        html)
            OUTPUT_DIR="$COVERAGE_DIR/html"
            grcov "$COVERAGE_DIR" \
                --binary-path "$BINARY_DIR" \
                --source-dir "$SOURCE_DIR" \
                --llvm \
                --branch \
                --ignore-not-existing \
                --ignore "**/target/**" \
                --ignore "**/external/**" \
                --output-type html \
                --output-path "$OUTPUT_DIR"
            echo "✅ HTML report generated: $OUTPUT_DIR/index.html"
            ;;
        json|codecov)
            OUTPUT_FILE="$COVERAGE_DIR/coverage.json"
            grcov "$COVERAGE_DIR" \
                --binary-path "$BINARY_DIR" \
                --source-dir "$SOURCE_DIR" \
                --llvm \
                --branch \
                --ignore-not-existing \
                --ignore "**/target/**" \
                --ignore "**/external/**" \
                --output-type codecov \
                --output-path "$OUTPUT_FILE"
            echo "✅ Codecov report generated: $OUTPUT_FILE"
            ;;
        *)
            echo "❌ Unknown output format: $OUTPUT_FORMAT"
            echo "   Supported formats: lcov, html, json, codecov"
            exit 1
            ;;
    esac
else
    echo "❌ Neither grcov nor cargo-llvm-cov found"
    echo ""
    echo "   Install one of them:"
    echo "   cargo install grcov"
    echo "   OR"
    echo "   cargo install cargo-llvm-cov"
    exit 1
fi

echo ""
echo "📈 Coverage processing complete!"
echo ""
echo "   To view HTML report (if generated):"
echo "   open $COVERAGE_DIR/html/index.html"
echo ""
echo "   To analyze dead code, look for files/functions with 0% coverage"

