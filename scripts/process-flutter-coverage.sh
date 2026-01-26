#!/bin/bash
# Script to process Flutter/Dart coverage data
# Processes coverage data collected via VM Service or test runs
#
# Usage:
#   ./scripts/process-flutter-coverage.sh [coverage-dir]
#
# Coverage directory defaults to: frontends/flutter/coverage
#
# Prerequisites:
#   - dart pub global activate coverage

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FLUTTER_DIR="$PROJECT_ROOT/frontends/flutter"
COVERAGE_DIR="${1:-$FLUTTER_DIR/coverage}"

echo "📊 Processing Flutter/Dart coverage data..."
echo ""

# Check if coverage directory exists
if [ ! -d "$COVERAGE_DIR" ]; then
    echo "❌ Coverage directory not found: $COVERAGE_DIR"
    echo "   Run the app with: ./scripts/run-with-flutter-coverage.sh"
    exit 1
fi

# Check for coverage tool
if ! command -v format_coverage &> /dev/null; then
    echo "📦 Installing coverage tool..."
    dart pub global activate coverage
fi

# Find coverage files
COVERAGE_FILES=$(find "$COVERAGE_DIR" -name "*.json" -o -name "coverage" | head -1)

if [ -z "$COVERAGE_FILES" ]; then
    echo "❌ No coverage data found in $COVERAGE_DIR"
    echo ""
    echo "   To collect coverage via VM Service:"
    echo "   1. Run app with: ./scripts/run-with-flutter-coverage.sh"
    echo "   2. Connect to VM Service (http://localhost:8181)"
    echo "   3. Enable coverage collection via VM Service API"
    echo "   4. Run your app scenarios"
    echo "   5. Collect coverage data"
    echo ""
    echo "   Or use test coverage:"
    echo "   flutter test --coverage"
    exit 1
fi

echo "   Found coverage data: $COVERAGE_FILES"
echo ""

# Process coverage
OUTPUT_FILE="$COVERAGE_DIR/coverage.lcov"
OUTPUT_HTML_DIR="$COVERAGE_DIR/html"

cd "$FLUTTER_DIR"

# Format coverage to LCOV
echo "📝 Formatting coverage to LCOV..."
format_coverage \
    --lcov \
    --in="$COVERAGE_DIR" \
    --out="$OUTPUT_FILE" \
    --packages=.dart_tool/package_config.json \
    --report-on=lib

if [ -f "$OUTPUT_FILE" ]; then
    echo "✅ LCOV report generated: $OUTPUT_FILE"
else
    echo "⚠️  LCOV report generation may have failed"
fi

# Generate HTML report if genhtml is available
if command -v genhtml &> /dev/null; then
    echo ""
    echo "📄 Generating HTML report..."
    mkdir -p "$OUTPUT_HTML_DIR"
    genhtml "$OUTPUT_FILE" -o "$OUTPUT_HTML_DIR" --no-function-coverage --no-branch-coverage
    echo "✅ HTML report generated: $OUTPUT_HTML_DIR/index.html"
    echo ""
    echo "   To view HTML report:"
    echo "   open $OUTPUT_HTML_DIR/index.html"
else
    echo ""
    echo "💡 Install genhtml for HTML reports:"
    echo "   macOS: brew install lcov"
    echo "   Linux: apt-get install lcov"
fi

echo ""
echo "📈 Coverage processing complete!"
echo ""
echo "   LCOV file: $OUTPUT_FILE"
echo "   To analyze dead code, look for files/functions with 0% coverage"

