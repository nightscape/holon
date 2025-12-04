# Code Coverage Implementation Summary

This document summarizes the code coverage implementation for runtime dead code elimination.

## What Was Implemented

### 1. Rust Coverage Support

- **Coverage Profile**: Added `[profile.coverage]` to workspace and Flutter Rust bridge `Cargo.toml` files
- **Build Script**: `scripts/run-with-rust-coverage.sh` - Runs Flutter app with Rust coverage instrumentation
- **Processing Script**: `scripts/process-rust-coverage.sh` - Processes collected coverage data into reports

### 2. Flutter/Dart Coverage Support

- **VM Service Script**: `scripts/run-with-flutter-coverage.sh` - Runs Flutter app with VM Service enabled
- **Processing Script**: `scripts/process-flutter-coverage.sh` - Processes Flutter coverage data
- **VM Service Helper**: `scripts/collect-flutter-coverage-vm-service.dart` - Helper script for VM Service API

### 3. Documentation

- **Full Guide**: `docs/CODE_COVERAGE.md` - Comprehensive coverage documentation
- **Quick Start**: `COVERAGE_QUICK_START.md` - Quick reference guide
- **Makefile Targets**: Added coverage targets to `frontends/flutter/Makefile`

## Key Features

✅ **Zero Performance Impact on Normal Builds**
- Coverage instrumentation only enabled when explicitly requested
- Normal builds remain unchanged

✅ **Runtime Coverage Collection**
- Collects coverage data during actual app usage (not just tests)
- Suitable for identifying dead code in production scenarios

✅ **Easy to Use**
- Simple scripts for running with coverage
- Automatic processing and report generation
- Multiple output formats (LCOV, HTML, JSON)

## Quick Usage

```bash
# Run app with Rust and Flutter coverage
./scripts/run-with-coverage.sh -d macos

# Process both coverage data sets
./scripts/process-rust-coverage.sh html
./scripts/process-flutter-coverage.sh

# View reports
open frontends/flutter/rust/target/coverage/html/index.html
open frontends/flutter/coverage/html/index.html  # if genhtml installed
```

## Files Created/Modified

### Created Files

- `scripts/run-with-coverage.sh` - **Runner for Rust and Flutter coverage** (enables both simultaneously)
- `scripts/process-rust-coverage.sh` - Rust coverage processor
- `scripts/process-flutter-coverage.sh` - Flutter coverage processor
- `scripts/collect-flutter-coverage-vm-service.dart` - VM Service helper
- `docs/CODE_COVERAGE.md` - Full documentation
- `COVERAGE_QUICK_START.md` - Quick reference
- `COVERAGE_IMPLEMENTATION.md` - This file

### Modified Files

- `Cargo.toml` - Added coverage profile
- `frontends/flutter/rust/Cargo.toml` - Added coverage profile
- `frontends/flutter/Makefile` - Added coverage targets

## Prerequisites

### Rust Coverage Tools

Install one of:
```bash
cargo install cargo-llvm-cov
# OR
cargo install grcov
```

### Flutter Coverage Tools

```bash
dart pub global activate coverage

# Optional: For HTML reports
brew install lcov  # macOS
# OR
apt-get install lcov  # Linux
```

## How It Works

### Rust Coverage

1. **Instrumentation**: Sets `RUSTFLAGS="-Cinstrument-coverage"` to enable LLVM source-based coverage
2. **Collection**: Coverage data is written to `.profraw` files during app execution
3. **Processing**: Merges `.profraw` files and generates coverage reports

### Flutter Coverage

1. **VM Service**: Enables Dart VM Service Protocol for runtime coverage collection
2. **Collection**: Coverage data collected via VM Service API during app execution
3. **Processing**: Formats coverage data using Dart's `format_coverage` tool

## Performance Impact

- **Normal Builds**: Zero overhead - no instrumentation
- **Coverage Builds**:
  - Rust: ~5-10% performance overhead
  - Flutter: Minimal VM Service overhead

## Next Steps

1. **Collect Coverage**: Run app with coverage enabled for extended periods
2. **Analyze Reports**: Identify code with 0% coverage
3. **Verify Dead Code**: Check references, git history, edge cases
4. **Remove Dead Code**: Safely eliminate unused code

## Troubleshooting

See `docs/CODE_COVERAGE.md` for detailed troubleshooting guide.

## References

- [LLVM Source-Based Code Coverage](https://clang.llvm.org/docs/SourceBasedCodeCoverage.html)
- [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov)
- [Flutter VM Service Protocol](https://github.com/dart-lang/sdk/blob/main/runtime/vm/service/service.md)

