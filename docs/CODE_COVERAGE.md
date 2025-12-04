# Code Coverage for Dead Code Elimination

This document explains how to collect runtime code coverage data from both Rust and Flutter code to identify and eliminate dead code.

## Running Coverage

The coverage script enables **both Rust and Flutter coverage simultaneously**. They are independent mechanisms:

- **Rust coverage**: Uses compile-time instrumentation (`RUSTFLAGS="-Cinstrument-coverage"`)
- **Flutter coverage**: Uses runtime VM Service Protocol

```bash
# Run with Rust and Flutter coverage
./scripts/run-with-coverage.sh -d macos

# Process both
./scripts/process-rust-coverage.sh html
./scripts/process-flutter-coverage.sh
```

This provides comprehensive coverage for dead code analysis.

## Overview

The coverage system is designed to have **zero performance impact** on normal builds. Coverage instrumentation is only enabled when explicitly requested via environment variables or build profiles.

## Rust Code Coverage

### Prerequisites

Install one of the coverage processing tools:

```bash
# Option 1: cargo-llvm-cov (recommended)
cargo install cargo-llvm-cov

# Option 2: grcov
cargo install grcov
```

### Running with Coverage

Use the provided script to run the Flutter app with Rust coverage instrumentation:

```bash
./scripts/run-with-coverage.sh -d macos
```

This script:
- Sets `RUSTFLAGS="-Cinstrument-coverage"` to enable LLVM source-based coverage
- Configures profile file output location
- Enables VM Service for Flutter coverage
- Runs the Flutter app with both coverage mechanisms active

Coverage data is written to:
- Rust: `frontends/flutter/rust/target/coverage/`
- Flutter: `frontends/flutter/coverage/`

### Processing Coverage Data

After running the app and exercising various features, process the coverage data:

```bash
# Generate LCOV format (default)
./scripts/process-rust-coverage.sh lcov

# Generate HTML report
./scripts/process-rust-coverage.sh html

# Generate Codecov JSON format
./scripts/process-rust-coverage.sh json
```

### Manual Setup

If you prefer to set up coverage manually:

```bash
# Set environment variables
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="target/coverage/holon-%p-%m.profraw"

# Build with coverage profile
cd frontends/flutter/rust
cargo build --profile coverage

# Run Flutter app
cd ..
flutter run -d macos

# Process coverage (using cargo-llvm-cov)
cargo llvm-cov --profile coverage --lcov --output-path target/coverage/coverage.lcov
```

### Analyzing Dead Code

1. Generate HTML report: `./scripts/process-rust-coverage.sh html`
2. Open `frontends/flutter/rust/target/coverage/html/index.html`
3. Look for files/functions with 0% coverage
4. Verify these are truly unused before removing

## Flutter/Dart Code Coverage

### Prerequisites

```bash
# Install coverage tool
dart pub global activate coverage
```

### Running with Coverage

The coverage script runs Flutter in debug mode, which automatically enables VM Service:

```bash
./scripts/run-with-coverage.sh -d macos
```

VM Service is automatically enabled in debug mode, allowing runtime coverage collection via the VM Service Protocol.

### Collecting Coverage via VM Service

The VM Service is accessible at `http://localhost:8181` (or the port shown in the output).

#### Option 1: Using VM Service API Directly

You can use the VM Service API to collect coverage programmatically:

```dart
// Example: Collect coverage via VM Service
import 'dart:io';
import 'package:vm_service/vm_service.dart';
import 'package:vm_service/vm_service_io.dart';

Future<void> collectCoverage() async {
  // Connect to VM Service
  final service = await vmServiceConnectUri('http://localhost:8181');

  // Get main isolate
  final vm = await service.getVM();
  final isolate = vm.isolates!.first;

  // Enable coverage collection
  await service.setVMTimelineFlags(['GC', 'Dart']);

  // Run your app scenarios here...

  // Collect coverage
  final coverage = await service.getSourceReport(
    isolate.id!,
    ['Coverage'],
  );

  // Save coverage data
  // Process with format_coverage tool
}
```

#### Option 2: Using Test Coverage

For test-based coverage (not runtime):

```bash
cd frontends/flutter
flutter test --coverage
```

This generates coverage data in `coverage/` directory.

### Processing Flutter Coverage

```bash
./scripts/process-flutter-coverage.sh
```

This generates:
- LCOV format: `frontends/flutter/coverage/coverage.lcov`
- HTML report (if genhtml installed): `frontends/flutter/coverage/html/index.html`

### Manual VM Service Coverage Collection

1. Run app in debug mode (VM Service is automatically enabled):
   ```bash
   flutter run --debug -d macos
   ```

2. Connect to VM Service (the URL is printed when the app starts, typically `http://localhost:XXXXX`)

3. Enable coverage collection via VM Service API

4. Exercise your app features

5. Collect coverage data

6. Process with `format_coverage` tool

## Workflow for Dead Code Elimination

### Recommended Workflow

1. **Collect Coverage Over Time**
   - Run app with coverage enabled for several days/weeks
   - Exercise all features and user workflows
   - Aggregate coverage data from multiple runs

2. **Process and Analyze**
   ```bash
   # Process Rust coverage
   ./scripts/process-rust-coverage.sh html

   # Process Flutter coverage
   ./scripts/process-flutter-coverage.sh
   ```

3. **Identify Dead Code**
   - Look for files/functions with 0% coverage
   - Cross-reference with static analysis (DCM for Dart)
   - Verify code is truly unused (check for conditional execution, error paths, etc.)

4. **Validate Before Removal**
   - Check git history for when code was last modified
   - Search codebase for references
   - Consider if code might be used in edge cases

5. **Remove Dead Code**
   - Remove unused code
   - Run tests to ensure nothing breaks
   - Commit removal

### Coverage Thresholds

Consider these thresholds for dead code elimination:

- **0% coverage over 30+ days**: Likely dead code (candidate for removal)
- **0% coverage but referenced**: May be error handling or edge cases (investigate)
- **Low coverage (< 10%)**: May be rarely used features (consider deprecation)

## Performance Impact

### Normal Builds

Normal builds have **zero performance impact**:
- No instrumentation overhead
- No coverage data collection
- Standard release/debug builds

### Coverage Builds

Coverage builds have minimal overhead:
- **Rust**: ~5-10% performance overhead from instrumentation
- **Flutter**: VM Service overhead is minimal (only when enabled)

## Troubleshooting

### Rust Coverage Issues

**Problem**: No `.profraw` files generated
- **Solution**: Ensure `RUSTFLAGS="-Cinstrument-coverage"` is set
- **Solution**: Check that `LLVM_PROFILE_FILE` is writable

**Problem**: Coverage shows 0% for everything
- **Solution**: Ensure you're running the instrumented binary
- **Solution**: Check that coverage data is being written

**Problem**: `cargo-llvm-cov` not found
- **Solution**: Install with `cargo install cargo-llvm-cov`

### Flutter Coverage Issues

**Problem**: VM Service not accessible
- **Solution**: Check firewall settings
- **Solution**: Verify port is not in use

**Problem**: No coverage data collected
- **Solution**: Ensure VM Service is enabled
- **Solution**: Verify coverage collection is enabled via API

**Problem**: `format_coverage` not found
- **Solution**: Run `dart pub global activate coverage`

## Integration with CI/CD

You can integrate coverage collection into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Collect Rust Coverage
  run: |
    export RUSTFLAGS="-Cinstrument-coverage"
    export LLVM_PROFILE_FILE="coverage-%p-%m.profraw"
    cargo build --profile coverage
    # Run tests or app
    cargo llvm-cov --profile coverage --lcov --output-path coverage.lcov
```

## References

- [LLVM Source-Based Code Coverage](https://clang.llvm.org/docs/SourceBasedCodeCoverage.html)
- [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov)
- [grcov](https://github.com/mozilla/grcov)
- [Flutter VM Service Protocol](https://github.com/dart-lang/sdk/blob/main/runtime/vm/service/service.md)
- [Dart Coverage Tool](https://pub.dev/packages/coverage)

