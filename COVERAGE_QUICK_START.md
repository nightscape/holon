# Code Coverage Quick Start

Quick reference for collecting runtime code coverage for dead code elimination.

## Quick Commands

### Run with Coverage

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

### Using Makefile (from frontends/flutter directory)

```bash
cd frontends/flutter

# Run with Rust and Flutter coverage
make coverage

# Process coverage data
make coverage-process-rust
make coverage-process-flutter
```

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
# macOS:
brew install lcov
# Linux:
apt-get install lcov
```

## Workflow

1. **Run with coverage** (exercise all features)
2. **Process coverage data** (generate reports)
3. **Analyze reports** (identify 0% coverage)
4. **Verify dead code** (check references, git history)
5. **Remove dead code** (commit removal)

## Important Notes

- **Normal builds have zero performance impact** - coverage is only enabled when explicitly requested
- Coverage data accumulates over time - run for days/weeks to get comprehensive data
- Look for 0% coverage over extended periods as candidates for removal
- Always verify code is truly unused before removal

## Full Documentation

See [docs/CODE_COVERAGE.md](docs/CODE_COVERAGE.md) for detailed instructions.

