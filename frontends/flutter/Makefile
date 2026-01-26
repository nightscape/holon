.PHONY: help codegen clean run-macos run-ios run-android build-macos build-ios build-android test watch coverage coverage-process-rust coverage-process-flutter

# Default target
help:
	@echo "Flutter + Rust Bridge Build Automation"
	@echo ""
	@echo "Available targets:"
	@echo "  codegen      - Generate Rust bridge code"
	@echo "  clean        - Clean build artifacts"
	@echo "  run-macos    - Run on macOS (generates code automatically)"
	@echo "  run-ios      - Run on iOS (generates code automatically)"
	@echo "  run-android  - Run on Android (generates code automatically)"
	@echo "  build-macos  - Build macOS app"
	@echo "  build-ios    - Build iOS app"
	@echo "  build-android- Build Android app"
	@echo "  test         - Run tests"
	@echo "  watch        - Watch for changes and regenerate code"
	@echo "  pods         - Install CocoaPods dependencies"
	@echo ""
	@echo "Coverage targets (for dead code elimination):"
	@echo "  coverage              - Run app with Rust and Flutter coverage"
	@echo "  coverage-process-rust  - Process Rust coverage data"
	@echo "  coverage-process-flutter - Process Flutter coverage data"

# Generate Rust bridge code
codegen:
	@echo "Generating Flutter Rust Bridge code..."
	@./run_frb_codegen.sh generate

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	flutter clean
	cd rust && cargo clean

# Run on macOS
run-macos: codegen
	@echo "Running on macOS..."
	export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin/:$$PATH" && flutter run -d macos

# Run on iOS
run-ios: codegen
	@echo "Running on iOS..."
	flutter run -d ios

# Run on Android
run-android: codegen
	@echo "Running on Android..."
	flutter run -d android

# Build for macOS
build-macos: codegen
	@echo "Building for macOS..."
	export PATH="/opt/homebrew/lib/ruby/gems/3.4.0/bin/:$$PATH" && flutter build macos

# Build for iOS
build-ios: codegen
	@echo "Building for iOS..."
	flutter build ios

# Build for Android
build-android: codegen
	@echo "Building for Android..."
	flutter build apk

# Run tests
test: codegen
	@echo "Running tests..."
	flutter test

# Watch mode for development
watch:
	@echo "Watching for changes (press Ctrl+C to stop)..."
	@./run_frb_codegen.sh generate --watch

# Install CocoaPods dependencies
pods:
	@echo "Installing CocoaPods dependencies..."
	cd macos && pod install

# Run with Rust and Flutter coverage
coverage:
	@echo "Running with Rust and Flutter coverage..."
	@cd ../.. && ./scripts/run-with-coverage.sh -d macos

# Process Rust coverage data
coverage-process-rust:
	@echo "Processing Rust coverage data..."
	@cd ../.. && ./scripts/process-rust-coverage.sh html

# Process Flutter coverage data
coverage-process-flutter:
	@echo "Processing Flutter coverage data..."
	@cd ../.. && ./scripts/process-flutter-coverage.sh
