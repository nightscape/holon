import 'package:mocktail/mocktail.dart';
import '../src/rust/frb_generated.dart';

/// Mock implementation of RustLibApi for running Flutter without native Rust libraries.
///
/// Usage:
///   flutter run --dart-define=USE_MOCK_BACKEND=true
///
/// This allows UI development when Rust code doesn't compile or native libraries
/// aren't built. The MockBackendService handles all business logic - this mock
/// just prevents RustLib.init() from loading native libraries.
class MockRustLibApi extends Mock implements RustLibApi {}

/// Configure the mock API.
///
/// Call this after creating the mock but before passing it to RustLib.init().
/// No stubs are needed since MockBackendService handles all backend calls.
void setupMockRustLibApi(MockRustLibApi mockApi) {
  // No stubs needed - MockBackendService handles all business logic.
  // The mock API just prevents native library loading.
}
