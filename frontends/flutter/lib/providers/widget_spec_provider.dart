import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show RustStreamSink;
import 'package:riverpod_annotation/riverpod_annotation.dart';
import '../src/rust/api/ffi_bridge.dart' as ffi;
import '../src/rust/third_party/holon_api/streaming.dart'
    show BatchMapChangeWithMetadata;
import '../src/rust/third_party/holon_api/widget_spec.dart';
import '../main.dart' show backendEngineProvider;

part 'widget_spec_provider.g.dart';

/// Provider for the initial widget specification (root layout)
///
/// WidgetSpec is the unified return type for all rendered widgets.
/// It contains everything the frontend needs to render:
/// - render_spec: How to render (e.g., columns, list, tree)
/// - data: The query result data (rows)
/// - actions: Available actions (global actions for root, contextual for others)
///
/// The CDC change stream is handled internally by the FFI bridge
/// and forwarded to a sink provided by the caller.
@Riverpod(keepAlive: true)
Future<WidgetSpec> initialWidget(Ref ref) async {
  // Ensure session is initialized
  final session = ref.watch(backendEngineProvider);
  if (session == null) {
    throw Exception(
      'Backend session not initialized. Call initRenderEngine first.',
    );
  }

  // Create a stream sink to receive CDC updates
  final batchSink = RustStreamSink<BatchMapChangeWithMetadata>();

  // Initialize the root widget (layout)
  return await ffi.initialWidget(sink: ffi.MapChangeSink(sink: batchSink));
}
