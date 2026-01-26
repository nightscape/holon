import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api/block.dart' as block_types;
import '../render_context.dart';
import '../source_block_widget.dart';
import 'widget_builder.dart';

/// Builds query_result() widget - displays execution results.
///
/// Usage: `query_result(result: result_data)` or `query_result()` to use context
class QueryResultWidgetBuilder {
  const QueryResultWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    // Try to get result from row data
    final resultValue =
        context.rowData['result'] ?? context.rowData['results'];

    block_types.BlockResult? result;
    if (resultValue is block_types.BlockResult) {
      result = resultValue;
    }

    if (result == null) {
      return const SizedBox.shrink();
    }

    return QueryResultWidget(result: result, colors: context.colors);
  }
}
