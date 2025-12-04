/// Custom block builder for rendering block content.
library;

import 'package:flutter/material.dart';
import '../../data/rust_block_ops.dart' show RustBlock;

/// Build the content widget for a block.
///
/// This builder is called for each block in the outliner. It receives:
/// - [context]: The build context
/// - [block]: The rust Block to render
/// - [isEditing]: Whether the block is currently being edited
///
/// Returns a widget that displays the block's content.
Widget buildBlockContent(
  BuildContext context,
  RustBlock block,
  bool isEditing,
) {
  // NOTE: With opaque types, we cannot access block.content directly
  // The outliner-flutter library handles content access via BlockOps
  // This builder just needs to return null to use the default renderer
  return const SizedBox.shrink();
}
