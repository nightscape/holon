/// Custom bullet builder for LogSeq-style bullets.
library;

import 'package:flutter/material.dart';
import 'package:mix/mix.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import '../../data/rust_block_ops.dart' show RustBlock;
import '../../styles/app_styles.dart';
import '../../providers/settings_provider.dart';

/// Build the bullet widget for a block (LogSeq-style).
///
/// This builder is called to render the bullet point/expand/collapse indicator
/// for each block. It receives:
/// - [context]: The build context
/// - [block]: The rust Block to render the bullet for
/// - [hasChildren]: Whether the block has children
/// - [isCollapsed]: Whether the block is currently collapsed
/// - [onToggle]: Callback to toggle the expand/collapse state
///
/// Returns a widget that displays the bullet and handles collapse toggling.
Widget buildBullet(
  BuildContext context,
  RustBlock block,
  bool hasChildren,
  bool isCollapsed,
  VoidCallback? onToggle,
) {
  return Consumer(
    builder: (context, ref, child) {
      final colors = ref.watch(appColorsProvider);

      return GestureDetector(
        onTap: hasChildren ? onToggle : null,
        behavior: HitTestBehavior.opaque,
        child: SizedBox(
          width: 20,
          height: 20,
          child: hasChildren
              ? _buildExpandableIcon(colors, isCollapsed)
              : _buildSimpleBullet(colors),
        ),
      );
    },
  );
}

/// Build the expandable/collapsible icon for blocks with children.
Widget _buildExpandableIcon(AppColors colors, bool isCollapsed) {
  return Icon(
    isCollapsed ? Icons.chevron_right : Icons.expand_more,
    size: 20,
    color: colors.textSecondary,
  );
}

/// Build a simple bullet point for blocks without children (LogSeq-style).
Widget _buildSimpleBullet(AppColors colors) {
  return Box(
    style: BoxStyler()
        .constraints(BoxConstraintsMix(minWidth: 8, minHeight: 8))
        .margin(EdgeInsetsGeometryMix.all(6))
        .decoration(DecorationMix.shape(BoxShape.circle))
        .border(
          BoxBorderMix.all(
            BorderSideMix(
              color: colors.primary.withValues(alpha: 0.6),
              width: 2,
            ),
          ),
        ),
  );
}
