/// Main outliner view widget for displaying and editing hierarchical blocks.
library;

import 'package:flutter/material.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:outliner_view/outliner_view.dart';
import '../data/rust_block_ops.dart' show RustBlock;
import '../providers/repository_provider.dart'
    show blockOpsProvider, rustOutlinerProvider;
import '../styles/app_styles.dart';
import '../providers/settings_provider.dart';

class OutlinerView extends HookConsumerWidget {
  const OutlinerView({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return Consumer(
      builder: (context, ref, child) {
        final asyncOps = ref.watch(blockOpsProvider);
        final colors = ref.watch(appColorsProvider);

        return asyncOps.when(
          data: (ops) => OutlinerListView<RustBlock>(
            opsProvider: Provider<BlockOps<RustBlock>>((ref) => ops),
            notifierProvider: rustOutlinerProvider,
            config: OutlinerConfig(
              keyboardShortcutsEnabled: true,
              blockStyle: BlockStyle(
                indentWidth: AppSpacing.lg,
                textStyle: TextStyle(
                  fontSize: AppTypography.fontSizeMd,
                  color: colors.textPrimary,
                ),
                emptyTextStyle: TextStyle(
                  color: colors.textSecondary,
                  fontStyle: FontStyle.italic,
                  fontSize: AppTypography.fontSizeMd,
                ),
                editingTextStyle: TextStyle(
                  fontSize: AppTypography.fontSizeMd,
                  color: colors.textPrimary,
                ),
                bulletColor: colors.primary,
                contentPadding: EdgeInsets.symmetric(
                  vertical: AppSpacing.xs / 2, // 4.0
                  horizontal: AppSpacing.sm, // 8.0
                ),
              ),
            ),
          ),
          loading: () => const Center(child: CircularProgressIndicator()),
          error: (error, stack) => Center(child: Text('Error: $error')),
        );
      },
    );
  }
}
