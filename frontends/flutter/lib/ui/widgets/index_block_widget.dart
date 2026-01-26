import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../models/layout_region.dart';
import '../../providers/navigation_provider.dart';
import '../../providers/settings_provider.dart' show appColorsProvider;
import '../../styles/app_styles.dart';
import 'query_block_widget.dart';

/// Widget that renders a single index block based on its view type.
///
/// This widget is responsible for rendering blocks from the index document.
/// Each block can be:
/// - A query block that executes PRQL and displays results
/// - A link to navigate to another document
/// - An action button (capture, sync, settings, etc.)
/// - Plain text content
class IndexBlockWidget extends ConsumerWidget {
  /// The index block to render.
  final IndexBlock block;

  /// Whether this block is rendered in the main content region.
  /// Affects styling (larger fonts, more spacing for main region).
  final bool isMainRegion;

  const IndexBlockWidget({
    super.key,
    required this.block,
    this.isMainRegion = false,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        // Section header
        if (block.title.isNotEmpty) _buildHeader(colors),

        // Content based on view type
        _buildContent(context, ref, colors),

        // Render children recursively
        ...block.children.map(
          (child) => Padding(
            padding: EdgeInsets.only(left: isMainRegion ? 0 : 16),
            child: IndexBlockWidget(block: child, isMainRegion: isMainRegion),
          ),
        ),
      ],
    );
  }

  Widget _buildHeader(AppColors colors) {
    return Padding(
      padding: EdgeInsets.fromLTRB(
        isMainRegion ? 0 : 20,
        isMainRegion ? 16 : 8,
        isMainRegion ? 0 : 20,
        isMainRegion ? 12 : 4,
      ),
      child: Text(
        block.title,
        style: TextStyle(
          fontSize: isMainRegion ? 18 : 12,
          fontWeight: FontWeight.w600,
          color: isMainRegion ? colors.textPrimary : colors.textSecondary,
          letterSpacing: isMainRegion ? 0 : 0.5,
        ),
      ),
    );
  }

  Widget _buildContent(BuildContext context, WidgetRef ref, AppColors colors) {
    switch (block.viewType) {
      case BlockViewType.query:
        return _buildQueryView(colors);
      case BlockViewType.link:
        return _buildLinkView(context, ref, colors);
      case BlockViewType.action:
        return _buildActionView(context, ref, colors);
      case BlockViewType.text:
        return _buildTextView(colors);
    }
  }

  Widget _buildQueryView(AppColors colors) {
    if (block.prqlSource == null || block.prqlSource!.isEmpty) {
      return Padding(
        padding: const EdgeInsets.all(16),
        child: Text(
          'No query defined',
          style: TextStyle(
            color: colors.textSecondary,
            fontStyle: FontStyle.italic,
          ),
        ),
      );
    }

    return Padding(
      padding: EdgeInsets.symmetric(
        horizontal: isMainRegion ? 0 : 8,
        vertical: 4,
      ),
      child: QueryBlockWidget(
        prqlSource: block.prqlSource!,
        isMainRegion: isMainRegion,
      ),
    );
  }

  Widget _buildLinkView(BuildContext context, WidgetRef ref, AppColors colors) {
    return ListTile(
      contentPadding: EdgeInsets.symmetric(
        horizontal: isMainRegion ? 0 : 20,
        vertical: 2,
      ),
      leading: Icon(
        Icons.article_outlined,
        size: 20,
        color: colors.textSecondary,
      ),
      title: Text(
        block.title,
        style: TextStyle(color: colors.textPrimary, fontSize: 14),
      ),
      onTap: () {
        if (block.targetDocumentId != null) {
          ref
              .read(navigationProvider.notifier)
              .navigateTo(block.targetDocumentId!);
        }
      },
    );
  }

  Widget _buildActionView(
    BuildContext context,
    WidgetRef ref,
    AppColors colors,
  ) {
    return ListTile(
      contentPadding: EdgeInsets.symmetric(
        horizontal: isMainRegion ? 0 : 20,
        vertical: 2,
      ),
      leading: Icon(
        _iconForAction(block.actionName),
        size: 20,
        color: colors.primary,
      ),
      title: Text(
        block.title,
        style: TextStyle(
          color: colors.primary,
          fontSize: 14,
          fontWeight: FontWeight.w500,
        ),
      ),
      onTap: () => _executeAction(context, ref, block.actionName),
    );
  }

  Widget _buildTextView(AppColors colors) {
    return Padding(
      padding: EdgeInsets.symmetric(
        horizontal: isMainRegion ? 0 : 20,
        vertical: 4,
      ),
      child: Text(
        block.title,
        style: TextStyle(color: colors.textPrimary, fontSize: 14),
      ),
    );
  }

  IconData _iconForAction(String? action) {
    switch (action?.toLowerCase()) {
      case 'capture':
        return Icons.add_circle_outline;
      case 'sync':
        return Icons.sync;
      case 'settings':
        return Icons.settings_outlined;
      case 'search':
        return Icons.search;
      case 'undo':
        return Icons.undo;
      case 'redo':
        return Icons.redo;
      default:
        return Icons.play_arrow;
    }
  }

  void _executeAction(BuildContext context, WidgetRef ref, String? action) {
    switch (action?.toLowerCase()) {
      case 'capture':
        // TODO: Trigger capture overlay
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Capture action not yet implemented')),
        );
        break;
      case 'sync':
        // TODO: Trigger sync operation
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Sync action not yet implemented')),
        );
        break;
      case 'settings':
        // Open settings dialog - import SettingsScreen where this is used
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Open settings from sidebar menu')),
        );
        break;
      default:
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('Unknown action: $action')));
    }
  }
}
