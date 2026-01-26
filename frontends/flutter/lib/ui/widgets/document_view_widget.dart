import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:pie_menu/pie_menu.dart';

import '../../models/navigation_state.dart';
import '../../providers/navigation_provider.dart';
import '../../providers/settings_provider.dart' show appColorsProvider;
import '../../styles/app_styles.dart';
import 'query_block_widget.dart';

/// Widget for viewing a specific document with navigation controls.
///
/// Displays a document's content with:
/// - A header bar with back button, document title, and home button
/// - The document content rendered via a PRQL query
///
/// Used when navigating from the index document to a linked document.
class DocumentViewWidget extends ConsumerWidget {
  /// The document ID to display.
  final String documentId;

  const DocumentViewWidget({super.key, required this.documentId});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);
    final navigation = ref.watch(navigationProvider);

    // Build PRQL query to fetch and render this document's blocks
    // The document_id filter ensures we only get blocks from this document
    final prql =
        '''
      from blocks
      filter document_id == "$documentId"
      derive { entity_name = "blocks", sort_key = created_at }
      render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
    ''';

    return Column(
      children: [
        // Document header with navigation controls
        _buildHeader(context, ref, colors, navigation),
        // Document content
        Expanded(
          child: PieCanvas(
            child: QueryBlockWidget(prqlSource: prql, isMainRegion: true),
          ),
        ),
      ],
    );
  }

  Widget _buildHeader(
    BuildContext context,
    WidgetRef ref,
    AppColors colors,
    NavigationState navigation,
  ) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: colors.backgroundSecondary,
        border: Border(bottom: BorderSide(color: colors.border, width: 1)),
      ),
      child: Row(
        children: [
          // Back button
          if (navigation.canGoBack)
            IconButton(
              icon: Icon(
                Icons.arrow_back,
                color: colors.textSecondary,
                size: 20,
              ),
              onPressed: () {
                ref.read(navigationProvider.notifier).goBack();
              },
              tooltip: 'Go back',
            ),
          const SizedBox(width: 8),
          // Document title
          Expanded(
            child: Text(
              _extractDocumentTitle(documentId),
              style: TextStyle(
                fontSize: 16,
                fontWeight: FontWeight.w600,
                color: colors.textPrimary,
              ),
              overflow: TextOverflow.ellipsis,
            ),
          ),
          const SizedBox(width: 8),
          // Home button
          IconButton(
            icon: Icon(
              Icons.home_outlined,
              color: colors.textSecondary,
              size: 20,
            ),
            onPressed: () {
              ref.read(navigationProvider.notifier).goHome();
            },
            tooltip: 'Go to home',
          ),
        ],
      ),
    );
  }

  /// Extract a human-readable title from the document ID.
  ///
  /// The document ID format depends on the Loro implementation,
  /// but typically includes some form of path or name.
  String _extractDocumentTitle(String documentId) {
    // If the ID looks like a path, extract the last component
    if (documentId.contains('/')) {
      final parts = documentId.split('/');
      final lastPart = parts.last;
      // Remove file extension if present
      if (lastPart.contains('.')) {
        return lastPart.substring(0, lastPart.lastIndexOf('.'));
      }
      return lastPart;
    }
    // If it's a UUID-like ID, show a shortened version
    if (documentId.length > 20) {
      return '${documentId.substring(0, 8)}...';
    }
    return documentId;
  }
}
