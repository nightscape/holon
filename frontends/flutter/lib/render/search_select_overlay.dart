import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../providers/settings_provider.dart';
import '../providers/ui_state_providers.dart';
import 'gesture_context.dart';
import 'render_context.dart';
import 'renderable_item_ext.dart';

/// Overlay widget that appears during drag operations.
///
/// Shows a search box that can receive dropped items. When dropped,
/// expands to show filtered results. Clicking a result commits
/// entity-typed params (e.g., task_id, project_id) and executes
/// the matching operation.
class SearchSelectOverlay extends ConsumerStatefulWidget {
  const SearchSelectOverlay({super.key});

  @override
  ConsumerState<SearchSelectOverlay> createState() =>
      _SearchSelectOverlayState();
}

class _SearchSelectOverlayState extends ConsumerState<SearchSelectOverlay> {
  final TextEditingController _searchController = TextEditingController();
  final FocusNode _focusNode = FocusNode();
  String _searchQuery = '';

  @override
  void dispose() {
    _searchController.dispose();
    _focusNode.dispose();
    super.dispose();
  }

  List<Map<String, dynamic>> _filterResults(SearchSelectOverlayState state) {
    if (_searchQuery.isEmpty) {
      // Show first 10 results when search is empty
      return state.rowCache.values
          .where((row) => row['id']?.toString() != state.draggedItem?.id)
          .take(10)
          .toList();
    }

    final pattern = RegExp(RegExp.escape(_searchQuery), caseSensitive: false);
    final sourceId = state.draggedItem?.id;

    return state.rowCache.values
        .where((row) {
          // Exclude the dragged item itself
          if (row['id']?.toString() == sourceId) return false;

          // Search in content/name fields
          final content = row['content']?.toString() ?? '';
          final name = row['name']?.toString() ?? '';
          return content.contains(pattern) || name.contains(pattern);
        })
        .take(10)
        .toList();
  }

  void _selectNode(String nodeId, SearchSelectOverlayState state) {
    final draggedItem = state.draggedItem;
    if (draggedItem == null) return;

    // Get the selected node's data and template
    final selectedNode = state.rowCache[nodeId];
    if (selectedNode == null) {
      debugPrint('[SearchSelect] Node $nodeId not found in rowCache');
      return;
    }

    // Get template for selected node via ui column
    final uiIndex = selectedNode['ui'] as int?;
    final selectedTemplate = uiIndex != null && state.rowTemplates.isNotEmpty
        ? state.rowTemplates.firstWhere(
            (t) => t.index.toInt() == uiIndex,
            orElse: () => state.rowTemplates.first,
          )
        : null;

    final selectedShortName = selectedTemplate?.entityShortName ?? '';
    if (selectedShortName.isEmpty) {
      throw StateError(
        'Selected node has no entity_short_name. '
        'Ensure the entity macro has short_name defined.',
      );
    }

    final colors = ref.read(appColorsProvider);
    final sourceEntityName = draggedItem.entityName;

    // Create GestureContext with source item
    final gestureContext = GestureContext(
      sourceItemId: draggedItem.id,
      sourceRenderContext: RenderContext(
        rowData: draggedItem.rowData,
        rowTemplates: state.rowTemplates,
        onOperation: state.onOperation,
        entityName: sourceEntityName,
        availableOperations: draggedItem.operations,
        colors: colors,
      ),
    );

    // Commit entity-typed param (e.g., task_id, project_id)
    final entityIdKey = '${selectedShortName}_id';
    gestureContext.commitParams({entityIdKey: nodeId});

    debugPrint('[SearchSelect] Committing $entityIdKey: $nodeId');
    debugPrint('[SearchSelect] Source id: ${draggedItem.id}');

    // Find and execute matching operation
    final matches = gestureContext.findSatisfiableOperations();
    debugPrint('[SearchSelect] Found ${matches.length} matching operations');

    for (final m in matches) {
      debugPrint(
        '[SearchSelect]   - ${m.operationName}: resolved=${m.resolvedParams}, '
        'missing=${m.missingParams}, fullySatisfied=${m.isFullySatisfied}',
      );
    }

    final match = matches.where((m) => m.isFullySatisfied).firstOrNull;

    if (match != null) {
      debugPrint(
        '[SearchSelect] Executing: ${match.operationName} '
        'with ${match.resolvedParams}',
      );
      state.onOperation?.call(
        sourceEntityName,
        match.operationName,
        match.resolvedParams,
      );
    } else {
      throw StateError(
        'No matching operation found for selection. '
        'Source: $sourceEntityName, Selected: ${selectedTemplate?.entityName}, '
        'Committed params: ${gestureContext.committedParams}',
      );
    }

    // Hide overlay and reset state
    _searchQuery = '';
    _searchController.clear();
    ref.read(searchSelectOverlayProvider.notifier).hide();
  }

  @override
  Widget build(BuildContext context) {
    final state = ref.watch(searchSelectOverlayProvider);
    final colors = ref.watch(appColorsProvider);

    // Don't render anything in idle mode
    if (state.mode == SearchSelectMode.idle) {
      return const SizedBox.shrink();
    }

    final filteredResults = _filterResults(state);

    return Positioned(
      left: state.position.dx,
      top: state.position.dy,
      child: Material(
        elevation: 8,
        borderRadius: BorderRadius.circular(8),
        color: colors.background,
        child: DragTarget<RenderableItem>(
          onWillAcceptWithDetails: (_) =>
              state.mode == SearchSelectMode.dragActive,
          onAcceptWithDetails: (_) {
            ref.read(searchSelectOverlayProvider.notifier).activateSearchMode();
            // Request focus after the state update
            WidgetsBinding.instance.addPostFrameCallback((_) {
              _focusNode.requestFocus();
            });
          },
          builder: (context, candidateData, rejectedData) {
            final isHovering = candidateData.isNotEmpty;
            final isSearchMode = state.mode == SearchSelectMode.searchMode;

            return AnimatedContainer(
              duration: const Duration(milliseconds: 200),
              width: isSearchMode ? 280 : 140,
              constraints: BoxConstraints(maxHeight: isSearchMode ? 300 : 40),
              decoration: BoxDecoration(
                color: isHovering
                    ? colors.primary.withValues(alpha: 0.1)
                    : colors.background,
                borderRadius: BorderRadius.circular(8),
                border: Border.all(
                  color: isHovering ? colors.primary : colors.border,
                  width: isHovering ? 2 : 1,
                ),
              ),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  // Search field
                  Padding(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 8,
                      vertical: 4,
                    ),
                    child: TextField(
                      controller: _searchController,
                      focusNode: _focusNode,
                      enabled: isSearchMode,
                      style: TextStyle(fontSize: 14, color: colors.textPrimary),
                      decoration: InputDecoration(
                        hintText: isSearchMode
                            ? 'Type to filter...'
                            : 'Drop here',
                        hintStyle: TextStyle(
                          fontSize: 13,
                          color: colors.textTertiary,
                        ),
                        prefixIcon: Icon(
                          Icons.search,
                          size: 18,
                          color: colors.textSecondary,
                        ),
                        isDense: true,
                        border: InputBorder.none,
                        contentPadding: const EdgeInsets.symmetric(vertical: 8),
                      ),
                      onChanged: (value) {
                        setState(() => _searchQuery = value);
                      },
                    ),
                  ),

                  // Filtered results (only when in search mode)
                  if (isSearchMode && filteredResults.isNotEmpty)
                    Flexible(
                      child: Container(
                        decoration: BoxDecoration(
                          border: Border(
                            top: BorderSide(color: colors.border, width: 1),
                          ),
                        ),
                        child: ListView.builder(
                          shrinkWrap: true,
                          padding: EdgeInsets.zero,
                          itemCount: filteredResults.length,
                          itemBuilder: (context, index) {
                            final node = filteredResults[index];
                            final nodeId = node['id']?.toString() ?? '';
                            final content =
                                node['content']?.toString() ??
                                node['name']?.toString() ??
                                nodeId;

                            return InkWell(
                              onTap: () => _selectNode(nodeId, state),
                              child: Padding(
                                padding: const EdgeInsets.symmetric(
                                  horizontal: 12,
                                  vertical: 8,
                                ),
                                child: Text(
                                  content,
                                  maxLines: 1,
                                  overflow: TextOverflow.ellipsis,
                                  style: TextStyle(
                                    fontSize: 13,
                                    color: colors.textPrimary,
                                  ),
                                ),
                              ),
                            );
                          },
                        ),
                      ),
                    ),

                  // Empty state when in search mode but no results
                  if (isSearchMode &&
                      filteredResults.isEmpty &&
                      _searchQuery.isNotEmpty)
                    Padding(
                      padding: const EdgeInsets.all(12),
                      child: Text(
                        'No matches',
                        style: TextStyle(
                          fontSize: 13,
                          color: colors.textTertiary,
                          fontStyle: FontStyle.italic,
                        ),
                      ),
                    ),
                ],
              ),
            );
          },
        ),
      ),
    );
  }
}
