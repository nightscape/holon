import 'package:flutter/material.dart';
import 'package:pie_menu/pie_menu.dart';
import 'package:outliner_view/outliner_view.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:intl/intl.dart';
import '../src/rust/third_party/holon_api.dart';
import '../src/rust/third_party/holon_api/block.dart' as block_types;
import '../src/rust/third_party/holon_api/render_types.dart';
import '../data/row_data_block_ops.dart';
import '../providers/ui_state_providers.dart';
import '../styles/animation_constants.dart';
import '../styles/app_styles.dart';
import 'render_context.dart';
export 'render_context.dart';
import 'editable_text_field.dart';
import 'tree_view_widget.dart';
import 'renderable_item_ext.dart';
import 'source_block_widget.dart';

/// Interprets generic RenderExpr AST and builds Flutter widgets.
///
/// This interpreter maps function calls to Flutter widgets:
/// - `list(...)` → ListView.builder
/// - `block(...)` → Column with indentation
/// - `editable_text(...)` → TextField
/// - `row(...)` → Row
/// - Custom functions can be added via extensibility
class RenderInterpreter {
  /// Creates a beautiful pie menu theme with glassy buttons and less transparent background.
  static PieTheme _createBeautifulPieTheme() {
    return PieTheme(
      regularPressShowsMenu: true,
      longPressShowsMenu: false,
      // Less transparent background overlay (only slight transparency)
      overlayColor: Colors.white.withOpacity(0.05),
      // Glassy button theme with semi-transparent circles
      buttonTheme: PieButtonTheme(
        backgroundColor: Colors.transparent, // Will be overridden by decoration
        iconColor: const Color(0xFF1F2937), // Dark icon color for visibility
        decoration: BoxDecoration(
          // Glassy effect: semi-transparent white with gradient
          gradient: LinearGradient(
            begin: Alignment.topLeft,
            end: Alignment.bottomRight,
            colors: [
              Colors.white.withOpacity(0.9),
              Colors.white.withOpacity(0.7),
            ],
          ),
          // Subtle border for glassy definition
          border: Border.all(color: Colors.white.withOpacity(0.5), width: 1.5),
          // Rounded circle
          shape: BoxShape.circle,
          // Shadow for depth and glassy effect
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.1),
              blurRadius: 8,
              offset: const Offset(0, 2),
            ),
            BoxShadow(
              color: Colors.white.withOpacity(0.5),
              blurRadius: 4,
              offset: const Offset(-1, -1),
            ),
          ],
        ),
      ),
      // Hovered state with slightly more opacity
      buttonThemeHovered: PieButtonTheme(
        backgroundColor: Colors.transparent, // Will be overridden by decoration
        iconColor: const Color(0xFF3B82F6), // Primary color on hover
        decoration: BoxDecoration(
          // More opaque on hover for better visibility
          gradient: LinearGradient(
            begin: Alignment.topLeft,
            end: Alignment.bottomRight,
            colors: [
              Colors.white.withOpacity(0.95),
              Colors.white.withOpacity(0.85),
            ],
          ),
          // Slightly more prominent border on hover
          border: Border.all(
            color: const Color(0xFF3B82F6).withOpacity(0.3),
            width: 2,
          ),
          // Rounded circle
          shape: BoxShape.circle,
          // Enhanced shadow on hover
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.15),
              blurRadius: 12,
              offset: const Offset(0, 3),
            ),
            BoxShadow(
              color: const Color(0xFF3B82F6).withOpacity(0.2),
              blurRadius: 6,
              offset: const Offset(0, 0),
            ),
          ],
        ),
      ),
    );
  }

  /// Build a widget from a RenderExpr using the provided context.
  Widget build(RenderExpr expr, RenderContext context) {
    return expr.when(
      functionCall: (name, args, wirings) =>
          _buildFunctionCall(name, args, wirings, context),
      columnRef: (name) => _buildColumnRef(name, context),
      literal: (value) => _buildLiteral(value),
      binaryOp: (op, left, right) => _buildBinaryOp(op, left, right, context),
      array: (items) => _buildArray(items, context),
      object: (fields) => _buildObject(fields, context),
    );
  }

  /// Build widget from function call (main widget mapping logic).
  ///
  /// Each FunctionCall node has its own operations attached based on the columns
  /// it references. These operations are passed directly via [wirings] - no aggregation needed.
  Widget _buildFunctionCall(
    String name,
    List<Arg> args,
    List<OperationWiring> wirings,
    RenderContext context,
  ) {
    final namedArgs = <String, RenderExpr>{};
    final positionalArgs = <RenderExpr>[];

    for (final arg in args) {
      if (arg.name != null) {
        namedArgs[arg.name!] = arg.value;
      } else {
        positionalArgs.add(arg.value);
      }
    }

    // Extract operations from this node's wirings (no aggregation from children)
    final nodeOperations = wirings.map((w) => w.descriptor).toList();

    // Extract entity name from first operation (all operations should have same entity_name)
    final entityName = nodeOperations.isNotEmpty
        ? nodeOperations.first.entityName
        : context.entityName;

    // Create context with this node's operations
    // For pie_menu with fields:this.*, merge parent operations if nodeOperations is empty
    final finalOperations = nodeOperations.isNotEmpty
        ? nodeOperations
        : (name == 'pie_menu' ? context.availableOperations : nodeOperations);

    final enrichedContext = RenderContext(
      rowData: context.rowData,
      rowTemplates: context.rowTemplates,
      onOperation: context.onOperation,
      nestedQueryConfig: context.nestedQueryConfig,
      availableOperations: finalOperations,
      entityName: entityName,
      rowIndex: context.rowIndex,
      previousRowData: context.previousRowData,
      rowCache: context.rowCache,
      changeStream: context.changeStream,
      parentIdColumn: context.parentIdColumn,
      sortKeyColumn: context.sortKeyColumn,
      colors: context.colors,
      focusDepth: context.focusDepth,
    );

    switch (name) {
      case 'list':
        return _buildList(namedArgs, enrichedContext);
      case 'outline':
        return _buildOutline(namedArgs, enrichedContext);
      case 'tree':
        return _buildTree(namedArgs, enrichedContext);
      case 'block':
        return _buildBlock(namedArgs, positionalArgs, enrichedContext);
      case 'row':
        return _buildRow(namedArgs, positionalArgs, enrichedContext);
      case 'editable_text':
        return _buildEditableText(namedArgs, enrichedContext);
      case 'text':
        return _buildText(namedArgs, positionalArgs, enrichedContext);
      case 'drop_zone':
        return _buildDropZone(namedArgs, enrichedContext);
      case 'collapse_button':
        return _buildCollapseButton(namedArgs, enrichedContext);
      case 'block_operations':
        return _buildBlockOperations(namedArgs, enrichedContext);
      case 'flexible':
        return _buildFlexible(namedArgs, positionalArgs, enrichedContext);
      case 'checkbox':
        return _buildCheckbox(namedArgs, enrichedContext);
      case 'badge':
        return _buildBadge(namedArgs, enrichedContext);
      case 'bullet':
        return _buildBullet(namedArgs, positionalArgs, enrichedContext);
      case 'pie_menu':
        return _buildPieMenu(namedArgs, positionalArgs, enrichedContext);
      case 'icon':
        return _buildIcon(namedArgs, positionalArgs, enrichedContext);
      case 'spacer':
        return _buildSpacer(namedArgs, enrichedContext);
      case 'draggable':
        return _buildDraggable(namedArgs, positionalArgs, enrichedContext);

      // Phase 2: Dashboard layout primitives
      case 'section':
        return _buildSection(namedArgs, positionalArgs, enrichedContext);
      case 'grid':
        return _buildGrid(namedArgs, positionalArgs, enrichedContext);
      case 'stack':
        return _buildStack(namedArgs, positionalArgs, enrichedContext);
      case 'scroll':
        return _buildScroll(namedArgs, positionalArgs, enrichedContext);

      // Phase 2: Dashboard widgets
      case 'date_header':
        return _buildDateHeader(namedArgs, enrichedContext);
      case 'progress':
        return _buildProgress(namedArgs, enrichedContext);
      case 'count_badge':
        return _buildCountBadge(namedArgs, enrichedContext);
      case 'status_indicator':
        return _buildStatusIndicator(namedArgs, enrichedContext);

      // Phase 2: Interactive enhancements
      case 'hover_row':
        return _buildHoverRow(namedArgs, positionalArgs, enrichedContext);
      case 'focusable':
        return _buildFocusable(namedArgs, positionalArgs, enrichedContext);

      // Phase 2: Animation support
      case 'staggered':
        return _buildStaggered(namedArgs, positionalArgs, enrichedContext);
      case 'animated':
        return _buildAnimated(namedArgs, positionalArgs, enrichedContext);
      case 'pulse':
        return _buildPulse(namedArgs, positionalArgs, enrichedContext);

      // Phase 7: Source block support
      case 'source_block':
        return _buildSourceBlock(namedArgs, positionalArgs, enrichedContext);
      case 'source_editor':
        return _buildSourceEditor(namedArgs, enrichedContext);
      case 'query_result':
        return _buildQueryResult(namedArgs, enrichedContext);

      default:
        return _buildUnknownFunction(name, args);
    }
  }

  /// Automatically attach pie menu to a widget based on field interests.
  ///
  /// If operations are available that affect any of the specified fields,
  /// wraps the child widget in a PieMenu (requires PieCanvas ancestor).
  Widget _autoAttachPieMenu(
    Widget child,
    List<String> fieldsOfInterest,
    RenderContext context,
  ) {
    // Find operations that affect any of these fields
    final relevantOps = context.operationsAffecting(fieldsOfInterest);
    if (relevantOps.isEmpty) return child;

    // Map operations to pie actions
    final actions = relevantOps.map((op) {
      return PieAction(
        tooltip: Text(op.displayName.isNotEmpty ? op.displayName : op.name),
        onSelect: () {
          // Build operation parameters from row data
          final params = <String, dynamic>{'id': context.rowData['id']};

          // Handle special cases (e.g., indent needs parent_id from previous row)
          if (op.name == 'indent' &&
              context.rowIndex != null &&
              context.rowIndex! > 0) {
            // Get parent_id from previous row
            if (context.previousRowData != null) {
              final previousId = context.previousRowData!['id'];
              if (previousId != null) {
                params['parent_id'] = previousId.toString();
              }
            }
          }

          // Add other required parameters if available in row data
          for (final param in op.requiredParams) {
            if (context.rowData.containsKey(param.name)) {
              params[param.name] = context.rowData[param.name];
            }
          }

          // Use entity_name from row data (for UNION queries), then operation descriptor, then context
          final entityName =
              context.rowData['entity_name']?.toString() ??
              (op.entityName.isNotEmpty ? op.entityName : null) ??
              context.entityName;

          if (entityName == null) {
            throw StateError(
              'Cannot dispatch operation "${op.name}": no entity_name found in row data, '
              'operation descriptor, or context. This is a bug.',
            );
          }

          context.onOperation?.call(entityName, op.name, params);
        },
        child: Icon(_iconForOperation(op)),
      );
    }).toList();

    // PieMenu without PieCanvas wrapper - expects a PieCanvas ancestor in the widget tree
    return PieMenu(
      theme: _createBeautifulPieTheme(),
      actions: actions,
      child: child,
    );
  }

  /// Attach pie menu triggered by tap (not long-press) with the given operations.
  Widget _attachTapPieMenu(
    Widget child,
    List<OperationDescriptor> operations,
    RenderContext context,
  ) {
    if (operations.isEmpty) return child;

    // Map operations to pie actions
    final actions = operations.map((op) {
      return PieAction(
        tooltip: Text(op.displayName.isNotEmpty ? op.displayName : op.name),
        onSelect: () {
          final params = <String, dynamic>{'id': context.rowData['id']};

          if (op.name == 'indent' &&
              context.rowIndex != null &&
              context.rowIndex! > 0) {
            if (context.previousRowData != null) {
              final previousId = context.previousRowData!['id'];
              if (previousId != null) {
                params['parent_id'] = previousId.toString();
              }
            }
          }

          for (final param in op.requiredParams) {
            if (context.rowData.containsKey(param.name)) {
              params[param.name] = context.rowData[param.name];
            }
          }

          final entityName =
              context.rowData['entity_name']?.toString() ??
              (op.entityName.isNotEmpty ? op.entityName : null) ??
              context.entityName;

          if (entityName == null) {
            throw StateError(
              'Cannot dispatch operation "${op.name}": no entity_name found in row data, '
              'operation descriptor, or context. This is a bug.',
            );
          }

          context.onOperation?.call(entityName, op.name, params);
        },
        child: Icon(_iconForOperation(op)),
      );
    }).toList();

    // PieMenu without PieCanvas wrapper - expects a PieCanvas ancestor in the widget tree
    return PieMenu(
      theme: _createBeautifulPieTheme(),
      actions: actions,
      child: child,
    );
  }

  /// Get icon for an operation based on its name.
  IconData _iconForOperation(OperationDescriptor op) {
    final name = op.name.toLowerCase();
    if (name.contains('indent')) {
      return Icons.subdirectory_arrow_right;
    } else if (name.contains('outdent')) {
      return Icons.subdirectory_arrow_left;
    } else if (name.contains('collapse') || name.contains('expand')) {
      return Icons.expand_more;
    } else if (name.contains('move_up') || name.contains('moveup')) {
      return Icons.arrow_upward;
    } else if (name.contains('move_down') || name.contains('movedown')) {
      return Icons.arrow_downward;
    } else if (name.contains('delete') || name.contains('remove')) {
      return Icons.delete;
    } else if (name.contains('status')) {
      return Icons.circle;
    } else if (name.contains('complete')) {
      return Icons.check_circle;
    } else if (name.contains('priority')) {
      return Icons.flag;
    } else if (name.contains('due') || name.contains('date')) {
      return Icons.calendar_today;
    } else if (name.contains('split')) {
      return Icons.content_cut;
    }
    // Default icon
    return Icons.more_horiz;
  }

  /// Build Flexible wrapper from flexible() function.
  /// Used to provide flex constraints to children in Row/Column.
  Widget _buildFlexible(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      throw ArgumentError('flexible() requires a child argument');
    }

    final child = build(positionalArgs[0], context);

    // Optional flex factor (default 1)
    final flexExpr = namedArgs['flex'];
    final flex = flexExpr != null ? _evaluateToInt(flexExpr, context) : 1;

    return Flexible(flex: flex, child: child);
  }

  /// Build ListView from list() function.
  /// LogSeq-style: better padding and spacing.
  Widget _buildList(Map<String, RenderExpr> args, RenderContext renderContext) {
    final itemExpr = args['item_template'] ?? args['item'];
    if (itemExpr == null) {
      throw ArgumentError('list() requires "item_template" or "item" argument');
    }

    // For now, build a single item. In Phase 4.1, this will be replaced
    // with StreamBuilder that listens to CDC events and builds multiple items.
    return ListView.builder(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      itemCount: 1,
      itemBuilder: (buildContext, index) => build(itemExpr, renderContext),
    );
  }

  /// Build AnimatedTreeView from tree() function.
  /// Uses flutter_fancy_tree_view2 for hierarchical tree display with drag-and-drop.
  Widget _buildTree(Map<String, RenderExpr> args, RenderContext context) {
    // Extract required parameters
    final parentIdExpr = args['parent_id'];
    final sortKeyExpr = args['sortkey'] ?? args['sort_key'];

    if (parentIdExpr == null) {
      throw ArgumentError('tree() requires "parent_id" argument');
    }
    if (sortKeyExpr == null) {
      throw ArgumentError('tree() requires "sortkey" or "sort_key" argument');
    }

    // Evaluate column names (should be string literals or column references)
    final parentIdColumn = (parentIdExpr as RenderExpr_ColumnRef).name;
    final sortKeyColumn = (sortKeyExpr as RenderExpr_ColumnRef).name;

    // Check if context has required data
    if (context.rowCache == null) {
      throw ArgumentError(
        'tree() requires rowCache in RenderContext. '
        'This should be provided by ReactiveQueryWidget.',
      );
    }

    if (context.entityName == null) {
      throw ArgumentError('tree() requires entityName in RenderContext.');
    }

    if (context.onOperation == null) {
      throw ArgumentError('tree() requires onOperation in RenderContext.');
    }

    if (context.rowTemplates.isEmpty) {
      throw ArgumentError('tree() requires rowTemplates in RenderContext.');
    }

    final rowCache = context.rowCache!;
    final entityName = context.entityName!;
    final onOperation = context.onOperation!;
    final rowTemplates = context.rowTemplates;
    final interpreter = this;

    // Helper function to compare sort keys
    int compareSortKeys(dynamic a, dynamic b) {
      if (a == null && b == null) return 0;
      if (a == null) return -1;
      if (b == null) return 1;
      if (a is num && b is num) {
        return a.compareTo(b);
      }
      return a.toString().compareTo(b.toString());
    }

    // Helper function to get ID from a row
    String getId(Map<String, dynamic> row) {
      return row['id']?.toString() ?? '';
    }

    // Build indices for O(1) lookups instead of O(n) filtering on every call.
    // This is critical for performance with large datasets (100k+ items).
    final rootNodes = <Map<String, dynamic>>[];
    final childrenIndex = <String, List<Map<String, dynamic>>>{};
    final parentMap = <Map<String, dynamic>, Map<String, dynamic>?>{};

    for (final row in rowCache.values) {
      final parentId = row[parentIdColumn]?.toString();
      final isRoot = parentId == null || parentId.isEmpty || parentId == 'null';

      if (isRoot) {
        rootNodes.add(row);
        parentMap[row] = null;
      } else {
        // Add to children index
        childrenIndex.putIfAbsent(parentId, () => []).add(row);
        // Build parent map
        if (rowCache.containsKey(parentId)) {
          parentMap[row] = rowCache[parentId];
        } else {
          parentMap[row] = null;
        }
      }
    }

    // Sort roots once
    rootNodes.sort(
      (a, b) => compareSortKeys(a[sortKeyColumn], b[sortKeyColumn]),
    );

    // Sort each children list once
    for (final children in childrenIndex.values) {
      children.sort(
        (a, b) => compareSortKeys(a[sortKeyColumn], b[sortKeyColumn]),
      );
    }

    // O(1) lookup functions using pre-built indices
    List<Map<String, dynamic>> getRootNodes() => rootNodes;

    List<Map<String, dynamic>> getChildren(Map<String, dynamic> node) {
      final id = getId(node);
      return childrenIndex[id] ?? const [];
    }

    // Create a unique key for this tree view instance based on entity name and parent column
    final treeKey = '${entityName}_${parentIdColumn}_$sortKeyColumn';

    // Return a ConsumerWidget wrapper to maintain TreeController across rebuilds
    return TreeViewWidget(
      treeKey: treeKey,
      rowCache: rowCache,
      parentIdColumn: parentIdColumn,
      sortKeyColumn: sortKeyColumn,
      entityName: entityName,
      onOperation: onOperation,
      rowTemplates: rowTemplates,
      interpreter: interpreter,
      context: context,
      getId: getId,
      getRootNodes: getRootNodes,
      getChildren: getChildren,
      parentMap: parentMap,
    );
  }

  /// Build OutlinerListView from outline() function.
  /// Uses OutlinerListView from outliner-flutter for hierarchical block editing.
  Widget _buildOutline(Map<String, RenderExpr> args, RenderContext context) {
    // Extract required parameters
    final parentIdExpr = args['parent_id'];
    final sortKeyExpr = args['sortkey'] ?? args['sort_key'];
    final itemTemplateExpr = args['item_template'] ?? args['item'];

    if (parentIdExpr == null) {
      throw ArgumentError('outline() requires "parent_id" argument');
    }
    if (sortKeyExpr == null) {
      throw ArgumentError(
        'outline() requires "sortkey" or "sort_key" argument',
      );
    }
    if (itemTemplateExpr == null) {
      throw ArgumentError(
        'outline() requires "item_template" or "item" argument',
      );
    }

    // Evaluate column names (should be string literals or column references)
    final parentIdColumn = _evaluateToString(parentIdExpr, context);
    final sortKeyColumn = _evaluateToString(sortKeyExpr, context);

    // Check if context has required data
    if (context.rowCache == null) {
      throw ArgumentError(
        'outline() requires rowCache in RenderContext. '
        'This should be provided by ReactiveQueryWidget.',
      );
    }

    if (context.entityName == null) {
      throw ArgumentError('outline() requires entityName in RenderContext.');
    }

    // Create RowDataBlockOps instance
    final blockOps = RowDataBlockOps(
      rowCache: context.rowCache!,
      parentIdColumn: parentIdColumn,
      sortKeyColumn: sortKeyColumn,
      entityName: context.entityName!,
      onOperation: context.onOperation,
    );

    // Create Riverpod provider for BlockOps
    final opsProvider = Provider<BlockOps<Map<String, dynamic>>>((ref) {
      return blockOps;
    });

    // Store item template and context for block builder
    final interpreter = this;
    final itemTemplate = itemTemplateExpr;
    final entityName = context.entityName!;
    final onOperation = context.onOperation;

    // Return OutlinerListView with custom block builder
    return OutlinerListView<Map<String, dynamic>>(
      opsProvider: opsProvider,
      config: const OutlinerConfig(
        keyboardShortcutsEnabled: true,
        padding: EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      ),
      blockBuilder: (buildContext, block) {
        // Create RenderContext for this block
        final blockContext = RenderContext(
          rowData: block,
          rowTemplates: context.rowTemplates,
          onOperation: onOperation,
          availableOperations: context.availableOperations,
          entityName: entityName,
          colors: context.colors,
        );
        // Build widget from item template
        return interpreter.build(itemTemplate, blockContext);
      },
    );
  }

  /// Build Column with indentation from block() function.
  /// LogSeq-style: uses 29px indent with left border guidelines.
  Widget _buildBlock(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    final children = <Widget>[];

    // Add all positional arguments as children
    for (final arg in positionalArgs) {
      children.add(build(arg, context));
    }

    // Get depth for indentation (optional)
    final depthExpr = namedArgs['depth'];
    final depth = depthExpr != null ? _evaluateToInt(depthExpr, context) : 0;

    // LogSeq uses 29px indent per level
    final indentPixels = depth * 29.0;

    // Build block container with LogSeq-style styling
    return Container(
      margin: EdgeInsets.only(left: indentPixels),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: children,
      ),
    );
  }

  /// Build Row from row() function.
  /// LogSeq-style: adds hover state and better spacing.
  Widget _buildRow(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    final children = positionalArgs.map((arg) => build(arg, context)).toList();

    // Wrap TextField widgets in Flexible to provide bounded width constraints
    // This prevents "unbounded width" errors when TextField is inside a Row
    final wrappedChildren = children.map((child) {
      // Check if the widget is a TextField by checking its runtime type
      // TextField widgets need bounded width constraints in Row
      if (child is TextField) {
        return Flexible(child: child);
      }
      return child;
    }).toList();

    // LogSeq-style block container with hover state
    return MouseRegion(
      cursor: SystemMouseCursors.text,
      child: Container(
        padding: const EdgeInsets.symmetric(vertical: 2, horizontal: 4),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(4),
          color: Colors.transparent,
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.center,
          children: wrappedChildren,
        ),
      ),
    );
  }

  /// Build TextField from editable_text() function.
  /// LogSeq-style: minimal borderless text field with better typography.
  Widget _buildEditableText(
    Map<String, RenderExpr> args,
    RenderContext context,
  ) {
    final contentExpr = args['content'];
    final content = contentExpr != null
        ? _evaluateToString(contentExpr, context)
        : '';

    final rowId = context.rowData['id'];

    // Find operations that affect the "content" field
    // First try to find operations that specifically affect "content"
    var contentOps = context.operationsAffecting(['content']);

    // If no operations found, look for generic "set_field" operation
    if (contentOps.isEmpty) {
      // Prefer set_field for the current entity, fall back to any set_field
      contentOps = context.availableOperations
          .where(
            (op) =>
                op.name == 'set_field' &&
                (context.entityName == null ||
                    op.entityName == context.entityName),
          )
          .toList();

      if (contentOps.isEmpty) {
        contentOps = context.availableOperations
            .where((op) => op.name == 'set_field')
            .toList();
      }
    }

    // Find set_field operation (or first operation if multiple)
    final updateOp = contentOps.isNotEmpty ? contentOps.first : null;

    // Function to execute the update operation
    void executeUpdate(String newValue) {
      if (updateOp == null || context.onOperation == null) {
        return;
      }

      // Execute set_field operation with new content
      final params = <String, dynamic>{
        'id': context.rowData[updateOp.idColumn] ?? context.rowData['id'],
        'field': 'content', // Always use 'content' for text field edits
        'value': newValue,
      };

      // Use entity_name from row data (for UNION queries), then operation descriptor, then context
      final entityName =
          context.rowData['entity_name']?.toString() ??
          (updateOp.entityName.isNotEmpty ? updateOp.entityName : null) ??
          context.entityName;

      if (entityName == null) {
        throw StateError(
          'Cannot dispatch operation "${updateOp.name}": no entity_name found in row data, '
          'operation descriptor, or context. This is a bug.',
        );
      }

      context.onOperation?.call(entityName, updateOp.name, params);
    }

    // Note: TextField needs bounded width constraints. When used in Row,
    // wrap it in Flexible/Expanded in the PRQL query itself, e.g.:
    // row(collapse_button(), flexible(editable_text(content)))
    //
    // Text editing behavior:
    // - Enter (without Shift): Save and unfocus
    // - Shift+Enter: Create newline
    return EditableTextField(
      text: content,
      onSave: updateOp != null && context.onOperation != null
          ? executeUpdate
          : null,
    );
  }

  /// Build Text widget from text() function.
  /// LogSeq-style: improved typography.
  Widget _buildText(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    String text;
    if (positionalArgs.isNotEmpty) {
      text = _evaluateToString(positionalArgs[0], context);
    } else if (namedArgs['value'] != null) {
      text = _evaluateToString(namedArgs['value']!, context);
    } else {
      text = '';
    }

    return Text(
      text,
      style: TextStyle(
        fontSize: 16,
        height: 1.5,
        color: context.colors.textSecondary,
        letterSpacing: 0,
      ),
    );
  }

  /// Build Spacer widget from spacer() function.
  /// Creates horizontal or vertical space between widgets.
  ///
  /// Usage: `spacer()` or `spacer(width:8)` or `spacer(height:8)` or `spacer(width:8, height:4)`
  Widget _buildSpacer(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    // Optional width parameter (default 8)
    final widthExpr = namedArgs['width'];
    final width = widthExpr != null ? _evaluateToInt(widthExpr, context) : 8;

    // Optional height parameter (default 0, meaning no vertical spacing)
    final heightExpr = namedArgs['height'];
    final height = heightExpr != null ? _evaluateToInt(heightExpr, context) : 0;

    return SizedBox(
      width: width.toDouble(),
      height: height > 0 ? height.toDouble() : null,
    );
  }

  /// Build Draggable widget from draggable() function.
  /// Wraps a child widget to make it draggable.
  ///
  /// Usage: `draggable(child)` or `draggable(child on:'longpress')`
  /// - `on:'longpress'` (default) - drag starts on long press
  /// - `on:'drag'` - drag starts immediately on drag gesture
  Widget _buildDraggable(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      return const SizedBox.shrink();
    }

    // Build the child widget
    final child = build(positionalArgs[0], context);

    // Get trigger type (default: longpress)
    final onExpr = namedArgs['on'];
    final trigger = onExpr != null
        ? _evaluateToString(onExpr, context)
        : 'longpress';

    // Create RenderableItem with row data and operations
    final uiIndex = context.rowData['ui'] as int?;
    final template = uiIndex != null
        ? context.rowTemplates.firstWhere(
            (t) => t.index.toInt() == uiIndex,
            orElse: () => context.rowTemplates.first,
          )
        : context.rowTemplates.first;

    final item = RenderableItem(
      rowData: context.rowData,
      template: template,
      operations: context.availableOperations,
    );

    // Create simple feedback widget with item content
    final contentText =
        context.rowData['content']?.toString() ??
        context.rowData['name']?.toString() ??
        'Item';
    final feedback = Material(
      elevation: 4,
      borderRadius: BorderRadius.circular(8),
      color: Colors.white,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        child: Text(
          contentText,
          style: const TextStyle(fontSize: 14, color: Colors.black87),
        ),
      ),
    );

    // Wrap with Consumer to access RiverPod providers for search overlay
    return Consumer(
      builder: (buildContext, ref, _) {
        void onDragStarted() {
          debugPrint('[Draggable] Drag started: $contentText');
          debugPrint(
            '[Draggable] rowCache is null: ${context.rowCache == null}',
          );
          debugPrint(
            '[Draggable] rowCache size: ${context.rowCache?.length ?? 0}',
          );

          // Get widget position for overlay placement
          final RenderBox? box = buildContext.findRenderObject() as RenderBox?;
          debugPrint('[Draggable] RenderBox: $box');

          if (box == null) {
            debugPrint('[Draggable] Cannot show overlay: RenderBox is null');
            return;
          }

          if (context.rowCache == null) {
            debugPrint('[Draggable] Cannot show overlay: rowCache is null');
            return;
          }

          final position = box.localToGlobal(Offset.zero);
          final overlayPosition = Offset(
            position.dx + box.size.width + 16,
            position.dy,
          );
          debugPrint('[Draggable] Showing overlay at: $overlayPosition');

          ref
              .read(searchSelectOverlayProvider.notifier)
              .showForDrag(
                position: overlayPosition,
                draggedItem: item,
                rowCache: context.rowCache!,
                rowTemplates: context.rowTemplates,
                onOperation: context.onOperation,
              );
        }

        void onDragEnd(DraggableDetails details) {
          debugPrint(
            '[Draggable] Drag ended: wasAccepted=${details.wasAccepted}',
          );

          // Only hide if still in dragActive mode (not if user entered search mode)
          final currentMode = ref.read(searchSelectOverlayProvider).mode;
          if (currentMode == SearchSelectMode.dragActive) {
            ref.read(searchSelectOverlayProvider.notifier).hide();
          }
        }

        void onDraggableCanceled(Velocity velocity, Offset offset) {
          debugPrint('[Draggable] Drag canceled');

          // Only hide if still in dragActive mode
          final currentMode = ref.read(searchSelectOverlayProvider).mode;
          if (currentMode == SearchSelectMode.dragActive) {
            ref.read(searchSelectOverlayProvider.notifier).hide();
          }
        }

        if (trigger == 'drag') {
          return Draggable<RenderableItem>(
            data: item,
            feedback: feedback,
            childWhenDragging: Opacity(opacity: 0.3, child: child),
            dragAnchorStrategy: pointerDragAnchorStrategy,
            onDragStarted: onDragStarted,
            onDragEnd: onDragEnd,
            onDraggableCanceled: onDraggableCanceled,
            child: child,
          );
        } else {
          // Default: longpress
          return LongPressDraggable<RenderableItem>(
            data: item,
            feedback: feedback,
            childWhenDragging: Opacity(opacity: 0.3, child: child),
            dragAnchorStrategy: pointerDragAnchorStrategy,
            hapticFeedbackOnStart: true,
            onDragStarted: onDragStarted,
            onDragEnd: onDragEnd,
            onDraggableCanceled: onDraggableCanceled,
            child: child,
          );
        }
      },
    );
  }

  /// Build Icon/Image widget from icon() function.
  /// Displays an image asset from assets/images/{name}.ico
  ///
  /// Usage: `icon('todoist')` or `icon(name:'todoist', size:16)`
  Widget _buildIcon(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    String iconName;
    if (positionalArgs.isNotEmpty) {
      iconName = _evaluateToString(positionalArgs[0], context);
    } else if (namedArgs['name'] != null) {
      iconName = _evaluateToString(namedArgs['name']!, context);
    } else {
      throw ArgumentError('icon() requires "name" argument or positional name');
    }

    // Optional size parameter (default 16)
    final sizeExpr = namedArgs['size'];
    final size = sizeExpr != null ? _evaluateToInt(sizeExpr, context) : 16;

    // Construct asset path: assets/images/{iconName}.ico
    final assetPath = 'assets/images/$iconName.ico';

    // Wrap in Center to ensure vertical centering within the row
    return Center(
      child: Image.asset(
        assetPath,
        width: size.toDouble(),
        height: size.toDouble(),
        errorBuilder: (buildContext, error, stackTrace) {
          // Fallback to a placeholder icon if image not found
          return Icon(
            Icons.image_not_supported,
            size: size.toDouble(),
            color: context.colors.textTertiary,
          );
        },
      ),
    );
  }

  /// Build Bullet widget from bullet() function.
  /// LogSeq-style: structural bullet point that's always present for blocks.
  ///
  /// Usage: `bullet()` or `bullet(sizeInPx: 6)` - displays a bullet point.
  Widget _buildBullet(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    // Optional size parameter (default 6px for the inner circle)
    final sizeExpr = namedArgs['sizeInPx'];
    final size = sizeExpr != null ? _evaluateToInt(sizeExpr, context) : 6;

    // Container size is always 20x20 to maintain consistent spacing
    return Container(
      width: 20,
      height: 20,
      margin: const EdgeInsets.only(right: 8, top: 2),
      child: Center(
        child: Container(
          width: size.toDouble(),
          height: size.toDouble(),
          decoration: BoxDecoration(
            shape: BoxShape.circle,
            color: context.colors.textTertiary.withValues(alpha: 0.8),
          ),
        ),
      ),
    );
  }

  /// Build PieMenu wrapper from pieMenu() function.
  /// Wraps a child widget and attaches a pie menu that opens on tap.
  ///
  /// Usage:
  /// - `pieMenu(bullet())` - wraps bullet with pie menu using all available operations
  /// - `pieMenu(bullet(), fields: ['content', 'parent_id'])` - only operations affecting specified fields
  /// - `pieMenu(bullet(), fields: ['this.*'])` or `pieMenu(bullet(), fields: this)` - all operations (semantic marker)
  /// - `pieMenu(row(...), operations: [...])` - wraps any widget with specific operations
  Widget _buildPieMenu(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      throw ArgumentError('pieMenu() requires a child widget argument');
    }

    // Build the child widget
    final child = build(positionalArgs[0], context);

    // Determine which operations to show
    List<OperationDescriptor> operations;

    // Option 1: Explicit operations list (not implemented yet, but reserved for future)
    // Option 2: Fields of interest - operations affecting these fields
    final fieldsExpr = namedArgs['fields'];
    if (fieldsExpr != null) {
      // Check if fieldsExpr is a column reference to 'this', 'this.*', or '*' (semantic marker for all operations)
      if (fieldsExpr is RenderExpr_ColumnRef) {
        final columnName = fieldsExpr.name;
        // `fields: this`, `fields: this.*`, or `fields: *` means all operations
        // Note: PRQL may parse `this.*` as column reference with name `*`
        if (columnName == 'this' ||
            columnName == 'this.*' ||
            columnName == '*') {
          operations = context.availableOperations;
        } else {
          // Single column reference - treat as single field
          operations = context.operationsAffecting([columnName]);
        }
      } else {
        // Evaluate fields argument (could be array or comma-separated string)
        final fieldsString = _evaluateToString(fieldsExpr, context);
        final fields = fieldsString.split(',').map((e) => e.trim()).toList();
        // Check if fields contains 'this.*' or 'this' (semantic marker for all operations)
        if (fields.contains('this.*') || fields.contains('this')) {
          operations = context.availableOperations;
        } else {
          operations = context.operationsAffecting(fields);
        }
      }
    } else {
      // Default: use all available operations
      operations = context.availableOperations;
    }

    // Debug: log operations count
    debugPrint(
      '[DEBUG] _buildPieMenu: found ${operations.length} operations for fields: ${fieldsExpr?.toString() ?? 'none'}',
    );

    // Wrap child with pie menu
    return _attachTapPieMenu(child, operations, context);
  }

  /// Build Checkbox widget from checkbox() function.
  /// LogSeq-style: todo status indicator (empty circle for unchecked, checkmark for checked).
  /// This is separate from the structural bullet point.
  Widget _buildCheckbox(Map<String, RenderExpr> args, RenderContext context) {
    final checkedExpr = args['checked'];
    final checked = checkedExpr != null
        ? _evaluateToBool(checkedExpr, context)
        : false;

    // Extract field name from the checked expression (e.g., columnRef -> 'completed')
    final fieldName = checkedExpr is RenderExpr_ColumnRef
        ? checkedExpr.name
        : null;

    // LogSeq-style: empty circle outline for unchecked, filled checkmark for checked
    return GestureDetector(
      onTap: () {
        final id = context.rowData['id'];
        if (id == null || context.onOperation == null || fieldName == null)
          return;

        // Use entity_name from row data (for UNION queries), then context
        final entityName =
            context.rowData['entity_name']?.toString() ?? context.entityName;
        if (entityName == null) {
          throw StateError(
            'Cannot dispatch checkbox operation: no entity_name found in row data or context. This is a bug.',
          );
        }

        context.onOperation!(entityName, 'set_field', {
          'id': id.toString(),
          'field': fieldName,
          'value': !checked,
        });
      },
      child: Container(
        width: 20,
        height: 20,
        margin: const EdgeInsets.only(right: 8, top: 2),
        child: checked
            ? const Icon(
                Icons.check_circle,
                size: 18,
                color: Color(0xFF10B981), // LogSeq green for completed
              )
            : Container(
                width: 16,
                height: 16,
                margin: const EdgeInsets.all(2),
                decoration: BoxDecoration(
                  shape: BoxShape.circle,
                  border: Border.all(
                    color: context.colors.textTertiary.withValues(alpha: 0.6),
                    width: 1.5,
                  ),
                ),
              ),
      ),
    );
  }

  /// Build Badge/Chip widget from badge() function.
  /// LogSeq-style: more subtle badges.
  Widget _buildBadge(Map<String, RenderExpr> args, RenderContext context) {
    final contentExpr = args['content'];
    final content = contentExpr != null
        ? _evaluateToString(contentExpr, context)
        : '';

    final colorExpr = args['color'];
    Color? badgeColor;
    if (colorExpr != null) {
      final colorStr = _evaluateToString(colorExpr, context).toLowerCase();
      // Map common color names to Flutter Colors (LogSeq-style subtle colors)
      badgeColor = switch (colorStr) {
        'cyan' => const Color(0xFF06B6D4),
        'blue' => const Color(0xFF3B82F6),
        'green' => const Color(0xFF10B981),
        'red' => const Color(0xFFEF4444),
        'orange' => const Color(0xFFF59E0B),
        'purple' => const Color(0xFF8B5CF6),
        'yellow' => const Color(0xFFEAB308),
        'grey' || 'gray' => context.colors.textSecondary,
        _ => null,
      };
    }

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
      decoration: BoxDecoration(
        color: (badgeColor ?? context.colors.textSecondary).withValues(
          alpha: 0.1,
        ),
        borderRadius: BorderRadius.circular(4),
      ),
      child: Text(
        content,
        style: TextStyle(
          fontSize: 11,
          color: badgeColor ?? context.colors.textSecondary,
          fontWeight: FontWeight.w500,
          letterSpacing: 0.2,
        ),
      ),
    );
  }

  /// Build drag target (drop zone) from drop_zone() function.
  Widget _buildDropZone(Map<String, RenderExpr> args, RenderContext context) {
    // TODO Phase 4.2: Implement full drag-drop with DragTarget
    // TODO: Parse invalid_targets from args
    // TODO: Wire up on_drop callback to operation execution
    // Position parameter will be used when drag-drop is implemented
    // final positionExpr = args['position'];
    // final position = positionExpr != null
    //     ? _evaluateToString(positionExpr, context)
    //     : 'before';

    return Container(
      height: 4,
      color: Colors.transparent,
      child: Center(
        child: Container(height: 2, color: Colors.blue.withValues(alpha: 0.0)),
      ),
    );
  }

  /// Build collapse/expand button from collapse_button() function.
  /// LogSeq-style: subtle bullet point that expands/collapses.
  Widget _buildCollapseButton(
    Map<String, RenderExpr> args,
    RenderContext context,
  ) {
    final isCollapsedExpr = args['is_collapsed'];
    final isCollapsed = isCollapsedExpr != null
        ? _evaluateToBool(isCollapsedExpr, context)
        : false;

    // LogSeq-style bullet point that acts as collapse button
    final button = GestureDetector(
      onTap: () {
        // Direct click still works for immediate toggle
        // Pie menu provides additional operations
      },
      child: Container(
        width: 20,
        height: 20,
        margin: const EdgeInsets.only(right: 8, top: 2),
        child: isCollapsed
            ? Icon(
                Icons.chevron_right,
                size: 16,
                color: context.colors.textTertiary,
              )
            : Container(
                width: 6,
                height: 6,
                margin: const EdgeInsets.all(7),
                decoration: BoxDecoration(
                  shape: BoxShape.circle,
                  color: context.colors.textTertiary.withValues(alpha: 0.8),
                ),
              ),
      ),
    );

    // Auto-attach pie menu for operations affecting collapsed state
    // Note: Field name might be "collapsed" or "is_collapsed" depending on schema
    return _autoAttachPieMenu(button, ['collapsed', 'is_collapsed'], context);
  }

  /// Build block operations menu from block_operations() function.
  Widget _buildBlockOperations(
    Map<String, RenderExpr> args,
    RenderContext context,
  ) {
    // Build a menu button that shows operations for common block fields
    final menuButton = IconButton(
      icon: const Icon(Icons.more_horiz),
      iconSize: 20,
      padding: EdgeInsets.zero,
      constraints: const BoxConstraints(),
      onPressed: () {
        // Direct click can show a dropdown menu if needed
        // Pie menu provides radial menu access
      },
    );

    // Auto-attach pie menu for operations affecting common block fields
    // This includes structural operations (indent, outdent, move) and content operations
    return _autoAttachPieMenu(menuButton, [
      'parent_id',
      'sort_key',
      'depth',
      'content',
    ], context);
  }

  /// Build placeholder for unknown functions.
  Widget _buildUnknownFunction(String name, List<Arg> args) {
    return Container(
      padding: const EdgeInsets.all(8),
      color: Colors.red.withValues(alpha: 0.1),
      child: Text(
        'Unknown function: $name',
        style: const TextStyle(color: Colors.red),
      ),
    );
  }

  /// Build widget from column reference (e.g., `block_id`, `content`).
  Widget _buildColumnRef(String name, RenderContext context) {
    debugPrint(
      '[DEBUG] _buildColumnRef: name=$name, available columns: ${context.rowData.keys.toList()}',
    );
    final value = context.getColumn(name);
    debugPrint(
      '[DEBUG] _buildColumnRef: value=$value (type: ${value.runtimeType})',
    );
    return Text(value?.toString() ?? '');
  }

  /// Build widget from literal value.
  Widget _buildLiteral(Value value) {
    return value.when(
      null_: () => const Text('null'),
      boolean: (b) => Text(b.toString()),
      integer: (i) => Text(i.toString()),
      float: (f) => Text(f.toString()),
      string: (s) => Text(s),
      dateTime: (s) => Text(s),
      json: (s) => Text(s),
      reference: (r) => Text(r),
      array: (items) => Text('[${items.length} items]'),
      object: (fields) => Text('{${fields.length} fields}'),
    );
  }

  /// Build widget from binary operation (e.g., `depth * 24`, `completed and visible`).
  Widget _buildBinaryOp(
    BinaryOperator op,
    RenderExpr left,
    RenderExpr right,
    RenderContext context,
  ) {
    // Evaluate binary operation to a value, then display
    final result = _evaluateBinaryOp(op, left, right, context);
    return Text(result.toString());
  }

  /// Build widget from array literal.
  Widget _buildArray(List<RenderExpr> items, RenderContext context) {
    final children = items.map((item) => build(item, context)).toList();
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      mainAxisSize: MainAxisSize.min,
      children: children,
    );
  }

  /// Build widget from object literal.
  Widget _buildObject(Map<String, RenderExpr> fields, RenderContext context) {
    // Objects are typically not rendered directly, but used as arguments
    return Text('{${fields.keys.join(', ')}}');
  }

  // --- Expression Evaluation Helpers ---

  /// Evaluate expression to integer value.
  int _evaluateToInt(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => value.when(
        integer: (i) => i.toInt(),
        float: (f) => f.toInt(),
        null_: () => 0,
        boolean: (_) => throw ArgumentError('Cannot convert bool to int'),
        string: (_) => throw ArgumentError('Cannot convert string to int'),
        dateTime: (_) => throw ArgumentError('Cannot convert dateTime to int'),
        json: (_) => throw ArgumentError('Cannot convert json to int'),
        reference: (_) =>
            throw ArgumentError('Cannot convert reference to int'),
        array: (_) => throw ArgumentError('Cannot convert array to int'),
        object: (_) => throw ArgumentError('Cannot convert object to int'),
      ),
      columnRef: (name) {
        final value = context.getColumn(name);
        if (value is int) return value;
        if (value is double) return value.toInt();
        throw ArgumentError('Column $name is not numeric');
      },
      binaryOp: (op, left, right) {
        final result = _evaluateBinaryOp(op, left, right, context);
        if (result is int) return result;
        if (result is double) return result.toInt();
        throw ArgumentError('Binary operation did not produce numeric result');
      },
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call to int'),
      array: (_) => throw ArgumentError('Cannot evaluate array to int'),
      object: (_) => throw ArgumentError('Cannot evaluate object to int'),
    );
  }

  /// Evaluate expression to string value.
  String _evaluateToString(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => value.when(
        string: (s) => s,
        integer: (i) => i.toString(),
        float: (f) => f.toString(),
        boolean: (b) => b.toString(),
        dateTime: (s) => s,
        json: (s) => s,
        reference: (r) => r,
        null_: () => '',
        array: (_) => throw ArgumentError('Cannot convert array to string'),
        object: (_) => throw ArgumentError('Cannot convert object to string'),
      ),
      columnRef: (name) {
        debugPrint('[DEBUG] _evaluateToString columnRef: name=$name');
        final value = context.getColumn(name);
        debugPrint('[DEBUG] _evaluateToString columnRef: value=$value');
        return value?.toString() ?? '';
      },
      binaryOp: (op, left, right) =>
          _evaluateBinaryOp(op, left, right, context).toString(),
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call to string'),
      array: (_) => throw ArgumentError('Cannot evaluate array to string'),
      object: (_) => throw ArgumentError('Cannot evaluate object to string'),
    );
  }

  /// Evaluate expression to boolean value.
  bool _evaluateToBool(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => value.when(
        boolean: (b) => b,
        null_: () => false,
        integer: (i) => i != 0,
        float: (f) => f != 0.0,
        string: (s) => s.isNotEmpty,
        dateTime: (s) => s.isNotEmpty,
        json: (s) => s.isNotEmpty,
        reference: (r) => r.isNotEmpty,
        array: (items) => items.isNotEmpty,
        object: (fields) => fields.isNotEmpty,
      ),
      columnRef: (name) {
        debugPrint('[DEBUG] _evaluateToBool columnRef: name=$name');
        final value = context.getColumn(name);
        debugPrint(
          '[DEBUG] _evaluateToBool columnRef: value=$value (type: ${value.runtimeType})',
        );
        if (value is bool) return value;
        if (value == null) return false;
        // Handle integer 0/1 as boolean
        if (value is int) return value != 0;
        throw ArgumentError(
          'Column $name is not boolean (got ${value.runtimeType})',
        );
      },
      binaryOp: (op, left, right) {
        final result = _evaluateBinaryOp(op, left, right, context);
        if (result is bool) return result;
        throw ArgumentError('Binary operation did not produce boolean result');
      },
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call to bool'),
      array: (_) => throw ArgumentError('Cannot evaluate array to bool'),
      object: (_) => throw ArgumentError('Cannot evaluate object to bool'),
    );
  }

  /// Evaluate binary operation to a value.
  dynamic _evaluateBinaryOp(
    BinaryOperator op,
    RenderExpr left,
    RenderExpr right,
    RenderContext context,
  ) {
    switch (op) {
      // Comparison operators
      case BinaryOperator.eq:
        return _evaluateGeneric(left, context) ==
            _evaluateGeneric(right, context);
      case BinaryOperator.neq:
        return _evaluateGeneric(left, context) !=
            _evaluateGeneric(right, context);
      case BinaryOperator.gt:
        return _compareNumeric(left, right, context, (a, b) => a > b);
      case BinaryOperator.lt:
        return _compareNumeric(left, right, context, (a, b) => a < b);
      case BinaryOperator.gte:
        return _compareNumeric(left, right, context, (a, b) => a >= b);
      case BinaryOperator.lte:
        return _compareNumeric(left, right, context, (a, b) => a <= b);

      // Arithmetic operators
      case BinaryOperator.add:
        return _evaluateToNum(left, context) + _evaluateToNum(right, context);
      case BinaryOperator.sub:
        return _evaluateToNum(left, context) - _evaluateToNum(right, context);
      case BinaryOperator.mul:
        return _evaluateToNum(left, context) * _evaluateToNum(right, context);
      case BinaryOperator.div:
        return _evaluateToNum(left, context) / _evaluateToNum(right, context);

      // Logical operators
      case BinaryOperator.and:
        return _evaluateToBool(left, context) &&
            _evaluateToBool(right, context);
      case BinaryOperator.or:
        return _evaluateToBool(left, context) ||
            _evaluateToBool(right, context);
    }
  }

  /// Evaluate expression to num (int or double).
  num _evaluateToNum(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => value.when(
        integer: (i) => i.toInt(),
        float: (f) => f,
        null_: () => 0,
        boolean: (_) => throw ArgumentError('Cannot convert bool to num'),
        string: (_) => throw ArgumentError('Cannot convert string to num'),
        dateTime: (_) => throw ArgumentError('Cannot convert dateTime to num'),
        json: (_) => throw ArgumentError('Cannot convert json to num'),
        reference: (_) =>
            throw ArgumentError('Cannot convert reference to num'),
        array: (_) => throw ArgumentError('Cannot convert array to num'),
        object: (_) => throw ArgumentError('Cannot convert object to num'),
      ),
      columnRef: (name) {
        final value = context.getColumn(name);
        if (value is num) return value;
        throw ArgumentError('Column $name is not numeric');
      },
      binaryOp: (op, left, right) {
        final result = _evaluateBinaryOp(op, left, right, context);
        if (result is num) return result;
        throw ArgumentError('Binary operation did not produce numeric result');
      },
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call to num'),
      array: (_) => throw ArgumentError('Cannot evaluate array to num'),
      object: (_) => throw ArgumentError('Cannot evaluate object to num'),
    );
  }

  /// Evaluate expression to generic dynamic value.
  dynamic _evaluateGeneric(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => _valueToNative(value),
      columnRef: (name) => context.getColumn(name),
      binaryOp: (op, left, right) =>
          _evaluateBinaryOp(op, left, right, context),
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call generically'),
      array: (items) =>
          items.map((item) => _evaluateGeneric(item, context)).toList(),
      object: (fields) => fields.map(
        (key, value) => MapEntry(key, _evaluateGeneric(value, context)),
      ),
    );
  }

  /// Convert Value to native Dart type.
  dynamic _valueToNative(Value value) {
    return value.when(
      null_: () => null,
      boolean: (b) => b,
      integer: (i) => i.toInt(),
      float: (f) => f,
      string: (s) => s,
      dateTime: (s) => s,
      json: (s) => s,
      reference: (r) => r,
      array: (items) => items.map(_valueToNative).toList(),
      object: (fields) =>
          fields.map((key, value) => MapEntry(key, _valueToNative(value))),
    );
  }

  /// Compare two numeric expressions.
  bool _compareNumeric(
    RenderExpr left,
    RenderExpr right,
    RenderContext context,
    bool Function(num, num) compare,
  ) {
    final leftVal = _evaluateToNum(left, context);
    final rightVal = _evaluateToNum(right, context);
    return compare(leftVal, rightVal);
  }

  // ==========================================================================
  // Phase 2: Dashboard Layout Primitives
  // ==========================================================================

  /// Build Section card from section() function.
  /// A card container with title header, used for dashboard sections.
  ///
  /// Usage: `section(title: "Today's Focus", child)` or `section(title: "Inbox", collapsible: true, child)`
  Widget _buildSection(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    // Get title
    final titleExpr = namedArgs['title'];
    final title = titleExpr != null
        ? _evaluateToString(titleExpr, context)
        : '';

    // Get collapsible flag
    final collapsibleExpr = namedArgs['collapsible'];
    final collapsible = collapsibleExpr != null
        ? _evaluateToBool(collapsibleExpr, context)
        : false;

    // Build child widget
    Widget? child;
    if (positionalArgs.isNotEmpty) {
      child = build(positionalArgs[0], context);
    } else if (namedArgs['child'] != null) {
      child = build(namedArgs['child']!, context);
    }

    return Container(
      margin: const EdgeInsets.only(bottom: 16),
      decoration: BoxDecoration(
        color: context.colors.backgroundSecondary,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: context.colors.border, width: 1),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: [
          // Header
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 12, 16, 8),
            child: Row(
              children: [
                if (collapsible)
                  Icon(
                    Icons.chevron_right,
                    size: 16,
                    color: context.colors.textTertiary,
                  ),
                Text(
                  title.toUpperCase(),
                  style: TextStyle(
                    fontSize: 11,
                    fontWeight: FontWeight.w600,
                    letterSpacing: 0.5,
                    color: context.colors.textTertiary,
                  ),
                ),
              ],
            ),
          ),
          // Divider
          Container(height: 1, color: context.colors.border),
          // Content
          if (child != null)
            Padding(padding: const EdgeInsets.all(12), child: child),
        ],
      ),
    );
  }

  /// Build Grid layout from grid() function.
  /// Multi-column layout similar to CSS Grid.
  ///
  /// Usage: `grid(columns: 2, gap: 16, child1, child2, ...)`
  Widget _buildGrid(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    // Get column count (default 2)
    final columnsExpr = namedArgs['columns'];
    final columns = columnsExpr != null
        ? _evaluateToInt(columnsExpr, context)
        : 2;

    // Get gap (default 16)
    final gapExpr = namedArgs['gap'];
    final gap = gapExpr != null
        ? _evaluateToInt(gapExpr, context).toDouble()
        : 16.0;

    // Build children
    final children = positionalArgs.map((arg) => build(arg, context)).toList();

    // Create rows based on column count
    final rows = <Widget>[];
    for (var i = 0; i < children.length; i += columns) {
      final rowChildren = <Widget>[];
      for (var j = 0; j < columns && i + j < children.length; j++) {
        if (j > 0) {
          rowChildren.add(SizedBox(width: gap));
        }
        rowChildren.add(Expanded(child: children[i + j]));
      }
      // Fill remaining columns with empty expanded widgets
      for (var j = children.length - i; j < columns; j++) {
        if (rowChildren.isNotEmpty) {
          rowChildren.add(SizedBox(width: gap));
        }
        rowChildren.add(const Expanded(child: SizedBox()));
      }
      if (rows.isNotEmpty) {
        rows.add(SizedBox(height: gap));
      }
      rows.add(
        Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: rowChildren,
        ),
      );
    }

    return Column(mainAxisSize: MainAxisSize.min, children: rows);
  }

  /// Build Stack from stack() function.
  /// Overlapping children for overlays.
  ///
  /// Usage: `stack(background, overlay)`
  Widget _buildStack(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    final children = positionalArgs.map((arg) => build(arg, context)).toList();

    return Stack(children: children);
  }

  /// Build ScrollView from scroll() function.
  /// Scrollable container.
  ///
  /// Usage: `scroll(child)` or `scroll(direction: 'horizontal', child)`
  Widget _buildScroll(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    // Get direction (default vertical)
    final directionExpr = namedArgs['direction'];
    final directionStr = directionExpr != null
        ? _evaluateToString(directionExpr, context)
        : 'vertical';
    final axis = directionStr == 'horizontal' ? Axis.horizontal : Axis.vertical;

    // Build child
    Widget? child;
    if (positionalArgs.isNotEmpty) {
      child = build(positionalArgs[0], context);
    } else if (namedArgs['child'] != null) {
      child = build(namedArgs['child']!, context);
    }

    return SingleChildScrollView(scrollDirection: axis, child: child);
  }

  // ==========================================================================
  // Phase 2: Dashboard Widgets
  // ==========================================================================

  /// Build date header from date_header() function.
  /// Displays formatted date (e.g., "Wednesday, December 4").
  ///
  /// Usage: `date_header()` or `date_header(format: 'EEEE, MMMM d')`
  Widget _buildDateHeader(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    // Get format (default: "EEEE, MMMM d")
    final formatExpr = namedArgs['format'];
    final format = formatExpr != null
        ? _evaluateToString(formatExpr, context)
        : 'EEEE, MMMM d';

    final now = DateTime.now();
    final formatter = DateFormat(format);
    final dateString = formatter.format(now);

    return Padding(
      padding: const EdgeInsets.only(bottom: 16),
      child: Text(
        dateString,
        style: TextStyle(
          fontSize: 18,
          fontWeight: FontWeight.w600,
          color: context.colors.textPrimary,
        ),
      ),
    );
  }

  /// Build progress indicator from progress() function.
  /// Shows progress as dots (●●●○) or bar.
  ///
  /// Usage: `progress(value: 3, max: 4)` or `progress(value: 0.75, style: 'bar')`
  Widget _buildProgress(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    // Get value
    final valueExpr = namedArgs['value'];
    final value = valueExpr != null ? _evaluateToNum(valueExpr, context) : 0;

    // Get max (default 4 for dots, 1.0 for bar)
    final maxExpr = namedArgs['max'];
    final styleExpr = namedArgs['style'];
    final style = styleExpr != null
        ? _evaluateToString(styleExpr, context)
        : 'dots';

    if (style == 'bar') {
      final max = maxExpr != null ? _evaluateToNum(maxExpr, context) : 1.0;
      final fraction = (value / max).clamp(0.0, 1.0);

      return SizedBox(
        width: 60,
        height: 4,
        child: ClipRRect(
          borderRadius: BorderRadius.circular(2),
          child: LinearProgressIndicator(
            value: fraction.toDouble(),
            backgroundColor: context.colors.border,
            valueColor: AlwaysStoppedAnimation<Color>(context.colors.primary),
          ),
        ),
      );
    }

    // Dots style
    final max = maxExpr != null ? _evaluateToInt(maxExpr, context) : 4;
    final filled = value.toInt().clamp(0, max);

    return Row(
      mainAxisSize: MainAxisSize.min,
      children: List.generate(max, (i) {
        final isFilled = i < filled;
        return Padding(
          padding: EdgeInsets.only(left: i > 0 ? 2 : 0),
          child: Container(
            width: 8,
            height: 8,
            decoration: BoxDecoration(
              shape: BoxShape.circle,
              color: isFilled ? context.colors.primary : context.colors.border,
            ),
          ),
        );
      }),
    );
  }

  /// Build count badge from count_badge() function.
  /// Displays a count with optional animation.
  ///
  /// Usage: `count_badge(count: 5)` or `count_badge(count: inbox_count, animate: true)`
  Widget _buildCountBadge(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    // Get count
    final countExpr = namedArgs['count'];
    final count = countExpr != null ? _evaluateToInt(countExpr, context) : 0;

    // Get animate flag (for future use with implicit animations)
    // final animateExpr = namedArgs['animate'];
    // final animate = animateExpr != null ? _evaluateToBool(animateExpr, context) : false;

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
      decoration: BoxDecoration(
        color: context.colors.primary.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(10),
      ),
      child: Text(
        count.toString(),
        style: TextStyle(
          fontSize: 12,
          fontWeight: FontWeight.w600,
          color: context.colors.primary,
        ),
      ),
    );
  }

  /// Build status indicator from status_indicator() function.
  /// Shows sync status with appropriate color.
  ///
  /// Usage: `status_indicator(status: 'synced')` or `status_indicator(status: 'pending')`
  Widget _buildStatusIndicator(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    // Get status
    final statusExpr = namedArgs['status'];
    final status = statusExpr != null
        ? _evaluateToString(statusExpr, context).toLowerCase()
        : 'synced';

    IconData icon;
    Color color;

    switch (status) {
      case 'synced':
        icon = Icons.check_circle;
        color = context.colors.success;
      case 'pending':
        icon = Icons.access_time;
        color = context.colors.warning;
      case 'attention':
        icon = Icons.warning_amber;
        color = const Color(0xFFE07A5F); // Soft coral
      case 'error':
        icon = Icons.error;
        color = context.colors.error;
      default:
        icon = Icons.circle;
        color = context.colors.textTertiary;
    }

    return Icon(icon, size: 16, color: color);
  }

  // ==========================================================================
  // Phase 2: Interactive Enhancements
  // ==========================================================================

  /// Build hover row from hover_row() function.
  /// Row with hover effects: text prominence, action icons, background tint.
  ///
  /// Usage: `hover_row(row(...))`
  Widget _buildHoverRow(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      return const SizedBox.shrink();
    }

    final child = build(positionalArgs[0], context);

    return _HoverRowWidget(colors: context.colors, child: child);
  }

  /// Build focusable wrapper from focusable() function.
  /// Block that can become the focus target for progressive concealment.
  ///
  /// Usage: `focusable(block_id: id, child)`
  Widget _buildFocusable(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      return const SizedBox.shrink();
    }

    final child = build(positionalArgs[0], context);

    // Get block ID for focus tracking
    final blockIdExpr = namedArgs['block_id'];
    final blockId = blockIdExpr != null
        ? _evaluateToString(blockIdExpr, context)
        : context.rowData['id']?.toString();

    return Consumer(
      builder: (buildContext, ref, _) {
        return GestureDetector(
          onTap: () {
            if (blockId != null) {
              ref
                  .read(focusedBlockIdProvider.notifier)
                  .setFocusedBlock(blockId);
            }
          },
          child: child,
        );
      },
    );
  }

  // ==========================================================================
  // Phase 2: Animation Support
  // ==========================================================================

  /// Build staggered animation container from staggered() function.
  /// Children fade in with delay between each.
  ///
  /// Usage: `staggered(delay: 50, child1, child2, ...)`
  Widget _buildStaggered(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    // Get delay in ms (default 50ms from AnimDurations.sectionStagger)
    final delayExpr = namedArgs['delay'];
    final delayMs = delayExpr != null
        ? _evaluateToInt(delayExpr, context)
        : AnimDurations.sectionStagger.inMilliseconds;

    final children = positionalArgs.map((arg) => build(arg, context)).toList();

    return _StaggeredColumn(delayMs: delayMs, children: children);
  }

  /// Build animated wrapper from animated() function.
  /// Generic animation wrapper for property transitions.
  ///
  /// Usage: `animated(duration: 300, child)`
  Widget _buildAnimated(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      return const SizedBox.shrink();
    }

    // Get duration in ms (default 200ms)
    final durationExpr = namedArgs['duration'];
    final durationMs = durationExpr != null
        ? _evaluateToInt(durationExpr, context)
        : 200;

    final child = build(positionalArgs[0], context);

    // For now, wrap in AnimatedSwitcher for content changes
    return AnimatedSwitcher(
      duration: Duration(milliseconds: durationMs),
      child: child,
    );
  }

  /// Build pulse animation from pulse() function.
  /// Single or continuous pulse effect.
  ///
  /// Usage: `pulse(child)` or `pulse(once: true, child)`
  Widget _buildPulse(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    if (positionalArgs.isEmpty) {
      return const SizedBox.shrink();
    }

    // Get once flag (default true - single pulse)
    final onceExpr = namedArgs['once'];
    final once = onceExpr != null ? _evaluateToBool(onceExpr, context) : true;

    final child = build(positionalArgs[0], context);

    return _PulseWidget(once: once, child: child);
  }

  // ==========================================================================
  // Phase 7: Source Block Support
  // ==========================================================================

  /// Build source block container from source_block() function.
  /// Displays a code block with language indicator, source code, and optional results.
  ///
  /// Usage: `source_block(language: 'prql', source: code_content)`
  /// or `source_block(content: block_content)` for BlockContent.Source
  Widget _buildSourceBlock(
    Map<String, RenderExpr> namedArgs,
    List<RenderExpr> positionalArgs,
    RenderContext context,
  ) {
    // Option 1: Direct language and source arguments
    final languageExpr = namedArgs['language'];
    final sourceExpr = namedArgs['source'];

    // Option 2: BlockContent from content column
    final contentExpr = namedArgs['content'];

    String language;
    String source;
    block_types.BlockResult? results;

    if (languageExpr != null && sourceExpr != null) {
      language = _evaluateToString(languageExpr, context);
      source = _evaluateToString(sourceExpr, context);
    } else if (contentExpr != null) {
      // Try to get BlockContent from row data
      final contentValue = context.rowData['content'];
      if (contentValue is block_types.BlockContent) {
        switch (contentValue) {
          case block_types.BlockContent_Source(:final field0):
            // FRB generates duplicate class names; use dynamic to access properties
            final sourceBlock = field0 as dynamic;
            language = sourceBlock.language as String;
            source = sourceBlock.source as String;
            results = sourceBlock.results as block_types.BlockResult?;
          case block_types.BlockContent_Text(:final raw):
            // Text content displayed as plain text block
            return Text(raw);
        }
      } else {
        // Fallback: treat as plain text
        language = 'text';
        source = contentValue?.toString() ?? '';
      }
    } else {
      // Try to get from row data directly (e.g., from column refs)
      language = context.rowData['language']?.toString() ?? 'text';
      source =
          context.rowData['source']?.toString() ??
          context.rowData['content']?.toString() ??
          '';
    }

    // Get optional name
    final nameExpr = namedArgs['name'];
    final name = nameExpr != null ? _evaluateToString(nameExpr, context) : null;

    // Get optional editable flag
    final editableExpr = namedArgs['editable'];
    final editable = editableExpr != null
        ? _evaluateToBool(editableExpr, context)
        : false;

    return SourceBlockWidget(
      language: language,
      source: source,
      name: name,
      results: results,
      editable: editable,
      onSourceChanged: editable && context.onOperation != null
          ? (newSource) {
              final id = context.rowData['id'];
              if (id != null && context.entityName != null) {
                context.onOperation!(context.entityName!, 'set_field', {
                  'id': id.toString(),
                  'field': 'source',
                  'value': newSource,
                });
              }
            }
          : null,
      onExecute: context.onOperation != null
          ? () async {
              final id = context.rowData['id'];
              if (id != null && context.entityName != null) {
                context.onOperation!(
                  context.entityName!,
                  'execute_source_block',
                  {'id': id.toString()},
                );
              }
            }
          : null,
      colors: context.colors,
    );
  }

  /// Build source editor widget from source_editor() function.
  /// A code editor for editing source blocks with syntax highlighting.
  ///
  /// Usage: `source_editor(language: 'prql', content: source_code)`
  Widget _buildSourceEditor(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    final languageExpr = namedArgs['language'];
    final contentExpr = namedArgs['content'];

    final language = languageExpr != null
        ? _evaluateToString(languageExpr, context)
        : 'text';
    final content = contentExpr != null
        ? _evaluateToString(contentExpr, context)
        : '';

    return SourceEditorWidget(
      language: language,
      initialContent: content,
      onChanged: context.onOperation != null
          ? (newContent) {
              final id = context.rowData['id'];
              if (id != null && context.entityName != null) {
                context.onOperation!(context.entityName!, 'set_field', {
                  'id': id.toString(),
                  'field': 'source',
                  'value': newContent,
                });
              }
            }
          : null,
      colors: context.colors,
    );
  }

  /// Build query result display from query_result() function.
  /// Displays execution results (table, text, or error).
  ///
  /// Usage: `query_result(result: result_data)` or `query_result()` to use context
  Widget _buildQueryResult(
    Map<String, RenderExpr> namedArgs,
    RenderContext context,
  ) {
    // Try to get result from named argument or row data
    final resultExpr = namedArgs['result'];
    block_types.BlockResult? result;

    if (resultExpr != null) {
      // Evaluate result expression
      final resultValue =
          context.rowData['result'] ?? context.rowData['results'];
      if (resultValue is block_types.BlockResult) {
        result = resultValue;
      }
    } else {
      // Check row data for result/results field
      final resultValue =
          context.rowData['result'] ?? context.rowData['results'];
      if (resultValue is block_types.BlockResult) {
        result = resultValue;
      }
    }

    if (result == null) {
      return const SizedBox.shrink();
    }

    return QueryResultWidget(result: result, colors: context.colors);
  }
}

// =============================================================================
// Helper Widgets for Phase 2 Render Functions
// =============================================================================

/// Widget that adds hover effects to its child.
class _HoverRowWidget extends StatefulWidget {
  final Widget child;
  final AppColors colors;

  const _HoverRowWidget({required this.child, required this.colors});

  @override
  State<_HoverRowWidget> createState() => _HoverRowWidgetState();
}

class _HoverRowWidgetState extends State<_HoverRowWidget> {
  bool _isHovered = false;

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      onEnter: (_) => setState(() => _isHovered = true),
      onExit: (_) => setState(() => _isHovered = false),
      child: AnimatedContainer(
        duration: AnimDurations.hoverEffect,
        curve: AnimCurves.hover,
        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
        decoration: BoxDecoration(
          color: _isHovered
              ? widget.colors.primary.withValues(alpha: 0.03)
              : Colors.transparent,
          borderRadius: BorderRadius.circular(6),
        ),
        child: widget.child,
      ),
    );
  }
}

/// Widget that staggers the appearance of children.
class _StaggeredColumn extends StatefulWidget {
  final int delayMs;
  final List<Widget> children;

  const _StaggeredColumn({required this.delayMs, required this.children});

  @override
  State<_StaggeredColumn> createState() => _StaggeredColumnState();
}

class _StaggeredColumnState extends State<_StaggeredColumn>
    with TickerProviderStateMixin {
  late List<AnimationController> _controllers;
  late List<Animation<double>> _animations;

  @override
  void initState() {
    super.initState();
    _controllers = List.generate(
      widget.children.length,
      (i) =>
          AnimationController(duration: AnimDurations.itemAppear, vsync: this),
    );
    _animations = _controllers.map((c) {
      return CurvedAnimation(parent: c, curve: AnimCurves.itemAppear);
    }).toList();

    // Start animations with stagger
    for (var i = 0; i < _controllers.length; i++) {
      Future.delayed(Duration(milliseconds: widget.delayMs * i), () {
        if (mounted) {
          _controllers[i].forward();
        }
      });
    }
  }

  @override
  void dispose() {
    for (final c in _controllers) {
      c.dispose();
    }
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      mainAxisSize: MainAxisSize.min,
      crossAxisAlignment: CrossAxisAlignment.start,
      children: List.generate(widget.children.length, (i) {
        return FadeTransition(
          opacity: _animations[i],
          child: SlideTransition(
            position: Tween<Offset>(
              begin: const Offset(0, 0.1),
              end: Offset.zero,
            ).animate(_animations[i]),
            child: widget.children[i],
          ),
        );
      }),
    );
  }
}

/// Widget that pulses once or continuously.
class _PulseWidget extends StatefulWidget {
  final bool once;
  final Widget child;

  const _PulseWidget({required this.once, required this.child});

  @override
  State<_PulseWidget> createState() => _PulseWidgetState();
}

class _PulseWidgetState extends State<_PulseWidget>
    with SingleTickerProviderStateMixin {
  late AnimationController _controller;
  late Animation<double> _animation;

  @override
  void initState() {
    super.initState();
    _controller = AnimationController(
      duration: AnimDurations.syncPulse,
      vsync: this,
    );
    _animation = Tween<double>(
      begin: 1.0,
      end: 0.6,
    ).animate(CurvedAnimation(parent: _controller, curve: Curves.easeInOut));

    if (widget.once) {
      _controller.forward().then((_) {
        if (mounted) {
          _controller.reverse();
        }
      });
    } else {
      _controller.repeat(reverse: true);
    }
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return FadeTransition(opacity: _animation, child: widget.child);
  }
}
