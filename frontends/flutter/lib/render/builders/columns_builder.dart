import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../../styles/app_styles.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// How a layout region behaves when it can't fit.
enum CollapseMode {
  drawer,
  sheet,
  modal,
  hidden,
}

/// Specification for a column with layout constraints.
class _ColumnSpec {
  final double? minWidth;
  final double? idealWidth;
  final int priority;
  final CollapseMode? collapseTo;
  final double? widthFraction; // Legacy: width as fraction (0.25, 0.5, etc.)
  final Widget child;

  const _ColumnSpec({
    this.minWidth,
    this.idealWidth,
    required this.priority,
    this.collapseTo,
    this.widthFraction,
    required this.child,
  });

  /// Check if this spec uses constraint-based properties
  bool get hasConstraints => collapseTo != null || minWidth != null || idealWidth != null;

  /// Get flex value for Row layout (from width fraction or ideal width ratio)
  int getFlexValue(double totalIdealWidth) {
    if (widthFraction != null) {
      return (widthFraction! * 100).round().clamp(1, 100);
    }
    if (idealWidth != null && totalIdealWidth > 0) {
      return (idealWidth! / totalIdealWidth * 100).round().clamp(1, 100);
    }
    return 1;
  }

  /// Create from row data properties
  factory _ColumnSpec.fromRowData(Map<String, dynamic> rowData, Widget child) {
    return _ColumnSpec(
      minWidth: _parseDouble(rowData['min_width']),
      idealWidth: _parseDouble(rowData['ideal_width']),
      priority: _parseInt(rowData['priority']) ?? 2,
      collapseTo: _parseCollapseMode(rowData['collapse_to']),
      widthFraction: _parseDouble(rowData['width']),
      child: child,
    );
  }

  static CollapseMode? _parseCollapseMode(dynamic value) {
    if (value == null) return null;
    return switch (value.toString().toLowerCase()) {
      'drawer' => CollapseMode.drawer,
      'sheet' => CollapseMode.sheet,
      'modal' => CollapseMode.modal,
      'hidden' => CollapseMode.hidden,
      _ => null,
    };
  }

  static double? _parseDouble(dynamic value) {
    if (value == null) return null;
    if (value is num) return value.toDouble();
    if (value is String) return double.tryParse(value);
    return null;
  }

  static int? _parseInt(dynamic value) {
    if (value == null) return null;
    if (value is int) return value;
    if (value is num) return value.toInt();
    if (value is String) return int.tryParse(value);
    return null;
  }
}

/// Builds columns() widget - horizontal layout with screen layout support.
///
/// Supports two property models:
/// - Legacy: `width: 0.25` (fraction), first child is sidebar
/// - Constraint-based: `min-width`, `ideal-width`, `priority`, `collapse-to`
///
/// When isScreenLayout is true, renders as Stack with sidebar positioning.
/// Sidebar detection:
/// - If any column has `collapse-to: drawer`, those go in sidebar
/// - Otherwise, first child is sidebar (legacy behavior)
///
/// Usage: `columns(item_template:(section ...) gap:8)`
class ColumnsWidgetBuilder {
  const ColumnsWidgetBuilder._();

  static const templateArgNames = {'item_template', 'item'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    // Screen layout mode: render as Stack with sidebar positioning
    if (context.isScreenLayout && context.drawerState != null) {
      return _buildScreenLayout(args, context, buildTemplate);
    }

    final gap = args.getDouble('gap', 8.0);
    final itemTemplateExpr = args.templates['item_template'] ?? args.templates['item'];

    // If we have rowCache, iterate over rows and build columns from data
    if (context.rowCache != null && context.rowCache!.isNotEmpty && itemTemplateExpr != null) {
      final specs = _extractColumnSpecs(context, itemTemplateExpr, buildTemplate);
      return _buildRow(specs, gap);
    }

    // Fallback: build from pre-built children (static children)
    if (args.children.isNotEmpty) {
      final rowChildren = <Widget>[];
      for (var i = 0; i < args.children.length; i++) {
        if (i > 0 && gap > 0) {
          rowChildren.add(SizedBox(width: gap));
        }
        rowChildren.add(Expanded(child: args.children[i]));
      }

      return Row(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: rowChildren,
      );
    }

    return const SizedBox.shrink();
  }

  /// Extract column specs from row cache
  static List<_ColumnSpec> _extractColumnSpecs(
    RenderContext context,
    RenderExpr itemTemplate,
    Widget Function(RenderExpr, RenderContext) buildTemplate,
  ) {
    final specs = <_ColumnSpec>[];

    for (final entry in context.rowCache!.entries) {
      final rowData = entry.value;
      final rowContext = RenderContext(
        rowData: rowData,
        rowTemplates: context.rowTemplates,
        onOperation: context.onOperation,
        nestedQueryConfig: context.nestedQueryConfig,
        availableOperations: context.availableOperations,
        entityName: context.entityName,
        rowCache: context.rowCache,
        changeStream: context.changeStream,
        parentIdColumn: context.parentIdColumn,
        sortKeyColumn: context.sortKeyColumn,
        colors: context.colors,
        focusDepth: context.focusDepth,
        queryParams: context.queryParams,
      );

      final child = buildTemplate(itemTemplate, rowContext);
      specs.add(_ColumnSpec.fromRowData(rowData, child));
    }

    return specs;
  }

  /// Build a simple Row from column specs
  static Widget _buildRow(List<_ColumnSpec> specs, double gap) {
    if (specs.isEmpty) return const SizedBox.shrink();

    final totalIdealWidth = specs.fold(0.0, (sum, s) => sum + (s.idealWidth ?? 300.0));
    final rowChildren = <Widget>[];

    for (var i = 0; i < specs.length; i++) {
      if (i > 0 && gap > 0) {
        rowChildren.add(SizedBox(width: gap));
      }
      rowChildren.add(
        Flexible(
          flex: specs[i].getFlexValue(totalIdealWidth),
          child: specs[i].child,
        ),
      );
    }

    return Row(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: rowChildren,
    );
  }

  /// Build screen layout from columns() when isScreenLayout is true.
  static Widget _buildScreenLayout(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    final sidebarWidth = context.sidebarWidth ?? 280.0;
    final drawerState = context.drawerState!;
    final colors = context.colors;

    final itemTemplateExpr = args.templates['item_template'] ?? args.templates['item'];

    // Build children from rowCache
    if (context.rowCache != null && context.rowCache!.isNotEmpty && itemTemplateExpr != null) {
      final specs = _extractColumnSpecsForScreenLayout(context, itemTemplateExpr, buildTemplate);

      if (specs.isEmpty) {
        return const SizedBox.shrink();
      }

      // Check if any spec has collapse-to: drawer (constraint-based mode)
      final hasConstraintMode = specs.any((s) => s.hasConstraints);

      List<_ColumnSpec> sidebarSpecs;
      List<_ColumnSpec> mainSpecs;

      if (hasConstraintMode) {
        // Constraint-based: collapse-to: drawer goes in sidebar
        sidebarSpecs = specs.where((s) => s.collapseTo == CollapseMode.drawer).toList();
        mainSpecs = specs.where((s) => s.collapseTo != CollapseMode.drawer).toList();
        // Sort main by priority
        mainSpecs.sort((a, b) => a.priority.compareTo(b.priority));
      } else {
        // Legacy: first child is sidebar
        sidebarSpecs = [specs.first];
        mainSpecs = specs.skip(1).toList();
      }

      // Build sidebar content
      Widget sidebarContent = const SizedBox.shrink();
      if (sidebarSpecs.isNotEmpty) {
        if (sidebarSpecs.length == 1) {
          sidebarContent = sidebarSpecs.first.child;
        } else {
          sidebarContent = Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: sidebarSpecs.map((s) => Expanded(child: s.child)).toList(),
          );
        }
      }

      // Build main content
      Widget mainContent = const SizedBox.shrink();
      if (mainSpecs.isNotEmpty) {
        if (mainSpecs.length == 1) {
          mainContent = mainSpecs.first.child;
        } else {
          final totalIdeal = mainSpecs.fold(0.0, (sum, s) => sum + (s.idealWidth ?? 300.0));
          mainContent = Row(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: mainSpecs.map((s) {
              return Flexible(
                flex: s.getFlexValue(totalIdeal),
                child: s.child,
              );
            }).toList(),
          );
        }
      }

      return ValueListenableBuilder<bool>(
        valueListenable: drawerState,
        builder: (buildContext, isOpen, _) => Stack(
          children: [
            AnimatedPositioned(
              duration: const Duration(milliseconds: 250),
              curve: Curves.easeInOut,
              left: isOpen ? 0 : -sidebarWidth,
              top: 0,
              bottom: 0,
              width: sidebarWidth,
              child: Material(
                color: _getBackgroundColor(colors),
                child: Container(
                  decoration: BoxDecoration(
                    border: Border(
                      right: BorderSide(color: _getBorderColor(colors), width: 1),
                    ),
                  ),
                  child: sidebarContent,
                ),
              ),
            ),
            AnimatedPositioned(
              duration: const Duration(milliseconds: 250),
              curve: Curves.easeInOut,
              left: isOpen ? sidebarWidth : 0,
              top: 0,
              right: 0,
              bottom: 0,
              child: mainContent,
            ),
          ],
        ),
      );
    }

    // Fallback for pre-built children (legacy: first child is sidebar)
    if (args.children.isNotEmpty) {
      final sidebarContent = args.children.first;
      final mainChildren = args.children.skip(1).toList();

      return ValueListenableBuilder<bool>(
        valueListenable: drawerState,
        builder: (buildContext, isOpen, _) => Stack(
          children: [
            AnimatedPositioned(
              duration: const Duration(milliseconds: 250),
              curve: Curves.easeInOut,
              left: isOpen ? 0 : -sidebarWidth,
              top: 0,
              bottom: 0,
              width: sidebarWidth,
              child: Material(
                color: _getBackgroundColor(colors),
                child: Container(
                  decoration: BoxDecoration(
                    border: Border(
                      right: BorderSide(color: _getBorderColor(colors), width: 1),
                    ),
                  ),
                  child: sidebarContent,
                ),
              ),
            ),
            AnimatedPositioned(
              duration: const Duration(milliseconds: 250),
              curve: Curves.easeInOut,
              left: isOpen ? sidebarWidth : 0,
              top: 0,
              right: 0,
              bottom: 0,
              child: mainChildren.isEmpty
                  ? const SizedBox.shrink()
                  : Row(
                      crossAxisAlignment: CrossAxisAlignment.stretch,
                      children: mainChildren.map((c) => Expanded(child: c)).toList(),
                    ),
            ),
          ],
        ),
      );
    }

    return const SizedBox.shrink();
  }

  /// Extract column specs for screen layout (with isScreenLayout: false for nested)
  static List<_ColumnSpec> _extractColumnSpecsForScreenLayout(
    RenderContext context,
    RenderExpr itemTemplate,
    Widget Function(RenderExpr, RenderContext) buildTemplate,
  ) {
    final specs = <_ColumnSpec>[];

    for (final entry in context.rowCache!.entries) {
      final rowData = entry.value;
      final rowContext = RenderContext(
        rowData: rowData,
        rowTemplates: context.rowTemplates,
        onOperation: context.onOperation,
        nestedQueryConfig: context.nestedQueryConfig,
        availableOperations: context.availableOperations,
        entityName: context.entityName,
        rowCache: context.rowCache,
        changeStream: context.changeStream,
        parentIdColumn: context.parentIdColumn,
        sortKeyColumn: context.sortKeyColumn,
        colors: context.colors,
        focusDepth: context.focusDepth,
        queryParams: context.queryParams,
        isScreenLayout: false, // Nested columns should not be screen layout
      );

      final child = buildTemplate(itemTemplate, rowContext);
      specs.add(_ColumnSpec.fromRowData(rowData, child));
    }

    return specs;
  }

  static Color _getBackgroundColor(dynamic colors) {
    if (colors is AppColors) {
      return colors.sidebarBackground;
    }
    return Colors.white;
  }

  static Color _getBorderColor(dynamic colors) {
    if (colors is AppColors) {
      return colors.border;
    }
    return Colors.grey[300]!;
  }
}
