import 'widget_builder.dart';

// Builder imports
import 'spacer_builder.dart';
import 'badge_builder.dart';
import 'icon_builder.dart';
import 'bullet_builder.dart';
import 'text_builder.dart';
import 'row_builder.dart';
import 'block_builder.dart';
import 'flexible_builder.dart';
import 'checkbox_builder.dart';
import 'collapse_button_builder.dart';
import 'section_builder.dart';
import 'stack_builder.dart';
import 'grid_builder.dart';
import 'scroll_builder.dart';
import 'date_header_builder.dart';
import 'progress_builder.dart';
import 'count_badge_builder.dart';
import 'status_indicator_builder.dart';
import 'animated_builder.dart';
import 'hover_row_builder.dart';
import 'staggered_builder.dart';
import 'pulse_builder.dart';
import 'drop_zone_builder.dart';
import 'block_operations_builder.dart';
import 'editable_text_builder.dart';
import 'pie_menu_builder.dart';
import 'focusable_builder.dart';
import 'draggable_builder.dart';
import 'clickable_builder.dart';
import 'list_builder.dart';
import 'tree_builder.dart';
import 'outline_builder.dart';
import 'state_toggle_builder.dart';
import 'columns_builder.dart';
import 'source_block_builder.dart';
import 'source_editor_builder.dart';
import 'query_result_builder.dart';
import 'live_query_builder.dart';
import 'render_block_builder.dart';

/// Metadata about a registered builder.
class BuilderEntry {
  /// Factory for standard builders (most widgets).
  final WidgetBuilderFactory? standard;

  /// Factory for template builders (list, tree, outline).
  final TemplateBuilderFactory? template;

  /// Named args that should be kept as RenderExpr (not pre-evaluated).
  final Set<String> templateArgNames;

  const BuilderEntry.standard(this.standard)
      : template = null,
        templateArgNames = const {};

  const BuilderEntry.template(this.template, this.templateArgNames)
      : standard = null;

  /// True if this is a template builder that needs buildTemplate function.
  bool get isTemplate => template != null;
}

/// Registry of widget builder factories.
///
/// Maps function names (from PRQL render expressions) to builder factories.
/// Supports two types of builders:
/// - Standard: receive pre-resolved args, no external dependencies
/// - Template: receive additional buildTemplate function for deferred instantiation
class BuilderRegistry {
  final Map<String, BuilderEntry> _entries = {};

  /// Register a standard builder factory.
  void register(String name, WidgetBuilderFactory factory) {
    _entries[name] = BuilderEntry.standard(factory);
  }

  /// Register a template builder factory with template arg names.
  void registerTemplate(
    String name,
    TemplateBuilderFactory factory,
    Set<String> templateArgNames,
  ) {
    _entries[name] = BuilderEntry.template(factory, templateArgNames);
  }

  /// Get builder entry by name.
  BuilderEntry? get(String name) => _entries[name];

  /// Check if a builder is registered.
  bool has(String name) => _entries.containsKey(name);

  /// Create registry with all default builders.
  static BuilderRegistry createDefault() {
    final registry = BuilderRegistry();

    // Leaf builders (no children)
    registry.register('spacer', SpacerWidgetBuilder.build);
    registry.register('badge', BadgeWidgetBuilder.build);
    registry.register('icon', IconWidgetBuilder.build);
    registry.register('bullet', BulletWidgetBuilder.build);
    registry.register('text', TextWidgetBuilder.build);
    registry.register('checkbox', CheckboxWidgetBuilder.build);
    registry.register('collapse_button', CollapseButtonWidgetBuilder.build);
    registry.register('date_header', DateHeaderWidgetBuilder.build);
    registry.register('progress', ProgressWidgetBuilder.build);
    registry.register('count_badge', CountBadgeWidgetBuilder.build);
    registry.register('status_indicator', StatusIndicatorWidgetBuilder.build);

    // Composition builders (use children)
    registry.register('row', RowWidgetBuilder.build);
    registry.register('block', BlockWidgetBuilder.build);
    registry.register('flexible', FlexibleWidgetBuilder.build);
    registry.register('section', SectionWidgetBuilder.build);
    registry.register('stack', StackWidgetBuilder.build);
    registry.register('grid', GridWidgetBuilder.build);
    registry.register('scroll', ScrollWidgetBuilder.build);

    // Animation builders
    registry.register('animated', AnimatedWidgetBuilder.build);
    registry.register('hover_row', HoverRowWidgetBuilder.build);
    registry.register('staggered', StaggeredWidgetBuilder.build);
    registry.register('pulse', PulseWidgetBuilder.build);

    // Interactive builders
    registry.register('drop_zone', DropZoneWidgetBuilder.build);
    registry.register('block_operations', BlockOperationsWidgetBuilder.build);
    registry.register('editable_text', EditableTextWidgetBuilder.build);
    registry.register('pie_menu', PieMenuWidgetBuilder.build);
    registry.register('focusable', FocusableWidgetBuilder.build);
    registry.register('draggable', DraggableWidgetBuilder.build);
    registry.register('source_block', SourceBlockWidgetBuilder.build);
    registry.register('source_editor', SourceEditorWidgetBuilder.build);
    registry.register('query_result', QueryResultWidgetBuilder.build);
    registry.register('live_query', LiveQueryWidgetBuilder.build);

    // Template builders - require buildTemplate function
    registry.registerTemplate(
      'clickable',
      ClickableWidgetBuilder.build,
      ClickableWidgetBuilder.templateArgNames,
    );
    registry.registerTemplate(
      'list',
      ListWidgetBuilder.build,
      ListWidgetBuilder.templateArgNames,
    );
    registry.registerTemplate(
      'tree',
      TreeWidgetBuilder.build,
      TreeWidgetBuilder.templateArgNames,
    );
    registry.registerTemplate(
      'outline',
      OutlineWidgetBuilder.build,
      OutlineWidgetBuilder.templateArgNames,
    );
    registry.registerTemplate(
      'state_toggle',
      StateToggleWidgetBuilder.build,
      StateToggleWidgetBuilder.templateArgNames,
    );
    registry.registerTemplate(
      'columns',
      ColumnsWidgetBuilder.build,
      ColumnsWidgetBuilder.templateArgNames,
    );
    registry.registerTemplate(
      'render_block',
      RenderBlockWidgetBuilder.build,
      RenderBlockWidgetBuilder.templateArgNames,
    );

    return registry;
  }
}
