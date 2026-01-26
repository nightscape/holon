import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds icon() widget - displays an image asset from assets/images/{name}.ico
///
/// Usage: `icon('todoist')` or `icon(name:'todoist', size:16)`
class IconWidgetBuilder {
  const IconWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    // Get icon name from first positional value or named arg
    final iconName = args.positionalValues.isNotEmpty
        ? args.getPositionalString(0)
        : args.getString('name', '');

    if (iconName.isEmpty) {
      throw ArgumentError('icon() requires "name" argument or positional name');
    }

    final size = args.getInt('size', 16);

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
}
