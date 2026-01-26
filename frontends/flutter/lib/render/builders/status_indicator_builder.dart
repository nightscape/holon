import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds status_indicator() widget - shows sync/status with icon.
///
/// Usage: `status_indicator(status: 'synced')` or `status_indicator(status: 'pending')`
class StatusIndicatorWidgetBuilder {
  const StatusIndicatorWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final status = args.getString('status', 'synced').toLowerCase();

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
        color = const Color(0xFFE07A5F);
      case 'error':
        icon = Icons.error;
        color = context.colors.error;
      default:
        icon = Icons.circle;
        color = context.colors.textTertiary;
    }

    return Icon(icon, size: 16, color: color);
  }
}
