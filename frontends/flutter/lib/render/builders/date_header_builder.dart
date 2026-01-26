import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds date_header() widget - displays formatted current date.
///
/// Usage: `date_header()` or `date_header(format: 'EEEE, MMMM d')`
class DateHeaderWidgetBuilder {
  const DateHeaderWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final format = args.getString('format', 'EEEE, MMMM d');

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
}
