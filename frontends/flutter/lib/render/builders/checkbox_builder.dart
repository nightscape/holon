import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds checkbox() widget - LogSeq-style checkbox for boolean fields.
///
/// Usage: `checkbox(checked: this.completed)`
class CheckboxWidgetBuilder {
  const CheckboxWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final checked = args.getBool('checked', false);
    final fieldName = args.getFieldName('checked');

    final updateOp = fieldName != null
        ? OperationHelpers.findSetFieldOperation(fieldName, context)
        : null;

    return GestureDetector(
      onTap: () {
        final id = context.rowData['id'];
        if (id == null || context.onOperation == null || updateOp == null) {
          return;
        }

        final entityName =
            context.rowData['entity_name']?.toString() ??
            (updateOp.entityName.isNotEmpty ? updateOp.entityName : null) ??
            context.entityName;
        if (entityName == null) {
          throw StateError(
            'Cannot dispatch checkbox operation: no entity_name found.',
          );
        }

        context.onOperation!(entityName, updateOp.name, {
          'id': id.toString(),
          fieldName!: !checked ? 'true' : 'false',
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
                color: Color(0xFF10B981),
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
}
