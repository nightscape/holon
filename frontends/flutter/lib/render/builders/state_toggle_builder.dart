import 'package:flutter/material.dart';
import 'package:pie_menu/pie_menu.dart';
import '../../src/rust/third_party/holon_api.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../../utils/value_converter.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Internal state representation for state_toggle widget.
class _ToggleState {
  final String value;
  final double? progress;
  final bool? isDone;
  final bool? isActive;

  const _ToggleState({
    required this.value,
    this.progress,
    this.isDone,
    this.isActive,
  });
}

/// Builds state_toggle() widget - cycles through predefined states on tap.
///
/// Usage: `state_toggle(this.status)` or `state_toggle(this.priority, states: ['p1', 'p2', 'p3', 'p4'])`
class StateToggleWidgetBuilder {
  const StateToggleWidgetBuilder._();

  /// Template arg names that should be kept as RenderExpr
  static const templateArgNames = {'states'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    // Get field name from first positional value or named arg
    String? fieldName;
    if (args.positionalValues.isNotEmpty) {
      fieldName = args.positionalValues.first?.toString();
    }
    fieldName ??= args.getFieldName('field');

    if (fieldName == null || fieldName.isEmpty) {
      throw ArgumentError('state_toggle() requires a field as first argument');
    }

    // Create non-nullable reference for use in closures
    final field = fieldName;

    // Parse states - either from explicit argument or from OneOf type hint
    final statesExpr = args.templates['states'];
    final List<_ToggleState> states;

    if (statesExpr != null) {
      states = _parseStatesArray(statesExpr, context);
      if (states.isEmpty) {
        throw ArgumentError('state_toggle() requires at least one state');
      }
    } else {
      final updateOp = OperationHelpers.findSetFieldOperation(field, context);
      if (updateOp == null) {
        throw ArgumentError(
          'state_toggle() requires either "states" argument or an operation descriptor '
          'with OneOf type hint for field "$field"',
        );
      }

      states = _extractStatesFromOneOf(updateOp, field);
      if (states.isEmpty) {
        throw ArgumentError(
          'state_toggle() could not extract states from operation descriptor '
          'for field "$field".',
        );
      }
    }

    // Get current value from row data
    final currentValue = context.getColumn(field)?.toString() ?? '';

    // Find current state index
    var currentIndex = states.indexWhere((s) => s.value == currentValue);
    if (currentIndex < 0) {
      currentIndex = 0;
    }

    final currentState = states[currentIndex];
    final updateOp = OperationHelpers.findSetFieldOperation(field, context);

    // Callback to update state
    void updateState(String newValue) {
      final id = context.rowData['id'];
      if (id == null || context.onOperation == null || updateOp == null) return;

      final entityName =
          context.rowData['entity_name']?.toString() ??
          (updateOp.entityName.isNotEmpty ? updateOp.entityName : null) ??
          context.entityName;
      if (entityName == null) {
        throw StateError(
          'Cannot dispatch state_toggle operation: no entity_name found.',
        );
      }

      context.onOperation!(entityName, updateOp.name, {
        'id': id.toString(),
        field: newValue,
      });
    }

    // For 5+ states, use pie menu; otherwise cycle on tap
    if (states.length >= 5) {
      return _buildStateTogglePieMenu(states, currentState, updateState, context);
    }

    // Cycle through states on tap
    return GestureDetector(
      onTap: () {
        final nextIndex = (currentIndex + 1) % states.length;
        updateState(states[nextIndex].value);
      },
      child: _buildStateDisplay(currentState, context),
    );
  }

  /// Extract states from OneOf type hint in operation descriptor.
  static List<_ToggleState> _extractStatesFromOneOf(
    OperationDescriptor op,
    String fieldName,
  ) {
    final param = op.requiredParams
        .where((p) => p.name == fieldName)
        .firstOrNull;
    if (param == null) {
      throw StateError(
        'Parameter $fieldName not found in operation ${op.name}',
      );
    }

    final typeHint = param.typeHint;
    if (typeHint is! TypeHint_OneOf) {
      return [];
    }

    final states = <_ToggleState>[];
    for (final value in typeHint.values) {
      final dynamicValue = valueToDynamic(value);

      if (dynamicValue is String) {
        states.add(_ToggleState(value: dynamicValue));
      } else if (dynamicValue is Map<String, dynamic>) {
        final stateName = dynamicValue['state']?.toString();
        if (stateName != null) {
          final progress = dynamicValue['progress'] is num
              ? (dynamicValue['progress'] as num).toDouble()
              : null;
          final isDone = dynamicValue['is_done'] as bool?;
          final isActive = dynamicValue['is_active'] as bool?;

          states.add(
            _ToggleState(
              value: stateName,
              progress: progress,
              isDone: isDone,
              isActive: isActive,
            ),
          );
        }
      }
    }

    return states;
  }

  /// Parse states array from RenderExpr.
  static List<_ToggleState> _parseStatesArray(
    RenderExpr statesExpr,
    RenderContext context,
  ) {
    return statesExpr.when(
      array: (items) {
        return items.map((item) => _parseToggleState(item, context)).toList();
      },
      literal: (value) {
        return value.when(
          string: (s) {
            return s
                .split(',')
                .map((v) => v.trim())
                .where((v) => v.isNotEmpty)
                .map((v) => _ToggleState(value: v))
                .toList();
          },
          array: (items) {
            return items.map((item) {
              final str = item.when(
                string: (s) => s,
                null_: () => '',
                boolean: (b) => b.toString(),
                integer: (i) => i.toString(),
                float: (f) => f.toString(),
                dateTime: (s) => s,
                json: (s) => s,
                reference: (r) => r,
                array: (_) => '',
                object: (_) => '',
              );
              return _ToggleState(value: str);
            }).toList();
          },
          null_: () => <_ToggleState>[],
          boolean: (_) => <_ToggleState>[],
          integer: (_) => <_ToggleState>[],
          float: (_) => <_ToggleState>[],
          dateTime: (_) => <_ToggleState>[],
          json: (_) => <_ToggleState>[],
          reference: (_) => <_ToggleState>[],
          object: (_) => <_ToggleState>[],
        );
      },
      columnRef: (name) {
        final value = context.getColumn(name);
        if (value is List) {
          return value.map((v) => _ToggleState(value: v.toString())).toList();
        }
        return <_ToggleState>[];
      },
      functionCall: (_, __, ___) => <_ToggleState>[],
      binaryOp: (_, __, ___) => <_ToggleState>[],
      object: (_) => <_ToggleState>[],
    );
  }

  /// Parse a single state from RenderExpr.
  static _ToggleState _parseToggleState(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) {
        return value.when(
          string: (s) => _ToggleState(value: s),
          null_: () => _ToggleState(value: ''),
          boolean: (b) => _ToggleState(value: b.toString()),
          integer: (i) => _ToggleState(value: i.toString()),
          float: (f) => _ToggleState(value: f.toString()),
          dateTime: (s) => _ToggleState(value: s),
          json: (s) => _ToggleState(value: s),
          reference: (r) => _ToggleState(value: r),
          array: (_) => _ToggleState(value: ''),
          object: (fields) {
            final v = fields['value'];
            final valueStr = v != null ? valueToDynamic(v)?.toString() ?? '' : '';
            return _ToggleState(value: valueStr);
          },
        );
      },
      columnRef: (name) =>
          _ToggleState(value: context.getColumn(name)?.toString() ?? ''),
      functionCall: (_, __, ___) => _ToggleState(value: ''),
      binaryOp: (_, __, ___) => _ToggleState(value: ''),
      array: (_) => _ToggleState(value: ''),
      object: (fields) {
        String valueStr = '';
        for (final entry in fields.entries) {
          if (entry.key == 'value') {
            valueStr = _exprToString(entry.value, context);
            break;
          }
        }
        return _ToggleState(value: valueStr);
      },
    );
  }

  /// Get icon and color for a state.
  static (IconData, Color) _getStateVisuals(
    _ToggleState state,
    RenderContext context,
  ) {
    final progress = state.progress ?? 0.0;
    final isDone = state.isDone ?? false;
    final isActive = state.isActive ?? false;

    if (isDone) {
      return (Icons.check_circle, const Color(0xFF10B981));
    } else if (progress == 0.0) {
      return (Icons.radio_button_unchecked, context.colors.textTertiary);
    } else if (isActive) {
      final intensity = (progress / 100.0).clamp(0.0, 1.0);
      final baseColor = const Color(0xFF3B82F6);
      final color = Color.fromRGBO(
        baseColor.red,
        baseColor.green,
        baseColor.blue,
        0.5 + (intensity * 0.5),
      );
      return (Icons.timelapse, color);
    } else {
      return (Icons.pause_circle, const Color(0xFFF59E0B));
    }
  }

  /// Build visual display for a state.
  static Widget _buildStateDisplay(_ToggleState state, RenderContext context) {
    final (icon, color) = _getStateVisuals(state, context);

    return Container(
      width: 20,
      height: 20,
      margin: const EdgeInsets.only(right: 8, top: 2),
      child: Center(child: Icon(icon, size: 16, color: color)),
    );
  }

  /// Build pie menu for state selection (5+ states).
  static Widget _buildStateTogglePieMenu(
    List<_ToggleState> states,
    _ToggleState currentState,
    void Function(String) onSelect,
    RenderContext context,
  ) {
    final actions = states.map((state) {
      final (icon, color) = _getStateVisuals(state, context);
      return PieAction(
        tooltip: Text(state.value),
        onSelect: () => onSelect(state.value),
        child: Icon(icon, color: color),
      );
    }).toList();

    return PieMenu(
      theme: OperationHelpers.createPieTheme(),
      actions: actions,
      child: _buildStateDisplay(currentState, context),
    );
  }

  static String _exprToString(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (v) => v.when(
        string: (s) => s,
        integer: (i) => i.toString(),
        float: (f) => f.toString(),
        boolean: (b) => b.toString(),
        null_: () => '',
        dateTime: (s) => s,
        json: (s) => s,
        reference: (r) => r,
        array: (_) => '',
        object: (_) => '',
      ),
      columnRef: (name) => context.getColumn(name)?.toString() ?? '',
      functionCall: (_, __, ___) => '',
      binaryOp: (_, __, ___) => '',
      array: (_) => '',
      object: (_) => '',
    );
  }
}
