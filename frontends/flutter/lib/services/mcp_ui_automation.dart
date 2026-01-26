import 'package:flutter/foundation.dart' show kDebugMode;
import 'package:flutter/gestures.dart';
import 'package:flutter/rendering.dart';
import 'package:flutter/widgets.dart';
import 'package:mcp_toolkit/mcp_toolkit.dart';

/// Minimal UI automation via MCP - enables tapping widgets without code changes.
///
/// Provides two approaches:
/// 1. Semantics-based: Query accessible elements and invoke their actions
/// 2. Coordinate-based: Tap at specific screen coordinates
///
/// Usage: Call `McpUiAutomation.initialize()` once during app startup.
class McpUiAutomation {
  static bool _initialized = false;

  /// Initialize MCP UI automation tools. Call once during app startup.
  static void initialize() {
    if (!kDebugMode || _initialized) return;
    _initialized = true;
    _registerMcpTools();
  }

  static void _registerMcpTools() {
    // Tool: get_semantics_tree - Query the semantics tree for tappable elements
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final binding = WidgetsBinding.instance;
            final renderView = binding.renderViews.first;
            final semanticsOwner = renderView.owner?.semanticsOwner;

            if (semanticsOwner == null) {
              return MCPCallResult(
                message:
                    'Semantics not available. Enable with Semantics widget or MaterialApp.',
                parameters: {'success': false, 'error': 'no_semantics_owner'},
              );
            }

            final rootNode = semanticsOwner.rootSemanticsNode;
            if (rootNode == null) {
              return MCPCallResult(
                message: 'No semantics tree available',
                parameters: {'success': false, 'error': 'no_root_node'},
              );
            }

            final onlyTappableRaw = params['only_tappable'];
            final onlyTappable =
                onlyTappableRaw == true || onlyTappableRaw == 'true';
            final nodes = <Map<String, dynamic>>[];
            _collectSemanticsNodes(rootNode, nodes, onlyTappable: onlyTappable);

            return MCPCallResult(
              message: 'Found ${nodes.length} semantics nodes',
              parameters: {
                'success': true,
                'nodes': nodes,
                'count': nodes.length,
              },
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error querying semantics: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'get_semantics_tree',
          description:
              'Get the semantics tree showing accessible UI elements with their bounds and actions. '
              'Use this to find elements that can be tapped via semantics_tap.',
          inputSchema: ObjectSchema(
            properties: {
              'only_tappable': BooleanSchema(
                description: 'If true, only return nodes with tap action',
              ),
            },
          ),
        ),
      ),
    );

    // Tool: semantics_tap - Tap a semantics node by ID
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final nodeIdRaw = params['node_id'];
            if (nodeIdRaw == null) {
              return MCPCallResult(
                message: 'Error: node_id is required',
                parameters: {'success': false},
              );
            }

            final id = _parseNodeId(nodeIdRaw);
            if (id == null) {
              return MCPCallResult(
                message: 'Error: node_id must be an integer',
                parameters: {'success': false},
              );
            }

            final binding = WidgetsBinding.instance;
            final renderView = binding.renderViews.first;
            final semanticsOwner = renderView.owner?.semanticsOwner;

            if (semanticsOwner == null) {
              return MCPCallResult(
                message: 'Semantics not available',
                parameters: {'success': false},
              );
            }

            // Perform the tap action
            semanticsOwner.performAction(id, SemanticsAction.tap);

            return MCPCallResult(
              message: 'Tapped semantics node $id',
              parameters: {'success': true, 'node_id': id},
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error tapping semantics node: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'semantics_tap',
          description:
              'Tap a UI element by its semantics node ID. '
              'Use get_semantics_tree first to find node IDs.',
          inputSchema: ObjectSchema(
            properties: {
              'node_id': IntegerSchema(
                description: 'The semantics node ID to tap',
              ),
            },
            required: ['node_id'],
          ),
        ),
      ),
    );

    // Tool: semantics_action - Perform any semantics action on a node
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final nodeIdRaw = params['node_id'];
            final actionName = params['action']?.toString();

            if (nodeIdRaw == null || actionName == null) {
              return MCPCallResult(
                message: 'Error: node_id and action are required',
                parameters: {'success': false},
              );
            }

            final id = _parseNodeId(nodeIdRaw);
            if (id == null) {
              return MCPCallResult(
                message: 'Error: node_id must be an integer',
                parameters: {'success': false},
              );
            }

            final action = _parseAction(actionName);
            if (action == null) {
              return MCPCallResult(
                message:
                    'Unknown action: $actionName. Available: tap, longPress, scrollUp, scrollDown, scrollLeft, scrollRight, increase, decrease, dismiss',
                parameters: {'success': false},
              );
            }

            final binding = WidgetsBinding.instance;
            final renderView = binding.renderViews.first;
            final semanticsOwner = renderView.owner?.semanticsOwner;

            if (semanticsOwner == null) {
              return MCPCallResult(
                message: 'Semantics not available',
                parameters: {'success': false},
              );
            }

            semanticsOwner.performAction(id, action);

            return MCPCallResult(
              message: 'Performed $actionName on node $id',
              parameters: {
                'success': true,
                'node_id': id,
                'action': actionName,
              },
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error performing action: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'semantics_action',
          description:
              'Perform a semantics action on a node. '
              'Actions: tap, longPress, scrollUp, scrollDown, scrollLeft, scrollRight, increase, decrease, dismiss',
          inputSchema: ObjectSchema(
            properties: {
              'node_id': IntegerSchema(description: 'The semantics node ID'),
              'action': StringSchema(
                description: 'Action name (tap, longPress, scrollUp, etc.)',
              ),
            },
            required: ['node_id', 'action'],
          ),
        ),
      ),
    );

    // Tool: tap_at - Tap at specific screen coordinates
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final x = (params['x'] as num?)?.toDouble();
            final y = (params['y'] as num?)?.toDouble();

            if (x == null || y == null) {
              return MCPCallResult(
                message: 'Error: x and y coordinates are required',
                parameters: {'success': false},
              );
            }

            await _tapAtCoordinates(x, y);

            return MCPCallResult(
              message: 'Tapped at ($x, $y)',
              parameters: {'success': true, 'x': x, 'y': y},
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error tapping: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'tap_at',
          description:
              'Tap at specific screen coordinates (logical pixels). '
              'Use get_screenshots to see the UI and determine coordinates.',
          inputSchema: ObjectSchema(
            properties: {
              'x': NumberSchema(description: 'X coordinate in logical pixels'),
              'y': NumberSchema(description: 'Y coordinate in logical pixels'),
            },
            required: ['x', 'y'],
          ),
        ),
      ),
    );

    // Tool: long_press_at - Long press at coordinates
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final x = (params['x'] as num?)?.toDouble();
            final y = (params['y'] as num?)?.toDouble();
            final durationMs = (params['duration_ms'] as num?)?.toInt() ?? 500;

            if (x == null || y == null) {
              return MCPCallResult(
                message: 'Error: x and y coordinates are required',
                parameters: {'success': false},
              );
            }

            await _longPressAtCoordinates(
              x,
              y,
              Duration(milliseconds: durationMs),
            );

            return MCPCallResult(
              message: 'Long pressed at ($x, $y) for ${durationMs}ms',
              parameters: {
                'success': true,
                'x': x,
                'y': y,
                'duration_ms': durationMs,
              },
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error long pressing: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'long_press_at',
          description: 'Long press at specific screen coordinates.',
          inputSchema: ObjectSchema(
            properties: {
              'x': NumberSchema(description: 'X coordinate in logical pixels'),
              'y': NumberSchema(description: 'Y coordinate in logical pixels'),
              'duration_ms': IntegerSchema(
                description: 'Duration in milliseconds (default: 500)',
              ),
            },
            required: ['x', 'y'],
          ),
        ),
      ),
    );
  }

  /// Collect semantics nodes recursively
  static void _collectSemanticsNodes(
    SemanticsNode node,
    List<Map<String, dynamic>> nodes, {
    bool onlyTappable = false,
  }) {
    final data = node.getSemanticsData();
    final actionFlags = data.actions;

    final actions = <String>[];
    if (actionFlags & SemanticsAction.tap.index != 0) actions.add('tap');
    if (actionFlags & SemanticsAction.longPress.index != 0) {
      actions.add('longPress');
    }
    if (actionFlags & SemanticsAction.scrollUp.index != 0) {
      actions.add('scrollUp');
    }
    if (actionFlags & SemanticsAction.scrollDown.index != 0) {
      actions.add('scrollDown');
    }
    if (actionFlags & SemanticsAction.scrollLeft.index != 0) {
      actions.add('scrollLeft');
    }
    if (actionFlags & SemanticsAction.scrollRight.index != 0) {
      actions.add('scrollRight');
    }
    if (actionFlags & SemanticsAction.increase.index != 0) {
      actions.add('increase');
    }
    if (actionFlags & SemanticsAction.decrease.index != 0) {
      actions.add('decrease');
    }
    if (actionFlags & SemanticsAction.dismiss.index != 0) {
      actions.add('dismiss');
    }

    final hasTap = actionFlags & SemanticsAction.tap.index != 0;

    if (!onlyTappable || hasTap) {
      final rect = node.rect;
      final transform = node.transform;

      // Calculate global position
      Rect globalRect = rect;
      if (transform != null) {
        globalRect = MatrixUtils.transformRect(transform, rect);
      }

      final flags = data.flags;

      nodes.add({
        'id': node.id,
        'label': data.label.isNotEmpty ? data.label : null,
        'value': data.value.isNotEmpty ? data.value : null,
        'hint': data.hint.isNotEmpty ? data.hint : null,
        'actions': actions,
        'bounds': {
          'left': globalRect.left,
          'top': globalRect.top,
          'right': globalRect.right,
          'bottom': globalRect.bottom,
          'width': globalRect.width,
          'height': globalRect.height,
          'center_x': globalRect.center.dx,
          'center_y': globalRect.center.dy,
        },
        'flags': {
          'isButton': flags & SemanticsFlag.isButton.index != 0,
          'isTextField': flags & SemanticsFlag.isTextField.index != 0,
          'isChecked': flags & SemanticsFlag.isChecked.index != 0,
          'isSelected': flags & SemanticsFlag.isSelected.index != 0,
          'isEnabled': flags & SemanticsFlag.isEnabled.index != 0,
          'isFocused': flags & SemanticsFlag.isFocused.index != 0,
        },
      });
    }

    // Recurse into children
    node.visitChildren((child) {
      _collectSemanticsNodes(child, nodes, onlyTappable: onlyTappable);
      return true;
    });
  }

  static int? _parseNodeId(Object? value) {
    if (value == null) return null;
    if (value is int) return value;
    if (value is num) return value.toInt();
    return int.tryParse(value.toString());
  }

  static SemanticsAction? _parseAction(String name) {
    return switch (name.toLowerCase()) {
      'tap' => SemanticsAction.tap,
      'longpress' => SemanticsAction.longPress,
      'scrollup' => SemanticsAction.scrollUp,
      'scrolldown' => SemanticsAction.scrollDown,
      'scrollleft' => SemanticsAction.scrollLeft,
      'scrollright' => SemanticsAction.scrollRight,
      'increase' => SemanticsAction.increase,
      'decrease' => SemanticsAction.decrease,
      'dismiss' => SemanticsAction.dismiss,
      _ => null,
    };
  }

  static Future<void> _tapAtCoordinates(double x, double y) async {
    final binding = GestureBinding.instance;
    final position = Offset(x, y);
    final now = Duration(milliseconds: DateTime.now().millisecondsSinceEpoch);

    final downEvent = PointerDownEvent(
      timeStamp: now,
      position: position,
      pointer: 1,
      kind: PointerDeviceKind.touch,
    );

    binding.handlePointerEvent(downEvent);
    await Future.delayed(const Duration(milliseconds: 50));

    final upEvent = PointerUpEvent(
      timeStamp: now + const Duration(milliseconds: 50),
      position: position,
      pointer: 1,
      kind: PointerDeviceKind.touch,
    );

    binding.handlePointerEvent(upEvent);
  }

  static Future<void> _longPressAtCoordinates(
    double x,
    double y,
    Duration duration,
  ) async {
    final binding = GestureBinding.instance;
    final position = Offset(x, y);
    final now = Duration(milliseconds: DateTime.now().millisecondsSinceEpoch);

    final downEvent = PointerDownEvent(
      timeStamp: now,
      position: position,
      pointer: 1,
      kind: PointerDeviceKind.touch,
    );

    binding.handlePointerEvent(downEvent);
    await Future.delayed(duration);

    final upEvent = PointerUpEvent(
      timeStamp: now + duration,
      position: position,
      pointer: 1,
      kind: PointerDeviceKind.touch,
    );

    binding.handlePointerEvent(upEvent);
  }
}
