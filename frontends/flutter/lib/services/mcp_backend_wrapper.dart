import 'package:flutter/foundation.dart' show kDebugMode;
import 'package:mcp_toolkit/mcp_toolkit.dart';
import '../src/rust/third_party/holon_api.dart' show Value;
import '../src/rust/third_party/holon_api/widget_spec.dart' show WidgetSpec;
import '../src/rust/third_party/holon_api/render_types.dart'
    show OperationDescriptor, RenderSpec;
import '../src/rust/api/ffi_bridge.dart' as ffi;
import '../src/rust/api/types.dart' show TraceContext;
import '../utils/value_converter.dart' show dynamicToValueMap;
import 'backend_service.dart';

/// MCP-enabled wrapper for BackendService that exposes operations via MCP tools.
///
/// This wrapper registers MCP tools that allow external agents (like Claude)
/// to interact with the app by executing operations, listing available operations,
/// and performing undo/redo.
class McpBackendWrapper implements BackendService {
  final BackendService _delegate;

  McpBackendWrapper(this._delegate) {
    if (kDebugMode) {
      _registerMcpTools();
    }
  }

  void _registerMcpTools() {
    // Tool: execute_operation - Execute an operation on an entity
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final entityName = params['entity_name'];
            final opName = params['operation_name'];
            final opParams = params['params'] as Map<String, dynamic>? ?? {};

            if (entityName == null || opName == null) {
              return MCPCallResult(
                message: 'Error: entity_name and operation_name are required',
                parameters: {'success': false},
              );
            }

            final rustParams = dynamicToValueMap(opParams);
            await _delegate.executeOperation(
              entityName: entityName.toString(),
              opName: opName.toString(),
              params: rustParams,
            );

            return MCPCallResult(
              message:
                  'Operation "$opName" executed successfully on "$entityName"',
              parameters: {
                'success': true,
                'entity_name': entityName,
                'operation_name': opName,
              },
            );
          } catch (e, stack) {
            return MCPCallResult(
              message: 'Error executing operation: $e',
              parameters: {
                'success': false,
                'error': e.toString(),
                'stack': stack.toString(),
              },
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'execute_operation',
          description:
              'Execute an operation on an entity in the database. '
              'Operations mutate the database and UI updates happen via CDC streams. '
              'Use available_operations first to discover what operations are available.',
          inputSchema: ObjectSchema(
            properties: {
              'entity_name': StringSchema(
                description:
                    'Name of the entity (e.g., "blocks", "*" for wildcard operations)',
              ),
              'operation_name': StringSchema(
                description:
                    'Name of the operation (e.g., "indent", "outdent", "toggle_done", "sync")',
              ),
              'params': ObjectSchema(
                description: 'Operation parameters as key-value pairs',
                properties: {},
              ),
            },
            required: ['entity_name', 'operation_name'],
          ),
        ),
      ),
    );

    // Tool: available_operations - List available operations for an entity
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final entityName = params['entity_name']?.toString() ?? '*';
            final operations = await _delegate.availableOperations(
              entityName: entityName,
            );

            final opList = operations
                .map(
                  (op) => {
                    'name': op.name,
                    'entity_name': op.entityName,
                    'display_name': op.displayName,
                    'description': op.description,
                    'required_params': op.requiredParams
                        .map(
                          (p) => {
                            'name': p.name,
                            'type_hint': p.typeHint.toString(),
                            'description': p.description,
                          },
                        )
                        .toList(),
                  },
                )
                .toList();

            return MCPCallResult(
              message:
                  'Found ${operations.length} operations for "$entityName"',
              parameters: {
                'success': true,
                'entity_name': entityName,
                'operations': opList,
              },
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error listing operations: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'available_operations',
          description:
              'List available operations for an entity. '
              'Use "*" as entity_name to get wildcard operations (like sync).',
          inputSchema: ObjectSchema(
            properties: {
              'entity_name': StringSchema(
                description:
                    'Name of the entity, or "*" for wildcard operations',
              ),
            },
          ),
        ),
      ),
    );

    // Tool: undo - Undo the last operation
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final canUndo = await _delegate.canUndo();
            if (!canUndo) {
              return MCPCallResult(
                message: 'Nothing to undo',
                parameters: {'success': false, 'reason': 'nothing_to_undo'},
              );
            }

            final result = await _delegate.undo();
            return MCPCallResult(
              message: result ? 'Undo successful' : 'Undo failed',
              parameters: {'success': result},
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error during undo: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'undo',
          description: 'Undo the last operation',
          inputSchema: ObjectSchema(properties: {}),
        ),
      ),
    );

    // Tool: redo - Redo the last undone operation
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final canRedo = await _delegate.canRedo();
            if (!canRedo) {
              return MCPCallResult(
                message: 'Nothing to redo',
                parameters: {'success': false, 'reason': 'nothing_to_redo'},
              );
            }

            final result = await _delegate.redo();
            return MCPCallResult(
              message: result ? 'Redo successful' : 'Redo failed',
              parameters: {'success': result},
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error during redo: $e',
              parameters: {'success': false, 'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'redo',
          description: 'Redo the last undone operation',
          inputSchema: ObjectSchema(properties: {}),
        ),
      ),
    );

    // Tool: can_undo_redo - Check if undo/redo are available
    addMcpTool(
      MCPCallEntry.tool(
        handler: (params) async {
          try {
            final canUndo = await _delegate.canUndo();
            final canRedo = await _delegate.canRedo();
            return MCPCallResult(
              message:
                  'Undo: ${canUndo ? "available" : "unavailable"}, '
                  'Redo: ${canRedo ? "available" : "unavailable"}',
              parameters: {'can_undo': canUndo, 'can_redo': canRedo},
            );
          } catch (e) {
            return MCPCallResult(
              message: 'Error checking undo/redo: $e',
              parameters: {'error': e.toString()},
            );
          }
        },
        definition: MCPToolDefinition(
          name: 'can_undo_redo',
          description: 'Check if undo and redo operations are available',
          inputSchema: ObjectSchema(properties: {}),
        ),
      ),
    );
  }

  // Forward all BackendService methods to the delegate

  @override
  Future<WidgetSpec> queryAndWatch({
    required String prql,
    required Map<String, Value> params,
    required ffi.MapChangeSink sink,
    TraceContext? traceContext,
    String? contextBlockId,
  }) {
    return _delegate.queryAndWatch(
      prql: prql,
      params: params,
      sink: sink,
      traceContext: traceContext,
      contextBlockId: contextBlockId,
    );
  }

  @override
  Future<List<OperationDescriptor>> availableOperations({
    required String entityName,
  }) {
    return _delegate.availableOperations(entityName: entityName);
  }

  @override
  Future<void> executeOperation({
    required String entityName,
    required String opName,
    required Map<String, Value> params,
    TraceContext? traceContext,
  }) {
    return _delegate.executeOperation(
      entityName: entityName,
      opName: opName,
      params: params,
      traceContext: traceContext,
    );
  }

  @override
  Future<bool> hasOperation({
    required String entityName,
    required String opName,
  }) {
    return _delegate.hasOperation(entityName: entityName, opName: opName);
  }

  @override
  Future<bool> undo() => _delegate.undo();

  @override
  Future<bool> redo() => _delegate.redo();

  @override
  Future<bool> canUndo() => _delegate.canUndo();

  @override
  Future<bool> canRedo() => _delegate.canRedo();
}
