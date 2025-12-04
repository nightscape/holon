import 'dart:math';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../src/rust/api/types.dart' show TraceContext;
import '../src/rust/third_party/holon_api/render_types.dart'
    show OperationDescriptor;
import '../providers/query_providers.dart';
import '../styles/app_styles.dart';

/// Generate a new random trace context for distributed tracing
TraceContext _generateTraceContext() {
  final random = Random.secure();

  // Generate 16-byte trace ID (32 hex chars)
  final traceIdBytes = List<int>.generate(16, (_) => random.nextInt(256));
  final traceId = traceIdBytes
      .map((b) => b.toRadixString(16).padLeft(2, '0'))
      .join();

  // Generate 8-byte span ID (16 hex chars)
  final spanIdBytes = List<int>.generate(8, (_) => random.nextInt(256));
  final spanId = spanIdBytes
      .map((b) => b.toRadixString(16).padLeft(2, '0'))
      .join();

  return TraceContext(
    traceId: traceId,
    spanId: spanId,
    traceFlags: 0x01, // sampled
    traceState: null,
  );
}

/// Provider for wildcard operations
final wildcardOperationsProvider = FutureProvider<List<OperationDescriptor>>((
  ref,
) async {
  final backendService = ref.watch(backendServiceProvider);
  return await backendService.availableOperations(entityName: '*');
});

/// Notifier for operation execution state (tracks which operations are currently executing)
class OperationExecutingNotifier extends Notifier<Set<String>> {
  @override
  Set<String> build() => <String>{};

  void addExecuting(String opName) {
    state = {...state, opName};
  }

  void removeExecuting(String opName) {
    final newState = <String>{...state};
    newState.remove(opName);
    state = newState;
  }
}

/// Provider for operation execution state
final operationExecutingProvider =
    NotifierProvider<OperationExecutingNotifier, Set<String>>(
      OperationExecutingNotifier.new,
    );

/// Widget that displays wildcard operations as icon buttons in a row
///
/// Similar to RenderInterpreter, this widget discovers operations dynamically
/// and displays them in the title bar.
class WildcardOperationsWidget extends ConsumerWidget {
  const WildcardOperationsWidget({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final operationsAsync = ref.watch(wildcardOperationsProvider);
    final executingOps = ref.watch(operationExecutingProvider);

    return operationsAsync.when(
      data: (operations) {
        if (operations.isEmpty) {
          return const SizedBox.shrink();
        }

        return Row(
          mainAxisSize: MainAxisSize.min,
          children: operations.map((op) {
            final isExecuting = executingOps.contains(op.name);
            return Padding(
              padding: const EdgeInsets.only(left: 8),
              child: _buildOperationButton(context, ref, op, isExecuting),
            );
          }).toList(),
        );
      },
      loading: () => const SizedBox(
        width: 16,
        height: 16,
        child: CircularProgressIndicator(strokeWidth: 2),
      ),
      error: (error, stack) {
        debugPrint(
          '[WildcardOperationsWidget] Error loading operations: $error',
        );
        return const SizedBox.shrink();
      },
    );
  }

  Widget _buildOperationButton(
    BuildContext context,
    WidgetRef ref,
    OperationDescriptor op,
    bool isExecuting,
  ) {
    final icon = _iconForOperation(op.name);
    final displayName = op.displayName.isNotEmpty
        ? op.displayName
        : _displayNameForOperation(op.name);

    return Container(
      decoration: BoxDecoration(
        color: isExecuting ? const Color(0xFFF3F4F6) : const Color(0xFFEFF6FF),
        borderRadius: BorderRadius.circular(20),
        border: Border.all(
          color: isExecuting
              ? const Color(0xFFE5E7EB)
              : const Color(0xFFBFDBFE),
          width: 1,
        ),
      ),
      child: Material(
        color: Colors.transparent,
        child: InkWell(
          onTap: isExecuting ? null : () => _executeOperation(context, ref, op),
          borderRadius: BorderRadius.circular(20),
          child: Padding(
            padding: EdgeInsets.symmetric(
              horizontal:
                  TitleBarDimensions.titleBarHeight *
                  0.3125, // 10px at 32px base
              vertical:
                  TitleBarDimensions.titleBarHeight * 0.125, // 4px at 32px base
            ),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                isExecuting
                    ? SizedBox(
                        width:
                            TitleBarDimensions.titleBarHeight *
                            0.4375, // 14px at 32px base
                        height: TitleBarDimensions.titleBarHeight * 0.4375,
                        child: CircularProgressIndicator(
                          strokeWidth: 2,
                          valueColor: const AlwaysStoppedAnimation<Color>(
                            Color(0xFF3B82F6),
                          ),
                        ),
                      )
                    : Icon(
                        icon,
                        size:
                            TitleBarDimensions.titleBarHeight *
                            0.4375, // 14px at 32px base
                        color: const Color(0xFF3B82F6),
                      ),
                SizedBox(
                  width: TitleBarDimensions.titleBarHeight * 0.15625,
                ), // 5px at 32px base
                Text(
                  displayName,
                  style: TextStyle(
                    fontSize:
                        TitleBarDimensions.titleBarHeight *
                        0.375, // 12px at 32px base
                    fontWeight: FontWeight.w500,
                    color: const Color(0xFF3B82F6),
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }

  Future<void> _executeOperation(
    BuildContext context,
    WidgetRef ref,
    OperationDescriptor op,
  ) async {
    final displayName = op.displayName.isNotEmpty
        ? op.displayName
        : _displayNameForOperation(op.name);

    // Mark operation as executing
    ref.read(operationExecutingProvider.notifier).addExecuting(op.name);

    try {
      // Generate trace context for distributed tracing
      final traceContext = _generateTraceContext();
      debugPrint(
        '[WildcardOperationsWidget] Executing operation: ${op.name} (trace_id=${traceContext.traceId})',
      );

      // Use backendService instead of direct FFI call for mock mode compatibility
      final backendService = ref.read(backendServiceProvider);
      await backendService.executeOperation(
        entityName: '*',
        opName: op.name,
        params: const {},
        traceContext: traceContext,
      );
      debugPrint('[WildcardOperationsWidget] Operation completed: ${op.name}');

      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('$displayName completed'),
            duration: const Duration(seconds: 1),
            behavior: SnackBarBehavior.floating,
          ),
        );
      }
    } catch (e) {
      debugPrint('[WildcardOperationsWidget] Operation failed: $e');
      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('$displayName failed: ${e.toString()}'),
            backgroundColor: Colors.red,
            behavior: SnackBarBehavior.floating,
          ),
        );
      }
    } finally {
      // Remove operation from executing set
      ref.read(operationExecutingProvider.notifier).removeExecuting(op.name);
    }
  }

  /// Get icon for an operation based on its name (similar to RenderInterpreter)
  IconData _iconForOperation(String opName) {
    final name = opName.toLowerCase();
    if (name.contains('sync')) {
      return Icons.sync;
    } else if (name.contains('indent')) {
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

  /// Get display name for an operation
  String _displayNameForOperation(String opName) {
    // Capitalize first letter and replace underscores with spaces
    final displayName = opName
        .split('_')
        .map(
          (word) =>
              word.isEmpty ? '' : word[0].toUpperCase() + word.substring(1),
        )
        .join(' ');
    return displayName;
  }
}
