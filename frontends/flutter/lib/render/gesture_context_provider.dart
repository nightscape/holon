import 'package:flutter/widgets.dart';

import 'gesture_context.dart';

/// Provides [GestureContext] to descendant widgets.
///
/// Wrap your widget tree with [GestureContextScope] to enable
/// gesture-based operation invocation. Widgets can then use
/// [GestureContextProvider.of] or [GestureContextProvider.maybeOf]
/// to access the current gesture context.
///
/// Example:
/// ```dart
/// GestureContextScope(
///   child: OutlinerTreeView(...),
/// )
/// ```
class GestureContextProvider extends InheritedWidget {
  final GestureContext? current;
  final void Function(GestureContext?) setContext;

  const GestureContextProvider({
    super.key,
    required this.current,
    required this.setContext,
    required super.child,
  });

  /// Get the current gesture context, or null if none active.
  static GestureContext? maybeOf(BuildContext context) {
    return context
        .dependOnInheritedWidgetOfExactType<GestureContextProvider>()
        ?.current;
  }

  /// Get the current gesture context. Throws if none active.
  static GestureContext of(BuildContext context) {
    final provider = context
        .dependOnInheritedWidgetOfExactType<GestureContextProvider>();
    assert(provider != null, 'No GestureContextProvider found in context');
    assert(provider!.current != null, 'No active gesture context');
    return provider!.current!;
  }

  /// Set a new gesture context.
  static void set(BuildContext context, GestureContext? gestureContext) {
    final provider = context
        .dependOnInheritedWidgetOfExactType<GestureContextProvider>();
    assert(provider != null, 'No GestureContextProvider found in context');
    provider!.setContext(gestureContext);
  }

  /// Check if there's an active gesture.
  static bool hasActiveGesture(BuildContext context) {
    return maybeOf(context) != null;
  }

  @override
  bool updateShouldNotify(GestureContextProvider oldWidget) {
    return current != oldWidget.current;
  }
}

/// Stateful wrapper that manages [GestureContext] state.
///
/// This widget manages the lifecycle of gesture contexts,
/// providing the [GestureContextProvider] to its descendants.
class GestureContextScope extends StatefulWidget {
  final Widget child;

  const GestureContextScope({super.key, required this.child});

  @override
  State<GestureContextScope> createState() => _GestureContextScopeState();
}

class _GestureContextScopeState extends State<GestureContextScope> {
  GestureContext? _current;

  void _setContext(GestureContext? context) {
    if (_current != context) {
      setState(() {
        _current = context;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return GestureContextProvider(
      current: _current,
      setContext: _setContext,
      child: widget.child,
    );
  }
}
