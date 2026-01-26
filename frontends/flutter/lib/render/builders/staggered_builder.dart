import 'package:flutter/material.dart';
import '../../styles/animation_constants.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds staggered() widget - children fade in with staggered delay.
///
/// Usage: `staggered(child1, child2, ...)` or `staggered(delay: 100, child1, child2)`
class StaggeredWidgetBuilder {
  const StaggeredWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final delayMs = args.getInt('delay', AnimDurations.sectionStagger.inMilliseconds);

    return _StaggeredColumn(delayMs: delayMs, children: args.children);
  }
}

class _StaggeredColumn extends StatefulWidget {
  final int delayMs;
  final List<Widget> children;

  const _StaggeredColumn({required this.delayMs, required this.children});

  @override
  State<_StaggeredColumn> createState() => _StaggeredColumnState();
}

class _StaggeredColumnState extends State<_StaggeredColumn>
    with TickerProviderStateMixin {
  late List<AnimationController> _controllers;
  late List<Animation<double>> _animations;

  @override
  void initState() {
    super.initState();
    _controllers = List.generate(
      widget.children.length,
      (i) =>
          AnimationController(duration: AnimDurations.itemAppear, vsync: this),
    );
    _animations = _controllers.map((c) {
      return CurvedAnimation(parent: c, curve: AnimCurves.itemAppear);
    }).toList();

    for (var i = 0; i < _controllers.length; i++) {
      Future.delayed(Duration(milliseconds: widget.delayMs * i), () {
        if (mounted) {
          _controllers[i].forward();
        }
      });
    }
  }

  @override
  void dispose() {
    for (final c in _controllers) {
      c.dispose();
    }
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      mainAxisSize: MainAxisSize.min,
      crossAxisAlignment: CrossAxisAlignment.start,
      children: List.generate(widget.children.length, (i) {
        return FadeTransition(
          opacity: _animations[i],
          child: SlideTransition(
            position: Tween<Offset>(
              begin: const Offset(0, 0.1),
              end: Offset.zero,
            ).animate(_animations[i]),
            child: widget.children[i],
          ),
        );
      }),
    );
  }
}
