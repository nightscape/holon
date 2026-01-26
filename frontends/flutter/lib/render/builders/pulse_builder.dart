import 'package:flutter/material.dart';
import '../../styles/animation_constants.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds pulse() widget - single or continuous pulse animation.
///
/// Usage: `pulse(child)` or `pulse(once: true, child)`
class PulseWidgetBuilder {
  const PulseWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      return const SizedBox.shrink();
    }

    final once = args.getBool('once', true);

    return _PulseWidget(once: once, child: args.children[0]);
  }
}

class _PulseWidget extends StatefulWidget {
  final bool once;
  final Widget child;

  const _PulseWidget({required this.once, required this.child});

  @override
  State<_PulseWidget> createState() => _PulseWidgetState();
}

class _PulseWidgetState extends State<_PulseWidget>
    with SingleTickerProviderStateMixin {
  late AnimationController _controller;
  late Animation<double> _animation;

  @override
  void initState() {
    super.initState();
    _controller = AnimationController(
      duration: AnimDurations.syncPulse,
      vsync: this,
    );
    _animation = Tween<double>(
      begin: 1.0,
      end: 0.6,
    ).animate(CurvedAnimation(parent: _controller, curve: Curves.easeInOut));

    if (widget.once) {
      _controller.forward().then((_) {
        if (mounted) {
          _controller.reverse();
        }
      });
    } else {
      _controller.repeat(reverse: true);
    }
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return FadeTransition(opacity: _animation, child: widget.child);
  }
}
