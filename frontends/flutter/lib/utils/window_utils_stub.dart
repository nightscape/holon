import 'package:flutter/material.dart';

void configureDesktopWindow() {}

Future<void> initializeAcrylic() async {}

Future<void> applyWindowEffect() async {}

class WindowBorder extends StatelessWidget {
  final Widget child;
  final Color color;
  final double width;

  const WindowBorder({
    super.key,
    required this.child,
    this.color = Colors.transparent,
    this.width = 0,
  });

  @override
  Widget build(BuildContext context) => child;
}

class WindowTitleBarBox extends StatelessWidget {
  final Widget? child;

  const WindowTitleBarBox({super.key, this.child});

  @override
  Widget build(BuildContext context) {
    final statusBarHeight = MediaQuery.of(context).padding.top;
    return Padding(
      padding: EdgeInsets.only(top: statusBarHeight),
      child: SizedBox(height: 32.0, child: child),
    );
  }
}

class MoveWindow extends StatelessWidget {
  final Widget? child;

  const MoveWindow({super.key, this.child});

  @override
  Widget build(BuildContext context) => child ?? const SizedBox.shrink();
}
