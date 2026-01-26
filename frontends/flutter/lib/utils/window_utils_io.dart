import 'dart:io' show Platform;

import 'window_utils_desktop.dart' as desktop;

export 'window_utils_stub.dart'
    show WindowBorder, MoveWindow, WindowTitleBarBox;

bool get _isDesktop =>
    Platform.isWindows || Platform.isMacOS || Platform.isLinux;

void configureDesktopWindow() {
  if (_isDesktop) {
    desktop.configureDesktopWindow();
  }
}

Future<void> initializeAcrylic() async {
  if (_isDesktop) {
    await desktop.initializeAcrylic();
  }
}

Future<void> applyWindowEffect() async {
  if (_isDesktop) {
    await desktop.applyWindowEffect();
  }
}
