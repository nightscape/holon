import 'dart:io' show Platform;
import 'package:flutter/material.dart';
import 'package:bitsdojo_window/bitsdojo_window.dart';
import 'package:flutter_acrylic/flutter_acrylic.dart';

export 'package:bitsdojo_window/bitsdojo_window.dart'
    show
        WindowBorder,
        MoveWindow,
        WindowTitleBarBox,
        doWhenWindowReady,
        appWindow;

Future<void> initializeAcrylic() async {
  await Window.initialize();
}

Future<void> applyWindowEffect() async {
  if (Platform.isMacOS) {
    await Window.setEffect(effect: WindowEffect.sidebar, dark: false);
  } else if (Platform.isWindows) {
    await Window.setEffect(
      effect: WindowEffect.acrylic,
      color: const Color(0x00000000),
    );
  } else if (Platform.isLinux) {
    await Window.setEffect(
      effect: WindowEffect.transparent,
      color: const Color(0x00000000),
    );
  }
}

void configureDesktopWindow() {
  doWhenWindowReady(() {
    const initialSize = Size(1280, 720);
    appWindow.minSize = initialSize;
    appWindow.size = initialSize;
    appWindow.alignment = Alignment.center;
    appWindow.title = "Holon";
    appWindow.show();
  });
}
