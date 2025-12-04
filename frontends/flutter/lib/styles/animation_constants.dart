/// Animation timing and curve constants for consistent UI motion.
///
/// Based on VISION_UI.md design specifications for calm, purposeful animation.
library;

import 'package:flutter/animation.dart';

/// Animation durations used throughout the app.
///
/// Designed for calm technology - fast enough to feel responsive,
/// slow enough to feel intentional, never jarring.
class AnimDurations {
  // Capture overlay
  static const captureIn = Duration(milliseconds: 100);
  static const captureOut = Duration(milliseconds: 80); // Faster = snappy

  // Focus depth transitions
  static const focusDeepen = Duration(milliseconds: 300);
  static const focusRelease = Duration(milliseconds: 250);

  // Section and content animations
  static const sectionStagger = Duration(milliseconds: 50);
  static const contextPanel = Duration(milliseconds: 200);

  // Micro-interactions
  static const checkmarkDraw = Duration(milliseconds: 150);
  static const confetti = Duration(milliseconds: 800);
  static const syncPulse = Duration(milliseconds: 1500);
  static const hoverEffect = Duration(milliseconds: 100);

  // Which-Key navigation
  static const whichKeyDelay = Duration(milliseconds: 300);
  static const whichKeyFade = Duration(milliseconds: 150);

  // Item appearance/disappearance
  static const itemAppear = Duration(milliseconds: 200);
  static const itemDisappear = Duration(milliseconds: 150);

  // Progressive concealment
  static const concealmentFade = Duration(milliseconds: 2500); // 2-3 seconds
  static const concealmentRestore = Duration(milliseconds: 300);
}

/// Animation curves used throughout the app.
///
/// Designed for natural, physical-feeling motion.
class AnimCurves {
  // Capture overlay
  static const captureIn = Curves.easeOut;
  static const captureOut = Curves.easeIn;

  // Focus depth transitions
  static const focusDeepen = Curves.easeOut;
  static const focusRelease = Curves.easeOut;

  // Content panels
  static const contextPanel = Curves.easeOut;

  // Micro-interactions
  static const spring = Curves.elasticOut; // for reordering, satisfying settle
  static const bounce = Curves.bounceOut; // for celebratory moments
  static const checkmark = Curves.easeOut;

  // Hover and subtle effects
  static const hover = Curves.easeOut;
  static const fade = Curves.easeInOut;

  // Item appearance
  static const itemAppear = Curves.easeOut;
  static const itemDisappear = Curves.easeIn;
}

/// Opacity values for progressive concealment.
class ConcealmentOpacity {
  /// Full visibility (no concealment)
  static const full = 1.0;

  /// Peripheral elements at medium focus
  static const peripheral = 0.4;

  /// Deeply concealed (high focus depth)
  static const concealed = 0.15;

  /// Interpolate opacity based on focus depth (0.0 to 1.0)
  static double forFocusDepth(double depth, {bool isPeripheral = false}) {
    if (isPeripheral) {
      // Peripheral elements fade faster
      return full - (depth * (full - concealed));
    }
    // Content elements fade more gently
    return full - (depth * 0.5 * (full - peripheral));
  }
}
