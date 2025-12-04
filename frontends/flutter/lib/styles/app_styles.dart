/// Centralized styling system using Flutter Mix 2.0.
///
/// This file provides reusable style definitions for the entire app,
/// following the utility-first approach of Mix.
library;

import 'package:flutter/material.dart';
import 'package:mix/mix.dart';

// ============================================================================
// Design Tokens (Colors, Spacing, Typography)
// ============================================================================

/// Theme mode enum
/// These keys must match the theme keys in the YAML files
enum AppThemeMode {
  // Holon themes (default)
  holonLight,
  holonDark,
  // Legacy default themes
  light,
  dark,
  // Other themes loaded from YAML
  solarizedLight,
  solarizedDark,
  dracula,
  nordDark,
  nordLight,
  gruvboxDark,
  gruvboxLight,
  oneDark,
  monokai,
  tomorrowNight,
  githubLight,
  githubDark,
  catppuccinLatte,
  catppuccinMocha,
}

/// App color palette - now instance-based to support themes
class AppColors {
  // Primary colors
  final Color primary;
  final Color primaryDark;
  final Color primaryLight;

  // Text colors
  final Color textPrimary;
  final Color textSecondary;
  final Color textTertiary;

  // Background colors
  final Color background;
  final Color backgroundSecondary;
  final Color sidebarBackground;

  // Border colors
  final Color border;
  final Color borderFocus;

  // Semantic colors
  final Color success;
  final Color error;
  final Color warning;

  const AppColors({
    required this.primary,
    required this.primaryDark,
    required this.primaryLight,
    required this.textPrimary,
    required this.textSecondary,
    required this.textTertiary,
    required this.background,
    required this.backgroundSecondary,
    required this.sidebarBackground,
    required this.border,
    required this.borderFocus,
    required this.success,
    required this.error,
    required this.warning,
  });

  /// Light theme fallback colors (Holon Light)
  /// Used as fallback when theme loading fails or is in progress.
  /// Matches holon.yaml holonLight theme.
  static const AppColors light = AppColors(
    // Primary colors - Deep teal
    primary: Color(0xFF2A7D7D),
    primaryDark: Color(0xFF1E5C5C),
    primaryLight: Color(0xFF3A9E9E),
    // Text colors - Warm charcoal tones
    textPrimary: Color(0xFF2D2D2A),
    textSecondary: Color(0xFF6B6B65),
    textTertiary: Color(0xFF9A9A92),
    // Background colors - Warm whites
    background: Color(0xFFFAFAF8),
    backgroundSecondary: Color(0xFFF5F4F0),
    sidebarBackground: Color(0xFFF0EFE9),
    // Border colors
    border: Color(0xFFE5E4DE),
    borderFocus: Color(0xFF2A7D7D),
    // Semantic colors - Muted, not alarming
    success: Color(0xFF7D9D7D), // Sage green
    error: Color(0xFFC97064), // Muted rose
    warning: Color(0xFFD4A373), // Warm amber
  );

  /// Dark theme fallback colors (Holon Dark)
  /// Used as fallback when theme loading fails or is in progress.
  /// Matches holon.yaml holonDark theme.
  static const AppColors dark = AppColors(
    // Primary colors - Light teal
    primary: Color(0xFF5DBDBD),
    primaryDark: Color(0xFF4A9999),
    primaryLight: Color(0xFF7DD4D4),
    // Text colors - Warm off-whites
    textPrimary: Color(0xFFE8E6E1),
    textSecondary: Color(0xFF9D9D95),
    textTertiary: Color(0xFF7A7A72),
    // Background colors - Warm darks
    background: Color(0xFF1A1A18),
    backgroundSecondary: Color(0xFF252522),
    sidebarBackground: Color(0xFF2A2A27),
    // Border colors
    border: Color(0xFF3A3A36),
    borderFocus: Color(0xFF5DBDBD),
    // Semantic colors - Same across themes
    success: Color(0xFF7D9D7D), // Sage green
    error: Color(0xFFC97064), // Muted rose
    warning: Color(0xFFD4A373), // Warm amber
  );
}

/// Spacing scale (8px base unit)
class AppSpacing {
  static const xs = 4.0;
  static const sm = 8.0;
  static const md = 16.0;
  static const lg = 24.0;
  static const xl = 32.0;
  static const xxl = 40.0;
}

/// Title bar dimensions - all sizes scale from titleBarHeight
class TitleBarDimensions {
  /// Base title bar height - change this to adjust all title bar elements
  static const double titleBarHeight = 32.0;

  /// Search field height (75% of title bar height)
  static double get searchFieldHeight => titleBarHeight * 0.75;

  /// Hamburger button size (75% of title bar height)
  static double get hamburgerButtonSize => titleBarHeight * 0.75;

  /// Hamburger icon size (56% of title bar height)
  static double get hamburgerIconSize => titleBarHeight * 0.5625;

  /// Search icon size (50% of title bar height)
  static double get searchIconSize => titleBarHeight * 0.5;

  /// Search collapsed width (87.5% of title bar height)
  static double get searchCollapsedWidth => titleBarHeight * 0.875;

  /// Search field padding (12.5% of title bar height)
  static double get searchFieldPadding => titleBarHeight * 0.125;

  /// Clear button size (62.5% of title bar height)
  static double get clearButtonSize => titleBarHeight * 0.625;
}

/// Border radius scale
class AppRadius {
  static const sm = 6.0;
  static const md = 8.0;
  static const lg = 12.0;
}

/// Typography scale
class AppTypography {
  static const fontSizeXs = 12.0;
  static const fontSizeSm = 14.0;
  static const fontSizeMd = 16.0;
  static const fontSizeLg = 18.0;
  static const fontSizeXl = 20.0;
}

// ============================================================================
// Reusable Style Definitions (Mix 2.0 API)
// ============================================================================

/// Primary button style
BoxStyler primaryButtonStyle(AppColors colors) => BoxStyler()
    .decoration(DecorationMix.color(colors.primary))
    .padding(
      EdgeInsetsGeometryMix.symmetric(
        horizontal: AppSpacing.md + AppSpacing.xs, // 20px
        vertical: AppSpacing.sm + 2, // 10px
      ),
    )
    .borderRadius(BorderRadiusGeometryMix.circular(AppRadius.sm));

/// Primary button text style
TextStyler primaryButtonTextStyle(AppColors colors) => TextStyler()
    .color(Colors.white)
    .fontSize(AppTypography.fontSizeSm)
    .fontWeight(FontWeight.w500);

/// Secondary/Text button style
BoxStyler secondaryButtonStyle(AppColors colors) => BoxStyler()
    .padding(
      EdgeInsetsGeometryMix.symmetric(
        horizontal: AppSpacing.md,
        vertical: AppSpacing.sm + 2, // 10px
      ),
    )
    .borderRadius(BorderRadiusGeometryMix.circular(AppRadius.sm));

/// Secondary button text style
TextStyler secondaryButtonTextStyle(AppColors colors) =>
    TextStyler().color(colors.textSecondary).fontSize(AppTypography.fontSizeSm);

/// Dialog container style
BoxStyler dialogStyle(AppColors colors) => BoxStyler()
    .decoration(DecorationMix.color(colors.background))
    .borderRadius(BorderRadiusGeometryMix.circular(AppRadius.lg))
    .constraints(BoxConstraintsMix(maxWidth: 600, maxHeight: 700));

/// Dialog header style
BoxStyler dialogHeaderStyle(AppColors colors) => BoxStyler()
    .padding(
      EdgeInsetsGeometryMix.only(
        left: AppSpacing.lg,
        right: AppSpacing.lg,
        top: AppSpacing.md + AppSpacing.xs, // 20px
        bottom: AppSpacing.md,
      ),
    )
    .border(BoxBorderMix.bottom(BorderSideMix(color: colors.border, width: 1)));

/// Dialog header title style
TextStyler dialogTitleStyle(AppColors colors) => TextStyler()
    .fontSize(AppTypography.fontSizeLg)
    .fontWeight(FontWeight.w600)
    .color(colors.textPrimary)
    .letterSpacing(-0.2);

/// Section title style
TextStyler sectionTitleStyle(AppColors colors) => TextStyler()
    .fontSize(AppTypography.fontSizeLg)
    .fontWeight(FontWeight.w600)
    .color(colors.textPrimary);

/// Section description style
TextStyler sectionDescriptionStyle(AppColors colors) =>
    TextStyler().fontSize(AppTypography.fontSizeSm).color(colors.textSecondary);

/// Input field border style (base)
BorderSide inputBorderStyle(AppColors colors) =>
    BorderSide(color: colors.border, width: 1);

/// Input field focused border style
BorderSide inputFocusedBorderStyle(AppColors colors) =>
    BorderSide(color: colors.borderFocus, width: 2);

/// Input field decoration (for use with TextField)
InputDecoration inputDecoration({
  required AppColors colors,
  String? labelText,
  String? hintText,
  bool alignLabelWithHint = false,
  Widget? suffixIcon,
}) {
  return InputDecoration(
    labelText: labelText,
    hintText: hintText,
    alignLabelWithHint: alignLabelWithHint,
    suffixIcon: suffixIcon,
    border: OutlineInputBorder(
      borderRadius: BorderRadius.circular(AppRadius.md),
      borderSide: inputBorderStyle(colors),
    ),
    enabledBorder: OutlineInputBorder(
      borderRadius: BorderRadius.circular(AppRadius.md),
      borderSide: inputBorderStyle(colors),
    ),
    focusedBorder: OutlineInputBorder(
      borderRadius: BorderRadius.circular(AppRadius.md),
      borderSide: inputFocusedBorderStyle(colors),
    ),
  );
}

/// Monospace text style (for code/PRQL queries)
final monospaceTextStyle = TextStyle(
  fontFamily: 'monospace',
  fontSize: AppTypography.fontSizeXs,
);

// ============================================================================
// Spacing Utilities
// ============================================================================

/// Vertical spacing widget
class VSpace extends StatelessWidget {
  final double height;
  const VSpace(this.height, {super.key});

  @override
  Widget build(BuildContext context) {
    return SizedBox(height: height);
  }
}

/// Horizontal spacing widget
class HSpace extends StatelessWidget {
  final double width;
  const HSpace(this.width, {super.key});

  @override
  Widget build(BuildContext context) {
    return SizedBox(width: width);
  }
}

// ============================================================================
// Pre-configured Spacing Widgets
// ============================================================================

const vSpaceSm = VSpace(AppSpacing.sm);
const vSpaceMd = VSpace(AppSpacing.md);
const vSpaceLg = VSpace(AppSpacing.lg);
const vSpaceXl = VSpace(AppSpacing.xl);
const vSpaceXxl = VSpace(AppSpacing.xxl);

const hSpaceSm = HSpace(AppSpacing.sm);
const hSpaceMd = HSpace(AppSpacing.md);
const hSpaceLg = HSpace(AppSpacing.lg);
