import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:yaml/yaml.dart';
import 'app_styles.dart';

/// Theme metadata loaded from YAML
class ThemeMetadata {
  final String name;
  final bool isDark;
  final AppColors colors;

  const ThemeMetadata({
    required this.name,
    required this.isDark,
    required this.colors,
  });
}

/// Loads themes from YAML files
class ThemeLoader {
  static final Map<String, Map<String, ThemeMetadata>> _cache = {};

  /// Load all themes from a YAML file
  static Future<Map<String, ThemeMetadata>> loadThemeFamily(
    String fileName,
  ) async {
    if (_cache.containsKey(fileName)) {
      return _cache[fileName]!;
    }

    try {
      final yamlString = await rootBundle.loadString('assets/themes/$fileName');
      final yaml = loadYaml(yamlString) as Map;
      final themes = yaml['themes'] as Map;

      final themeMap = <String, ThemeMetadata>{};

      for (final entry in themes.entries) {
        final themeKey = entry.key as String;
        final themeData = entry.value as Map;
        final name = themeData['name'] as String;
        final isDark = themeData['isDark'] as bool;
        final colors = themeData['colors'] as Map;

        themeMap[themeKey] = ThemeMetadata(
          name: name,
          isDark: isDark,
          colors: _parseColors(colors),
        );
      }

      _cache[fileName] = themeMap;
      return themeMap;
    } catch (e) {
      throw Exception('Failed to load theme file $fileName: $e');
    }
  }

  /// Load all theme families
  static Future<Map<String, ThemeMetadata>> loadAllThemes() async {
    final allThemes = <String, ThemeMetadata>{};

    final themeFiles = [
      'holon.yaml', // Default theme - warm, professional
      'default.yaml',
      'solarized.yaml',
      'dracula.yaml',
      'nord.yaml',
      'gruvbox.yaml',
      'onedark.yaml',
      'monokai.yaml',
      'tomorrow.yaml',
      'github.yaml',
      'catppuccin.yaml',
    ];

    for (final file in themeFiles) {
      try {
        final themes = await loadThemeFamily(file);
        allThemes.addAll(themes);
      } catch (e) {
        // Log error but continue loading other themes
        debugPrint('Warning: Failed to load $file: $e');
      }
    }

    return allThemes;
  }

  /// Parse color map to AppColors
  static AppColors _parseColors(Map colors) {
    Color parseColor(String hex) {
      // Remove # if present
      hex = hex.replaceAll('#', '');
      // Handle 6-digit hex
      if (hex.length == 6) {
        return Color(int.parse('FF$hex', radix: 16));
      }
      // Handle 8-digit hex (with alpha)
      if (hex.length == 8) {
        return Color(int.parse(hex, radix: 16));
      }
      throw FormatException('Invalid color format: $hex');
    }

    return AppColors(
      primary: parseColor(colors['primary'] as String),
      primaryDark: parseColor(colors['primaryDark'] as String),
      primaryLight: parseColor(colors['primaryLight'] as String),
      textPrimary: parseColor(colors['textPrimary'] as String),
      textSecondary: parseColor(colors['textSecondary'] as String),
      textTertiary: parseColor(colors['textTertiary'] as String),
      background: parseColor(colors['background'] as String),
      backgroundSecondary: parseColor(colors['backgroundSecondary'] as String),
      sidebarBackground: parseColor(colors['sidebarBackground'] as String),
      border: parseColor(colors['border'] as String),
      borderFocus: parseColor(colors['borderFocus'] as String),
      success: parseColor(colors['success'] as String),
      error: parseColor(colors['error'] as String),
      warning: parseColor(colors['warning'] as String),
    );
  }

  /// Get theme metadata by key
  static Future<ThemeMetadata?> getTheme(String themeKey) async {
    final allThemes = await loadAllThemes();
    return allThemes[themeKey];
  }

  /// Get all available theme keys
  static Future<List<String>> getAvailableThemeKeys() async {
    final allThemes = await loadAllThemes();
    return allThemes.keys.toList()..sort();
  }
}
