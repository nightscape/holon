import 'dart:io' show Directory, Platform;
import 'package:flutter/foundation.dart' show kIsWeb;
import 'package:flutter/services.dart' show rootBundle;
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:macos_secure_bookmarks/macos_secure_bookmarks.dart';
import '../styles/app_styles.dart';
import '../styles/theme_loader.dart';

part 'settings_provider.g.dart';

const String _todoistApiKeyKey = 'todoist_api_key';
const String _prqlQueryKey = 'prql_query';
const String _themeModeKey = 'theme_mode';
const String _orgModeRootDirectoryKey = 'orgmode_root_directory';
const String _orgModeBookmarkKey = 'orgmode_bookmark';

/// Default PRQL query asset path (symlinked from holon-todoist crate)
const String _defaultQueryAsset = 'assets/queries/todoist_hierarchy.prql';

/// Load the default PRQL query from assets
Future<String> _loadDefaultQuery() async {
  try {
    return await rootBundle.loadString(_defaultQueryAsset);
  } catch (e) {
    // Fallback if asset loading fails
    return r'''
from todoist_tasks
select {id, content, completed, priority, due_date, project_id, parent_id}
derive sort_key = id
render (tree parent_id:parent_id sortkey:sort_key item_template:(row (bullet) (checkbox checked:this.completed) (editable_text content:this.content)))
''';
  }
}

/// Provider for getting Todoist API key from preferences
@riverpod
Future<String> todoistApiKey(Ref ref) async {
  final prefs = await SharedPreferences.getInstance();
  return prefs.getString(_todoistApiKeyKey) ?? '';
}

/// Provider for setting Todoist API key
Future<void> setTodoistApiKey(WidgetRef ref, String apiKey) async {
  final prefs = await SharedPreferences.getInstance();
  await prefs.setString(_todoistApiKeyKey, apiKey);
  // Invalidate the provider to reload the value
  ref.invalidate(todoistApiKeyProvider);
}

/// Provider for getting PRQL query from preferences (or default from assets)
@Riverpod(keepAlive: true)
Future<String> prqlQuery(Ref ref) async {
  final prefs = await SharedPreferences.getInstance();
  final savedQuery = prefs.getString(_prqlQueryKey);
  if (savedQuery != null && savedQuery.isNotEmpty) {
    return savedQuery;
  }
  return _loadDefaultQuery();
}

/// Provider for setting PRQL query
Future<void> setPrqlQuery(WidgetRef ref, String query) async {
  final prefs = await SharedPreferences.getInstance();
  await prefs.setString(_prqlQueryKey, query);
  // Invalidate the provider to reload the value
  ref.invalidate(prqlQueryProvider);
}

/// Provider for getting OrgMode root directory from preferences
/// On macOS, this resolves the security-scoped bookmark to restore access
@riverpod
Future<String?> orgModeRootDirectory(Ref ref) async {
  final prefs = await SharedPreferences.getInstance();

  if (!kIsWeb && Platform.isMacOS) {
    final bookmarkData = prefs.getString(_orgModeBookmarkKey);
    if (bookmarkData != null && bookmarkData.isNotEmpty) {
      final secureBookmarks = SecureBookmarks();
      final resolvedFile = await secureBookmarks.resolveBookmark(bookmarkData);
      await secureBookmarks.startAccessingSecurityScopedResource(resolvedFile);
      return resolvedFile.path;
    }
    return null;
  }

  final path = prefs.getString(_orgModeRootDirectoryKey);
  return (path != null && path.isNotEmpty) ? path : null;
}

/// Provider for setting OrgMode root directory
/// On macOS, this saves a security-scoped bookmark for persistent sandbox access
Future<void> setOrgModeRootDirectory(WidgetRef ref, String? path) async {
  final prefs = await SharedPreferences.getInstance();

  if (!kIsWeb && Platform.isMacOS) {
    if (path != null && path.isNotEmpty) {
      final secureBookmarks = SecureBookmarks();
      final bookmarkData = await secureBookmarks.bookmark(Directory(path));
      await prefs.setString(_orgModeBookmarkKey, bookmarkData);
      await prefs.setString(_orgModeRootDirectoryKey, path);
    } else {
      await prefs.remove(_orgModeBookmarkKey);
      await prefs.remove(_orgModeRootDirectoryKey);
    }
  } else {
    if (path != null && path.isNotEmpty) {
      await prefs.setString(_orgModeRootDirectoryKey, path);
    } else {
      await prefs.remove(_orgModeRootDirectoryKey);
    }
  }
  // Invalidate the provider to reload the value
  ref.invalidate(orgModeRootDirectoryProvider);
}

/// Provider for getting theme mode from preferences
@riverpod
Future<AppThemeMode> themeMode(Ref ref) async {
  final prefs = await SharedPreferences.getInstance();
  final themeModeString = prefs.getString(_themeModeKey);
  if (themeModeString == null) {
    return AppThemeMode.holonLight; // Default to Holon Light theme
  }
  return AppThemeMode.values.firstWhere(
    (mode) => mode.name == themeModeString,
    orElse: () => AppThemeMode.holonLight,
  );
}

/// Provider for setting theme mode
Future<void> setThemeMode(WidgetRef ref, AppThemeMode mode) async {
  final prefs = await SharedPreferences.getInstance();
  await prefs.setString(_themeModeKey, mode.name);
  // Invalidate the provider to reload the value
  ref.invalidate(themeModeProvider);
}

/// Provider for loading all themes from YAML files
@Riverpod(keepAlive: true)
Future<Map<String, ThemeMetadata>> allThemes(Ref ref) async {
  return await ThemeLoader.loadAllThemes();
}

/// Provider for getting AppColors based on current theme mode
/// Returns synchronous AppColors, using cached values or defaults while loading
@riverpod
AppColors appColors(Ref ref) {
  final themeModeAsync = ref.watch(themeModeProvider);
  final allThemesAsync = ref.watch(allThemesProvider);

  return allThemesAsync.when(
    data: (themes) {
      return themeModeAsync.when(
        data: (mode) {
          final themeKey = mode.name;
          final themeMetadata = themes[themeKey];

          if (themeMetadata != null) {
            return themeMetadata.colors;
          }

          // Fallback to Holon Light theme if not found
          final holonTheme = themes['holonLight'];
          return holonTheme?.colors ?? AppColors.light;
        },
        loading: () {
          final holonTheme = themes['holonLight'];
          return holonTheme?.colors ?? AppColors.light;
        },
        error: (_, __) {
          final holonTheme = themes['holonLight'];
          return holonTheme?.colors ?? AppColors.light;
        },
      );
    },
    loading: () => AppColors.light, // Default while loading themes
    error: (_, __) => AppColors.light, // Default on error
  );
}
