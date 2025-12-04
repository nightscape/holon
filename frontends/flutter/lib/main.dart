import 'package:flutter/foundation.dart' show kIsWeb;
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:pie_menu/pie_menu.dart';
import 'dart:io' show Platform; // For platform detection
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:flutter_hooks/flutter_hooks.dart';
import 'package:path_provider/path_provider.dart';
import 'package:path/path.dart' as path;
// bitsdojo_window is desktop-only (Windows, macOS, Linux), not available on Android/iOS/web
import 'package:bitsdojo_window/bitsdojo_window.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:macos_secure_bookmarks/macos_secure_bookmarks.dart';
import 'src/rust/frb_generated.dart' as frb;
import 'src/rust/api/ffi_bridge.dart' as ffi;
import 'render/reactive_query_widget.dart';
import 'package:mcp_toolkit/mcp_toolkit.dart';
import 'dart:async';
import 'ui/settings_screen.dart';
import 'providers/settings_provider.dart';
import 'providers/query_providers.dart';
import 'providers/ui_state_providers.dart';
import 'utils/value_converter.dart' show dynamicToValueMap;
import 'styles/app_styles.dart';
import 'styles/theme_loader.dart';
import 'render/wildcard_operations_widget.dart';
import 'render/search_select_overlay.dart';
import 'services/logging_service.dart';
import 'services/backend_service.dart';
import 'services/mock_backend_service.dart';
import 'services/mock_rust_api.dart';
import 'services/mcp_backend_wrapper.dart';
import 'services/mcp_ui_automation.dart';
import 'utils/log.dart';

/// Enable mock backend mode to run Flutter without native Rust libraries.
/// Run with: flutter run --dart-define=USE_MOCK_BACKEND=true
const useMockBackend = bool.fromEnvironment(
  'USE_MOCK_BACKEND',
  defaultValue: false,
);

Future<void> main() async {
  // Track whether runApp has been called to prevent multiple calls
  bool appStarted = false;
  Zone? appZone;

  runZonedGuarded(
    () async {
      appZone = Zone.current;
      try {
        // Initialize bindings INSIDE runZonedGuarded to ensure same zone
        WidgetsFlutterBinding.ensureInitialized();

        // Initialize OpenTelemetry logging (before Rust initialization)
        await LoggingService.initialize();

        // Send a test log to verify logging is working
        if (LoggingService.isInitialized) {
          log.info('Flutter app starting - logging initialized');
          // Force flush to ensure test log is sent immediately
          await Future.delayed(const Duration(milliseconds: 200));
          await LoggingService.flush();
        }

        MCPToolkitBinding.instance
          ..initialize() // Initializes the Toolkit
          ..initializeFlutterToolkit(); // Adds Flutter related methods to the MCP server

        // Initialize UI automation tools (semantics + coordinate tapping)
        McpUiAutomation.initialize();

        // Load settings from preferences (needed for both mock and real mode)
        final prefs = await SharedPreferences.getInstance();
        final themeModeString = prefs.getString('theme_mode');
        final initialThemeMode = themeModeString != null
            ? AppThemeMode.values.firstWhere(
                (mode) => mode.name == themeModeString,
                orElse: () => AppThemeMode.light,
              )
            : AppThemeMode.light;

        // Initialize Rust library (or mock for UI-only development)
        if (useMockBackend) {
          log.info('Using mock backend - no native Rust libraries loaded');
          final mockApi = MockRustLibApi();
          setupMockRustLibApi(mockApi);
          await frb.RustLib.init(api: mockApi);
          await MockBackendService.loadMockData();
        } else {
          await frb.RustLib.init();

          final todoistApiKey =
              prefs.getString('todoist_api_key') ?? ''; // Default fallback

          // On macOS, resolve security-scoped bookmark to restore sandbox access
          String? orgModeRootDirectory;
          if (!kIsWeb && Platform.isMacOS) {
            final bookmarkData = prefs.getString('orgmode_bookmark');
            if (bookmarkData != null && bookmarkData.isNotEmpty) {
              final secureBookmarks = SecureBookmarks();
              final resolvedFile = await secureBookmarks.resolveBookmark(
                bookmarkData,
              );
              await secureBookmarks.startAccessingSecurityScopedResource(
                resolvedFile,
              );
              orgModeRootDirectory = resolvedFile.path;
            }
          } else {
            orgModeRootDirectory = prefs.getString('orgmode_root_directory');
          }

          String dbPath;
          if (kIsWeb) {
            dbPath = "holon.db"; // In-memory or virtual FS on web
          } else {
            // Get application support directory for database storage
            final appSupportDir = await getApplicationSupportDirectory();
            dbPath = path.join(appSupportDir.path, 'holon.db');
            // Ensure the directory exists
            await appSupportDir.create(recursive: true);
          }

          // Build configuration map (e.g., API keys, paths)
          final config = <String, String>{};
          config['TODOIST_API_KEY'] = todoistApiKey;
          if (orgModeRootDirectory != null && orgModeRootDirectory.isNotEmpty) {
            config['ORGMODE_ROOT_DIRECTORY'] = orgModeRootDirectory;
          }

          // Initialize BackendEngine using DI (similar to launcher.rs)
          final engine = await ffi.initRenderEngine(
            dbPath: dbPath,
            config: config,
          );

          // Store engine in global variable to prevent FRB from disposing it when main() completes
          // This is CRITICAL to prevent "DroppableDisposedException" errors
          _globalEngine = engine;
        }

        // Preload themes before running app to prevent theme flash
        final preloadedThemes = await ThemeLoader.loadAllThemes();

        // Get the initial theme colors based on the preloaded theme mode
        final initialThemeMetadata = preloadedThemes[initialThemeMode.name];
        final initialColors = initialThemeMetadata?.colors ?? AppColors.light;

        appStarted = true;
        runApp(
          ProviderScope(
            // Disable automatic retry for all providers globally
            // Query errors (syntax, schema) won't resolve themselves - user must fix in settings
            retry: (retryCount, error) => null,
            overrides: [
              // In mock mode, use MockBackendService instead of RustBackendService
              // Still wrap with McpBackendWrapper to enable MCP tools
              if (useMockBackend)
                backendServiceProvider.overrideWithValue(
                  McpBackendWrapper(MockBackendService()),
                ),
              // Override allThemesProvider with preloaded themes to prevent flash
              // Using Future.value() ensures it resolves immediately (synchronously in next microtask)
              allThemesProvider.overrideWith(
                (ref) => Future.value(preloadedThemes),
              ),
              // Override themeModeProvider with preloaded preference to prevent flash
              themeModeProvider.overrideWith(
                (ref) => Future.value(initialThemeMode),
              ),
              // Override appColorsProvider to use preloaded data but still react to theme changes
              // Since we override the async providers with Future.value(), they resolve immediately
              // This ensures the correct theme is shown immediately without flash
              appColorsProvider.overrideWith((ref) {
                // Use the overridden providers which resolve immediately via Future.value()
                final themeModeAsync = ref.watch(themeModeProvider);
                final allThemesAsync = ref.watch(allThemesProvider);

                // Since we override with Future.value(), these should resolve immediately
                return allThemesAsync.when(
                  data: (themes) {
                    return themeModeAsync.when(
                      data: (mode) {
                        final themeKey = mode.name;
                        final themeMetadata = themes[themeKey];
                        return themeMetadata?.colors ?? initialColors;
                      },
                      loading: () =>
                          initialColors, // Fallback during brief loading
                      error: (_, __) => initialColors,
                    );
                  },
                  loading: () => initialColors, // Fallback during brief loading
                  error: (_, __) => initialColors,
                );
              }),
            ],
            child: const MyApp(),
          ),
        );

        // Configure window chrome using bitsdojo_window (desktop only)
        if (!kIsWeb &&
            (Platform.isWindows || Platform.isMacOS || Platform.isLinux)) {
          doWhenWindowReady(() {
            const initialSize = Size(1280, 720);
            appWindow.minSize = initialSize;
            appWindow.size = initialSize;
            appWindow.alignment = Alignment.center;
            appWindow.title = "Rusty Knowledge";
            appWindow.show();
          });
        }
      } catch (e, stackTrace) {
        // Log error before rethrowing so it gets caught by the zone error handler
        log.error(
          'Error during app initialization',
          error: e,
          stackTrace: stackTrace,
        );
        // Re-throw so the zone error handler can process it
        rethrow;
      }
    },
    (error, stack) {
      // You can place it in your error handling tool, or directly in the zone. The most important thing is to have it - otherwise the errors will not be captured and MCP server will not return error results.
      log.error('Zone error handler caught', error: error, stackTrace: stack);
      MCPToolkitBinding.instance.handleZoneError(error, stack);
      // Show error UI if app hasn't started yet
      // This ensures the app still renders something even if initialization fails
      if (!appStarted) {
        // Ensure bindings are initialized before running app in error handler
        // Use the captured zone to ensure we match where ensureInitialized was called
        final runErrorApp = () {
          WidgetsFlutterBinding.ensureInitialized();
          runApp(
            MaterialApp(
              home: Scaffold(
                body: Center(
                  child: SingleChildScrollView(
                    padding: EdgeInsets.all(AppSpacing.lg),
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        const Icon(Icons.error, color: Colors.red, size: 48),
                        const SizedBox(height: 16),
                        Text(
                          'Initialization Error',
                          style: ThemeData.light().textTheme.headlineSmall,
                        ),
                        const SizedBox(height: 8),
                        Text(
                          error.toString(),
                          style: ThemeData.light().textTheme.bodyMedium,
                          textAlign: TextAlign.center,
                        ),
                        const SizedBox(height: 16),
                        if (error.toString().contains('no such table: blocks'))
                          Padding(
                            padding: const EdgeInsets.all(16.0),
                            child: Column(
                              children: [
                                const Text(
                                  'Please configure your Todoist API key in Settings.',
                                  style: TextStyle(
                                    fontSize: 16,
                                    fontWeight: FontWeight.w500,
                                  ),
                                  textAlign: TextAlign.center,
                                ),
                                const SizedBox(height: 8),
                                ElevatedButton(
                                  onPressed: () {
                                    // This won't work here, but shows the intent
                                  },
                                  child: const Text('Open Settings'),
                                ),
                              ],
                            ),
                          ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
          );
        };

        if (appZone != null) {
          appZone!.run(runErrorApp);
        } else {
          runErrorApp();
        }
      }
    },
  );
}

// Global reference to keep the engine alive throughout the app's lifetime.
// This prevents Flutter Rust Bridge from disposing the engine when main() completes.
// CRITICAL: Without this, the engine gets disposed after main() returns, causing
// "DroppableDisposedException: Try to use RustArc after it has been disposed" errors.
// In mock mode, this is null and not used.
ffi.ArcBackendEngine? _globalEngine;

// Provider for BackendEngine (kept for backward compatibility with MainScreen).
// The engine is initialized in main() and stored in _globalEngine.
// Returns null in mock mode - callers should check before using.
final backendEngineProvider = Provider<ffi.ArcBackendEngine?>((ref) {
  return _globalEngine;
});

class MyApp extends ConsumerWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);
    final themeModeAsync = ref.watch(themeModeProvider);
    final allThemesAsync = ref.watch(allThemesProvider);
    final backendService = ref.read(backendServiceProvider);

    return PlatformMenuBar(
      menus: <PlatformMenuItem>[
        PlatformMenu(
          label: 'File',
          menus: <PlatformMenuItem>[
            if (PlatformProvidedMenuItem.hasMenu(
              PlatformProvidedMenuItemType.quit,
            ))
              const PlatformProvidedMenuItem(
                type: PlatformProvidedMenuItemType.quit,
              ),
          ],
        ),
        PlatformMenu(
          label: 'Help',
          menus: <PlatformMenuItem>[
            PlatformMenuItem(
              label: 'About Rusty Knowledge',
              onSelected: () {
                showAboutDialog(
                  context: context,
                  applicationName: 'Rusty Knowledge',
                  applicationVersion: '1.0.0',
                  applicationIcon: const Icon(Icons.info_outline),
                );
              },
            ),
          ],
        ),
      ],
      child: WindowBorder(
        color: colors.border,
        width: 1,
        child: Shortcuts(
          shortcuts: <LogicalKeySet, Intent>{
            // Undo: Ctrl+Z (Windows/Linux) or Cmd+Z (macOS)
            LogicalKeySet(
              Platform.isMacOS
                  ? LogicalKeyboardKey.meta
                  : LogicalKeyboardKey.control,
              LogicalKeyboardKey.keyZ,
            ): const UndoIntent(),
            // Redo: Ctrl+Shift+Z (Windows/Linux) or Cmd+Shift+Z (macOS)
            LogicalKeySet(
              Platform.isMacOS
                  ? LogicalKeyboardKey.meta
                  : LogicalKeyboardKey.control,
              LogicalKeyboardKey.shift,
              LogicalKeyboardKey.keyZ,
            ): const RedoIntent(),
            // Alternative redo: Ctrl+Y (Windows/Linux)
            if (!Platform.isMacOS)
              LogicalKeySet(
                LogicalKeyboardKey.control,
                LogicalKeyboardKey.keyY,
              ): const RedoIntent(),
          },
          child: Actions(
            actions: <Type, Action<Intent>>{
              UndoIntent: UndoAction(backendService),
              RedoIntent: RedoAction(backendService),
            },
            child: MaterialApp(
              title: 'Rusty Knowledge',
              debugShowCheckedModeBanner: false,
              theme: ThemeData(
                // LogSeq-style minimal theme
                colorScheme: allThemesAsync.when(
                  data: (themes) {
                    return themeModeAsync.when(
                      data: (mode) {
                        final themeMetadata = themes[mode.name];
                        final isDark = themeMetadata?.isDark ?? false;
                        return isDark
                            ? ColorScheme.dark(
                                primary: colors.primary,
                                surface: colors.background,
                                onSurface: colors.textPrimary,
                              )
                            : ColorScheme.light(
                                primary: colors.primary,
                                surface: colors.background,
                                onSurface: colors.textPrimary,
                              );
                      },
                      loading: () => ColorScheme.light(
                        primary: colors.primary,
                        surface: colors.background,
                        onSurface: colors.textPrimary,
                      ),
                      error: (_, __) => ColorScheme.light(
                        primary: colors.primary,
                        surface: colors.background,
                        onSurface: colors.textPrimary,
                      ),
                    );
                  },
                  loading: () => ColorScheme.light(
                    primary: colors.primary,
                    surface: colors.background,
                    onSurface: colors.textPrimary,
                  ),
                  error: (_, __) => ColorScheme.light(
                    primary: colors.primary,
                    surface: colors.background,
                    onSurface: colors.textPrimary,
                  ),
                ),
                scaffoldBackgroundColor: colors.background,
                useMaterial3: true,
                // LogSeq-style typography
                textTheme: TextTheme(
                  bodyLarge: TextStyle(
                    fontSize: AppTypography.fontSizeMd,
                    height: 1.5,
                    color: colors.textPrimary,
                    letterSpacing: 0,
                  ),
                  bodyMedium: TextStyle(
                    fontSize: AppTypography.fontSizeSm,
                    height: 1.5,
                    color: colors.textSecondary,
                    letterSpacing: 0,
                  ),
                ),
                // Minimal app bar
                appBarTheme: AppBarTheme(
                  backgroundColor: colors.background,
                  foregroundColor: colors.textPrimary,
                  elevation: 0,
                  centerTitle: false,
                  titleTextStyle: TextStyle(
                    fontSize: AppTypography.fontSizeLg,
                    fontWeight: FontWeight.w500,
                    color: colors.textPrimary,
                  ),
                ),
              ),
              home: const MainScreen(),
            ),
          ),
        ),
      ),
    );
  }
}

/// Intent for undo operation
class UndoIntent extends Intent {
  const UndoIntent();
}

/// Intent for redo operation
class RedoIntent extends Intent {
  const RedoIntent();
}

/// Action to handle undo
class UndoAction extends Action<UndoIntent> {
  final BackendService backendService;

  UndoAction(this.backendService);

  @override
  Future<void> invoke(UndoIntent intent) async {
    final canUndo = await backendService.canUndo();
    if (canUndo) {
      await backendService.undo();
    }
  }
}

/// Action to handle redo
class RedoAction extends Action<RedoIntent> {
  final BackendService backendService;

  RedoAction(this.backendService);

  @override
  Future<void> invoke(RedoIntent intent) async {
    final canRedo = await backendService.canRedo();
    if (canRedo) {
      await backendService.redo();
    }
  }
}

class MainScreen extends HookConsumerWidget {
  const MainScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Use hooks for controllers and focus nodes
    final searchController = useTextEditingController();
    final searchFocusNode = useFocusNode();

    // Watch providers for state
    final isSearchExpanded = ref.watch(searchExpandedProvider);

    // Watch backendEngineProvider to ensure engine stays alive
    ref.watch(backendEngineProvider);

    // Watch the query result provider which reactively executes PRQL query
    // Note: queryResultProvider already depends on prqlQueryProvider, so no manual invalidation needed
    final queryResult = ref.watch(queryResultProvider);
    final prqlQueryAsync = ref.watch(prqlQueryProvider);

    // Collapse search when focus is lost (only if empty)
    useEffect(() {
      void listener() {
        if (!searchFocusNode.hasFocus &&
            isSearchExpanded &&
            searchController.text.isEmpty) {
          ref.read(searchExpandedProvider.notifier).setExpanded(false);
        }
      }

      searchFocusNode.addListener(listener);
      return () => searchFocusNode.removeListener(listener);
    }, [searchFocusNode, isSearchExpanded, searchController]);

    // Create operation callback provider - uses backendService for abstraction
    final backendService = ref.read(backendServiceProvider);
    final operationCallback = useMemoized(() {
      return (
        String entityName,
        String operationName,
        Map<String, dynamic> params,
      ) async {
        try {
          log.debug(
            'Executing operation: entity=$entityName, op=$operationName, params=$params',
          );

          // Convert Dart params to Rust Value types
          final rustParams = dynamicToValueMap(params);

          // Execute the operation through BackendService (works in both real and mock mode)
          await backendService.executeOperation(
            entityName: entityName,
            opName: operationName,
            params: rustParams,
            traceContext: null,
          );

          log.debug('Operation "$operationName" executed successfully');
        } catch (e, stackTrace) {
          log.error(
            'Operation execution failed',
            error: e,
            stackTrace: stackTrace,
          );
          if (context.mounted) {
            final colors = ref.read(appColorsProvider);
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(
                content: Text('Operation failed: ${e.toString()}'),
                backgroundColor: colors.error,
              ),
            );
          }
        }
      };
    }, [backendService, context]);

    // Create scaffold key
    final scaffoldKey = useMemoized(() => GlobalKey<ScaffoldState>());

    // Track drawer open state - moved outside queryResult.when so sidebar always works
    final isDrawerOpen = useState(false);

    // Build the main content widget based on query state
    final mainContent = queryResult.when(
      data: (result) {
        final renderSpec = result.renderSpec;
        final initialData = ref.watch(transformedInitialDataProvider);
        final changeStream = result.changeStream;

        return ReactiveQueryWidget(
          sql: '', // Not used - we already have renderSpec and data
          params: const {},
          renderSpec: renderSpec,
          changeStream: changeStream,
          initialData: initialData,
          onOperation: operationCallback,
        );
      },
      loading: () {
        // Show more detailed loading state
        return Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              const CircularProgressIndicator(),
              const SizedBox(height: 16),
              Text(
                'Loading query...',
                style: Theme.of(context).textTheme.bodyMedium,
              ),
              const SizedBox(height: 8),
              Text(
                'PRQL Query: ${prqlQueryAsync.when(data: (_) => 'Ready', loading: () => 'Loading...', error: (e, _) => 'Error: $e')}',
                style: Theme.of(context).textTheme.bodySmall,
              ),
              const SizedBox(height: 8),
              Text(
                'Engine: ${_globalEngine != null ? 'Ready' : 'Not initialized'}',
                style: Theme.of(context).textTheme.bodySmall,
              ),
            ],
          ),
        );
      },
      error: (error, stack) => Center(
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(24.0),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              const Icon(Icons.error, color: Colors.red, size: 48),
              const SizedBox(height: 16),
              Text(
                'Error loading query',
                style: Theme.of(context).textTheme.headlineSmall,
              ),
              const SizedBox(height: 8),
              SelectableText(
                error.toString(),
                style: Theme.of(context).textTheme.bodyMedium,
                textAlign: TextAlign.center,
              ),
              const SizedBox(height: 24),
              Text(
                'Open the sidebar (≡) and go to Settings to fix the PRQL query.',
                style: Theme.of(
                  context,
                ).textTheme.bodyMedium?.copyWith(fontWeight: FontWeight.w500),
                textAlign: TextAlign.center,
              ),
            ],
          ),
        ),
      ),
    );

    return _buildScaffoldWithSidebar(
      context,
      ref,
      mainContent,
      searchController,
      searchFocusNode,
      isSearchExpanded,
      scaffoldKey,
      isDrawerOpen,
    );
  }

  Widget _buildScaffoldWithSidebar(
    BuildContext context,
    WidgetRef ref,
    Widget mainContent,
    TextEditingController searchController,
    FocusNode searchFocusNode,
    bool isSearchExpanded,
    GlobalKey<ScaffoldState> scaffoldKey,
    ValueNotifier<bool> isDrawerOpen,
  ) {
    const sidebarWidth = 280.0;

    final colors = ref.watch(appColorsProvider);

    return Scaffold(
      key: scaffoldKey,
      backgroundColor: colors.background,
      drawer: null, // Disable default drawer
      drawerEdgeDragWidth: 0, // Disable edge drag
      body: Column(
        children: [
          // Custom title bar with window controls
          WindowTitleBarBox(
            child: Stack(
              children: [
                // Sidebar title bar background (slides horizontally)
                AnimatedPositioned(
                  duration: const Duration(milliseconds: 250),
                  curve: Curves.easeInOut,
                  left: isDrawerOpen.value ? 0 : -sidebarWidth,
                  top: 0,
                  width: sidebarWidth,
                  height: TitleBarDimensions.titleBarHeight,
                  child: Container(
                    decoration: BoxDecoration(
                      color: colors.sidebarBackground,
                      border: Border(
                        bottom: BorderSide(color: colors.border, width: 1),
                        right: BorderSide(color: colors.border, width: 1),
                      ),
                    ),
                  ),
                ),
                // Main content title bar (shifts right when sidebar opens)
                AnimatedPositioned(
                  duration: const Duration(milliseconds: 250),
                  curve: Curves.easeInOut,
                  left: isDrawerOpen.value ? sidebarWidth : 0,
                  top: 0,
                  right: 0,
                  height: TitleBarDimensions.titleBarHeight,
                  child: Container(
                    decoration: BoxDecoration(
                      color: colors.background,
                      border: Border(
                        bottom: BorderSide(color: colors.border, width: 1),
                      ),
                    ),
                    child: Row(
                      children: [
                        Expanded(
                          child: MoveWindow(
                            child: Container(
                              padding: const EdgeInsets.symmetric(
                                horizontal: 16,
                              ),
                              child: Row(
                                crossAxisAlignment: CrossAxisAlignment.center,
                                children: [
                                  // Left padding for macOS window controls + hamburger button space
                                  SizedBox(
                                    width: !kIsWeb ? 72 + 32 + 16 : 32 + 16,
                                  ),
                                  // Spacer to push buttons to the right
                                  const Spacer(),
                                  // Search button with expandable search field
                                  _buildSearchField(
                                    ref,
                                    searchController,
                                    searchFocusNode,
                                    isSearchExpanded,
                                  ),
                                  const SizedBox(width: 8),
                                  // Wildcard operations widget (sync button, etc.)
                                  const WildcardOperationsWidget(),
                                  const SizedBox(width: 8),
                                ],
                              ),
                            ),
                          ),
                        ),
                        const WindowButtons(),
                      ],
                    ),
                  ),
                ),
                // Fixed hamburger menu button (doesn't move with sidebar)
                Positioned(
                  left: !kIsWeb ? 72 + 16 : 16,
                  top: 0,
                  height: TitleBarDimensions.titleBarHeight,
                  child: Center(
                    child: IconButton(
                      icon: Icon(
                        isDrawerOpen.value ? Icons.menu_open : Icons.menu,
                        size: TitleBarDimensions.hamburgerIconSize,
                        color: colors.textSecondary,
                      ),
                      onPressed: () {
                        isDrawerOpen.value = !isDrawerOpen.value;
                      },
                      padding: EdgeInsets.zero,
                      constraints: BoxConstraints(
                        minWidth: TitleBarDimensions.hamburgerButtonSize,
                        minHeight: TitleBarDimensions.hamburgerButtonSize,
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
          // Main body with sidebar and content
          Expanded(
            child: Stack(
              children: [
                // Sidebar panel - slides horizontally
                AnimatedPositioned(
                  duration: const Duration(milliseconds: 250),
                  curve: Curves.easeInOut,
                  left: isDrawerOpen.value ? 0 : -sidebarWidth,
                  top: 0,
                  bottom: 0,
                  width: sidebarWidth,
                  child: Material(
                    color: colors.sidebarBackground,
                    child: Container(
                      decoration: BoxDecoration(
                        border: Border(
                          right: BorderSide(color: colors.border, width: 1),
                        ),
                      ),
                      child: Column(
                        children: [
                          // Favorites section
                          Expanded(
                            child: ListView(
                              padding: EdgeInsets.zero,
                              children: [
                                const SizedBox(height: 8),
                                Padding(
                                  padding: const EdgeInsets.fromLTRB(
                                    20,
                                    8,
                                    20,
                                    4,
                                  ),
                                  child: Text(
                                    'Favorites',
                                    style: TextStyle(
                                      fontSize: 12,
                                      fontWeight: FontWeight.w600,
                                      color: colors.textSecondary,
                                      letterSpacing: 0.5,
                                    ),
                                  ),
                                ),
                                // TODO: Add favorite items
                              ],
                            ),
                          ),
                          // Bottom section with Settings and About
                          Container(
                            decoration: BoxDecoration(
                              border: Border(
                                top: BorderSide(color: colors.border, width: 1),
                              ),
                            ),
                            child: Column(
                              children: [
                                ListTile(
                                  contentPadding: const EdgeInsets.symmetric(
                                    horizontal: 20,
                                    vertical: 4,
                                  ),
                                  leading: Icon(
                                    Icons.settings_outlined,
                                    size: 20,
                                    color: colors.textSecondary,
                                  ),
                                  title: Text(
                                    'Settings',
                                    style: TextStyle(
                                      color: colors.textPrimary,
                                      fontSize: 14,
                                      fontWeight: FontWeight.w400,
                                    ),
                                  ),
                                  onTap: () {
                                    showDialog(
                                      context: context,
                                      builder: (context) =>
                                          const SettingsScreen(),
                                    );
                                  },
                                ),
                                Divider(
                                  height: 1,
                                  indent: 20,
                                  endIndent: 20,
                                  color: colors.border,
                                ),
                                ListTile(
                                  contentPadding: const EdgeInsets.symmetric(
                                    horizontal: 20,
                                    vertical: 4,
                                  ),
                                  leading: Icon(
                                    Icons.info_outline,
                                    size: 20,
                                    color: colors.textSecondary,
                                  ),
                                  title: Text(
                                    'About',
                                    style: TextStyle(
                                      color: colors.textPrimary,
                                      fontSize: 14,
                                      fontWeight: FontWeight.w400,
                                    ),
                                  ),
                                  onTap: () {
                                    showAboutDialog(
                                      context: context,
                                      applicationName: 'Rusty Knowledge',
                                      applicationVersion: '1.0.0',
                                      applicationIcon: const Icon(
                                        Icons.menu_book,
                                      ),
                                    );
                                  },
                                ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
                // Main content - shifts right when sidebar opens
                // PieCanvas wraps content to provide overlay context for all PieMenu widgets
                AnimatedPositioned(
                  duration: const Duration(milliseconds: 250),
                  curve: Curves.easeInOut,
                  left: isDrawerOpen.value ? sidebarWidth : 0,
                  top: 0,
                  right: 0,
                  bottom: 0,
                  child: Stack(
                    children: [
                      PieCanvas(child: mainContent),
                      const SearchSelectOverlay(),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildSearchField(
    WidgetRef ref,
    TextEditingController searchController,
    FocusNode searchFocusNode,
    bool isSearchExpanded,
  ) {
    final colors = ref.watch(appColorsProvider);

    return MouseRegion(
      onEnter: (_) {
        ref.read(searchExpandedProvider.notifier).setExpanded(true);
        searchFocusNode.requestFocus();
      },
      onExit: (_) {
        // Only collapse if not focused and search is empty
        if (!searchFocusNode.hasFocus && searchController.text.isEmpty) {
          ref.read(searchExpandedProvider.notifier).setExpanded(false);
        }
      },
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 200),
        curve: Curves.easeInOut,
        width: isSearchExpanded ? 240 : TitleBarDimensions.searchCollapsedWidth,
        height: TitleBarDimensions.searchFieldHeight,
        decoration: BoxDecoration(
          color: isSearchExpanded
              ? colors.backgroundSecondary
              : Colors.transparent,
          borderRadius: BorderRadius.circular(AppSpacing.md),
          border: Border.all(
            color: isSearchExpanded ? colors.border : Colors.transparent,
            width: 1,
          ),
        ),
        child: isSearchExpanded
            ? Row(
                children: [
                  Padding(
                    padding: const EdgeInsets.only(left: 10),
                    child: Icon(
                      Icons.search,
                      size: TitleBarDimensions.searchIconSize,
                      color: colors.textTertiary,
                    ),
                  ),
                  Expanded(
                    child: TextField(
                      controller: searchController,
                      focusNode: searchFocusNode,
                      onChanged: (value) {
                        ref.read(searchTextProvider.notifier).setText(value);
                      },
                      style: TextStyle(
                        fontSize: AppTypography.fontSizeXs + 1,
                        color: colors.textPrimary,
                      ),
                      decoration: InputDecoration(
                        hintText: 'Search...',
                        hintStyle: TextStyle(
                          fontSize: AppTypography.fontSizeXs + 1,
                          color: colors.textTertiary,
                        ),
                        border: InputBorder.none,
                        contentPadding: EdgeInsets.symmetric(
                          horizontal: AppSpacing.sm,
                          vertical: AppSpacing.xs + 2,
                        ),
                        isDense: true,
                      ),
                      onSubmitted: (value) {
                        ref.read(searchTextProvider.notifier).setText(value);
                      },
                    ),
                  ),
                  if (searchController.text.isNotEmpty)
                    IconButton(
                      icon: Icon(
                        Icons.clear,
                        size: TitleBarDimensions.clearButtonSize * 0.7,
                      ),
                      color: colors.textTertiary,
                      padding: EdgeInsets.zero,
                      constraints: BoxConstraints(
                        minWidth: TitleBarDimensions.clearButtonSize,
                        minHeight: TitleBarDimensions.clearButtonSize,
                      ),
                      onPressed: () {
                        searchController.clear();
                        ref.read(searchTextProvider.notifier).setText('');
                      },
                    ),
                ],
              )
            : Material(
                color: Colors.transparent,
                child: InkWell(
                  onTap: () {
                    ref.read(searchExpandedProvider.notifier).setExpanded(true);
                    searchFocusNode.requestFocus();
                  },
                  borderRadius: BorderRadius.circular(AppSpacing.md),
                  child: Container(
                    padding: EdgeInsets.all(
                      TitleBarDimensions.searchFieldPadding,
                    ),
                    child: Icon(
                      Icons.search,
                      size: TitleBarDimensions.searchIconSize,
                      color: colors.textSecondary,
                    ),
                  ),
                ),
              ),
      ),
    );
  }
}

// Custom window button colors matching the app theme
// final _buttonColors = WindowButtonColors(
//   iconNormal: const Color(0xFF1F2937),
//   mouseOver: const Color(0xFFF3F4F6),
//   mouseDown: const Color(0xFFE5E7EB),
//   iconMouseOver: const Color(0xFF1F2937),
//   iconMouseDown: const Color(0xFF1F2937),
// );

// final _closeButtonColors = WindowButtonColors(
//   mouseOver: const Color(0xFFEF4444),
//   mouseDown: const Color(0xFFDC2626),
//   iconNormal: const Color(0xFF1F2937),
//   iconMouseOver: Colors.white,
//   iconMouseDown: Colors.white,
// );

class WindowButtons extends StatelessWidget {
  const WindowButtons({super.key});

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        // MinimizeWindowButton(colors: _buttonColors),
        // MaximizeWindowButton(colors: _buttonColors),
        // CloseWindowButton(colors: _closeButtonColors),
      ],
    );
  }
}
