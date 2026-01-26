import 'package:flutter/foundation.dart' show kIsWeb;
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'dart:io' show Platform; // For platform detection
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:flutter_hooks/flutter_hooks.dart';
import 'package:path_provider/path_provider.dart';
import 'package:path/path.dart' as path;
import 'utils/window_utils.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:macos_secure_bookmarks/macos_secure_bookmarks.dart';
import 'src/rust/frb_generated.dart' as frb;
import 'src/rust/api/ffi_bridge.dart' as ffi;
import 'package:mcp_toolkit/mcp_toolkit.dart';
import 'dart:async';
import 'providers/settings_provider.dart';
import 'providers/query_providers.dart';
import 'providers/ui_state_providers.dart';
import 'providers/navigation_provider.dart';
import 'providers/index_layout_provider.dart';
import 'providers/widget_spec_provider.dart';
import 'models/layout_region.dart';
import 'src/rust/third_party/holon_api/widget_spec.dart';
import 'render/render_interpreter.dart';
import 'models/navigation_state.dart';
import 'ui/widgets/sidebar_widgets.dart';
import 'ui/widgets/document_view_widget.dart';
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
import 'utils/value_converter.dart' show valueMapToDynamic, dynamicToValueMap;

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

        // Initialize flutter_acrylic for transparent/blur window effects (desktop only)
        await initializeAcrylic();

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

          // Initialize FrontendSession using DI (similar to launcher.rs)
          // This creates a FrontendSession which guarantees all schema initialization
          // is complete before returning, preventing race conditions with initial_widget()
          final session = await ffi.initRenderEngine(
            dbPath: dbPath,
            config: config,
          );

          // Store session in global variable to prevent FRB from disposing it when main() completes
          // This is CRITICAL to prevent "DroppableDisposedException" errors
          _globalSession = session;
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
              // Don't override themeModeProvider - let it work normally so invalidation works
              // The initial load should be fast enough to prevent noticeable flash
              // Override appColorsProvider to use preloaded data but still react to theme changes
              appColorsProvider.overrideWith((ref) {
                // Watch the normal providers (not overridden) so invalidation works correctly
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
        configureDesktopWindow();

        // Apply acrylic/transparency effect (desktop only)
        await applyWindowEffect();
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

// Global reference to keep the session alive throughout the app's lifetime.
// This prevents Flutter Rust Bridge from disposing the session when main() completes.
// CRITICAL: Without this, the session gets disposed after main() returns, causing
// "DroppableDisposedException: Try to use RustArc after it has been disposed" errors.
// In mock mode, this is null and not used.
ffi.ArcFrontendSession? _globalSession;

// Provider for FrontendSession (kept for backward compatibility with MainScreen).
// The session is initialized in main() and stored in _globalSession.
// Returns null in mock mode - callers should check before using.
//
// Note: This provider exists to keep the session alive. All query operations
// should go through the FFI functions (query_and_watch, initial_widget, etc.)
// which internally use the global session.
final backendEngineProvider = Provider<ffi.ArcFrontendSession?>((ref) {
  return _globalSession;
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

    // Watch navigation state for document-driven layout
    final navigation = ref.watch(navigationProvider);
    final indexLayoutAsync = ref.watch(indexLayoutProvider);

    // Watch WidgetSpec for backend-driven UI
    final widgetSpecAsync = ref.watch(initialWidgetProvider);

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

    // Create scaffold key
    final scaffoldKey = useMemoized(() => GlobalKey<ScaffoldState>());

    // Track drawer open state
    final isDrawerOpen = useState(false);

    return _buildScaffoldWithSidebar(
      context,
      ref,
      searchController,
      searchFocusNode,
      isSearchExpanded,
      scaffoldKey,
      isDrawerOpen,
      navigation,
      indexLayoutAsync,
      widgetSpecAsync,
    );
  }

  Widget _buildScaffoldWithSidebar(
    BuildContext context,
    WidgetRef ref,
    TextEditingController searchController,
    FocusNode searchFocusNode,
    bool isSearchExpanded,
    GlobalKey<ScaffoldState> scaffoldKey,
    ValueNotifier<bool> isDrawerOpen,
    NavigationState navigation,
    AsyncValue<IndexLayout> indexLayoutAsync,
    AsyncValue<WidgetSpec> widgetSpecAsync,
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
                                    width:
                                        TitleBarDimensions
                                            .macOsWindowControlsWidth +
                                        32 +
                                        16,
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
                  left: TitleBarDimensions.macOsWindowControlsWidth + 16,
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
          // Conditional rendering based on navigation state and WidgetSpec
          Expanded(
            child: _buildBodyContent(
              context,
              ref,
              colors,
              sidebarWidth,
              isDrawerOpen,
              navigation,
              indexLayoutAsync,
              widgetSpecAsync,
            ),
          ),
        ],
      ),
    );
  }

  /// Builds the main body content based on navigation state and WidgetSpec.
  ///
  /// Three possible states:
  /// 1. Viewing a document - show DocumentViewWidget
  /// 2. WidgetSpec available - render dynamic layout from renderSpec
  /// 3. Loading/Error - show appropriate states
  Widget _buildBodyContent(
    BuildContext context,
    WidgetRef ref,
    AppColors colors,
    double sidebarWidth,
    ValueNotifier<bool> isDrawerOpen,
    NavigationState navigation,
    AsyncValue<IndexLayout> indexLayoutAsync,
    AsyncValue<WidgetSpec> widgetSpecAsync,
  ) {
    // Case 1: Viewing a specific document
    if (navigation.isViewingDocument) {
      return Stack(
        children: [
          // Left sidebar (from WidgetSpec if available)
          AnimatedPositioned(
            duration: const Duration(milliseconds: 250),
            curve: Curves.easeInOut,
            left: isDrawerOpen.value ? 0 : -sidebarWidth,
            top: 0,
            bottom: 0,
            width: sidebarWidth,
            child: _buildLeftSidebar(widgetSpecAsync, indexLayoutAsync, colors),
          ),
          // Document view - shifts right when sidebar opens
          AnimatedPositioned(
            duration: const Duration(milliseconds: 250),
            curve: Curves.easeInOut,
            left: isDrawerOpen.value ? sidebarWidth : 0,
            top: 0,
            right: 0,
            bottom: 0,
            child: Stack(
              children: [
                DocumentViewWidget(documentId: navigation.currentDocumentId!),
                const SearchSelectOverlay(),
              ],
            ),
          ),
        ],
      );
    }

    // Case 2: Use WidgetSpec for dynamic layout
    // columns() at root level handles sidebar positioning via isScreenLayout
    return widgetSpecAsync.when(
      data: (widgetSpec) {
        return Stack(
          children: [
            _buildDynamicLayout(
              context,
              ref,
              widgetSpec,
              colors,
              drawerState: isDrawerOpen,
              sidebarWidth: sidebarWidth,
            ),
            const SearchSelectOverlay(),
          ],
        );
      },
      loading: () => _buildLoadingLayout(context, colors, sidebarWidth, isDrawerOpen),
      error: (error, stack) => _buildErrorLayout(
        context,
        colors,
        sidebarWidth,
        isDrawerOpen,
        error,
      ),
    );
  }

  /// Build left sidebar from WidgetSpec or fallback to index layout
  Widget _buildLeftSidebar(
    AsyncValue<WidgetSpec> widgetSpecAsync,
    AsyncValue<IndexLayout> indexLayoutAsync,
    AppColors colors,
  ) {
    // Left sidebar is now handled dynamically by renderSpec
    // Fall back to index layout sidebar for now
    return LeftSidebarWidget(
      blocks: indexLayoutAsync.maybeWhen(
        data: (layout) => layout.leftSidebarBlocks,
        orElse: () => const [],
      ),
    );
  }

  /// Build dynamic layout from WidgetSpec's renderSpec
  Widget _buildDynamicLayout(
    BuildContext context,
    WidgetRef ref,
    WidgetSpec widgetSpec,
    AppColors colors, {
    ValueNotifier<bool>? drawerState,
    double? sidebarWidth,
  }) {
    final renderSpec = widgetSpec.renderSpec;
    final backendService = ref.read(backendServiceProvider);

    // Convert data from List<Map<String, Value>> to rowCache Map<String, Map<String, dynamic>>
    final Map<String, Map<String, dynamic>> rowCache = {};
    for (final row in widgetSpec.data) {
      final convertedRow = valueMapToDynamic(row);
      final id = convertedRow['id']?.toString();
      if (id != null) {
        rowCache[id] = convertedRow;
      }
    }

    log.info('[_buildDynamicLayout] widgetSpec.data.length=${widgetSpec.data.length}, rowCache.length=${rowCache.length}');
    log.info('[_buildDynamicLayout] renderSpec.root=${renderSpec.root}');
    if (widgetSpec.data.isNotEmpty) {
      log.info('[_buildDynamicLayout] First row keys: ${widgetSpec.data.first.keys.toList()}');
    }

    // Create render context with the data
    final renderContext = RenderContext(
      rowData: rowCache.isNotEmpty ? rowCache.values.first : const {},
      rowTemplates: renderSpec.rowTemplates,
      rowCache: rowCache,
      colors: colors,
      onOperation: (entityName, opName, params) async {
        await backendService.executeOperation(
          entityName: entityName,
          opName: opName,
          params: dynamicToValueMap(params),
        );
      },
      isScreenLayout: drawerState != null,
      drawerState: drawerState,
      sidebarWidth: sidebarWidth,
    );

    // Use RenderInterpreter to build the widget tree
    final interpreter = RenderInterpreter();
    return interpreter.build(renderSpec.root, renderContext);
  }

  /// Build loading layout
  Widget _buildLoadingLayout(
    BuildContext context,
    AppColors colors,
    double sidebarWidth,
    ValueNotifier<bool> isDrawerOpen,
  ) {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          const CircularProgressIndicator(),
          const SizedBox(height: 16),
          Text(
            'Loading application...',
            style: Theme.of(context).textTheme.bodyMedium,
          ),
        ],
      ),
    );
  }

  /// Build error layout
  Widget _buildErrorLayout(
    BuildContext context,
    AppColors colors,
    double sidebarWidth,
    ValueNotifier<bool> isDrawerOpen,
    Object error,
  ) {
    return Center(
      child: SingleChildScrollView(
        padding: const EdgeInsets.all(24.0),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            const Icon(Icons.error, color: Colors.red, size: 48),
            const SizedBox(height: 16),
            Text(
              'Error loading application',
              style: Theme.of(context).textTheme.headlineSmall,
            ),
            const SizedBox(height: 8),
            SelectableText(
              error.toString(),
              style: Theme.of(context).textTheme.bodyMedium,
              textAlign: TextAlign.center,
            ),
          ],
        ),
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
