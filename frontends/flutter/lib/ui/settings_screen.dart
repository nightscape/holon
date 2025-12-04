import 'package:flutter/material.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:flutter_hooks/flutter_hooks.dart';
import 'package:mix/mix.dart';
import 'package:file_picker/file_picker.dart';
import '../providers/settings_provider.dart';
import '../providers/ui_state_providers.dart';
import '../styles/app_styles.dart';

class SettingsScreen extends HookConsumerWidget {
  const SettingsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Use hooks for controllers
    final apiKeyController = useTextEditingController();
    final prqlQueryController = useTextEditingController();

    // Use provider for password visibility
    final isObscured = ref.watch(passwordVisibilityProvider);

    // Watch providers and sync controllers when they load
    final apiKeyAsync = ref.watch(todoistApiKeyProvider);
    final prqlQueryAsync = ref.watch(prqlQueryProvider);
    final themeModeAsync = ref.watch(themeModeProvider);
    final allThemesAsync = ref.watch(allThemesProvider);
    final orgModeRootDirAsync = ref.watch(orgModeRootDirectoryProvider);
    final colors = ref.watch(appColorsProvider);

    // Sync API key controller with provider value
    useEffect(() {
      apiKeyAsync.whenData((apiKey) {
        if (apiKeyController.text != apiKey) {
          apiKeyController.text = apiKey;
        }
      });
      return null;
    }, [apiKeyAsync]);

    // Sync PRQL query controller with provider value
    useEffect(() {
      prqlQueryAsync.whenData((query) {
        if (prqlQueryController.text != query) {
          prqlQueryController.text = query;
        }
      });
      return null;
    }, [prqlQueryAsync]);

    Future<void> saveApiKey() async {
      final apiKey = apiKeyController.text.trim();
      await setTodoistApiKey(ref, apiKey);
      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Todoist API key saved'),
            duration: Duration(seconds: 2),
          ),
        );
      }
    }

    Future<void> savePrqlQuery() async {
      final query = prqlQueryController.text.trim();
      await setPrqlQuery(ref, query);
      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('PRQL query saved. Changes are being applied...'),
            duration: Duration(seconds: 2),
          ),
        );
      }
    }

    return Dialog(
      backgroundColor: colors.background,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(AppRadius.lg),
      ),
      child: Box(
        style: dialogStyle(colors),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            // Dialog header
            Box(
              style: dialogHeaderStyle(colors),
              child: Row(
                children: [
                  StyledText('Settings', style: dialogTitleStyle(colors)),
                  const Spacer(),
                  IconButton(
                    icon: Icon(
                      Icons.close,
                      size: 20,
                      color: colors.textSecondary,
                    ),
                    onPressed: () => Navigator.of(context).pop(),
                    padding: EdgeInsets.zero,
                    constraints: const BoxConstraints(
                      minWidth: 32,
                      minHeight: 32,
                    ),
                  ),
                ],
              ),
            ),
            // Scrollable content
            Flexible(
              child: SingleChildScrollView(
                padding: const EdgeInsets.all(AppSpacing.lg),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    // Theme Section
                    StyledText('Theme', style: sectionTitleStyle(colors)),
                    vSpaceSm,
                    StyledText(
                      'Choose your preferred theme.',
                      style: sectionDescriptionStyle(colors),
                    ),
                    vSpaceLg,
                    themeModeAsync.when(
                      data: (currentMode) {
                        return allThemesAsync.when(
                          data: (themes) {
                            return DropdownButton<AppThemeMode>(
                              value: currentMode,
                              isExpanded: true,
                              items: AppThemeMode.values.map((mode) {
                                final themeMetadata = themes[mode.name];
                                final displayName =
                                    themeMetadata?.name ?? mode.name;
                                return DropdownMenuItem<AppThemeMode>(
                                  value: mode,
                                  child: Text(displayName),
                                );
                              }).toList(),
                              onChanged: (newMode) {
                                if (newMode != null) {
                                  setThemeMode(ref, newMode);
                                }
                              },
                            );
                          },
                          loading: () => const SizedBox(
                            height: 48,
                            child: Center(child: CircularProgressIndicator()),
                          ),
                          error: (error, stack) =>
                              Text('Error loading themes: $error'),
                        );
                      },
                      loading: () => const SizedBox(
                        height: 48,
                        child: Center(child: CircularProgressIndicator()),
                      ),
                      error: (error, stack) => Text('Error: $error'),
                    ),
                    vSpaceXxl,
                    const Divider(height: 1),
                    vSpaceLg,
                    // Todoist API Key Section
                    StyledText(
                      'Todoist API Key',
                      style: sectionTitleStyle(colors),
                    ),
                    vSpaceSm,
                    StyledText(
                      'Enter your Todoist API key to sync tasks. You can find your API key in Todoist Settings > Integrations.',
                      style: sectionDescriptionStyle(colors),
                    ),
                    vSpaceLg,
                    TextField(
                      controller: apiKeyController,
                      obscureText: isObscured,
                      decoration: inputDecoration(
                        colors: colors,
                        labelText: 'API Key',
                        hintText: 'Enter your Todoist API key',
                        suffixIcon: IconButton(
                          icon: Icon(
                            isObscured
                                ? Icons.visibility
                                : Icons.visibility_off,
                            color: colors.textSecondary,
                          ),
                          onPressed: () {
                            ref
                                .read(passwordVisibilityProvider.notifier)
                                .toggle();
                          },
                        ),
                      ),
                    ),
                    vSpaceLg,
                    // Buttons row - not stretched
                    Row(
                      mainAxisAlignment: MainAxisAlignment.start,
                      children: [
                        GestureDetector(
                          onTap: saveApiKey,
                          child: Box(
                            style: primaryButtonStyle(colors),
                            child: StyledText(
                              'Save API Key',
                              style: primaryButtonTextStyle(colors),
                            ),
                          ),
                        ),
                        hSpaceMd,
                        GestureDetector(
                          onTap: () {
                            apiKeyController.clear();
                          },
                          child: Box(
                            style: secondaryButtonStyle(colors),
                            child: StyledText(
                              'Clear',
                              style: secondaryButtonTextStyle(colors),
                            ),
                          ),
                        ),
                      ],
                    ),
                    vSpaceXxl,
                    const Divider(height: 1),
                    vSpaceLg,
                    // OrgMode Root Directory Section
                    StyledText(
                      'OrgMode Directory',
                      style: sectionTitleStyle(colors),
                    ),
                    vSpaceSm,
                    StyledText(
                      'Select the root directory containing your .org files. The directory will be scanned recursively.',
                      style: sectionDescriptionStyle(colors),
                    ),
                    vSpaceLg,
                    orgModeRootDirAsync.when(
                      data: (currentPath) {
                        return Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Container(
                              padding: const EdgeInsets.all(AppSpacing.md),
                              decoration: BoxDecoration(
                                color: colors.backgroundSecondary,
                                borderRadius: BorderRadius.circular(
                                  AppRadius.md,
                                ),
                                border: Border.all(color: colors.border),
                              ),
                              child: Row(
                                children: [
                                  Icon(
                                    Icons.folder_outlined,
                                    color: currentPath != null
                                        ? colors.primary
                                        : colors.textTertiary,
                                    size: 20,
                                  ),
                                  const SizedBox(width: AppSpacing.md),
                                  Expanded(
                                    child: Text(
                                      currentPath ?? 'No directory selected',
                                      style: TextStyle(
                                        color: currentPath != null
                                            ? colors.textPrimary
                                            : colors.textTertiary,
                                        fontSize: 14,
                                      ),
                                      overflow: TextOverflow.ellipsis,
                                    ),
                                  ),
                                ],
                              ),
                            ),
                            vSpaceLg,
                            Row(
                              mainAxisAlignment: MainAxisAlignment.start,
                              children: [
                                GestureDetector(
                                  onTap: () async {
                                    try {
                                      final result = await FilePicker.platform
                                          .getDirectoryPath(
                                            dialogTitle:
                                                'Select OrgMode Root Directory',
                                          );
                                      if (result != null) {
                                        await setOrgModeRootDirectory(
                                          ref,
                                          result,
                                        );
                                        if (context.mounted) {
                                          ScaffoldMessenger.of(
                                            context,
                                          ).showSnackBar(
                                            SnackBar(
                                              content: Text(
                                                'OrgMode directory set to: $result',
                                              ),
                                              duration: const Duration(
                                                seconds: 2,
                                              ),
                                            ),
                                          );
                                        }
                                      }
                                    } catch (e) {
                                      if (context.mounted) {
                                        ScaffoldMessenger.of(
                                          context,
                                        ).showSnackBar(
                                          SnackBar(
                                            content: Text(
                                              'Error selecting directory: $e',
                                            ),
                                            backgroundColor: Colors.red,
                                            duration: const Duration(
                                              seconds: 4,
                                            ),
                                          ),
                                        );
                                      }
                                    }
                                  },
                                  child: Box(
                                    style: primaryButtonStyle(colors),
                                    child: StyledText(
                                      currentPath != null
                                          ? 'Change Directory'
                                          : 'Select Directory',
                                      style: primaryButtonTextStyle(colors),
                                    ),
                                  ),
                                ),
                                if (currentPath != null) ...[
                                  hSpaceMd,
                                  GestureDetector(
                                    onTap: () async {
                                      await setOrgModeRootDirectory(ref, null);
                                      if (context.mounted) {
                                        ScaffoldMessenger.of(
                                          context,
                                        ).showSnackBar(
                                          const SnackBar(
                                            content: Text(
                                              'OrgMode directory cleared',
                                            ),
                                            duration: Duration(seconds: 2),
                                          ),
                                        );
                                      }
                                    },
                                    child: Box(
                                      style: secondaryButtonStyle(colors),
                                      child: StyledText(
                                        'Clear',
                                        style: secondaryButtonTextStyle(colors),
                                      ),
                                    ),
                                  ),
                                ],
                              ],
                            ),
                          ],
                        );
                      },
                      loading: () => const SizedBox(
                        height: 48,
                        child: Center(child: CircularProgressIndicator()),
                      ),
                      error: (error, stack) => Text('Error: $error'),
                    ),
                    vSpaceXxl,
                    const Divider(height: 1),
                    vSpaceLg,
                    // PRQL Query Section
                    StyledText('PRQL Query', style: sectionTitleStyle(colors)),
                    vSpaceSm,
                    StyledText(
                      'Configure the PRQL query used to fetch and render data. Changes are applied immediately.',
                      style: sectionDescriptionStyle(colors),
                    ),
                    vSpaceLg,
                    TextField(
                      controller: prqlQueryController,
                      maxLines: 15,
                      minLines: 10,
                      decoration: inputDecoration(
                        colors: colors,
                        labelText: 'PRQL Query',
                        hintText: 'Enter your PRQL query',
                        alignLabelWithHint: true,
                      ),
                      style: monospaceTextStyle,
                    ),
                    vSpaceLg,
                    // Buttons row - not stretched
                    Row(
                      mainAxisAlignment: MainAxisAlignment.start,
                      children: [
                        GestureDetector(
                          onTap: savePrqlQuery,
                          child: Box(
                            style: primaryButtonStyle(colors),
                            child: StyledText(
                              'Save PRQL Query',
                              style: primaryButtonTextStyle(colors),
                            ),
                          ),
                        ),
                        hSpaceMd,
                        GestureDetector(
                          onTap: () {
                            // Reset to default query
                            final defaultQuery = r"""
from todoist_tasks
select {
    id,
    content,
    completed,
    priority,
    due_date,
    project_id,
    parent_id,
    created_at
}
derive sort_key = id
render (tree parent_id:parent_id sortkey:sort_key item_template:(row (bullet this.*) (checkbox checked:this.completed) (editable_text content:this.content) (badge content:this.priority color:"cyan")))
""";
                            prqlQueryController.text = defaultQuery;
                          },
                          child: Box(
                            style: secondaryButtonStyle(colors),
                            child: StyledText(
                              'Reset to Default',
                              style: secondaryButtonTextStyle(colors),
                            ),
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
