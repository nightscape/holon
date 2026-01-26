import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:pie_menu/pie_menu.dart';

import '../../models/layout_region.dart';
import '../../providers/settings_provider.dart' show appColorsProvider;
import '../../styles/app_styles.dart';
import '../settings_screen.dart';
import 'index_block_widget.dart';

/// Left sidebar widget - renders index document blocks for the left sidebar region.
///
/// This widget displays:
/// - Index blocks assigned to the left_sidebar region
/// - A bottom section with Settings and About buttons (always present)
class LeftSidebarWidget extends ConsumerWidget {
  /// Blocks to render in the sidebar.
  final List<IndexBlock> blocks;

  const LeftSidebarWidget({super.key, required this.blocks});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);

    return Material(
      color: colors.sidebarBackground,
      child: Container(
        decoration: BoxDecoration(
          border: Border(right: BorderSide(color: colors.border, width: 1)),
        ),
        child: Column(
          children: [
            // Index blocks content
            Expanded(
              child: blocks.isEmpty
                  ? _buildEmptyState(colors)
                  : ListView.builder(
                      padding: EdgeInsets.zero,
                      itemCount: blocks.length,
                      itemBuilder: (context, index) => IndexBlockWidget(
                        block: blocks[index],
                        isMainRegion: false,
                      ),
                    ),
            ),
            // Bottom section with Settings and About (always present)
            _buildBottomSection(context, colors),
          ],
        ),
      ),
    );
  }

  Widget _buildEmptyState(AppColors colors) {
    return ListView(
      padding: const EdgeInsets.all(20),
      children: [
        const SizedBox(height: 8),
        Text(
          'Favorites',
          style: TextStyle(
            fontSize: 12,
            fontWeight: FontWeight.w600,
            color: colors.textSecondary,
            letterSpacing: 0.5,
          ),
        ),
        const SizedBox(height: 16),
        Text(
          'Configure your sidebar in the index document',
          style: TextStyle(
            fontSize: 12,
            color: colors.textTertiary,
            fontStyle: FontStyle.italic,
          ),
        ),
      ],
    );
  }

  Widget _buildBottomSection(BuildContext context, AppColors colors) {
    return Container(
      decoration: BoxDecoration(
        border: Border(top: BorderSide(color: colors.border, width: 1)),
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
                builder: (context) => const SettingsScreen(),
              );
            },
          ),
          Divider(height: 1, indent: 20, endIndent: 20, color: colors.border),
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
                applicationIcon: const Icon(Icons.menu_book),
              );
            },
          ),
        ],
      ),
    );
  }
}

/// Right sidebar widget - renders index document blocks for the right sidebar region.
///
/// This widget displays context-sensitive content like:
/// - Quick actions
/// - Related documents
/// - Undo history
class RightSidebarWidget extends ConsumerWidget {
  /// Blocks to render in the right sidebar.
  final List<IndexBlock> blocks;

  const RightSidebarWidget({super.key, required this.blocks});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);

    return Material(
      color: colors.sidebarBackground,
      child: Container(
        decoration: BoxDecoration(
          border: Border(left: BorderSide(color: colors.border, width: 1)),
        ),
        child: blocks.isEmpty
            ? _buildEmptyState(colors)
            : ListView.builder(
                padding: const EdgeInsets.all(16),
                itemCount: blocks.length,
                itemBuilder: (context, index) =>
                    IndexBlockWidget(block: blocks[index], isMainRegion: false),
              ),
      ),
    );
  }

  Widget _buildEmptyState(AppColors colors) {
    return Center(
      child: Padding(
        padding: const EdgeInsets.all(20),
        child: Text(
          'Right sidebar content',
          style: TextStyle(
            fontSize: 12,
            color: colors.textTertiary,
            fontStyle: FontStyle.italic,
          ),
        ),
      ),
    );
  }
}

/// Main content widget - renders index document blocks for the main region.
///
/// This widget displays the primary content area, which can include:
/// - Dashboard views with multiple query blocks
/// - Journal/daily notes
/// - Document content
class MainContentWidget extends ConsumerWidget {
  /// Blocks to render in the main content area.
  final List<IndexBlock> blocks;

  const MainContentWidget({super.key, required this.blocks});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);

    return PieCanvas(
      child: blocks.isEmpty
          ? _buildEmptyState(colors)
          : ListView.builder(
              padding: const EdgeInsets.all(24),
              itemCount: blocks.length,
              itemBuilder: (context, index) =>
                  IndexBlockWidget(block: blocks[index], isMainRegion: true),
            ),
    );
  }

  Widget _buildEmptyState(AppColors colors) {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(Icons.dashboard_outlined, size: 48, color: colors.textTertiary),
          const SizedBox(height: 16),
          Text(
            'No content defined',
            style: TextStyle(fontSize: 16, color: colors.textSecondary),
          ),
          const SizedBox(height: 8),
          Text(
            'Configure your home screen in the index document',
            style: TextStyle(fontSize: 12, color: colors.textTertiary),
          ),
        ],
      ),
    );
  }
}
