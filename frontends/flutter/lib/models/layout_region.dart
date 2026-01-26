/// Layout regions for the document-driven home screen.
///
/// Blocks in index.org use the :REGION: property to specify
/// which region they should be rendered in.
enum LayoutRegion {
  leftSidebar,
  main,
  rightSidebar,
}

/// Extension to parse REGION property values from org-mode blocks.
extension LayoutRegionParsing on String {
  LayoutRegion? toLayoutRegion() {
    switch (toLowerCase().replaceAll('_', '').replaceAll('-', '')) {
      case 'leftsidebar':
      case 'left':
        return LayoutRegion.leftSidebar;
      case 'main':
      case 'center':
        return LayoutRegion.main;
      case 'rightsidebar':
      case 'right':
        return LayoutRegion.rightSidebar;
      default:
        return null;
    }
  }
}

/// Block view type from :VIEW: property in org-mode blocks.
enum BlockViewType {
  /// Contains a PRQL source block that should be executed and rendered.
  query,

  /// Navigation link to another document.
  link,

  /// Action button (capture, sync, settings, etc.).
  action,

  /// Plain text content.
  text,
}

/// Extension to parse VIEW property values from org-mode blocks.
extension BlockViewTypeParsing on String {
  BlockViewType toBlockViewType() {
    switch (toLowerCase()) {
      case 'query':
        return BlockViewType.query;
      case 'link':
        return BlockViewType.link;
      case 'action':
        return BlockViewType.action;
      default:
        return BlockViewType.text;
    }
  }
}

/// Parsed block from index.org with region and view information.
///
/// Represents a block that has been parsed from the index document
/// and is ready for rendering in the appropriate layout region.
class IndexBlock {
  /// Unique identifier for this block.
  final String id;

  /// Block title (headline text).
  final String title;

  /// Layout region where this block should be rendered.
  final LayoutRegion region;

  /// View type determining how this block should be rendered.
  final BlockViewType viewType;

  /// PRQL source code for query views.
  final String? prqlSource;

  /// Target document ID for link views.
  /// This is a Loro document identifier, not a file path.
  final String? targetDocumentId;

  /// Action name for action views (e.g., 'capture', 'sync', 'settings').
  final String? actionName;

  /// Child blocks (for nested structure).
  final List<IndexBlock> children;

  const IndexBlock({
    required this.id,
    required this.title,
    required this.region,
    required this.viewType,
    this.prqlSource,
    this.targetDocumentId,
    this.actionName,
    this.children = const [],
  });

  @override
  String toString() =>
      'IndexBlock(id: $id, title: $title, region: $region, viewType: $viewType)';
}

/// Parsed layout from index.org grouped by regions.
///
/// Contains all blocks from the index document organized by their
/// target layout region for efficient rendering.
class IndexLayout {
  /// Blocks to render in the left sidebar.
  final List<IndexBlock> leftSidebarBlocks;

  /// Blocks to render in the main content area.
  final List<IndexBlock> mainBlocks;

  /// Blocks to render in the right sidebar.
  final List<IndexBlock> rightSidebarBlocks;

  const IndexLayout({
    this.leftSidebarBlocks = const [],
    this.mainBlocks = const [],
    this.rightSidebarBlocks = const [],
  });

  /// Returns true if no blocks are defined in any region.
  bool get isEmpty =>
      leftSidebarBlocks.isEmpty &&
      mainBlocks.isEmpty &&
      rightSidebarBlocks.isEmpty;

  /// Returns true if at least one region has blocks.
  bool get isNotEmpty => !isEmpty;

  /// Returns true if the right sidebar has content.
  bool get hasRightSidebar => rightSidebarBlocks.isNotEmpty;

  @override
  String toString() =>
      'IndexLayout(left: ${leftSidebarBlocks.length}, main: ${mainBlocks.length}, right: ${rightSidebarBlocks.length})';
}
