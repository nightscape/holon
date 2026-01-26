import 'package:riverpod_annotation/riverpod_annotation.dart';

import '../models/layout_region.dart';
import '../src/rust/third_party/holon_api.dart';
import '../utils/log.dart';
import '../utils/value_converter.dart' show valueMapToDynamic;
import 'query_providers.dart';

part 'index_layout_provider.g.dart';

/// Provider for the parsed index document layout.
///
/// This provider:
/// 1. Queries blocks that have a REGION property
/// 2. Parses block properties (REGION, VIEW, TARGET, ACTION)
/// 3. Extracts PRQL source from source blocks
/// 4. Groups blocks by region into IndexLayout
///
/// When no blocks with REGION exist, returns an empty layout.
/// The UI should fall back to the legacy hard-coded layout in this case.
@Riverpod(keepAlive: true)
Future<IndexLayout> indexLayout(Ref ref) async {
  log.debug('[indexLayoutProvider] Querying blocks with REGION property');

  // Query blocks that have a REGION property
  // These blocks define the home screen layout
  final prql = '''
    from blocks
    filter s"properties LIKE '%REGION%'"
    render (list sortkey:id item_template:(render_block this))
  ''';

  final result = await ref.watch(queryResultByPrqlProvider(prql).future);
  final rows = result.initialData.map((row) => valueMapToDynamic(row)).toList();

  if (rows.isEmpty) {
    log.debug('[indexLayoutProvider] Index document is empty or not found');
    return const IndexLayout();
  }

  // Parse blocks and group by region
  final leftSidebarBlocks = <IndexBlock>[];
  final mainBlocks = <IndexBlock>[];
  final rightSidebarBlocks = <IndexBlock>[];

  for (final row in rows) {
    final block = _parseIndexBlock(row);
    if (block != null) {
      switch (block.region) {
        case LayoutRegion.leftSidebar:
          leftSidebarBlocks.add(block);
        case LayoutRegion.main:
          mainBlocks.add(block);
        case LayoutRegion.rightSidebar:
          rightSidebarBlocks.add(block);
      }
    }
  }

  log.debug(
    '[indexLayoutProvider] Parsed layout: left=${leftSidebarBlocks.length}, '
    'main=${mainBlocks.length}, right=${rightSidebarBlocks.length}',
  );

  return IndexLayout(
    leftSidebarBlocks: leftSidebarBlocks,
    mainBlocks: mainBlocks,
    rightSidebarBlocks: rightSidebarBlocks,
  );
}

/// Parse a row from the query into an IndexBlock.
///
/// Returns null if the block doesn't have a valid REGION property.
IndexBlock? _parseIndexBlock(Map<String, dynamic> row) {
  final id = row['id']?.toString();
  assert(id != null && id.isNotEmpty, 'Block must have an id, got row: $row');

  // Properties are already parsed as Map<String, Value> from Loro
  final propertiesRaw = row['properties'];
  Map<String, dynamic> properties;
  if (propertiesRaw == null) {
    properties = {};
  } else if (propertiesRaw is Map) {
    // Convert Value objects to dynamic
    properties = {};
    for (final entry in propertiesRaw.entries) {
      final key = entry.key.toString();
      final value = entry.value;
      if (value is Value) {
        properties[key] = _valueToString(value);
      } else {
        properties[key] = value?.toString();
      }
    }
  } else {
    throw StateError('properties must be null or Map, got: ${propertiesRaw.runtimeType}');
  }

  // Extract REGION property - blocks without REGION are skipped
  final regionStr = properties['REGION']?.toString() ??
      properties['region']?.toString() ??
      '';
  final region = regionStr.toLayoutRegion();
  if (region == null) {
    return null;
  }

  // Extract VIEW property
  final viewStr = properties['VIEW']?.toString() ??
      properties['view']?.toString() ??
      'text';
  final viewType = viewStr.toBlockViewType();

  // Extract title from content (first line)
  final content = row['content']?.toString() ?? '';
  final title = content.split('\n').first.trim();

  // Extract view-specific properties
  String? prqlSource;
  String? targetDocumentId;
  String? actionName;

  switch (viewType) {
    case BlockViewType.query:
      // For query blocks, get PRQL from content field (source blocks store code in content)
      final contentType = row['content_type']?.toString();
      final sourceLanguage = row['source_language']?.toString()?.toLowerCase();
      if (contentType == 'source' && sourceLanguage == 'prql') {
        prqlSource = row['content']?.toString();
      }
      break;
    case BlockViewType.link:
      targetDocumentId = properties['TARGET']?.toString() ??
          properties['target']?.toString();
      break;
    case BlockViewType.action:
      actionName = properties['ACTION']?.toString() ??
          properties['action']?.toString();
      break;
    case BlockViewType.text:
      break;
  }

  return IndexBlock(
    id: id!,
    title: title,
    region: region,
    viewType: viewType,
    prqlSource: prqlSource,
    targetDocumentId: targetDocumentId,
    actionName: actionName,
  );
}

/// Convert a holon_api Value to a string.
String? _valueToString(Value value) {
  return switch (value) {
    Value_String(:final field0) => field0,
    Value_Integer(:final field0) => field0.toString(),
    Value_Float(:final field0) => field0.toString(),
    Value_Boolean(:final field0) => field0.toString(),
    Value_DateTime(:final field0) => field0,
    Value_Json(:final field0) => field0,
    Value_Reference(:final field0) => field0,
    Value_Array(:final field0) => field0.toString(),
    Value_Object(:final field0) => field0.toString(),
    Value_Null() => null,
  };
}
