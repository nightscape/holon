import 'package:dartproptest/dartproptest.dart';
import '../../lib/src/rust/api/types.dart'
    show
        MapChange,
        ChangeOrigin,
        RowChange_Created,
        RowChange_Updated,
        RowChange_Deleted;
import '../../lib/src/rust/third_party/query_render/types.dart'
    show Value, RenderSpec, RenderExpr, Arg, Value_String;
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show PlatformInt64Util;

/// Generate a random float between min and max
Generator<double> _floatGen(double min, double max) {
  return Gen.interval(0, 1000000).map((i) {
    final normalized = i / 1000000.0;
    return min + (normalized * (max - min));
  });
}

/// Generate a random boolean
Generator<bool> _boolGen() {
  return Gen.interval(0, 1).map((i) => i == 1);
}

/// Generate a map using key and value generators
Generator<Map<String, Value>> _mapGen(
  Generator<String> keyGen,
  Generator<Value> valueGen,
  int minLength,
  int maxLength,
) {
  return Gen.interval(minLength, maxLength).flatMap((length) {
    return Gen.array(
      keyGen.flatMap((key) => valueGen.map((value) => MapEntry(key, value))),
      minLength: length,
      maxLength: length,
    ).map((entries) {
      final map = <String, Value>{};
      for (final entry in entries) {
        map[entry.key] = entry.value;
      }
      return map;
    });
  });
}

/// Weighted selection helper - selects one of the generators based on weights
Generator<T> _weightedSelect<T>(List<(int weight, Generator<T> gen)> options) {
  final totalWeight = options.fold(0, (sum, opt) => sum + opt.$1);
  return Gen.interval(0, totalWeight - 1).flatMap((selected) {
    var current = 0;
    for (final (weight, gen) in options) {
      if (selected < current + weight) {
        return gen;
      }
      current += weight;
    }
    return options.last.$2;
  });
}

/// Generate a random MapChange event.
///
/// Creates either a created, updated, or deleted event with random data.
Generator<MapChange> rowChangeArbitrary() {
  return _weightedSelect([
    (1, _rowChangeCreatedArbitrary()),
    (1, _rowChangeUpdatedArbitrary()),
    (1, _rowChangeDeletedArbitrary()),
  ]);
}

Generator<MapChange> _rowChangeCreatedArbitrary() {
  return Gen.asciiString(minLength: 1, maxLength: 20).flatMap((id) {
    return valueMapArbitrary().flatMap((data) {
      return Gen.elementOf([ChangeOrigin.local, ChangeOrigin.remote]).map((
        origin,
      ) {
        final dataWithId = Map<String, Value>.from(data);
        dataWithId['id'] = Value.string(id);
        return MapChange.created(data: dataWithId, origin: origin);
      });
    });
  });
}

Generator<MapChange> _rowChangeUpdatedArbitrary() {
  return Gen.asciiString(minLength: 1, maxLength: 20).flatMap((id) {
    return valueMapArbitrary().flatMap((data) {
      return Gen.elementOf([ChangeOrigin.local, ChangeOrigin.remote]).map((
        origin,
      ) {
        final dataWithId = Map<String, Value>.from(data);
        dataWithId['id'] = Value.string(id);
        return MapChange.updated(id: id, data: dataWithId, origin: origin);
      });
    });
  });
}

Generator<MapChange> _rowChangeDeletedArbitrary() {
  return Gen.asciiString(minLength: 1, maxLength: 20).flatMap((id) {
    return Gen.elementOf([ChangeOrigin.local, ChangeOrigin.remote]).map((
      origin,
    ) {
      return MapChange.deleted(id: id, origin: origin);
    });
  });
}

/// Generate a random Map<String, Value> representing row data.
Generator<Map<String, Value>> valueMapArbitrary() {
  return _mapGen(
    Gen.asciiString(minLength: 1, maxLength: 15),
    valueArbitrary(),
    1,
    10,
  );
}

/// Generate a random Value (non-recursive version to avoid stack overflow).
Generator<Value> valueArbitrary() {
  return _weightedSelect([
    (
      5,
      Gen.asciiString(minLength: 0, maxLength: 50).map((s) => Value.string(s)),
    ),
    (
      3,
      Gen.interval(
        -1000,
        1000,
      ).map((i) => Value.integer(PlatformInt64Util.from(i))),
    ),
    (3, _floatGen(-1000.0, 1000.0).map((f) => Value.float(f))),
    (2, _boolGen().map((b) => Value.boolean(b))),
    (1, Gen.just(const Value.null_())),
    (
      1,
      Gen.asciiString(
        minLength: 1,
        maxLength: 20,
      ).map((s) => Value.reference(s)),
    ),
    // Removed recursive array and object to avoid stack overflow
    // Can be added back with depth limiting if needed
  ]);
}

/// Generate a random Value with limited recursion depth.
Generator<Value> valueArbitraryWithDepth([int depth = 2]) {
  if (depth <= 0) {
    // At max depth, only generate simple values
    return _weightedSelect([
      (
        3,
        Gen.asciiString(
          minLength: 0,
          maxLength: 50,
        ).map((s) => Value.string(s)),
      ),
      (
        2,
        Gen.interval(
          -1000,
          1000,
        ).map((i) => Value.integer(PlatformInt64Util.from(i))),
      ),
      (1, _boolGen().map((b) => Value.boolean(b))),
      (1, Gen.just(const Value.null_())),
    ]);
  }

  return _weightedSelect([
    (
      5,
      Gen.asciiString(minLength: 0, maxLength: 50).map((s) => Value.string(s)),
    ),
    (
      3,
      Gen.interval(
        -1000,
        1000,
      ).map((i) => Value.integer(PlatformInt64Util.from(i))),
    ),
    (3, _floatGen(-1000.0, 1000.0).map((f) => Value.float(f))),
    (2, _boolGen().map((b) => Value.boolean(b))),
    (1, Gen.just(const Value.null_())),
    (
      1,
      Gen.asciiString(
        minLength: 1,
        maxLength: 20,
      ).map((s) => Value.reference(s)),
    ),
    (
      1,
      Gen.array(
        valueArbitraryWithDepth(depth - 1),
        minLength: 0,
        maxLength: 3,
      ).map((l) => Value.array(l)),
    ),
    (1, _simpleValueMapArbitrary(depth - 1).map((m) => Value.object(m))),
  ]);
}

/// Generate a simple value map with limited depth.
Generator<Map<String, Value>> _simpleValueMapArbitrary([int depth = 1]) {
  return _mapGen(
    Gen.asciiString(minLength: 1, maxLength: 10),
    valueArbitraryWithDepth(depth),
    1,
    5,
  );
}

/// Generate a random list of MapChange events.
Generator<List<MapChange>> rowChangeListArbitrary({
  int minLength = 0,
  int maxLength = 20,
}) {
  return Gen.array(
    rowChangeArbitrary(),
    minLength: minLength,
    maxLength: maxLength,
  );
}

/// Generate a random RenderSpec.
///
/// Creates a simple RenderSpec with a columnRef root.
Generator<RenderSpec> renderSpecArbitrary() {
  return Gen.asciiString(minLength: 1, maxLength: 20).map((name) {
    final root = RenderExpr.columnRef(name: name.replaceAll(' ', '_'));

    return RenderSpec(
      root: root,
      nestedQueries: const [],
      operations: const {},
    );
  });
}

/// Generate a random RenderSpec with a list() function call.
Generator<RenderSpec> listRenderSpecArbitrary() {
  return Gen.asciiString(minLength: 1, maxLength: 20).map((columnName) {
    final root = RenderExpr.functionCall(
      name: 'list',
      args: [
        Arg(
          name: 'item_template',
          value: RenderExpr.columnRef(name: columnName.replaceAll(' ', '_')),
        ),
      ],
      operations: const [],
    );

    return RenderSpec(
      root: root,
      nestedQueries: const [],
      operations: const {},
    );
  });
}

/// Generate a random RenderSpec with an outline() function call.
Generator<RenderSpec> outlineRenderSpecArbitrary() {
  return Gen.just(
    RenderSpec(
      root: RenderExpr.functionCall(
        name: 'outline',
        args: [
          Arg(
            name: 'parent_id',
            value: const RenderExpr.columnRef(name: 'parent_id'),
          ),
          Arg(
            name: 'sortkey',
            value: const RenderExpr.columnRef(name: 'sort_key'),
          ),
          Arg(
            name: 'item_template',
            value: const RenderExpr.columnRef(name: 'content'),
          ),
        ],
        operations: const [],
      ),
      nestedQueries: const [],
      operations: const {},
    ),
  );
}

/// Generate a random list of row data maps.
Generator<List<Map<String, Value>>> rowDataListArbitrary({
  int minLength = 0,
  int maxLength = 20,
}) {
  return Gen.array(
    valueMapArbitrary(),
    minLength: minLength,
    maxLength: maxLength,
  );
}

/// Helper to extract ID from a MapChange event.
String? extractRowId(MapChange change) {
  switch (change) {
    case RowChange_Created(data: final data, origin: _):
      final idValue = data['id'];
      if (idValue is Value_String) {
        return idValue.field0;
      }
      return null;
    case RowChange_Updated(id: final id, data: _, origin: _):
      return id;
    case RowChange_Deleted(id: final id, origin: _):
      return id;
  }
}

/// Helper to extract data from a MapChange event.
Map<String, Value>? extractRowData(MapChange change) {
  switch (change) {
    case RowChange_Created(data: final data, origin: _):
      return data;
    case RowChange_Updated(id: _, data: final data, origin: _):
      return data;
    case RowChange_Deleted(id: _, origin: _):
      return null;
  }
}

/// Helper to create a MapChange.created event with specific data.
MapChange createRowChange({
  required String id,
  required Map<String, Value> data,
  ChangeOrigin origin = ChangeOrigin.local,
}) {
  // Ensure id is in the data map
  final dataWithId = Map<String, Value>.from(data);
  dataWithId['id'] = Value.string(id);

  return MapChange.created(data: dataWithId, origin: origin);
}

/// Helper to create a MapChange.updated event with specific data.
MapChange updateRowChange({
  required String id,
  required Map<String, Value> data,
  ChangeOrigin origin = ChangeOrigin.local,
}) {
  return MapChange.updated(id: id, data: data, origin: origin);
}

/// Helper to create a MapChange.deleted event.
MapChange deleteRowChange({
  required String id,
  ChangeOrigin origin = ChangeOrigin.local,
}) {
  return MapChange.deleted(id: id, origin: origin);
}
