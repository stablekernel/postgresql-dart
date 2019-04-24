import 'dart:collection';

import 'query.dart';

export 'query.dart' show PostgreSQLResultColumn;

class PostgreSQLResult extends UnmodifiableListView<PostgreSQLResultRow> {
  final PostgreSQLResultMetaData metaData;

  PostgreSQLResult(this.metaData, List<PostgreSQLResultRow> rows) : super(rows);
}

class PostgreSQLResultMetaData {
  final List<PostgreSQLResultColumn> columns;
  List<String> _tableNames;

  PostgreSQLResultMetaData({this.columns});

  List<String> get tableNames {
    _tableNames ??=
        columns.map((column) => column.resolvedTableName).toSet().toList();
    return _tableNames;
  }
}

class PostgreSQLResultRow extends UnmodifiableListView {
  final PostgreSQLResultMetaData metaData;
  Map<String, Map<String, dynamic>> _tableMap;

  PostgreSQLResultRow(this.metaData, List columns) : super(columns);

  /// Returns a two-level map that on the first level contains the resolved
  /// table name, and on the second level the column name (or its alias).
  Map<String, Map<String, dynamic>> toTableMap() {
    if (_tableMap == null) {
      _tableMap = Map<String, Map<String, dynamic>>.fromIterable(
          metaData.tableNames,
          key: (name) => name as String,
          value: (_) => <String, dynamic>{});

      final iterator = metaData.columns.iterator;
      forEach((column) {
        iterator.moveNext();
        final col = iterator.current;
        _tableMap[col.resolvedTableName][col.name] = column;
      });
    }
    return _tableMap;
  }
}
