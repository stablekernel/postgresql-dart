import 'dart:collection';

import 'query.dart';

class PostgreSQLQueryResult extends UnmodifiableListView<PostgreSQLRow> {
  final PostgreSQLQueryMetaData metaData;

  PostgreSQLQueryResult(this.metaData, List<PostgreSQLRow> rows) : super(rows);
}

class PostgreSQLQueryMetaData {
  final List<FieldDescription> fieldDescriptions;
  List<String> _tableNames;

  PostgreSQLQueryMetaData({this.fieldDescriptions});

  List<String> get tableNames {
    _tableNames ??= fieldDescriptions
        .map((column) => column.resolvedTableName)
        .toSet()
        .toList();
    return _tableNames;
  }
}

class PostgreSQLRow extends UnmodifiableListView {
  final PostgreSQLQueryMetaData metaData;
  Map<String, Map<String, dynamic>> _tableMap;

  PostgreSQLRow(this.metaData, List columns) : super(columns);

  Map<String, Map<String, dynamic>> toTableMap() {
    if (_tableMap == null) {
      final columns = metaData.fieldDescriptions;
      _tableMap = Map<String, Map<String, dynamic>>.fromIterable(
          metaData.tableNames,
          key: (name) => name as String,
          value: (_) => <String, dynamic>{});

      final iterator = columns.iterator;
      forEach((column) {
        iterator.moveNext();
        _tableMap[iterator.current.resolvedTableName]
            [iterator.current.fieldName] = column;
      });
    }
    return _tableMap;
  }
}
