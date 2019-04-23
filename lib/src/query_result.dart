import 'dart:collection';

import 'query.dart';

class PostgreSQLQueryResult extends UnmodifiableListView<PostgreSQLRow> {
  final List<FieldDescription> fieldDescriptions;

  PostgreSQLQueryResult(List<PostgreSQLRow> rows, {this.fieldDescriptions})
      : super(rows);
}

class PostgreSQLRow extends UnmodifiableListView {
  PostgreSQLRow(List columns) : super(columns);
}
