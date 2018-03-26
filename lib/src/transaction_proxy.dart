part of postgres.connection;

typedef Future<dynamic> _TransactionQuerySignature(
    PostgreSQLExecutionContext connection);

class _TransactionProxy implements PostgreSQLExecutionContext {
  _TransactionProxy(this.connection, this.executionBlock) {
    beginQuery = new Query<int>("BEGIN", {}, connection, this)
      ..onlyReturnAffectedRowCount = true;

    beginQuery.future
        .then(startTransaction)
        .catchError(handleTransactionQueryError);
  }

  Query<dynamic> beginQuery;
  Completer completer = new Completer();

  Future get future => completer.future;

  Query<dynamic> get pendingQuery {
    if (queryQueue.length > 0) {
      return queryQueue.first;
    }

    return null;
  }

  List<Query<dynamic>> queryQueue = [];
  PostgreSQLConnection connection;
  _TransactionQuerySignature executionBlock;

  Future commit() async {
    await execute("COMMIT");
  }

  Future<List<List<dynamic>>> query(String fmtString,
      {Map<String, dynamic> substitutionValues: null,
      bool allowReuse: true}) async {
    if (connection.isClosed) {
      throw new PostgreSQLException(
          "Attempting to execute query, but connection is not open.");
    }

    var query = new Query<List<List<dynamic>>>(
        fmtString, substitutionValues, connection, this);

    if (allowReuse) {
      query.statementIdentifier = connection._reuseIdentifierForQuery(query);
    }

    final rows = await enqueue(query);
    return rows.map((Iterable<dynamic> row) => row.toList()).toList();
  }

  Future<int> execute(String fmtString,
      {Map<String, dynamic> substitutionValues: null}) async {
    if (connection.isClosed) {
      throw new PostgreSQLException(
          "Attempting to execute query, but connection is not open.");
    }

    var query = new Query<int>(fmtString, substitutionValues, connection, this)
      ..onlyReturnAffectedRowCount = true;

    return enqueue(query);
  }

  void cancelTransaction({String reason: null}) {
    throw new _TransactionRollbackException(reason);
  }

  Future startTransaction(dynamic beginResults) async {
    var result;
    try {
      result = await executionBlock(this);
    } on _TransactionRollbackException catch (rollback) {
      queryQueue = [];
      await execute("ROLLBACK");
      completer.complete(new PostgreSQLRollback._(rollback.reason));
      return;
    } catch (e, st) {
      queryQueue = [];

      await execute("ROLLBACK");
      completer.completeError(e, st);
      return;
    }

    await execute("COMMIT");

    completer.complete(result);
  }

  Future handleTransactionQueryError(dynamic err) async {
  }

  Future<T> enqueue<T>(Query<T> query) async {
    queryQueue.add(query);
    connection._transitionToState(connection._connectionState.awake());

    var result = null;
    try {
      result = await query.future;

      connection._cacheQuery(query);
      queryQueue.remove(query);
    } catch (e, st) {
      queryQueue = [];

      await execute("ROLLBACK");
      completer.completeError(e, st);
      return null;
    }

    return result;
  }
}

/// Represents a rollback from a transaction.
///
/// If a transaction is cancelled using [PostgreSQLExecutionContext.cancelTransaction], the value of the [Future]
/// returned from [PostgreSQLConnection.transaction] will be an instance of this type. [reason] will be the [String]
/// value of the optional argument to [PostgreSQLExecutionContext.cancelTransaction].
class PostgreSQLRollback {
  PostgreSQLRollback._(this.reason);

  /// The reason the transaction was cancelled.
  String reason;
}
