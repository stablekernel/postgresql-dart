import 'dart:async';

import 'dart:io';
import 'package:postgres/src/client_messages.dart';
import 'package:postgres/src/exceptions.dart';
import 'package:postgres/src/message_window.dart';
import 'package:postgres/src/query.dart';
import 'package:postgres/src/server_messages.dart';

abstract class PostgreSQLExecutionContext {
  /// Executes a query on this context.
  ///
  /// This method sends the query described by [fmtString] to the database and returns a [Future] whose value is the returned rows from the query after the query completes.
  /// The format string may contain parameters that are provided in [substitutionValues]. Parameters are prefixed with the '@' character. Keys to replace the parameters
  /// do not include the '@' character. For example:
  ///
  ///         connection.query("SELECT * FROM table WHERE id = @idParam", {"idParam" : 2});
  ///
  /// The type of the value is inferred by default, but can be made more specific by adding ':type" to the parameter pattern in the format string. The possible values
  /// are declared as static variables in [PostgreSQLCodec] (e.g., [PostgreSQLCodec.TypeInt4]). For example:
  ///
  ///         connection.query("SELECT * FROM table WHERE id = @idParam:int4", {"idParam" : 2});
  ///
  /// You may also use [PostgreSQLFormat.id] to create parameter patterns.
  ///
  /// If successful, the returned [Future] completes with a [List] of rows. Each is row is represented by a [List] of column values for that row that were returned by the query.
  ///
  /// By default, instances of this class will reuse queries. This allows significantly more efficient transport to and from the database. You do not have to do
  /// anything to opt in to this behavior, this connection will track the necessary information required to reuse queries without intervention. (The [fmtString] is
  /// the unique identifier to look up reuse information.) You can disable reuse by passing false for [allowReuse].
  Future<List<List<dynamic>>> query(String fmtString,
      {Map<String, dynamic> substitutionValues: null, bool allowReuse: true});

  /// Executes a query on this context.
  ///
  /// This method sends a SQL string to the database this instance is connected to. Parameters can be provided in [fmtString], see [query] for more details.
  ///
  /// This method returns the number of rows affected and no additional information. This method uses the least efficient and less secure command
  /// for executing queries in the PostgreSQL protocol; [query] is preferred for queries that will be executed more than once, will contain user input,
  /// or return rows.
  Future<int> execute(String fmtString,
      {Map<String, dynamic> substitutionValues: null});

  /// Cancels a transaction on this context.
  ///
  /// If this context is an instance of [PostgreSQLConnection], this method has no effect. If the context is a transaction context (passed as the argument
  /// to [PostgreSQLConnection.transaction]), this will rollback the transaction.
  void cancelTransaction({String reason: null});
}

/// Instances of this class connect to and communicate with a PostgreSQL database.
///
/// The primary type of this library, a connection is responsible for connecting to databases and executing queries.
/// A connection may be opened with [open] after it is created.
class PostgreSQLConnection implements PostgreSQLExecutionContext {
  /// Creates an instance of [PostgreSQLConnection].
  ///
  /// [host] must be a hostname, e.g. "foobar.com" or IP address. Do not include scheme or port.
  /// [port] is the port to connect to the database on. It is typically 5432 for default PostgreSQL settings.
  /// [databaseName] is the name of the database to connect to.
  /// [username] and [password] are optional if the database requires user authentication.
  /// [timeoutInSeconds] refers to the amount of time [PostgreSQLConnection] will wait while establishing a connection before it gives up.
  /// [timeZone] is the timezone the connection is in. Defaults to 'UTC'.
  /// [useSSL] when true, uses a secure socket when connecting to a PostgreSQL database.
  PostgreSQLConnection(this.host, this.port, this.databaseName,
      {this.username: null,
      this.password: null,
      this.timeoutInSeconds: 30,
      this.timeZone: "UTC",
      this.useSSL: false}) {
    _connectionState = new PostgreSQLConnectionStateClosed();
    _connectionState.connection = this;
  }

  // Add flag for debugging that captures stack trace prior to execution

  /// Hostname of database this connection refers to.
  String host;

  /// Port of database this connection refers to.
  int port;

  /// Name of database this connection refers to.
  String databaseName;

  /// Username for authenticating this connection.
  String username;

  /// Password for authenticating this connection.
  String password;

  /// Whether or not this connection should connect securely.
  bool useSSL;

  /// The amount of time this connection will wait during connecting before giving up.
  int timeoutInSeconds;

  /// The timezone of this connection for date operations that don't specify a timezone.
  String timeZone;

  /// Whether or not this connection is open or not.
  ///
  /// This is [true] when this instance is first created and after it has been closed or encountered an unrecoverable error.
  /// If a connection has already been opened and this value is now true, the connection cannot be reopened and a new instance
  /// must be created.
  bool get isClosed => _connectionState is PostgreSQLConnectionStateClosed;

  /// Settings values from the connected database.
  ///
  /// After connecting to a database, this map will contain the settings values that the database returns.
  /// Prior to connection, it is the empty map.
  Map<String, String> settings = {};

  Socket _socket;
  MessageFramer _framer = new MessageFramer();

  Map<String, QueryCache> _reuseMap = {};
  int _reuseCounter = 0;

  int _processID;
  int _secretKey;
  List<int> _salt;

  bool _hasConnectedPreviously = false;
  PostgreSQLConnectionState _connectionState;

  List<Query> _queryQueue = [];

  Query get _pendingQuery {
    if (_queryQueue.isEmpty) {
      return null;
    }
    return _queryQueue.first;
  }

  /// Establishes a connection with a PostgreSQL database.
  ///
  /// This method will return a [Future] that completes when the connection is established. Queries can be executed
  /// on this connection afterwards. If the connection fails to be established for any reason - including authentication -
  /// the returned [Future] will return with an error.
  ///
  /// Connections may not be reopened after they are closed or opened more than once. If a connection has already been opened and this method is called, an exception will be thrown.
  Future open() async {
    if (_hasConnectedPreviously) {
      throw new PostgreSQLException(
          "Attempting to reopen a closed connection. Create a new instance instead.");
    }

    _hasConnectedPreviously = true;

    if (useSSL) {
      _socket = await SecureSocket
          .connect(host, port)
          .timeout(new Duration(seconds: timeoutInSeconds));
    } else {
      _socket = await Socket
          .connect(host, port)
          .timeout(new Duration(seconds: timeoutInSeconds));
    }

    _framer = new MessageFramer();
    _socket.listen(_readData,
        onError: _handleSocketError, onDone: _handleSocketClosed);

    var connectionComplete = new Completer();
    _transitionToState(
        new PostgreSQLConnectionStateSocketConnected(connectionComplete));

    return connectionComplete.future
        .timeout(new Duration(seconds: timeoutInSeconds), onTimeout: () {
      _connectionState = new PostgreSQLConnectionStateClosed();
      _socket?.destroy();

      _cancelCurrentQueries();
      throw new PostgreSQLException(
          "Timed out trying to connect to database postgres://$host:$port/$databaseName.");
    });
  }

  /// Closes a connection.
  ///
  /// After the returned [Future] completes, this connection can no longer be used to execute queries. Any queries in progress or queued are cancelled.
  Future close() async {
    _connectionState = new PostgreSQLConnectionStateClosed();

    await _socket?.close();

    _cancelCurrentQueries();
  }

  /// Executes a query on this connection.
  ///
  /// This method sends the query described by [fmtString] to the database and returns a [Future] whose value returned rows from the query after the query completes.
  /// The format string may contain parameters that are provided in [substitutionValues]. Parameters are prefixed with the '@' character. Keys to replace the parameters
  /// do not include the '@' character. For example:
  ///
  ///         connection.query("SELECT * FROM table WHERE id = @idParam", {"idParam" : 2});
  ///
  /// The type of the value is inferred by default, but can be made more specific by adding ':type" to the parameter pattern in the format string. The possible values
  /// are declared as static variables in [PostgreSQLCodec] (e.g., [PostgreSQLCodec.TypeInt4]). For example:
  ///
  ///         connection.query("SELECT * FROM table WHERE id = @idParam:int4", {"idParam" : 2});
  ///
  /// You may also use [PostgreSQLFormat.id] to create parameter patterns.
  ///
  /// If successful, the returned [Future] completes with a [List] of rows. Each is row is represented by a [List] of column values for that row that were returned by the query.
  ///
  /// By default, instances of this class will reuse queries. This allows significantly more efficient transport to and from the database. You do not have to do
  /// anything to opt in to this behavior, this connection will track the necessary information required to reuse queries without intervention. (The [fmtString] is
  /// the unique identifier to look up reuse information.) You can disable reuse by passing false for [allowReuse].
  ///
  Future<List<List<dynamic>>> query(String fmtString,
      {Map<String, dynamic> substitutionValues: null,
      bool allowReuse: true}) async {
    if (isClosed) {
      throw new PostgreSQLException(
          "Attempting to execute query, but connection is not open.");
    }

    var query = new Query<List<List<dynamic>>>(
        fmtString, substitutionValues, this, null);
    if (allowReuse) {
      query.statementIdentifier = _reuseIdentifierForQuery(query);
    }

    return await _enqueue(query);
  }

  /// Executes a query on this connection.
  ///
  /// This method sends a SQL string to the database this instance is connected to. Parameters can be provided in [fmtString], see [query] for more details.
  ///
  /// This method returns the number of rows affected and no additional information. This method uses the least efficient and less secure command
  /// for executing queries in the PostgreSQL protocol; [query] is preferred for queries that will be executed more than once, will contain user input,
  /// or return rows.
  Future<int> execute(String fmtString,
      {Map<String, dynamic> substitutionValues: null}) async {
    if (isClosed) {
      throw new PostgreSQLException(
          "Attempting to execute query, but connection is not open.");
    }

    var query = new Query<int>(fmtString, substitutionValues, this, null)
      ..onlyReturnAffectedRowCount = true;

    return await _enqueue(query);
  }

  /// Executes a series of queries inside a transaction on this connection.
  ///
  /// Queries executed inside [queryBlock] will be grouped together in a transaction. The return value of the [queryBlock]
  /// will be the wrapped in the [Future] returned by this method if the transaction completes successfully.
  ///
  /// If a query or execution fails - for any reason - within a transaction block,
  /// the transaction will fail and previous statements within the transaction will not be committed. The [Future]
  /// returned from this method will be completed with the error from the first failing query.
  ///
  /// Do not catch exceptions within a transaction block, as it will prevent the transaction exception handler from fulfilling a
  /// transaction.
  ///
  /// Transactions may be cancelled by issuing [PostgreSQLExecutionContext.cancelTransaction]
  /// within the transaction. This will cause this method to return a [Future] with a value of [PostgreSQLRollback]. This method does not throw an exception
  /// if the transaction is cancelled in this way.
  ///
  /// All queries within a transaction block must be executed using the [PostgreSQLExecutionContext] passed into the [queryBlock].
  /// You must not issue queries to the receiver of this method from within the [queryBlock], otherwise the connection will deadlock.
  ///
  /// Queries within a transaction may be executed asynchronously or be awaited on. The order is still guaranteed. Example:
  ///
  ///         connection.transaction((ctx) {
  ///           var rows = await ctx.query("SELECT id FROM t);
  ///           if (!rows.contains([2])) {
  ///             ctx.query("INSERT INTO t (id) VALUES (2)");
  ///           }
  ///         });
  Future<dynamic> transaction(
      Future<dynamic> queryBlock(PostgreSQLExecutionContext connection)) async {
    if (isClosed) {
      throw new PostgreSQLException(
          "Attempting to execute query, but connection is not open.");
    }

    var proxy = new TransactionProxy(this, queryBlock);

    await _enqueue(proxy.beginQuery);

    return await proxy.completer.future;
  }

  void cancelTransaction({String reason: null}) {
    // We aren't in a transaction if sent to PostgreSQLConnection instances, so this is a no-op.
  }

  ////////

  Future<dynamic> _enqueue(Query query) async {
    _queryQueue.add(query);
    _transitionToState(_connectionState.awake());

    var result = null;
    try {
      result = await query.future;

      _cacheQuery(query);
      _queryQueue.remove(query);
    } catch (e) {
      _cacheQuery(query);
      _queryQueue.remove(query);
      rethrow;
    }

    return result;
  }

  void _cancelCurrentQueries() {
    var queries = _queryQueue;
    _queryQueue = [];

    // We need to jump this to the next event so that the queries
    // get the error and not the close message, since completeError is
    // synchronous.
    scheduleMicrotask(() {
      var exception =
          new PostgreSQLException("Connection closed or query cancelled.");
      queries?.forEach((q) {
        q.completeError(exception);
      });
    });
  }

  void _transitionToState(PostgreSQLConnectionState newState) {
    if (identical(newState, _connectionState)) {
      return;
    }

    _connectionState.onExit();

    _connectionState = newState;
    _connectionState.connection = this;

    _connectionState = _connectionState.onEnter();
    _connectionState.connection = this;
  }

  void _readData(List<int> bytes) {
    // Note that the way this method works, if a query is in-flight, and we move to the closed state
    // manually, the delivery of the bytes from the socket is sent to the 'Closed State',
    // and the state node managing delivering data to the query no longer exists. Therefore,
    // as soon as a close occurs, we detach the data stream from anything that actually does
    // anything with that data.
    _framer.addBytes(bytes);

    while (_framer.hasMessage) {
      var msg = _framer.popMessage().message;

      try {
        if (msg is ErrorResponseMessage) {
          _transitionToState(_connectionState.onErrorResponse(msg));
        } else {
          _transitionToState(_connectionState.onMessage(msg));
        }
      } catch (e) {
        _handleSocketError(e);
      }
    }
  }

  void _handleSocketError(dynamic error) {
    _connectionState = new PostgreSQLConnectionStateClosed();
    _socket.destroy();

    _cancelCurrentQueries();
  }

  void _handleSocketClosed() {
    _connectionState = new PostgreSQLConnectionStateClosed();

    _cancelCurrentQueries();
  }

  void _cacheQuery(Query query) {
    if (query.cache == null) {
      return;
    }

    if (query.cache.isValid) {
      _reuseMap[query.statement] = query.cache;
    }
  }

  QueryCache _cachedQuery(String statementIdentifier) {
    if (statementIdentifier == null) {
      return null;
    }

    return _reuseMap[statementIdentifier];
  }

  String _reuseIdentifierForQuery(Query q) {
    var existing = _reuseMap[q.statement];
    if (existing != null) {
      return existing.preparedStatementName;
    }

    var string = "$_reuseCounter".padLeft(12, "0");

    _reuseCounter++;

    return string;
  }
}

class TransactionRollbackException implements Exception {
  TransactionRollbackException(this.reason);

  String reason;
}

typedef Future<dynamic> TransactionQuerySignature(
    PostgreSQLExecutionContext connection);

class TransactionProxy implements PostgreSQLExecutionContext {
  TransactionProxy(this.connection, this.executionBlock) {
    beginQuery = new Query<int>("BEGIN", {}, connection, this)
      ..onlyReturnAffectedRowCount = true;

    beginQuery.onComplete.future
        .then(startTransaction)
        .catchError(handleTransactionQueryError);
  }

  Query beginQuery;
  Completer completer = new Completer();

  Future get future => completer.future;

  Query get pendingQuery {
    if (queryQueue.length > 0) {
      return queryQueue.first;
    }

    return null;
  }

  List<Query> queryQueue = [];
  PostgreSQLConnection connection;
  TransactionQuerySignature executionBlock;

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

    return await enqueue(query);
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
    throw new TransactionRollbackException(reason);
  }

  Future startTransaction(dynamic beginResults) async {
    var result;
    try {
      result = await executionBlock(this);
    } on TransactionRollbackException catch (rollback) {
      queryQueue = [];
      await execute("ROLLBACK");
      completer.complete(new PostgreSQLRollback._(rollback.reason));
      return;
    } catch (e) {
      queryQueue = [];

      await execute("ROLLBACK");
      completer.completeError(e);
      return;
    }

    await execute("COMMIT");

    completer.complete(result);
  }

  Future handleTransactionQueryError(dynamic err) async {}

  Future<dynamic> enqueue(Query query) async {
    queryQueue.add(query);
    connection._transitionToState(connection._connectionState.awake());

    var result = null;
    try {
      result = await query.future;

      connection._cacheQuery(query);
      queryQueue.remove(query);
    } catch (e) {
      connection._cacheQuery(query);
      queryQueue.remove(query);
      rethrow;
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

abstract class PostgreSQLConnectionState {
  PostgreSQLConnection connection;

  PostgreSQLConnectionState onEnter() {
    return this;
  }

  PostgreSQLConnectionState awake() {
    return this;
  }

  PostgreSQLConnectionState onMessage(ServerMessage message) {
    return this;
  }

  PostgreSQLConnectionState onErrorResponse(ErrorResponseMessage message) {
    var exception = new PostgreSQLException.fromFields(message.fields);

    if (exception.severity == PostgreSQLSeverity.fatal ||
        exception.severity == PostgreSQLSeverity.panic) {
      return new PostgreSQLConnectionStateClosed();
    }

    return this;
  }

  void onExit() {}
}

/*
  Closed State; starts here and ends here.
 */

class PostgreSQLConnectionStateClosed extends PostgreSQLConnectionState {}

/*
  Socket connected, prior to any PostgreSQL handshaking - initiates that handshaking
 */

class PostgreSQLConnectionStateSocketConnected
    extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateSocketConnected(this.completer);

  Completer completer;

  PostgreSQLConnectionState onEnter() {
    var startupMessage = new StartupMessage(
        connection.databaseName, connection.timeZone,
        username: connection.username);

    connection._socket.add(startupMessage.asBytes());

    return this;
  }

  PostgreSQLConnectionState onErrorResponse(ErrorResponseMessage message) {
    var exception = new PostgreSQLException.fromFields(message.fields);

    completer.completeError(exception);

    return new PostgreSQLConnectionStateClosed();
  }

  PostgreSQLConnectionState onMessage(ServerMessage message) {
    AuthenticationMessage authMessage = message;

    // Pass on the pending op to subsequent stages
    if (authMessage.type == AuthenticationMessage.KindOK) {
      return new PostgreSQLConnectionStateAuthenticated(completer);
    } else if (authMessage.type == AuthenticationMessage.KindMD5Password) {
      connection._salt = authMessage.salt;

      return new PostgreSQLConnectionStateAuthenticating(completer);
    }

    completer.completeError(
        new PostgreSQLException("Unsupported authentication type ${authMessage
            .type}, closing connection."));

    return new PostgreSQLConnectionStateClosed();
  }
}

/*
  Authenticating state
 */

class PostgreSQLConnectionStateAuthenticating
    extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateAuthenticating(this.completer);

  Completer completer;

  PostgreSQLConnectionState onEnter() {
    var authMessage = new AuthMD5Message(
        connection.username, connection.password, connection._salt);

    connection._socket.add(authMessage.asBytes());

    return this;
  }

  PostgreSQLConnectionState onErrorResponse(ErrorResponseMessage message) {
    var exception = new PostgreSQLException.fromFields(message.fields);

    completer.completeError(exception);

    return new PostgreSQLConnectionStateClosed();
  }

  PostgreSQLConnectionState onMessage(ServerMessage message) {
    if (message is ParameterStatusMessage) {
      connection.settings[message.name] = message.value;
    } else if (message is BackendKeyMessage) {
      connection._secretKey = message.secretKey;
      connection._processID = message.processID;
    } else if (message is ReadyForQueryMessage) {
      if (message.state == ReadyForQueryMessage.StateIdle) {
        return new PostgreSQLConnectionStateIdle(openCompleter: completer);
      }
    }

    return this;
  }
}

/*
  Authenticated state
 */

class PostgreSQLConnectionStateAuthenticated
    extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateAuthenticated(this.completer);

  Completer completer;

  PostgreSQLConnectionState onErrorResponse(ErrorResponseMessage message) {
    var exception = new PostgreSQLException.fromFields(message.fields);

    completer.completeError(exception);

    return new PostgreSQLConnectionStateClosed();
  }

  PostgreSQLConnectionState onMessage(ServerMessage message) {
    if (message is ParameterStatusMessage) {
      connection.settings[message.name] = message.value;
    } else if (message is BackendKeyMessage) {
      connection._secretKey = message.secretKey;
      connection._processID = message.processID;
    } else if (message is ReadyForQueryMessage) {
      if (message.state == ReadyForQueryMessage.StateIdle) {
        return new PostgreSQLConnectionStateIdle(openCompleter: completer);
      }
    }

    return this;
  }
}

/*
  Ready/idle state
 */

class PostgreSQLConnectionStateIdle extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateIdle({this.openCompleter});

  Completer openCompleter;

  PostgreSQLConnectionState awake() {
    var pendingQuery = connection._pendingQuery;
    if (pendingQuery != null) {
      return processQuery(pendingQuery);
    }

    return this;
  }

  PostgreSQLConnectionState processQuery(Query q) {
    try {
      if (q.onlyReturnAffectedRowCount) {
        q.sendSimple(connection._socket);
        return new PostgreSQLConnectionStateBusy(q);
      }

      var cached = connection._cachedQuery(q.statement);
      q.sendExtended(connection._socket, cacheQuery: cached);

      return new PostgreSQLConnectionStateBusy(q);
    } catch (e) {
      scheduleMicrotask(() {
        q.completeError(e);
        connection._transitionToState(new PostgreSQLConnectionStateIdle());
      });

      return new PostgreSQLConnectionStateDeferredFailure();
    }
  }

  PostgreSQLConnectionState onEnter() {
    openCompleter?.complete();

    return awake();
  }

  PostgreSQLConnectionState onMessage(ServerMessage message) {
    return this;
  }
}

/*
  Busy state, query in progress
 */

class PostgreSQLConnectionStateBusy extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateBusy(this.query);

  Query query;
  PostgreSQLException returningException = null;
  int rowsAffected = 0;

  PostgreSQLConnectionState onErrorResponse(ErrorResponseMessage message) {
    // If we get an error here, then we should eat the rest of the messages
    // and we are always confirmed to get a ReadyForQueryMessage to finish up.
    // We should only report the error once that is done.
    var exception = new PostgreSQLException.fromFields(message.fields);
    returningException ??= exception;

    if (exception.severity == PostgreSQLSeverity.fatal ||
        exception.severity == PostgreSQLSeverity.panic) {
      return new PostgreSQLConnectionStateClosed();
    }

    return this;
  }

  PostgreSQLConnectionState onMessage(ServerMessage message) {
    // We ignore NoData, as it doesn't tell us anything we don't already know
    // or care about.

    //print("(${query.statement}) -> $message");

    if (message is ReadyForQueryMessage) {
      if (message.state == ReadyForQueryMessage.StateIdle) {
        if (returningException != null) {
          query.completeError(returningException);
        } else {
          query.complete(rowsAffected);
        }

        return new PostgreSQLConnectionStateIdle();
      } else if (message.state == ReadyForQueryMessage.StateTransaction) {
        if (returningException != null) {
          query.completeError(returningException);
        } else {
          query.complete(rowsAffected);
        }

        return new PostgreSQLConnectionStateReadyInTransaction(
            query.transaction);
      } else if (message.state == ReadyForQueryMessage.StateTransactionError) {
        // This should cancel the transaction, we may have to send a commit here
        query.completeError(returningException);
        return new PostgreSQLConnectionStateTransactionFailure(
            query.transaction);
      }
    } else if (message is CommandCompleteMessage) {
      rowsAffected = message.rowsAffected;
    } else if (message is RowDescriptionMessage) {
      query.fieldDescriptions = message.fieldDescriptions;
    } else if (message is DataRowMessage) {
      query.addRow(message.values);
    } else if (message is ParameterDescriptionMessage) {
      var validationException =
      query.validateParameters(message.parameterTypeIDs);
      if (validationException != null) {
        query.cache = null;
      }
      returningException ??= validationException;
    }

    return this;
  }
}

/* Idle Transaction State */

class PostgreSQLConnectionStateReadyInTransaction
    extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateReadyInTransaction(this.transaction);

  TransactionProxy transaction;

  PostgreSQLConnectionState onEnter() {
    return awake();
  }

  PostgreSQLConnectionState awake() {
    var pendingQuery = transaction.pendingQuery;
    if (pendingQuery != null) {
      return processQuery(pendingQuery);
    }

    return this;
  }

  PostgreSQLConnectionState processQuery(Query q) {
    try {
      if (q.onlyReturnAffectedRowCount) {
        q.sendSimple(connection._socket);
        return new PostgreSQLConnectionStateBusy(q);
      }

      var cached = connection._cachedQuery(q.statement);
      q.sendExtended(connection._socket, cacheQuery: cached);

      return new PostgreSQLConnectionStateBusy(q);
    } catch (e) {
      scheduleMicrotask(() {
        q.completeError(e);
        connection._transitionToState(new PostgreSQLConnectionStateIdle());
      });

      return new PostgreSQLConnectionStateDeferredFailure();
    }
  }
}

/*
  Transaction error state
 */

class PostgreSQLConnectionStateTransactionFailure
    extends PostgreSQLConnectionState {
  PostgreSQLConnectionStateTransactionFailure(this.transaction);

  TransactionProxy transaction;

  PostgreSQLConnectionState awake() {
    return new PostgreSQLConnectionStateReadyInTransaction(transaction);
  }
}

/*
  Hack for deferred error
 */

class PostgreSQLConnectionStateDeferredFailure
    extends PostgreSQLConnectionState {}
